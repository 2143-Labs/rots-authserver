//! The fleet coordinator: one workflow per task queue (one per environment) that owns
//! the directory of game-server instances, the character index (which instance each
//! character is on), reservations for `/play` calls that have not joined yet, and the
//! decision to place a character on an existing instance or spawn a new one.
//!
//! Every rule lives in the pure [`FleetState`] methods, which take `now` (unix
//! seconds). Spawns go onto an outbox the run loop drains in order. Liveness is not
//! tracked here: each instance has its own [`GameServerWorkflow`] that holds its lease
//! and roster and reports lifecycle changes (`server_ready`, `server_status`,
//! `server_gone`, `release`) as signals, so the coordinator sees no heartbeats.
//!
//! A coordinator started without carried state (first start, or after it was
//! terminated) rebuilds the directory from the running `GameServerWorkflow`s before
//! it answers any placement or claim.
//!
//! Ceiling: one workflow serializes every placement and lifecycle event, and a run is
//! capped at [`MAX_HISTORY`] events or [`RUN_SECS`]. Per-instance heartbeats never
//! reach it, so this holds to a few hundred live instances.
//!
//! [`GameServerWorkflow`]: super::server::GameServerWorkflow
use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use temporalio_common::RetryPolicy;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    CancellableFuture, ContinueAsNewOptions, LocalActivityOptions, SignalWorkflowOptions,
    SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult,
};

use super::activities::{GameServerActivities, ProvisionerKind, Rebuilt, StartServerIn};
use super::now_secs;
use super::server::GameServerWorkflow;
use super::server::ServerInfo;

/// How long an assignment holds a slot for a player who has not joined yet: the
/// ticket lifetime plus jsonwebtoken's default 60 s leeway.
pub const RESERVATION_SECS: u64 = crate::tickets::TICKET_TTL_SECS + 60;
/// A run continues-as-new after this long, carrying the fleet state.
pub const RUN_SECS: u64 = 3600;
/// ... or after this many history events, whichever comes first.
pub const MAX_HISTORY: u32 = 2000;
/// Closes the coordinator of a deleted preview (no worker left to continue it).
pub const RUN_TIMEOUT: Duration = Duration::from_secs(3 * 3600);
/// Consecutive start failures that pause spawning, and for how long.
pub const START_FAILURES_TO_PAUSE: u32 = 3;
pub const SPAWN_PAUSE_SECS: u64 = 300;
/// How long `servers_full` is answered after the cluster had no room for an instance.
pub const FULL_SECS: u64 = 30;
/// Join codes avoid the look-alikes I/O/0/1.
const JOIN_CODE_ALPHABET: &[u8; 32] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct CoordinatorArgs {
    pub task_queue: String,
    pub provisioner: ProvisionerKind,
    /// Slots are `0..slots`.
    pub slots: u32,
    /// Static mode: players (bound plus reserved) per server before it counts as full.
    pub capacity_per_server: u32,
    pub ready_timeout_secs: u64,
    /// Managed mode: instances kept booted in pre-game and activated on placement.
    #[serde(default)]
    pub warm_instances: u32,
    pub carried: Option<FleetState>,
}

impl CoordinatorArgs {
    fn managed(&self) -> bool {
        self.provisioner != ProvisionerKind::Static
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Spawned; no heartbeat yet.
    Provisioning,
    Ready,
    /// Asked to shut down; takes no new players.
    Draining,
    /// Killed after a failed probe; its pod is restarting on the same volume.
    Recovering,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ServerEntry {
    pub generation: u64,
    pub phase: Phase,
    pub join_code: Option<String>,
    pub udp_port: Option<u16>,
    pub paused: bool,
    pub connected: u32,
    /// In the warm pool: booted in pre-game, waiting to be handed to a player. Never set
    /// by a report (an in-flight `server_status` after a hand-out must not re-pool it).
    #[serde(default)]
    pub warm: bool,
}

/// Where a character is: the instance whose world holds its units.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Binding {
    pub slot: u32,
    pub generation: u64,
    pub sid: String,
}

/// An instance promised to a `/play` whose player has not joined yet.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Reservation {
    pub slot: u32,
    pub generation: u64,
    pub sid: String,
    pub until: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Effect {
    StartServer {
        slot: u32,
        generation: u64,
        join_code: Option<String>,
        /// Start it in pre-game, as a pool instance.
        #[serde(default)]
        warm: bool,
    },
    /// Ask the instance's workflow to probe its server now.
    Probe(InstanceRef),
    /// Tell the instance's workflow the coordinator is handing it to a player.
    Activate(InstanceRef),
}

/// One instance: `(slot, generation)`.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstanceRef {
    pub slot: u32,
    pub generation: u64,
}

/// A client lost its server and asks for it to be checked.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CrashedIn {
    pub character_id: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct FleetState {
    pub servers: BTreeMap<u32, ServerEntry>,
    pub index: BTreeMap<i64, Binding>,
    pub reservations: BTreeMap<i64, Reservation>,
    /// Never reused, across slots and across continue-as-new.
    pub next_generation: u64,
    pub code_seed: u64,
    /// Consecutive `StartFailed`; reset by any `server_ready`.
    pub failures: u32,
    pub spawn_paused_until: u64,
    pub full_until: u64,
    pub outbox: Vec<Effect>,
    /// The directory has been rebuilt from the running instances (or carried over).
    pub rebuilt: bool,
}

impl Default for FleetState {
    fn default() -> Self {
        Self {
            servers: BTreeMap::new(),
            index: BTreeMap::new(),
            reservations: BTreeMap::new(),
            next_generation: 1,
            code_seed: 0,
            failures: 0,
            spawn_paused_until: 0,
            full_until: 0,
            outbox: Vec::new(),
            rebuilt: false,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PlayerRef {
    pub character_id: i64,
    pub sid: String,
}

/// Where a character is in the world, once characters have saved locations. The single
/// extension point for custom placement; always `None` today, so every unbound
/// character gets a new tutorial instance.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CharacterLocation {
    pub world: String,
    pub position: [f32; 3],
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PlaceIn {
    pub character_id: i64,
    pub sid: String,
    pub location: Option<CharacterLocation>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum Placement {
    Ready {
        slot: u32,
        generation: u64,
        udp_port: Option<u16>,
        join_code: Option<String>,
        paused: bool,
    },
    Starting {
        slot: u32,
        generation: u64,
    },
    Recovering {
        slot: u32,
        generation: u64,
    },
    NoGameServer,
    ServersFull,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ClaimIn {
    pub slot: u32,
    pub generation: u64,
    pub character_id: i64,
    pub sid: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum JoinOutcome {
    Admitted,
    AlreadyOnOtherServer {
        slot: u32,
    },
    ServerDraining,
    UnknownInstance,
    /// The instance's workflow could not reach the coordinator.
    Failed {
        reason: String,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ReleaseIn {
    pub slot: u32,
    pub generation: u64,
    pub character_id: i64,
}

/// An instance's state as its workflow reports it (`server_ready` and `server_status`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ServerReport {
    pub slot: u32,
    pub generation: u64,
    pub udp_port: Option<u16>,
    pub paused: bool,
    pub connected: u32,
    /// Every character with units in the world, connected or not.
    pub players: Vec<PlayerRef>,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GoneReason {
    Drained,
    Destroyed,
    Lost,
    StartFailed,
    ServersFull,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ServerGoneIn {
    pub slot: u32,
    pub generation: u64,
    pub reason: GoneReason,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShutdownReason {
    Idle,
    Sigterm,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ShutdownIn {
    pub slot: u32,
    pub generation: u64,
    pub reason: ShutdownReason,
    pub players: Vec<i64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "plan", rename_all = "snake_case")]
pub enum ShutdownPlan {
    /// Nobody to move: the world and its volume are destroyed.
    Destroy,
    /// Send every client back to the main menu, then destroy.
    ToMenu,
}

/// The coordinator's settings, from its args.
#[derive(Clone, Copy, Debug)]
pub struct Rules {
    pub managed: bool,
    pub slots: u32,
    pub capacity_per_server: u32,
    pub ready_timeout_secs: u64,
    /// Instances kept booted in pre-game.
    pub warm_instances: u32,
}

/// The pool size a worker started with (`configure`).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
pub struct PoolIn {
    pub warm_instances: u32,
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl FleetState {
    fn entry(&self, slot: u32, generation: u64) -> Option<&ServerEntry> {
        self.servers
            .get(&slot)
            .filter(|e| e.generation == generation)
    }

    /// The answer for a character bound to or reserved on `(slot, generation)`, or
    /// `None` if that instance is gone or draining.
    fn answer(&self, slot: u32, generation: u64) -> Option<Placement> {
        let e = self.entry(slot, generation)?;
        Some(match e.phase {
            Phase::Ready => Placement::Ready {
                slot,
                generation,
                udp_port: e.udp_port,
                join_code: e.join_code.clone(),
                paused: e.paused,
            },
            Phase::Provisioning => Placement::Starting { slot, generation },
            Phase::Recovering => Placement::Recovering { slot, generation },
            Phase::Draining => return None,
        })
    }

    /// Characters bound to plus reserved on an instance, not counting `except`.
    fn load(&self, slot: u32, generation: u64, except: i64) -> usize {
        let on = |s: u32, g: u64| s == slot && g == generation;
        self.index
            .iter()
            .filter(|(c, b)| **c != except && on(b.slot, b.generation))
            .count()
            + self
                .reservations
                .iter()
                .filter(|(c, r)| **c != except && on(r.slot, r.generation))
                .count()
    }

    fn new_join_code(&mut self) -> String {
        loop {
            self.code_seed = splitmix64(self.code_seed);
            let mut bits = self.code_seed;
            let code: String = (0..4)
                .map(|_| {
                    let c = JOIN_CODE_ALPHABET[(bits & 31) as usize] as char;
                    bits >>= 5;
                    c
                })
                .collect();
            if !self
                .servers
                .values()
                .any(|e| e.join_code.as_deref() == Some(code.as_str()))
            {
                return code;
            }
        }
    }

    /// Tops the pool up to `rules.warm_instances`. A warm instance is a slot with
    /// `warm: true` that no character is bound or reserved to.
    fn refill(&mut self, rules: &Rules, now: u64) {
        // Before the rebuild the directory is empty: filling now would spawn a second
        // instance on a slot that already has one.
        if !self.rebuilt {
            return;
        }
        if !rules.managed || now < self.full_until || now < self.spawn_paused_until {
            return;
        }
        let want = rules.warm_instances as usize;
        while self.servers.values().filter(|e| e.warm).count() < want {
            let Some(slot) = (0..rules.slots).find(|s| !self.servers.contains_key(s)) else {
                return;
            };
            let generation = self.next_generation;
            self.next_generation += 1;
            let join_code = Some(self.new_join_code());
            self.servers.insert(
                slot,
                ServerEntry {
                    generation,
                    phase: Phase::Provisioning,
                    join_code: join_code.clone(),
                    udp_port: None,
                    paused: false,
                    connected: 0,
                    warm: true,
                },
            );
            self.outbox.push(Effect::StartServer {
                slot,
                generation,
                join_code,
                warm: true,
            });
        }
    }

    /// First match wins: the character's instance, its reservation, its location (not
    /// built), a warm pool instance, a new instance (managed) or the least-loaded ready
    /// server (static). Every answer tops the pool back up.
    pub fn place(&mut self, input: &PlaceIn, rules: &Rules, now: u64) -> Placement {
        let placement = self.place_inner(input, rules, now);
        self.refill(rules, now);
        placement
    }

    fn place_inner(&mut self, input: &PlaceIn, rules: &Rules, now: u64) -> Placement {
        let c = input.character_id;
        self.reservations.retain(|_, r| r.until > now);
        if let Some(b) = self.index.get(&c).cloned() {
            match self.answer(b.slot, b.generation) {
                Some(p) => return p,
                None => {
                    self.index.remove(&c);
                }
            }
        }
        if let Some(r) = self.reservations.get(&c).cloned() {
            match self.answer(r.slot, r.generation) {
                Some(p) => {
                    if matches!(p, Placement::Ready { .. }) {
                        // A ticket is minted now: hold the slot for its lifetime.
                        let until = now + RESERVATION_SECS;
                        let r = self.reservations.get_mut(&c).expect("checked above");
                        r.until = until;
                        r.sid = input.sid.clone();
                    }
                    return p;
                }
                None => {
                    self.reservations.remove(&c);
                }
            }
        }
        // `input.location` would pick the world here; no character has one yet.
        if rules.managed {
            // A warm instance costs nothing to use and is already booted: hand it out even
            // when the cluster would refuse a new one.
            if let Some((slot, generation, join_code, udp_port)) = self
                .servers
                .iter()
                .find(|&(slot, e)| {
                    // A warm entry no character is bound or reserved to: one that has
                    // players is already in use, whatever its flags say.
                    e.warm && e.phase == Phase::Ready && self.load(*slot, e.generation, c) == 0
                })
                .map(|(slot, e)| (*slot, e.generation, e.join_code.clone(), e.udp_port))
            {
                let e = self.servers.get_mut(&slot).expect("found above");
                e.warm = false;
                self.outbox
                    .push(Effect::Activate(InstanceRef { slot, generation }));
                self.reservations.insert(
                    c,
                    Reservation {
                        slot,
                        generation,
                        sid: input.sid.clone(),
                        until: now + RESERVATION_SECS,
                    },
                );
                return Placement::Ready {
                    slot,
                    generation,
                    udp_port,
                    join_code,
                    paused: false,
                };
            }
            if now < self.full_until {
                return Placement::ServersFull;
            }
            if now < self.spawn_paused_until {
                return Placement::NoGameServer;
            }
            let Some(slot) = (0..rules.slots).find(|s| !self.servers.contains_key(s)) else {
                return Placement::NoGameServer;
            };
            let generation = self.next_generation;
            self.next_generation += 1;
            let join_code = Some(self.new_join_code());
            self.servers.insert(
                slot,
                ServerEntry {
                    generation,
                    phase: Phase::Provisioning,
                    join_code: join_code.clone(),
                    udp_port: None,
                    paused: false,
                    connected: 0,
                    warm: false,
                },
            );
            self.outbox.push(Effect::StartServer {
                slot,
                generation,
                join_code,
                warm: false,
            });
            self.reservations.insert(
                c,
                Reservation {
                    slot,
                    generation,
                    sid: input.sid.clone(),
                    until: now + rules.ready_timeout_secs + RESERVATION_SECS,
                },
            );
            return Placement::Starting { slot, generation };
        }
        let capacity = rules.capacity_per_server as usize;
        let pick = self
            .servers
            .iter()
            .filter(|(_, e)| e.phase == Phase::Ready)
            .map(|(slot, e)| (self.load(*slot, e.generation, c), *slot, e.generation))
            .filter(|(load, _, _)| *load < capacity)
            .min();
        let Some((_, slot, generation)) = pick else {
            return Placement::NoGameServer;
        };
        self.reservations.insert(
            c,
            Reservation {
                slot,
                generation,
                sid: input.sid.clone(),
                until: now + RESERVATION_SECS,
            },
        );
        self.answer(slot, generation)
            .expect("picked a ready server")
    }

    /// Records an instance this coordinator did not know (rebuilt state, or a report
    /// that overtook its spawn). A newer generation on the slot wins; an older one is
    /// replaced and its characters unbound. Returns false if the report is stale.
    fn adopt_instance(&mut self, slot: u32, generation: u64, phase: Phase) -> bool {
        match self.servers.get(&slot) {
            Some(e) if e.generation == generation => true,
            Some(e) if e.generation > generation => false,
            _ => {
                self.forget(slot);
                self.servers.insert(
                    slot,
                    ServerEntry {
                        generation,
                        phase,
                        join_code: None,
                        udp_port: None,
                        paused: false,
                        connected: 0,
                        // Only `rebuild` can learn that a running instance is warm.
                        warm: false,
                    },
                );
                self.next_generation = self.next_generation.max(generation + 1);
                true
            }
        }
    }

    /// Drops a slot's entry, its bindings and its reservations.
    fn forget(&mut self, slot: u32) {
        let Some(e) = self.servers.remove(&slot) else {
            return;
        };
        let on = |s: u32, g: u64| s == slot && g == e.generation;
        self.index.retain(|_, b| !on(b.slot, b.generation));
        self.reservations.retain(|_, r| !on(r.slot, r.generation));
    }

    /// Binds every listed character not already on another live instance.
    fn adopt_players(&mut self, slot: u32, generation: u64, players: &[PlayerRef]) {
        for p in players {
            let elsewhere = self.index.get(&p.character_id).is_some_and(|b| {
                (b.slot, b.generation) != (slot, generation)
                    && self.entry(b.slot, b.generation).is_some()
            });
            if !elsewhere {
                self.index.insert(
                    p.character_id,
                    Binding {
                        slot,
                        generation,
                        sid: p.sid.clone(),
                    },
                );
                self.reservations.remove(&p.character_id);
            }
        }
    }

    /// Admits the character on the instance unless it is bound to another live one.
    pub fn claim(&mut self, input: &ClaimIn) -> JoinOutcome {
        if !self.adopt_instance(input.slot, input.generation, Phase::Ready) {
            return JoinOutcome::UnknownInstance;
        }
        if self.servers[&input.slot].phase == Phase::Draining {
            return JoinOutcome::ServerDraining;
        }
        if let Some(b) = self.index.get(&input.character_id)
            && (b.slot, b.generation) != (input.slot, input.generation)
            && self.entry(b.slot, b.generation).is_some()
        {
            return JoinOutcome::AlreadyOnOtherServer { slot: b.slot };
        }
        self.index.insert(
            input.character_id,
            Binding {
                slot: input.slot,
                generation: input.generation,
                sid: input.sid.clone(),
            },
        );
        self.reservations.remove(&input.character_id);
        // A player the pool did not hand out (it connected before the push) takes the
        // instance out of the pool, so it is never handed out twice.
        if let Some(e) = self.servers.get_mut(&input.slot)
            && e.generation == input.generation
        {
            e.warm = false;
        }
        JoinOutcome::Admitted
    }

    /// The character's units left the instance's world.
    pub fn release(&mut self, input: &ReleaseIn) {
        if self
            .index
            .get(&input.character_id)
            .is_some_and(|b| (b.slot, b.generation) == (input.slot, input.generation))
        {
            self.index.remove(&input.character_id);
        }
    }

    /// The instance's first heartbeat (or first after a restart on its volume).
    pub fn server_ready(&mut self, r: &ServerReport) {
        if !self.adopt_instance(r.slot, r.generation, Phase::Ready) {
            return;
        }
        let e = self.servers.get_mut(&r.slot).expect("adopted");
        if matches!(e.phase, Phase::Provisioning | Phase::Recovering) {
            e.phase = Phase::Ready;
        }
        e.udp_port = r.udp_port;
        e.paused = r.paused;
        e.connected = r.connected;
        self.failures = 0;
        self.adopt_players(r.slot, r.generation, &r.players);
    }

    /// A change in `paused`/`connected`, or the periodic anti-entropy report. An
    /// unknown instance is taken as ready.
    pub fn server_status(&mut self, r: &ServerReport) {
        if self.entry(r.slot, r.generation).is_none() {
            return self.server_ready(r);
        }
        let e = self.servers.get_mut(&r.slot).expect("checked above");
        e.udp_port = r.udp_port;
        e.paused = r.paused;
        e.connected = r.connected;
        self.adopt_players(r.slot, r.generation, &r.players);
    }

    /// The instance no longer exists: free its slot and unbind its characters.
    pub fn server_gone(&mut self, input: &ServerGoneIn, now: u64) {
        if self.entry(input.slot, input.generation).is_some() {
            self.forget(input.slot);
        }
        match input.reason {
            GoneReason::StartFailed => {
                self.failures += 1;
                if self.failures >= START_FAILURES_TO_PAUSE {
                    self.failures = 0;
                    self.spawn_paused_until = now + SPAWN_PAUSE_SECS;
                }
            }
            GoneReason::ServersFull => self.full_until = now + FULL_SECS,
            GoneReason::Drained | GoneReason::Destroyed | GoneReason::Lost => {}
        }
    }

    /// The server chose to shut down. It takes no new players from now on, and every
    /// character on it is unbound: its world is about to be destroyed.
    pub fn request_shutdown(&mut self, input: &ShutdownIn) -> ShutdownPlan {
        if let Some(e) = self.servers.get_mut(&input.slot)
            && e.generation == input.generation
        {
            e.phase = Phase::Draining;
            let on = |b_slot: u32, b_gen: u64| b_slot == input.slot && b_gen == input.generation;
            self.index.retain(|_, b| !on(b.slot, b.generation));
            self.reservations.retain(|_, r| !on(r.slot, r.generation));
        }
        match input.reason {
            ShutdownReason::Sigterm if !input.players.is_empty() => ShutdownPlan::ToMenu,
            _ => ShutdownPlan::Destroy,
        }
    }

    /// A client lost the server its character is on: have that instance probed. False
    /// when the character is on no live, ready instance.
    pub fn server_crashed(&mut self, input: &CrashedIn) -> bool {
        let Some(b) = self.index.get(&input.character_id) else {
            return false;
        };
        let target = InstanceRef {
            slot: b.slot,
            generation: b.generation,
        };
        if !self
            .entry(target.slot, target.generation)
            .is_some_and(|e| e.phase == Phase::Ready)
        {
            return false;
        }
        self.outbox.push(Effect::Probe(target));
        true
    }

    /// The instance's server did not answer its probe and its pod was killed; it is
    /// restarting on the same volume.
    pub fn server_recovering(&mut self, input: &InstanceRef) {
        if let Some(e) = self.servers.get_mut(&input.slot)
            && e.generation == input.generation
            && e.phase == Phase::Ready
        {
            e.phase = Phase::Recovering;
        }
    }

    /// Merges the running instances found at startup. Entries already known (from a
    /// report that arrived first) are kept.
    pub fn rebuild(&mut self, found: &[ServerInfo]) {
        for info in found {
            if self.entry(info.slot, info.generation).is_none()
                && self.adopt_instance(info.slot, info.generation, info.phase)
            {
                let e = self.servers.get_mut(&info.slot).expect("adopted");
                e.phase = info.phase;
                e.join_code = info.join_code.clone();
                e.udp_port = info.udp_port;
                e.paused = info.paused;
                e.connected = info.connected;
            }
            // The only place after startup that can turn `warm` back on: an instance whose
            // workflow still holds it. `place` and `claim` wait for the rebuild, so it
            // cannot race a hand-out.
            if let Some(e) = self.servers.get_mut(&info.slot)
                && e.generation == info.generation
            {
                e.warm |= info.warm;
            }
            self.next_generation = self.next_generation.max(info.generation + 1);
            if info.phase != Phase::Draining {
                self.adopt_players(info.slot, info.generation, &info.players);
            }
        }
        self.rebuilt = true;
    }
}

#[workflow]
pub struct CoordinatorWorkflow {
    args: CoordinatorArgs,
    fleet: FleetState,
}

impl CoordinatorWorkflow {
    fn rules(&self) -> Rules {
        Rules {
            managed: self.args.managed(),
            slots: self.args.slots,
            capacity_per_server: self.args.capacity_per_server,
            ready_timeout_secs: self.args.ready_timeout_secs,
            warm_instances: self.args.warm_instances,
        }
    }
}

#[workflow_methods]
impl CoordinatorWorkflow {
    #[init]
    pub fn new(_ctx: &WorkflowContextView, args: CoordinatorArgs) -> Self {
        let fleet = args.carried.clone().unwrap_or_default();
        Self {
            args: CoordinatorArgs {
                carried: None,
                ..args
            },
            fleet,
        }
    }

    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let started = now_secs(ctx.workflow_time());
        let deadline = started + RUN_SECS;
        if !ctx.state(|s| s.fleet.rebuilt) {
            let found = ctx
                .execute_local_activity(
                    GameServerActivities::rebuild_directory,
                    (),
                    LocalActivityOptions::builder()
                        .schedule_to_close_timeout(Duration::from_secs(10))
                        .retry_policy(RetryPolicy::builder().maximum_attempts(2).build())
                        .build(),
                )
                .await;
            let found = found.unwrap_or_else(|e| {
                tracing::warn!("coordinator: directory rebuild failed, starting empty: {e}");
                Rebuilt::default()
            });
            ctx.state_mut(|s| {
                if s.fleet.code_seed == 0 {
                    s.fleet.code_seed = started;
                }
                s.fleet.rebuild(&found.servers);
                // Instance ids are unique forever: never reuse a closed instance's.
                s.fleet.next_generation = s.fleet.next_generation.max(found.max_generation + 1);
            });
        }
        // A fresh coordinator with stale-but-correct args fills the pool before the first
        // `/play`; a continued one re-fills after a rollout.
        let now = now_secs(ctx.workflow_time());
        ctx.state_mut(|s| {
            let rules = s.rules();
            s.fleet.refill(&rules, now)
        });
        loop {
            // Wake at the run deadline or when the outbox fills; a timer that lost the
            // race is cancelled, or it would keep running on the server.
            if ctx.state(|s| s.fleet.outbox.is_empty()) {
                let remaining = deadline
                    .saturating_sub(now_secs(ctx.workflow_time()))
                    .max(1);
                let mut timer = std::pin::pin!(ctx.timer(Duration::from_secs(remaining)));
                let fired = temporalio_sdk::workflows::select! {
                    _ = timer => true,
                    _ = ctx.wait_condition(|s: &Self| !s.fleet.outbox.is_empty()) => false,
                };
                if !fired {
                    timer.cancel();
                }
            }
            while let Some(effect) =
                ctx.state_mut(|s| (!s.fleet.outbox.is_empty()).then(|| s.fleet.outbox.remove(0)))
            {
                let (slot, generation, join_code, warm) = match effect {
                    Effect::StartServer {
                        slot,
                        generation,
                        join_code,
                        warm,
                    } => (slot, generation, join_code, warm),
                    Effect::Activate(instance) => {
                        let queue = ctx.state(|s| s.args.task_queue.clone());
                        let sent = ctx
                            .external_workflow(
                                super::server_id(&queue, instance.slot, instance.generation),
                                None,
                            )
                            .signal(
                                GameServerWorkflow::activate,
                                (),
                                SignalWorkflowOptions::default(),
                            )
                            .await;
                        if let Err(e) = sent {
                            tracing::warn!(?instance, "coordinator: activate not delivered: {e}");
                        }
                        continue;
                    }
                    Effect::Probe(instance) => {
                        let queue = ctx.state(|s| s.args.task_queue.clone());
                        let sent = ctx
                            .external_workflow(
                                super::server_id(&queue, instance.slot, instance.generation),
                                None,
                            )
                            .signal(
                                GameServerWorkflow::probe,
                                (),
                                SignalWorkflowOptions::default(),
                            )
                            .await;
                        if let Err(e) = sent {
                            tracing::warn!(?instance, "coordinator: probe not delivered: {e}");
                        }
                        continue;
                    }
                };
                if warm {
                    tracing::info!(slot, generation, "coordinator: warming the pool");
                }
                let started = ctx
                    .execute_local_activity(
                        GameServerActivities::start_server_workflow,
                        StartServerIn {
                            slot,
                            generation,
                            join_code,
                            warm,
                        },
                        LocalActivityOptions::builder()
                            .schedule_to_close_timeout(Duration::from_secs(15))
                            .retry_policy(
                                RetryPolicy::builder()
                                    .maximum_attempts(3)
                                    .initial_interval(Duration::from_secs(1))
                                    .build(),
                            )
                            .build(),
                    )
                    .await;
                if let Err(e) = started {
                    tracing::warn!(slot, generation, "coordinator: instance start failed: {e}");
                    let now = now_secs(ctx.workflow_time());
                    ctx.state_mut(|s| {
                        s.fleet.server_gone(
                            &ServerGoneIn {
                                slot,
                                generation,
                                reason: GoneReason::StartFailed,
                            },
                            now,
                        )
                    });
                }
            }
            // Checked on every wake, not only when the timer fires.
            let due = ctx.continue_as_new_suggested()
                || ctx.history_length() > MAX_HISTORY
                || now_secs(ctx.workflow_time()) >= deadline;
            if due && ctx.all_handlers_finished() && ctx.state(|s| s.fleet.outbox.is_empty()) {
                let args = ctx.state(|s| CoordinatorArgs {
                    carried: Some(s.fleet.clone()),
                    ..s.args.clone()
                });
                ctx.continue_as_new(
                    args,
                    ContinueAsNewOptions::builder()
                        .run_timeout(RUN_TIMEOUT)
                        .build(),
                )?;
            }
        }
    }

    #[update]
    pub async fn place(ctx: &mut WorkflowContext<Self>, input: PlaceIn) -> Placement {
        let _ = ctx.wait_condition(|s: &Self| s.fleet.rebuilt).await;
        let now = now_secs(ctx.workflow_time());
        ctx.state_mut(|s| {
            let rules = s.rules();
            s.fleet.place(&input, &rules, now)
        })
    }

    #[update]
    pub async fn claim(ctx: &mut WorkflowContext<Self>, input: ClaimIn) -> JoinOutcome {
        let _ = ctx.wait_condition(|s: &Self| s.fleet.rebuilt).await;
        let now = now_secs(ctx.workflow_time());
        ctx.state_mut(|s| {
            let rules = s.rules();
            let outcome = s.fleet.claim(&input);
            // An admitted player takes its instance out of the pool through its own
            // workflow: keep the pool topped up.
            s.fleet.refill(&rules, now);
            outcome
        })
    }

    #[signal]
    pub fn release(&mut self, _ctx: &mut SyncWorkflowContext<Self>, input: ReleaseIn) {
        self.fleet.release(&input);
    }

    #[signal]
    pub fn server_ready(&mut self, _ctx: &mut SyncWorkflowContext<Self>, input: ServerReport) {
        self.fleet.server_ready(&input);
    }

    #[signal]
    pub fn server_status(&mut self, _ctx: &mut SyncWorkflowContext<Self>, input: ServerReport) {
        self.fleet.server_status(&input);
    }

    #[signal]
    pub fn server_gone(&mut self, ctx: &mut SyncWorkflowContext<Self>, input: ServerGoneIn) {
        let now = now_secs(ctx.workflow_time());
        let rules = self.rules();
        self.fleet.server_gone(&input, now);
        // A destroyed instance must be replaced.
        self.fleet.refill(&rules, now);
    }

    /// Applies the pool size the worker started with, and fills the pool for it.
    #[update]
    pub fn configure(&mut self, ctx: &mut SyncWorkflowContext<Self>, input: PoolIn) -> u32 {
        self.args.warm_instances = input.warm_instances;
        let now = now_secs(ctx.workflow_time());
        let rules = self.rules();
        self.fleet.refill(&rules, now);
        input.warm_instances
    }

    #[update]
    pub fn request_shutdown(
        &mut self,
        _ctx: &mut SyncWorkflowContext<Self>,
        input: ShutdownIn,
    ) -> ShutdownPlan {
        self.fleet.request_shutdown(&input)
    }

    /// Returns true when a probe of the character's instance was requested.
    #[update]
    pub async fn server_crashed(ctx: &mut WorkflowContext<Self>, input: CrashedIn) -> bool {
        // Before the rebuild the index is empty and every report would look unbound.
        let _ = ctx.wait_condition(|s: &Self| s.fleet.rebuilt).await;
        ctx.state_mut(|s| s.fleet.server_crashed(&input))
    }

    #[signal]
    pub fn server_recovering(&mut self, _ctx: &mut SyncWorkflowContext<Self>, input: InstanceRef) {
        self.fleet.server_recovering(&input);
    }

    #[query]
    pub fn fleet(&self, _ctx: &WorkflowContextView) -> FleetState {
        self.fleet.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANAGED: Rules = Rules {
        managed: true,
        slots: 4,
        capacity_per_server: 32,
        ready_timeout_secs: 120,
        warm_instances: 0,
    };
    const STATIC: Rules = Rules {
        managed: false,
        slots: 4,
        capacity_per_server: 2,
        ready_timeout_secs: 120,
        warm_instances: 0,
    };
    /// Managed, with one instance kept in pre-game.
    const POOLED: Rules = Rules {
        warm_instances: 1,
        ..MANAGED
    };

    fn place(c: i64, sid: &str) -> PlaceIn {
        PlaceIn {
            character_id: c,
            sid: sid.into(),
            location: None,
        }
    }

    fn hb(slot: u32, generation: u64, players: &[(i64, &str)]) -> ServerReport {
        ServerReport {
            slot,
            generation,
            udp_port: Some(36000 + slot as u16),
            paused: false,
            connected: players.len() as u32,
            players: players
                .iter()
                .map(|(c, s)| PlayerRef {
                    character_id: *c,
                    sid: (*s).into(),
                })
                .collect(),
        }
    }

    fn claim(slot: u32, generation: u64, c: i64, sid: &str) -> ClaimIn {
        ClaimIn {
            slot,
            generation,
            character_id: c,
            sid: sid.into(),
        }
    }

    fn gone(slot: u32, generation: u64, reason: GoneReason) -> ServerGoneIn {
        ServerGoneIn {
            slot,
            generation,
            reason,
        }
    }

    fn rebuilt() -> FleetState {
        FleetState {
            rebuilt: true,
            ..Default::default()
        }
    }

    #[test]
    fn managed_place_spawns_a_new_instance_per_new_character() {
        let mut f = rebuilt();
        assert_eq!(
            f.place(&place(1, "s1"), &MANAGED, 100),
            Placement::Starting {
                slot: 0,
                generation: 1
            }
        );
        assert_eq!(
            f.place(&place(2, "s2"), &MANAGED, 100),
            Placement::Starting {
                slot: 1,
                generation: 2
            }
        );
        assert_eq!(f.outbox.len(), 2);
        let codes: Vec<_> = f.servers.values().map(|e| e.join_code.clone()).collect();
        assert!(
            codes
                .iter()
                .all(|c| c.as_ref().is_some_and(|c| c.len() == 4))
        );
        assert_ne!(codes[0], codes[1]);
        // Ready after the first heartbeat; the reservation now answers with the address.
        f.server_ready(&hb(0, 1, &[]));
        let Placement::Ready {
            slot: 0,
            generation: 1,
            udp_port: Some(36000),
            join_code: Some(_),
            paused: false,
        } = f.place(&place(1, "s1"), &MANAGED, 110)
        else {
            panic!("{:?}", f.place(&place(1, "s1"), &MANAGED, 110));
        };
        // Every slot taken: no game server.
        f.place(&place(3, "s3"), &MANAGED, 110);
        f.place(&place(4, "s4"), &MANAGED, 110);
        assert_eq!(
            f.place(&place(5, "s5"), &MANAGED, 110),
            Placement::NoGameServer
        );
        // A freed slot is reused with a new generation.
        f.server_gone(&gone(1, 2, GoneReason::Destroyed), 120);
        assert_eq!(
            f.place(&place(5, "s5"), &MANAGED, 120),
            Placement::Starting {
                slot: 1,
                generation: 5
            }
        );
    }

    #[test]
    fn static_place_picks_the_least_loaded_ready_server() {
        let mut f = rebuilt();
        assert_eq!(
            f.place(&place(1, "s1"), &STATIC, 100),
            Placement::NoGameServer
        );
        f.server_ready(&hb(0, 1, &[(9, "s9")]));
        f.server_ready(&hb(1, 1, &[]));
        let ready = |slot| Placement::Ready {
            slot,
            generation: 1,
            udp_port: Some(36000 + slot as u16),
            join_code: None,
            paused: false,
        };
        assert_eq!(f.place(&place(1, "s1"), &STATIC, 100), ready(1));
        // Both carry one now (a player on 0, a reservation on 1): ties go low.
        assert_eq!(f.place(&place(2, "s2"), &STATIC, 100), ready(0));
        assert_eq!(f.place(&place(3, "s3"), &STATIC, 100), ready(1));
        // Capacity 2 reached on both.
        assert_eq!(
            f.place(&place(4, "s4"), &STATIC, 100),
            Placement::NoGameServer
        );
        // The bound character goes back to its server, whatever the load.
        assert_eq!(f.place(&place(9, "s9"), &STATIC, 100), ready(0));
        assert!(f.outbox.is_empty(), "static never spawns");
    }

    #[test]
    fn reservation_prevents_a_second_instance_for_the_same_character() {
        let mut f = rebuilt();
        let first = f.place(&place(1, "s1"), &MANAGED, 100);
        assert_eq!(f.place(&place(1, "s1"), &MANAGED, 102), first);
        assert_eq!(f.outbox.len(), 1);
        // The reservation outlives the ready timeout.
        assert_eq!(
            f.place(&place(1, "s1"), &MANAGED, 100 + 120 + RESERVATION_SECS - 1),
            first
        );
        // Joining binds the character and clears the reservation.
        f.server_ready(&hb(0, 1, &[]));
        assert_eq!(f.claim(&claim(0, 1, 1, "s1")), JoinOutcome::Admitted);
        assert!(f.reservations.is_empty());
        assert_eq!(f.index[&1].slot, 0);
    }

    #[test]
    fn claim_refused_while_on_another_live_instance() {
        let mut f = rebuilt();
        f.server_ready(&hb(0, 1, &[(9, "s9")]));
        f.server_ready(&hb(1, 2, &[]));
        assert_eq!(
            f.claim(&claim(1, 2, 9, "s9")),
            JoinOutcome::AlreadyOnOtherServer { slot: 0 }
        );
        f.server_gone(&gone(0, 1, GoneReason::Lost), 200);
        assert_eq!(f.claim(&claim(1, 2, 9, "s9")), JoinOutcome::Admitted);
        // A report from an older generation of a slot is stale.
        assert_eq!(f.claim(&claim(1, 1, 7, "s7")), JoinOutcome::UnknownInstance);
    }

    #[test]
    fn idle_shutdown_unbinds_ghosts_and_destroys() {
        let mut f = rebuilt();
        f.place(&place(1, "s1"), &MANAGED, 100);
        f.server_ready(&hb(0, 1, &[]));
        f.claim(&claim(0, 1, 1, "s1"));
        // Character 1 disconnected but its units stay (a ghost): it is still bound.
        f.server_status(&ServerReport {
            connected: 0,
            ..hb(0, 1, &[(1, "s1")])
        });
        assert!(f.index.contains_key(&1));
        let plan = f.request_shutdown(&ShutdownIn {
            slot: 0,
            generation: 1,
            reason: ShutdownReason::Idle,
            players: vec![],
        });
        assert_eq!(plan, ShutdownPlan::Destroy);
        assert!(f.index.is_empty(), "the ghost is unbound with its world");
        assert_eq!(f.servers[&0].phase, Phase::Draining);
        assert_eq!(f.claim(&claim(0, 1, 2, "s2")), JoinOutcome::ServerDraining);
        // The character gets a new instance, not the draining one.
        assert_eq!(
            f.place(&place(1, "s1"), &MANAGED, 130),
            Placement::Starting {
                slot: 1,
                generation: 2
            }
        );
        f.server_gone(&gone(0, 1, GoneReason::Destroyed), 140);
        assert!(!f.servers.contains_key(&0));
        // SIGTERM with players on board sends them to the menu.
        f.server_ready(&hb(1, 2, &[(1, "s1")]));
        assert_eq!(
            f.request_shutdown(&ShutdownIn {
                slot: 1,
                generation: 2,
                reason: ShutdownReason::Sigterm,
                players: vec![1],
            }),
            ShutdownPlan::ToMenu
        );
    }

    #[test]
    fn three_start_failures_pause_spawning_for_300_seconds() {
        let mut f = rebuilt();
        for (c, now) in [(1, 100), (2, 101), (3, 102)] {
            let Placement::Starting { slot, generation } = f.place(&place(c, "s"), &MANAGED, now)
            else {
                panic!("expected a spawn");
            };
            f.server_gone(&gone(slot, generation, GoneReason::StartFailed), now);
        }
        assert_eq!(
            f.place(&place(4, "s4"), &MANAGED, 103),
            Placement::NoGameServer
        );
        assert_eq!(
            f.place(&place(4, "s4"), &MANAGED, 102 + SPAWN_PAUSE_SECS - 1),
            Placement::NoGameServer
        );
        assert!(matches!(
            f.place(&place(4, "s4"), &MANAGED, 102 + SPAWN_PAUSE_SECS),
            Placement::Starting { .. }
        ));
        // A ready server resets the count: two more failures do not pause.
        f.server_ready(&hb(0, f.servers[&0].generation, &[]));
        for c in [5, 6] {
            let Placement::Starting { slot, generation } = f.place(&place(c, "s"), &MANAGED, 500)
            else {
                panic!("expected a spawn");
            };
            f.server_gone(&gone(slot, generation, GoneReason::StartFailed), 500);
        }
        assert!(matches!(
            f.place(&place(7, "s7"), &MANAGED, 501),
            Placement::Starting { .. }
        ));
    }

    #[test]
    fn servers_full_frees_the_slot_without_counting_a_failure() {
        let mut f = rebuilt();
        f.place(&place(1, "s1"), &MANAGED, 100);
        f.server_gone(&gone(0, 1, GoneReason::ServersFull), 105);
        assert!(f.servers.is_empty());
        assert!(f.reservations.is_empty());
        assert_eq!(f.failures, 0);
        assert_eq!(
            f.place(&place(1, "s1"), &MANAGED, 106),
            Placement::ServersFull
        );
        assert_eq!(
            f.place(&place(1, "s1"), &MANAGED, 105 + FULL_SECS - 1),
            Placement::ServersFull
        );
        assert!(matches!(
            f.place(&place(1, "s1"), &MANAGED, 105 + FULL_SECS),
            Placement::Starting { .. }
        ));
    }

    #[test]
    fn release_only_unbinds_from_the_reporting_instance() {
        let mut f = rebuilt();
        f.server_ready(&hb(0, 1, &[(1, "s1")]));
        f.release(&ReleaseIn {
            slot: 0,
            generation: 7,
            character_id: 1,
        });
        assert!(f.index.contains_key(&1));
        f.release(&ReleaseIn {
            slot: 0,
            generation: 1,
            character_id: 1,
        });
        assert!(f.index.is_empty());
    }

    #[test]
    fn rebuild_restores_bindings_and_generations() {
        let mut f = FleetState::default();
        f.rebuild(&[
            ServerInfo {
                slot: 2,
                generation: 7,
                join_code: Some("ABCD".into()),
                phase: Phase::Ready,
                udp_port: Some(36002),
                paused: false,
                connected: 1,
                players: vec![PlayerRef {
                    character_id: 1,
                    sid: "s1".into(),
                }],
                warm: false,
            },
            ServerInfo {
                slot: 0,
                generation: 3,
                join_code: None,
                phase: Phase::Provisioning,
                udp_port: None,
                paused: false,
                connected: 0,
                players: vec![],
                warm: false,
            },
        ]);
        assert!(f.rebuilt);
        assert_eq!(f.next_generation, 8);
        assert!(matches!(
            f.place(&place(1, "s1"), &MANAGED, 100),
            Placement::Ready {
                slot: 2,
                generation: 7,
                ..
            }
        ));
        assert_eq!(
            f.place(&place(2, "s2"), &MANAGED, 100),
            Placement::Starting {
                slot: 1,
                generation: 8
            }
        );
    }

    #[test]
    fn crash_report_probes_the_characters_ready_instance() {
        let mut f = rebuilt();
        f.server_ready(&hb(3, 9, &[(1, "s1")]));
        let crashed = |c| CrashedIn { character_id: c };
        assert!(f.server_crashed(&crashed(1)));
        assert_eq!(
            f.outbox,
            vec![Effect::Probe(InstanceRef {
                slot: 3,
                generation: 9
            })]
        );
        assert!(!f.server_crashed(&crashed(2)), "unbound character");
        f.server_recovering(&InstanceRef {
            slot: 3,
            generation: 9,
        });
        assert_eq!(
            f.place(&place(1, "s1"), &MANAGED, 100),
            Placement::Recovering {
                slot: 3,
                generation: 9
            }
        );
        assert!(!f.server_crashed(&crashed(1)), "already recovering");
        // Its first heartbeat after the restart makes it ready again.
        f.server_ready(&hb(3, 9, &[(1, "s1")]));
        assert!(matches!(
            f.place(&place(1, "s1"), &MANAGED, 101),
            Placement::Ready { slot: 3, .. }
        ));
    }

    #[test]
    fn refill_fills_the_pool_to_the_target() {
        let mut f = rebuilt();
        let rules = Rules {
            warm_instances: 2,
            ..MANAGED
        };
        f.refill(&rules, 100);
        assert_eq!(f.servers.len(), 2);
        assert!(
            f.servers
                .values()
                .all(|e| e.warm && e.phase == Phase::Provisioning)
        );
        assert_eq!(
            f.outbox,
            vec![
                Effect::StartServer {
                    slot: 0,
                    generation: 1,
                    join_code: f.servers[&0].join_code.clone(),
                    warm: true,
                },
                Effect::StartServer {
                    slot: 1,
                    generation: 2,
                    join_code: f.servers[&1].join_code.clone(),
                    warm: true,
                },
            ]
        );
        // Nothing more when the pool is full.
        f.outbox.clear();
        f.refill(&rules, 101);
        assert_eq!(f.servers.len(), 2);
        assert!(f.outbox.is_empty());
    }

    #[test]
    fn place_hands_out_a_warm_instance_and_refills() {
        let mut f = rebuilt();
        f.refill(&POOLED, 100);
        f.server_ready(&hb(0, 1, &[]));
        f.outbox.clear();
        let answer = f.place(&place(7, "s7"), &POOLED, 110);
        let Placement::Ready {
            slot: 0,
            generation: 1,
            join_code,
            paused: false,
            ..
        } = answer
        else {
            panic!("expected the warm instance, got {answer:?}");
        };
        assert!(join_code.is_some());
        assert_eq!(
            f.reservations[&7],
            Reservation {
                slot: 0,
                generation: 1,
                sid: "s7".into(),
                until: 110 + RESERVATION_SECS,
            }
        );
        assert!(!f.servers[&0].warm, "handed out");
        // The pool was topped up on the next free slot, in the same call.
        let refilled = &f.servers[&1];
        assert!(refilled.warm);
        assert_eq!(
            (refilled.generation, refilled.phase),
            (2, Phase::Provisioning)
        );
        assert_eq!(
            f.outbox,
            vec![
                Effect::Activate(InstanceRef {
                    slot: 0,
                    generation: 1
                }),
                Effect::StartServer {
                    slot: 1,
                    generation: 2,
                    join_code: refilled.join_code.clone(),
                    warm: true,
                },
            ]
        );
    }

    #[test]
    fn a_warm_entry_is_not_handed_out_twice() {
        let mut f = rebuilt();
        f.refill(&POOLED, 100);
        f.server_ready(&hb(0, 1, &[]));
        assert!(matches!(
            f.place(&place(1, "s1"), &POOLED, 110),
            Placement::Ready { slot: 0, .. }
        ));
        // The pool's replacement is still provisioning, so the next character waits for a
        // fresh instance rather than sharing the handed-out one.
        assert_eq!(
            f.place(&place(2, "s2"), &POOLED, 111),
            Placement::Starting {
                slot: 2,
                generation: 3
            }
        );
        assert!(
            !f.servers
                .values()
                .any(|e| e.warm && e.phase == Phase::Ready && e.generation == 1)
        );
    }

    #[test]
    fn refill_does_nothing_when_the_cluster_is_full_or_paused() {
        let mut f = rebuilt();
        f.full_until = 200;
        f.refill(&POOLED, 100);
        assert!(f.servers.is_empty() && f.outbox.is_empty());
        f.full_until = 0;
        f.spawn_paused_until = 200;
        f.refill(&POOLED, 100);
        assert!(f.servers.is_empty() && f.outbox.is_empty());
        f.spawn_paused_until = 0;
        // No room for one at all.
        for slot in 0..MANAGED.slots {
            f.server_ready(&hb(slot, 1, &[]));
        }
        f.refill(&POOLED, 101);
        assert!(!f.servers.values().any(|e| e.warm));
    }

    #[test]
    fn a_rebuilt_instance_stays_warm() {
        let mut f = FleetState::default();
        f.rebuild(&[ServerInfo {
            slot: 0,
            generation: 4,
            join_code: Some("ABCD".into()),
            phase: Phase::Ready,
            udp_port: Some(36000),
            paused: false,
            connected: 0,
            players: vec![],
            warm: true,
        }]);
        assert!(f.servers[&0].warm);
        // A full pool is not topped up again after the rebuild.
        f.refill(&POOLED, 100);
        assert_eq!(f.servers.len(), 1);
        assert!(f.outbox.is_empty());
        // Handing it out still works and clears the flag.
        assert!(matches!(
            f.place(&place(1, "s1"), &POOLED, 101),
            Placement::Ready { slot: 0, .. }
        ));
        assert!(!f.servers[&0].warm);
    }

    #[test]
    fn wire_shapes() {
        assert_eq!(
            serde_json::to_value(Placement::Starting {
                slot: 1,
                generation: 2
            })
            .unwrap(),
            serde_json::json!({"result":"starting","slot":1,"generation":2})
        );
        assert_eq!(
            serde_json::to_value(ShutdownPlan::ToMenu).unwrap(),
            serde_json::json!({"plan":"to_menu"})
        );
        assert_eq!(
            serde_json::to_value(JoinOutcome::Admitted).unwrap(),
            serde_json::json!({"result":"admitted"})
        );
    }
}
