//! The Kubernetes provisioner: each game-server instance is a one-replica StatefulSet
//! `rots-gs-<slot>-g<generation>` built from the PodTemplate `rots-gameserver`, with a
//! pre-created PVC and a headless Service of the same name. A StatefulSet (not a bare
//! Pod or Job) because Longhorn only frees a dead node's volume for StatefulSet and
//! Deployment pods. The per-slot Services and routes are rendered by the chart.
//!
//! Plain `reqwest` against the in-cluster API with the pod's service-account token,
//! which is re-read on every request because projected tokens rotate.
use std::time::Duration;

use anyhow::{Context, bail};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use crate::temporal::activities::{InstanceHealth, InstanceIn, ProbeResult, ProvisionOutcome};

const SA_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";
const API: &str = "https://kubernetes.default.svc";
pub const POD_TEMPLATE: &str = "rots-gameserver";
pub const QUOTA: &str = "rots-gameservers";
pub const STORAGE_CLASS: &str = "longhorn-rots-gs";
pub const VOLUME_SIZE: &str = "1Gi";
const CONTAINER: &str = "rots-gameserver";

pub fn instance_name(slot: u32, generation: u64) -> String {
    format!("rots-gs-{slot}-g{generation}")
}

pub struct Kube {
    http: reqwest::Client,
    namespace: String,
}

impl Kube {
    /// Reads the pod's namespace and the cluster CA; fails outside a pod.
    pub fn in_cluster() -> anyhow::Result<Self> {
        let namespace = std::fs::read_to_string(format!("{SA_DIR}/namespace"))
            .context("reading the service-account namespace")?
            .trim()
            .to_owned();
        let ca = std::fs::read(format!("{SA_DIR}/ca.crt")).context("reading the cluster CA")?;
        let http = reqwest::Client::builder()
            .add_root_certificate(reqwest::Certificate::from_pem(&ca)?)
            .timeout(Duration::from_secs(10))
            .build()
            .context("building the Kubernetes client")?;
        Ok(Self { http, namespace })
    }

    fn url(&self, api: &str, resource: &str, name: Option<&str>) -> String {
        let ns = &self.namespace;
        match name {
            Some(name) => format!("{API}{api}/namespaces/{ns}/{resource}/{name}"),
            None => format!("{API}{api}/namespaces/{ns}/{resource}"),
        }
    }

    async fn send(
        &self,
        method: Method,
        url: &str,
        body: Option<&Value>,
    ) -> anyhow::Result<(StatusCode, Value)> {
        let token = std::fs::read_to_string(format!("{SA_DIR}/token"))
            .context("reading the service-account token")?;
        let mut req = self
            .http
            .request(method.clone(), url)
            .bearer_auth(token.trim());
        if let Some(body) = body {
            req = req.json(body);
        }
        let resp = req
            .send()
            .await
            .with_context(|| format!("{method} {url}"))?;
        let status = resp.status();
        let body = resp.json().await.unwrap_or(Value::Null);
        Ok((status, body))
    }

    /// `None` on 404.
    pub async fn get_json(
        &self,
        api: &str,
        resource: &str,
        name: &str,
    ) -> anyhow::Result<Option<Value>> {
        let url = self.url(api, resource, Some(name));
        match self.send(Method::GET, &url, None).await? {
            (StatusCode::NOT_FOUND, _) => Ok(None),
            (s, body) if s.is_success() => Ok(Some(body)),
            (s, body) => bail!("GET {url}: {s}: {}", body["message"]),
        }
    }

    /// 409 (already exists) counts as success: creates are retried.
    pub async fn create(&self, api: &str, resource: &str, body: &Value) -> anyhow::Result<()> {
        let url = self.url(api, resource, None);
        match self.send(Method::POST, &url, Some(body)).await? {
            (s, _) if s.is_success() || s == StatusCode::CONFLICT => Ok(()),
            (s, body) => bail!("POST {url}: {s}: {}", body["message"]),
        }
    }

    /// 404 (already gone) counts as success.
    pub async fn delete(&self, api: &str, resource: &str, name: &str) -> anyhow::Result<()> {
        let opts =
            json!({"kind": "DeleteOptions", "apiVersion": "v1", "propagationPolicy": "Background"});
        self.delete_with(api, resource, name, opts).await
    }

    async fn delete_with(
        &self,
        api: &str,
        resource: &str,
        name: &str,
        opts: Value,
    ) -> anyhow::Result<()> {
        let url = self.url(api, resource, Some(name));
        match self.send(Method::DELETE, &url, Some(&opts)).await? {
            (s, _) if s.is_success() || s == StatusCode::NOT_FOUND => Ok(()),
            (s, body) => bail!("DELETE {url}: {s}: {}", body["message"]),
        }
    }
}

/// Millicores of a CPU quantity (`200m`, `2`, `0.5`).
fn cpu_millis(q: &str) -> Option<u64> {
    match q.strip_suffix('m') {
        Some(m) => m.parse().ok(),
        None => q.parse::<f64>().ok().map(|c| (c * 1000.0).round() as u64),
    }
}

/// Bytes of a memory quantity (`256Mi`, `40Gi`, `4G`, `1000`).
fn mem_bytes(q: &str) -> Option<u64> {
    const SUFFIXES: [(&str, u64); 12] = [
        ("Ki", 1 << 10),
        ("Mi", 1 << 20),
        ("Gi", 1 << 30),
        ("Ti", 1 << 40),
        ("Pi", 1 << 50),
        ("Ei", 1 << 60),
        ("k", 1_000),
        ("M", 1_000_000),
        ("G", 1_000_000_000),
        ("T", 1_000_000_000_000),
        ("P", 1_000_000_000_000_000),
        ("E", 1_000_000_000_000_000_000),
    ];
    for (suffix, factor) in SUFFIXES {
        if let Some(n) = q.strip_suffix(suffix) {
            return n.parse::<f64>().ok().map(|n| (n * factor as f64) as u64);
        }
    }
    q.parse::<f64>().ok().map(|n| n as u64)
}

/// The pod template's summed container requests: (millicores, bytes).
fn requests(template: &Value) -> (u64, u64) {
    let containers = template["spec"]["containers"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    containers.iter().fold((0, 0), |(cpu, mem), c| {
        let r = &c["resources"]["requests"];
        (
            cpu + r["cpu"].as_str().and_then(cpu_millis).unwrap_or(0),
            mem + r["memory"].as_str().and_then(mem_bytes).unwrap_or(0),
        )
    })
}

/// Whether one more pod with these requests would exceed the quota.
fn quota_full(quota: &Value, (cpu, mem): (u64, u64)) -> bool {
    let status = &quota["status"];
    let over = |key: &str, need: u64, parse: fn(&str) -> Option<u64>| {
        let hard = status["hard"][key].as_str().and_then(parse);
        let used = status["used"][key].as_str().and_then(parse).unwrap_or(0);
        hard.is_some_and(|hard| used + need > hard)
    };
    over("requests.cpu", cpu, cpu_millis) || over("requests.memory", mem, mem_bytes)
}

/// The StatefulSet for an instance: the PodTemplate plus the instance's labels and env.
/// The template's `data` volume is dropped: the claim template provides it.
pub fn statefulset(template: &Value, input: &InstanceIn, udp_port: u16) -> Value {
    let name = instance_name(input.slot, input.generation);
    let labels = json!({
        "app": "rots-gameserver",
        "rots.2143.me/slot": input.slot.to_string(),
        "rots.2143.me/generation": input.generation.to_string(),
        "rots.2143.me/instance": name,
    });
    let mut template = template.clone();
    let meta_labels = &mut template["metadata"]["labels"];
    if !meta_labels.is_object() {
        *meta_labels = json!({});
    }
    for (k, v) in labels.as_object().expect("object") {
        meta_labels[k] = v.clone();
    }
    let spec = &mut template["spec"];
    if let Some(volumes) = spec["volumes"].as_array_mut() {
        volumes.retain(|v| v["name"] != "data");
    }
    let env = [
        ("ROTS_SERVER_SLOT", input.slot.to_string()),
        ("ROTS_SERVER_GENERATION", input.generation.to_string()),
        (
            "ROTS_SERVER_JOIN_CODE",
            input.join_code.clone().unwrap_or_default(),
        ),
        ("ROTS_PUBLIC_UDP_PORT", udp_port.to_string()),
    ];
    if let Some(containers) = spec["containers"].as_array_mut() {
        let named = containers
            .iter()
            .position(|c| c["name"] == CONTAINER)
            .unwrap_or(0);
        if let Some(c) = containers.get_mut(named) {
            if !c["env"].is_array() {
                c["env"] = json!([]);
            }
            let vars = c["env"].as_array_mut().expect("array");
            for (key, value) in env {
                vars.retain(|v| v["name"] != key);
                vars.push(json!({"name": key, "value": value}));
            }
        }
    }
    json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": name, "labels": labels},
        "spec": {
            "serviceName": name,
            "replicas": 1,
            "selector": {"matchLabels": {"rots.2143.me/instance": name}},
            "template": template,
            "volumeClaimTemplates": [volume_claim("data", &labels)],
        },
    })
}

fn volume_claim(name: &str, labels: &Value) -> Value {
    json!({
        "metadata": {"name": name, "labels": labels},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "storageClassName": STORAGE_CLASS,
            "resources": {"requests": {"storage": VOLUME_SIZE}},
        },
    })
}

pub async fn provision(
    kube: &Kube,
    input: &InstanceIn,
    udp_port: u16,
) -> anyhow::Result<ProvisionOutcome> {
    let name = instance_name(input.slot, input.generation);
    let Some(pod_template) = kube
        .get_json("/api/v1", "podtemplates", POD_TEMPLATE)
        .await?
    else {
        return Ok(ProvisionOutcome::StartFailed {
            reason: format!("PodTemplate {POD_TEMPLATE} not found"),
        });
    };
    let template = &pod_template["template"];
    if let Some(quota) = kube.get_json("/api/v1", "resourcequotas", QUOTA).await?
        && quota_full(&quota, requests(template))
    {
        return Ok(ProvisionOutcome::ServersFull);
    }
    let sts = statefulset(template, input, udp_port);
    let labels = &sts["metadata"]["labels"];
    // The StatefulSet adopts the PVC its claim template would have created; creating
    // it first lets a successor be cloned from another instance's volume later.
    let mut pvc = volume_claim(&format!("data-{name}-0"), labels);
    pvc["apiVersion"] = json!("v1");
    pvc["kind"] = json!("PersistentVolumeClaim");
    kube.create("/api/v1", "persistentvolumeclaims", &pvc)
        .await?;
    let service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "labels": labels},
        "spec": {
            "clusterIP": "None",
            "selector": {"rots.2143.me/instance": name},
            "ports": [{"name": "health", "port": 25566, "targetPort": 25566, "protocol": "TCP"}],
        },
    });
    kube.create("/api/v1", "services", &service).await?;
    kube.create("/apis/apps/v1", "statefulsets", &sts).await?;
    Ok(ProvisionOutcome::Provisioned)
}

/// `Unschedulable` when the pod reports it, or when there is no pod at all (the quota
/// rejected it); the workflow decides how long that may last.
pub async fn instance_health(kube: &Kube, input: &InstanceIn) -> anyhow::Result<InstanceHealth> {
    let pod = format!("{}-0", instance_name(input.slot, input.generation));
    let Some(pod) = kube.get_json("/api/v1", "pods", &pod).await? else {
        return Ok(InstanceHealth::Unschedulable);
    };
    let unschedulable = pod["status"]["conditions"].as_array().is_some_and(|conds| {
        conds.iter().any(|c| {
            c["type"] == "PodScheduled" && c["status"] == "False" && c["reason"] == "Unschedulable"
        })
    });
    Ok(if unschedulable {
        InstanceHealth::Unschedulable
    } else {
        InstanceHealth::Scheduled
    })
}

/// `GET <url>` answered 200 within 2 s.
pub async fn healthz(http: &reqwest::Client, url: &str) -> ProbeResult {
    match http.get(url).timeout(Duration::from_secs(2)).send().await {
        Ok(resp) if resp.status().is_success() => ProbeResult::Alive,
        _ => ProbeResult::Dead,
    }
}

/// The pod's `/healthz` on its health port; no pod or no pod IP means dead.
pub async fn probe(
    kube: &Kube,
    http: &reqwest::Client,
    input: &InstanceIn,
) -> anyhow::Result<ProbeResult> {
    let pod = format!("{}-0", instance_name(input.slot, input.generation));
    let Some(pod) = kube.get_json("/api/v1", "pods", &pod).await? else {
        return Ok(ProbeResult::Dead);
    };
    let Some(ip) = pod["status"]["podIP"].as_str() else {
        return Ok(ProbeResult::Dead);
    };
    Ok(healthz(http, &format!("http://{ip}:25566/healthz")).await)
}

/// Deletes the instance's pod at once (`gracePeriodSeconds: 0`); the StatefulSet
/// recreates it on the same PVC.
pub async fn kill_pod(kube: &Kube, input: &InstanceIn) -> anyhow::Result<()> {
    let pod = format!("{}-0", instance_name(input.slot, input.generation));
    let opts = json!({"kind": "DeleteOptions", "apiVersion": "v1", "gracePeriodSeconds": 0});
    kube.delete_with("/api/v1", "pods", &pod, opts).await
}

pub async fn delete_instance(kube: &Kube, input: &InstanceIn) -> anyhow::Result<()> {
    let name = instance_name(input.slot, input.generation);
    kube.delete("/apis/apps/v1", "statefulsets", &name).await?;
    kube.delete("/api/v1", "services", &name).await?;
    kube.delete(
        "/api/v1",
        "persistentvolumeclaims",
        &format!("data-{name}-0"),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantities() {
        assert_eq!(cpu_millis("200m"), Some(200));
        assert_eq!(cpu_millis("32"), Some(32_000));
        assert_eq!(cpu_millis("0.5"), Some(500));
        assert_eq!(mem_bytes("256Mi"), Some(256 << 20));
        assert_eq!(mem_bytes("40Gi"), Some(40 << 30));
        assert_eq!(mem_bytes("4G"), Some(4_000_000_000));
        assert_eq!(mem_bytes("1024"), Some(1024));
    }

    #[test]
    fn quota_pre_check() {
        let quota = |used_cpu: &str| {
            json!({"status": {
                "hard": {"requests.cpu": "2", "requests.memory": "4Gi"},
                "used": {"requests.cpu": used_cpu, "requests.memory": "1Gi"},
            }})
        };
        let one = (200, 256 << 20);
        assert!(!quota_full(&quota("1800m"), one));
        assert!(quota_full(&quota("1801m"), one));
        assert!(!quota_full(&json!({}), one), "no limits, never full");
    }

    #[test]
    fn statefulset_adds_identity_and_drops_the_template_volume() {
        let template = json!({
            "metadata": {"labels": {"app": "rots-gameserver"}},
            "spec": {
                "containers": [{
                    "name": "rots-gameserver",
                    "env": [{"name": "ROTS_PORT", "value": "25565"}, {"name": "ROTS_SERVER_SLOT", "value": "9"}],
                    "resources": {"requests": {"cpu": "200m", "memory": "256Mi"}},
                    "volumeMounts": [{"name": "data", "mountPath": "/data"}],
                }],
                "volumes": [{"name": "data", "emptyDir": {}}, {"name": "tmp", "emptyDir": {}}],
            },
        });
        assert_eq!(requests(&template), (200, 256 << 20));
        let input = InstanceIn {
            slot: 3,
            generation: 12,
            join_code: Some("ABCD".into()),
        };
        let sts = statefulset(&template, &input, 36003);
        assert_eq!(sts["metadata"]["name"], "rots-gs-3-g12");
        assert_eq!(sts["spec"]["serviceName"], "rots-gs-3-g12");
        let pod = &sts["spec"]["template"];
        assert_eq!(pod["metadata"]["labels"]["rots.2143.me/slot"], "3");
        assert_eq!(
            pod["metadata"]["labels"]["rots.2143.me/instance"],
            "rots-gs-3-g12"
        );
        assert_eq!(
            pod["spec"]["volumes"],
            json!([{"name": "tmp", "emptyDir": {}}])
        );
        let env: Vec<(String, String)> = pod["spec"]["containers"][0]["env"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| {
                (
                    v["name"].as_str().unwrap().into(),
                    v["value"].as_str().unwrap().into(),
                )
            })
            .collect();
        let get = |k: &str| {
            env.iter()
                .filter(|(n, _)| n == k)
                .map(|(_, v)| v.as_str())
                .collect::<Vec<_>>()
        };
        assert_eq!(get("ROTS_SERVER_SLOT"), vec!["3"]);
        assert_eq!(get("ROTS_SERVER_GENERATION"), vec!["12"]);
        assert_eq!(get("ROTS_SERVER_JOIN_CODE"), vec!["ABCD"]);
        assert_eq!(get("ROTS_PUBLIC_UDP_PORT"), vec!["36003"]);
        assert_eq!(get("ROTS_PORT"), vec!["25565"]);
        let claim = &sts["spec"]["volumeClaimTemplates"][0];
        assert_eq!(claim["metadata"]["name"], "data");
        assert_eq!(claim["spec"]["storageClassName"], STORAGE_CLASS);
    }
}
