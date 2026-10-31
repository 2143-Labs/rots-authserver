//! Login providers: Steam (OpenID 2.0 + Web API tickets) and OAuth2/OIDC
//! (Discord static config, au.2143.me via runtime OIDC discovery).
//! Adapted from steam-lobby `auth_providers.rs` / `steam_auth.rs`.
use std::collections::HashMap;

use anyhow::{Context, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Deserialize;

use crate::Config;

/// OIDC discovery document (RFC 8414), the subset we consume.
#[derive(Debug, Deserialize)]
struct OidcDiscovery {
    authorization_endpoint: String,
    token_endpoint: String,
    userinfo_endpoint: String,
}

#[derive(Debug, Clone)]
pub struct ProviderConfig {
    pub id: String, // "discord" | "au2143"
    pub client_id: String,
    pub client_secret: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    pub userinfo_endpoint: String,
    pub scopes: Vec<String>,
    pub id_field: String,   // "id" (discord) | "sub" (au2143)
    pub name_field: String, // "global_name" (discord) | "preferred_username" (au2143)
    pub use_pkce: bool,     // false for discord (its OAuth2 has no PKCE)
}

/// Enabled providers, resolved once at startup from `Config`.
pub struct Providers {
    pub steam_openid: bool,
    /// `STEAM_API_KEY`; `Some` iff `steam_ticket` is enabled.
    pub steam_api_key: Option<String>,
    pub steam_app_id: u32,
    pub oauth: Vec<ProviderConfig>,
}

impl Providers {
    pub async fn from_config(config: &Config, http: &reqwest::Client) -> Self {
        let disabled = |id: &str| {
            config
                .disabled_providers
                .split(',')
                .any(|p| p.trim().eq_ignore_ascii_case(id))
        };
        let steam_enabled = !disabled("steam");
        let discord = if disabled("discord") {
            (None, None)
        } else {
            (
                config.discord_client_id.clone(),
                config.discord_client_secret.clone(),
            )
        };
        let au2143_client_id = if disabled("au2143") {
            None
        } else {
            config.au2143_client_id.clone()
        };
        let oauth = build(
            discord.0,
            discord.1,
            au2143_client_id,
            config.au2143_client_secret.clone(),
            &config.au2143_issuer,
            http,
        )
        .await;
        Self {
            steam_openid: steam_enabled,
            steam_api_key: config.steam_api_key.clone().filter(|_| steam_enabled),
            steam_app_id: config.steam_app_id,
            oauth,
        }
    }

    pub fn oauth(&self, id: &str) -> Option<&ProviderConfig> {
        self.oauth.iter().find(|p| p.id == id)
    }

    /// Wire names for `GET /providers`.
    pub fn enabled(&self) -> Vec<&str> {
        let mut out = Vec::with_capacity(4);
        if self.steam_openid {
            out.push("steam_openid");
        }
        out.extend(self.oauth.iter().map(|p| p.id.as_str()));
        if self.steam_api_key.is_some() {
            out.push("steam_ticket");
        }
        out
    }
}

/// Discord is static; au2143 runs runtime OIDC discovery and is DISABLED
/// (with a log) when discovery fails — never a boot error.
pub async fn build(
    discord_client_id: Option<String>,
    discord_client_secret: Option<String>,
    au2143_client_id: Option<String>,
    au2143_client_secret: Option<String>,
    au2143_issuer: &str,
    http: &reqwest::Client,
) -> Vec<ProviderConfig> {
    let mut providers = Vec::new();

    if let (Some(client_id), Some(client_secret)) = (discord_client_id, discord_client_secret) {
        providers.push(ProviderConfig {
            id: "discord".into(),
            client_id,
            client_secret,
            authorization_endpoint: "https://discord.com/oauth2/authorize".into(),
            token_endpoint: "https://discord.com/api/oauth2/token".into(),
            userinfo_endpoint: "https://discord.com/api/users/@me".into(),
            scopes: vec!["identify".into()],
            id_field: "id".into(),
            name_field: "global_name".into(),
            // Discord's OAuth2 documents only `state` + `client_secret`
            // protections (no PKCE); its token exchange accepts
            // client_id+client_secret in the form body, which this flow sends.
            use_pkce: false,
        });
    }

    if let Some(client_id) = au2143_client_id {
        // Browser-like UA: au.2143.me's bot protection 403s default reqwest/curl UAs.
        let discovery_url = format!(
            "{issuer}/.well-known/openid-configuration",
            issuer = au2143_issuer.trim_end_matches('/')
        );
        let resp = http
            .get(&discovery_url)
            .header(reqwest::header::USER_AGENT, BROWSER_UA)
            .send()
            .await;
        let discovered = match resp {
            Ok(r) if r.status().is_success() => match r.json::<OidcDiscovery>().await {
                Ok(d) => Some(d),
                Err(e) => {
                    tracing::warn!("au2143 discovery JSON parse failed: {e} — provider disabled");
                    None
                }
            },
            Ok(r) => {
                tracing::warn!(
                    "au2143 discovery returned HTTP {} — provider disabled",
                    r.status()
                );
                None
            }
            Err(e) => {
                tracing::warn!("au2143 discovery request failed: {e} — provider disabled");
                None
            }
        };
        if let Some(d) = discovered {
            providers.push(ProviderConfig {
                id: "au2143".into(),
                client_id,
                client_secret: au2143_client_secret.unwrap_or_default(),
                authorization_endpoint: d.authorization_endpoint,
                token_endpoint: d.token_endpoint,
                userinfo_endpoint: d.userinfo_endpoint,
                scopes: vec!["openid".into(), "profile".into()],
                id_field: "sub".into(),
                name_field: "preferred_username".into(),
                use_pkce: true,
            });
        }
    }

    providers
}

/// Sent on every request to au.2143.me (discovery, token, userinfo).
pub const BROWSER_UA: &str =
    "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0";

/// PKCE pair: verifier = base64url(3 × uuid bytes); challenge = base64url(sha256(verifier)).
pub fn pkce_pair() -> (String, String) {
    let mut bytes = Vec::with_capacity(48);
    for _ in 0..3 {
        bytes.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    }
    let verifier = URL_SAFE_NO_PAD.encode(&bytes);
    use sha2::{Digest, Sha256};
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    (verifier, challenge)
}

/// Build the provider authorization URL. The caller persists any verifier in
/// `oauth_states`; it never leaves the server.
pub fn authorization_url(
    cfg: &ProviderConfig,
    redirect_uri: &str,
    state: &str,
    verifier: Option<&str>,
    challenge: &str,
) -> String {
    let scopes = cfg.scopes.join("%20");
    let mut url = format!(
        "{ep}?response_type=code&client_id={cid}&redirect_uri={cb}&scope={scopes}&state={state}",
        ep = cfg.authorization_endpoint,
        cid = urlencoding::encode(&cfg.client_id),
        cb = urlencoding::encode(redirect_uri),
    );
    if verifier.is_some() {
        url.push_str(&format!(
            "&code_challenge={challenge}&code_challenge_method=S256"
        ));
    }
    url
}

/// The player's display name from provider userinfo: `name_field` first, then
/// `username`, then `name`, then "Unknown".
pub fn userinfo_name(json: &serde_json::Value, cfg: &ProviderConfig) -> String {
    json[&cfg.name_field]
        .as_str()
        .or_else(|| json["username"].as_str())
        .or_else(|| json["name"].as_str())
        .unwrap_or("Unknown")
        .to_string()
}

/// `openid.return_to` for a login started with `state`.
pub fn steam_return_to(public_url: &str, state: &str) -> String {
    format!("{public_url}/auth/api/v1/steam/callback?state={state}")
}

/// Build the Steam OpenID redirect URL. Every value is percent-encoded by
/// `url::query_pairs_mut`.
pub fn openid_redirect_url(public_url: &str, state: &str) -> String {
    let mut url = url::Url::parse("https://steamcommunity.com/openid/login").expect("static URL");
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("openid.ns", "http://specs.openid.net/auth/2.0");
        q.append_pair("openid.mode", "checkid_setup");
        q.append_pair("openid.return_to", &steam_return_to(public_url, state));
        q.append_pair("openid.realm", public_url);
        q.append_pair(
            "openid.identity",
            "http://specs.openid.net/auth/2.0/identifier_select",
        );
        q.append_pair(
            "openid.claimed_id",
            "http://specs.openid.net/auth/2.0/identifier_select",
        );
    }
    url.to_string()
}

/// Verify OpenID callback params with Steam; returns the SteamID64.
/// `expected_return_to` must equal the signed `openid.return_to`, so an
/// assertion minted for another site or login cannot be replayed here.
pub async fn verify_openid(
    http: &reqwest::Client,
    params: &HashMap<String, String>,
    expected_return_to: &str,
) -> anyhow::Result<u64> {
    if params.get("openid.mode").map(String::as_str) != Some("id_res") {
        bail!("unexpected openid.mode");
    }
    if params.get("openid.return_to").map(String::as_str) != Some(expected_return_to) {
        bail!("openid.return_to mismatch");
    }

    let mut verify_params: HashMap<&str, &str> = params
        .iter()
        .filter(|(k, _)| k.starts_with("openid."))
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    verify_params.insert("openid.mode", "check_authentication");

    let body = http
        .post("https://steamcommunity.com/openid/login")
        .form(&verify_params)
        .send()
        .await
        .context("steam openid verify request")?
        .text()
        .await
        .context("steam openid verify body")?;

    if !body.lines().any(|l| l.trim() == "is_valid:true") {
        bail!("OpenID validation failed");
    }

    let claimed_id = params
        .get("openid.claimed_id")
        .context("missing claimed_id")?;
    let steam_id = claimed_id
        .strip_prefix("https://steamcommunity.com/openid/id/")
        .with_context(|| format!("unexpected claimed_id: {claimed_id}"))?;
    steam_id.parse::<u64>().context("invalid steam id")
}

/// Verify an in-game Web API ticket via Steam; returns the SteamID64.
pub async fn verify_ticket(
    http: &reqwest::Client,
    api_key: &str,
    app_id: u32,
    ticket_hex: &str,
    identity: &str,
) -> anyhow::Result<u64> {
    if ticket_hex.is_empty()
        || ticket_hex.len() > 8192
        || ticket_hex.len() % 2 != 0
        || !ticket_hex.bytes().all(|b| b.is_ascii_hexdigit())
    {
        bail!("malformed ticket");
    }
    if identity != "rots-auth" {
        bail!("invalid ticket identity");
    }

    // POST form fields so neither the ticket nor API key is embedded in a URL
    // that an HTTP client error or proxy access log might disclose.
    let resp = http
        .post("https://partner.steam-api.com/ISteamUserAuth/AuthenticateUserTicket/v1/")
        .form(&[
            ("key", api_key),
            ("appid", &app_id.to_string()),
            ("ticket", ticket_hex),
            ("identity", identity),
        ])
        .send()
        .await
        .map_err(|_| anyhow::anyhow!("ticket provider request failed"))?;
    if !resp.status().is_success() {
        bail!("ticket rejected: HTTP {}", resp.status());
    }
    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|_| anyhow::anyhow!("invalid ticket provider response"))?;
    let steam_id = json["response"]["params"]["steamid"]
        .as_str()
        .context("ticket rejected")?;
    steam_id.parse::<u64>().context("invalid Steam identity")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(id: &str, name_field: &str, use_pkce: bool) -> ProviderConfig {
        ProviderConfig {
            id: id.into(),
            client_id: "cid".into(),
            client_secret: "sec".into(),
            authorization_endpoint: "https://au.2143.me/authorize".into(),
            token_endpoint: "t".into(),
            userinfo_endpoint: "u".into(),
            scopes: vec!["openid".into(), "profile".into()],
            id_field: "sub".into(),
            name_field: name_field.into(),
            use_pkce,
        }
    }

    #[test]
    fn authorization_url_contains_challenge_state_and_scopes() {
        let (verifier, challenge) = pkce_pair();
        let url = authorization_url(
            &cfg("au2143", "preferred_username", true),
            "https://rots.example/auth/api/v1/au2143/callback",
            "state-123",
            Some(&verifier),
            &challenge,
        );
        assert!(url.starts_with("https://au.2143.me/authorize?response_type=code&client_id=cid"));
        assert!(url.contains(
            "redirect_uri=https%3A%2F%2Frots.example%2Fauth%2Fapi%2Fv1%2Fau2143%2Fcallback"
        ));
        assert!(url.contains("scope=openid%20profile"));
        assert!(url.contains("state=state-123"));
        assert!(url.contains(&format!(
            "code_challenge={challenge}&code_challenge_method=S256"
        )));
    }

    #[test]
    fn userinfo_name_uses_name_field_with_fallbacks() {
        let c = cfg("discord", "global_name", false);
        let j = serde_json::json!({"global_name": "Alice", "username": "alice#1"});
        assert_eq!(userinfo_name(&j, &c), "Alice");
        // Discord sends `global_name: null` for users without one.
        let j2 = serde_json::json!({"global_name": null, "username": "alice#1"});
        assert_eq!(userinfo_name(&j2, &c), "alice#1");
        assert_eq!(
            userinfo_name(&serde_json::json!({"name": "bob"}), &c),
            "bob"
        );
        assert_eq!(userinfo_name(&serde_json::json!({}), &c), "Unknown");
    }

    #[test]
    fn openid_redirect_points_back_at_steam_callback() {
        let url = url::Url::parse(&openid_redirect_url("https://rots.example", "abc")).unwrap();
        let q: HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(
            q["openid.return_to"],
            "https://rots.example/auth/api/v1/steam/callback?state=abc"
        );
        assert_eq!(q["openid.realm"], "https://rots.example");
        assert_eq!(q["openid.mode"], "checkid_setup");
    }

    #[tokio::test]
    async fn verify_openid_rejects_foreign_return_to_before_network() {
        let http = reqwest::Client::new();
        let mut params = HashMap::new();
        params.insert("openid.mode".to_string(), "id_res".to_string());
        params.insert(
            "openid.return_to".to_string(),
            "https://evil.example/cb?state=abc".to_string(),
        );
        let err = verify_openid(
            &http,
            &params,
            &steam_return_to("https://rots.example", "abc"),
        )
        .await
        .unwrap_err();
        assert_eq!(err.to_string(), "openid.return_to mismatch");
    }

    #[tokio::test]
    async fn verify_ticket_rejects_malformed_and_wrong_identity_before_network() {
        let http = reqwest::Client::new();
        for bad in ["", "abc", "zz", &"a".repeat(8194)] {
            let err = verify_ticket(&http, "k", 1, bad, "rots-auth")
                .await
                .unwrap_err();
            assert_eq!(err.to_string(), "malformed ticket");
        }
        let err = verify_ticket(&http, "k", 1, "abcd", "matchmaking")
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "invalid ticket identity");
    }
}
