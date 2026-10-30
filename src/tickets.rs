//! Game tickets: short-lived EdDSA JWTs the game server verifies offline with
//! the public key served at `/api/v1/ticket/public-key`.
use anyhow::{Context, bail};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use ed25519_dalek::pkcs8::EncodePrivateKey;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};

pub const TICKET_ISSUER: &str = "rots-authserver";
pub const TICKET_TTL_SECS: u64 = 300;

pub struct TicketKeys {
    encoding: EncodingKey,
    pub public_key_b64url: String,
}

/// The JSON contract shared with the game server (`bevy2025/server/src/ticket.rs`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GameTicketClaims {
    pub iss: String,          // "rots-authserver"
    pub aud: String,          // config.public_url
    pub sub: String,          // character_id as decimal string
    pub sid: String,          // sha256 hex prefix (16 chars) of the session token_hash
    pub kind: String,         // "guest" | "account"
    pub prov: Option<String>, // "steam" | "discord" | "au2143" for accounts
    pub acc: Option<i64>,     // account_id for accounts
    pub name: String,         // character name
    pub iat: u64,
    pub exp: u64, // iat + 300
}

impl TicketKeys {
    /// `seed_b64` is standard base64 of a 32-byte ed25519 seed (`TICKET_SIGNING_KEY`).
    pub fn from_seed_b64(seed_b64: &str) -> anyhow::Result<Self> {
        let Some(seed) = STANDARD
            .decode(seed_b64.trim())
            .ok()
            .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
        else {
            bail!("TICKET_SIGNING_KEY must be base64 of 32 bytes");
        };
        Self::from_seed(&seed)
    }

    pub fn from_seed(seed: &[u8; 32]) -> anyhow::Result<Self> {
        let signing_key = ed25519_dalek::SigningKey::from_bytes(seed);
        let der = signing_key
            .to_pkcs8_der()
            .context("encoding ticket signing key as PKCS#8")?;
        Ok(Self {
            encoding: EncodingKey::from_ed_der(der.as_bytes()),
            public_key_b64url: URL_SAFE_NO_PAD.encode(signing_key.verifying_key().to_bytes()),
        })
    }

    pub fn sign(&self, claims: &GameTicketClaims) -> anyhow::Result<String> {
        jsonwebtoken::encode(&Header::new(Algorithm::EdDSA), claims, &self.encoding)
            .context("signing game ticket")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{DecodingKey, Validation};

    const AUD: &str = "https://rots.example";

    fn claims() -> GameTicketClaims {
        let iat = jsonwebtoken::get_current_timestamp();
        GameTicketClaims {
            iss: TICKET_ISSUER.into(),
            aud: AUD.into(),
            sub: "42".into(),
            sid: "0123456789abcdef".into(),
            kind: "account".into(),
            prov: Some("steam".into()),
            acc: Some(7),
            name: "Steam 7656".into(),
            iat,
            exp: iat + TICKET_TTL_SECS,
        }
    }

    fn validation(aud: &str) -> Validation {
        let mut v = Validation::new(Algorithm::EdDSA);
        v.set_audience(&[aud]);
        v.set_issuer(&[TICKET_ISSUER]);
        v
    }

    #[test]
    fn ticket_roundtrips_through_public_key() {
        let seed = STANDARD.encode([9u8; 32]);
        let keys = TicketKeys::from_seed_b64(&seed).unwrap();
        let original = claims();
        let ticket = keys.sign(&original).unwrap();

        let key = DecodingKey::from_ed_components(&keys.public_key_b64url).unwrap();
        let decoded =
            jsonwebtoken::decode::<GameTicketClaims>(&ticket, &key, &validation(AUD)).unwrap();
        assert_eq!(decoded.header.alg, Algorithm::EdDSA);
        assert_eq!(decoded.claims, original);
    }

    #[test]
    fn ticket_rejected_for_other_audience_or_key() {
        let keys = TicketKeys::from_seed(&[1u8; 32]).unwrap();
        let ticket = keys.sign(&claims()).unwrap();
        let key = DecodingKey::from_ed_components(&keys.public_key_b64url).unwrap();
        assert!(
            jsonwebtoken::decode::<GameTicketClaims>(&ticket, &key, &validation("https://other"))
                .is_err()
        );

        let other = TicketKeys::from_seed(&[2u8; 32]).unwrap();
        let other_key = DecodingKey::from_ed_components(&other.public_key_b64url).unwrap();
        assert!(
            jsonwebtoken::decode::<GameTicketClaims>(&ticket, &other_key, &validation(AUD))
                .is_err()
        );
    }

    #[test]
    fn signing_key_must_be_32_bytes() {
        for bad in [
            "",
            "not base64!",
            &STANDARD.encode([0u8; 31]),
            &STANDARD.encode([0u8; 64]),
        ] {
            let err = TicketKeys::from_seed_b64(bad).err().expect("must reject");
            assert_eq!(
                err.to_string(),
                "TICKET_SIGNING_KEY must be base64 of 32 bytes"
            );
        }
    }
}
