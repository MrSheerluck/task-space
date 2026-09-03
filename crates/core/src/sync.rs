//! Transport-neutral sync contracts shared by the browser and server.
//!
//! The actual transport can be HTTP, SSE, or another provider. Yrs updates
//! are kept binary and encoded as URL-safe base64 only at this JSON boundary.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::EntityId;

pub const SYNC_PROTOCOL_VERSION: u32 = 1;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EncodedUpdate(String);

impl EncodedUpdate {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(URL_SAFE_NO_PAD.encode(bytes))
    }

    pub fn from_base64(encoded: impl Into<String>) -> Result<Self, base64::DecodeError> {
        let encoded = encoded.into();
        URL_SAFE_NO_PAD.decode(encoded.as_bytes())?;
        Ok(Self(encoded))
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, base64::DecodeError> {
        URL_SAFE_NO_PAD.decode(self.0.as_bytes())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Serialize for EncodedUpdate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for EncodedUpdate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        URL_SAFE_NO_PAD
            .decode(encoded.as_bytes())
            .map_err(serde::de::Error::custom)?;
        Ok(Self(encoded))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SyncPullRequest {
    pub protocol_version: u32,
    pub space_id: EntityId,
    pub state_vector: EncodedUpdate,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SyncPushRequest {
    pub protocol_version: u32,
    pub space_id: EntityId,
    pub mutation_id: String,
    pub update: EncodedUpdate,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SyncPullResponse {
    pub protocol_version: u32,
    pub space_id: EntityId,
    pub update: EncodedUpdate,
    pub has_more: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SyncPushResponse {
    pub protocol_version: u32,
    pub space_id: EntityId,
    pub accepted: bool,
    pub event_id: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SyncEvent {
    pub protocol_version: u32,
    pub event_id: u64,
    pub space_id: EntityId,
    pub update: EncodedUpdate,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_update_is_compact_and_round_trips() {
        let encoded = EncodedUpdate::from_bytes(&[0, 1, 2, 250, 255]);
        let raw = serde_json::to_string(&encoded).expect("update should serialize");
        let restored: EncodedUpdate =
            serde_json::from_str(&raw).expect("update should deserialize");

        assert_eq!(restored.to_bytes().unwrap(), vec![0, 1, 2, 250, 255]);
        assert!(!raw.contains("250"));
    }

    #[test]
    fn invalid_encoded_update_is_rejected() {
        let result = serde_json::from_str::<EncodedUpdate>(r#""not base64!""#);
        assert!(result.is_err());
    }

    #[test]
    fn push_contract_round_trips() {
        let request = SyncPushRequest {
            protocol_version: SYNC_PROTOCOL_VERSION,
            space_id: 42,
            mutation_id: "device-1:17".into(),
            update: EncodedUpdate::from_bytes(&[3, 4, 5]),
        };
        let raw = serde_json::to_string(&request).expect("request should serialize");
        let restored: SyncPushRequest =
            serde_json::from_str(&raw).expect("request should deserialize");
        assert_eq!(restored, request);
    }
}
