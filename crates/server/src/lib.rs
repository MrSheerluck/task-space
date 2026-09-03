//! Server-side Yrs merge and delivery primitives.
//!
//! This layer deliberately does not know about HTTP, authentication, or
//! billing. It accepts validated sync contracts and can be mounted under any
//! transport. The in-memory store is a development
//! implementation; production persistence will replace its document map with
//! a snapshot/update-log repository.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use task_core::EntityId;
use task_core::crdt::SpaceDoc;
use task_core::sync::{
    EncodedUpdate, SYNC_PROTOCOL_VERSION, SyncEvent, SyncPullRequest, SyncPullResponse,
    SyncPushRequest,
};
use thiserror::Error;
use tokio::sync::broadcast;

pub mod billing;
pub mod http;

#[derive(Debug, Error)]
pub enum SyncStoreError {
    #[error("unsupported sync protocol version {0}")]
    UnsupportedProtocol(u32),
    #[error("invalid Yrs update: {0}")]
    InvalidUpdate(String),
    #[error("invalid Yrs state vector: {0}")]
    InvalidStateVector(String),
    #[error("mutation id was reused with a different update")]
    MutationIdReused,
    #[error("account is not authorized for this space")]
    SpaceAccessDenied,
    #[error("sync store lock was poisoned")]
    LockPoisoned,
}

struct StoreInner {
    documents: Mutex<HashMap<(String, EntityId), SpaceDoc>>,
    mutations: Mutex<HashMap<(String, EntityId, String), EncodedUpdate>>,
    events: broadcast::Sender<DeliveryEvent>,
    next_event_id: AtomicU64,
}

#[derive(Clone, Debug)]
pub(crate) struct DeliveryEvent {
    pub(crate) account_id: String,
    pub(crate) event: SyncEvent,
}

/// A merge engine for one server instance.
#[derive(Clone)]
pub struct SyncStore {
    inner: Arc<StoreInner>,
}

impl Default for SyncStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncStore {
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(256);
        Self {
            inner: Arc::new(StoreInner {
                documents: Mutex::new(HashMap::new()),
                mutations: Mutex::new(HashMap::new()),
                events,
                next_event_id: AtomicU64::new(1),
            }),
        }
    }

    /// Register a space after the authenticated account has created it. Sync
    /// requests cannot implicitly create spaces, which keeps authorization at
    /// the account boundary instead of trusting client-supplied ids.
    pub fn register_space(
        &self,
        account_id: &str,
        space_id: EntityId,
    ) -> Result<(), SyncStoreError> {
        self.inner
            .documents
            .lock()
            .map_err(|_| SyncStoreError::LockPoisoned)?
            .entry((account_id.to_owned(), space_id))
            .or_insert_with(SpaceDoc::new);
        Ok(())
    }

    /// Apply a client update once. A retry with the same mutation id and the
    /// same payload is safely ignored.
    pub fn push(
        &self,
        account_id: &str,
        request: &SyncPushRequest,
    ) -> Result<Option<SyncEvent>, SyncStoreError> {
        validate_protocol(request.protocol_version)?;
        let update = request
            .update
            .to_bytes()
            .map_err(|error| SyncStoreError::InvalidUpdate(error.to_string()))?;

        {
            let mutations = self
                .inner
                .mutations
                .lock()
                .map_err(|_| SyncStoreError::LockPoisoned)?;
            if let Some(previous) = mutations.get(&(
                account_id.to_owned(),
                request.space_id,
                request.mutation_id.clone(),
            )) {
                if previous == &request.update {
                    return Ok(None);
                }
                return Err(SyncStoreError::MutationIdReused);
            }
        }

        {
            let documents = self
                .inner
                .documents
                .lock()
                .map_err(|_| SyncStoreError::LockPoisoned)?;
            let Some(document) = documents.get(&(account_id.to_owned(), request.space_id)) else {
                return Err(SyncStoreError::SpaceAccessDenied);
            };
            document
                .apply_update(&update)
                .map_err(|error| SyncStoreError::InvalidUpdate(error.to_string()))?;
        }

        self.inner
            .mutations
            .lock()
            .map_err(|_| SyncStoreError::LockPoisoned)?
            .insert(
                (
                    account_id.to_owned(),
                    request.space_id,
                    request.mutation_id.clone(),
                ),
                request.update.clone(),
            );

        let event = SyncEvent {
            protocol_version: SYNC_PROTOCOL_VERSION,
            event_id: self.inner.next_event_id.fetch_add(1, Ordering::Relaxed),
            space_id: request.space_id,
            update: EncodedUpdate::from_bytes(&update),
        };
        // There may be no active SSE subscribers. That is normal.
        let _ = self.inner.events.send(DeliveryEvent {
            account_id: account_id.to_owned(),
            event: event.clone(),
        });
        Ok(Some(event))
    }

    /// Return only the document changes not represented by the client's state
    /// vector. This is also the recovery path after an SSE disconnect.
    pub fn pull(
        &self,
        account_id: &str,
        request: &SyncPullRequest,
    ) -> Result<SyncPullResponse, SyncStoreError> {
        validate_protocol(request.protocol_version)?;
        let state_vector = request
            .state_vector
            .to_bytes()
            .map_err(|error| SyncStoreError::InvalidStateVector(error.to_string()))?;
        let documents = self
            .inner
            .documents
            .lock()
            .map_err(|_| SyncStoreError::LockPoisoned)?;
        let update = documents
            .get(&(account_id.to_owned(), request.space_id))
            .map(|document| document.encode_update(&state_vector))
            .transpose()
            .map_err(|error| SyncStoreError::InvalidStateVector(error.to_string()))?
            .unwrap_or_default();

        Ok(SyncPullResponse {
            protocol_version: SYNC_PROTOCOL_VERSION,
            space_id: request.space_id,
            update: EncodedUpdate::from_bytes(&update),
            has_more: false,
        })
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<DeliveryEvent> {
        self.inner.events.subscribe()
    }
}

fn validate_protocol(version: u32) -> Result<(), SyncStoreError> {
    if version == SYNC_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(SyncStoreError::UnsupportedProtocol(version))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use task_core::crdt::SpaceDoc;
    use task_core::{BoardData, Note};

    fn source_update() -> (EncodedUpdate, EncodedUpdate) {
        let source = SpaceDoc::new();
        source.import_board(&BoardData {
            notes: vec![Note {
                id: 1,
                text: "server task".into(),
                ..Default::default()
            }],
            ..Default::default()
        });
        let empty = SpaceDoc::new();
        let update = source
            .encode_update(&empty.state_vector())
            .expect("source diff should encode");
        (
            EncodedUpdate::from_bytes(&update),
            EncodedUpdate::from_bytes(&empty.state_vector()),
        )
    }

    #[test]
    fn push_then_pull_returns_only_missing_state() {
        let store = SyncStore::new();
        let (update, state_vector) = source_update();
        store.register_space("account-a", 8).unwrap();
        let push = SyncPushRequest {
            protocol_version: SYNC_PROTOCOL_VERSION,
            space_id: 8,
            mutation_id: "device-a:1".into(),
            update,
        };
        store.push("account-a", &push).expect("push should succeed");

        let response = store
            .pull(
                "account-a",
                &SyncPullRequest {
                    protocol_version: SYNC_PROTOCOL_VERSION,
                    space_id: 8,
                    state_vector,
                },
            )
            .expect("pull should succeed");
        let replica = SpaceDoc::from_update(&response.update.to_bytes().unwrap())
            .expect("pulled update should apply");
        assert_eq!(replica.board().notes[0].text, "server task");
    }

    #[test]
    fn duplicate_push_is_idempotent() {
        let store = SyncStore::new();
        let (update, _) = source_update();
        store.register_space("account-a", 9).unwrap();
        let request = SyncPushRequest {
            protocol_version: SYNC_PROTOCOL_VERSION,
            space_id: 9,
            mutation_id: "device-a:2".into(),
            update,
        };
        assert!(store.push("account-a", &request).unwrap().is_some());
        assert!(store.push("account-a", &request).unwrap().is_none());
    }
}
