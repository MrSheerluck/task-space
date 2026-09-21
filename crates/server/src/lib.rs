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
    EncodedUpdate, MAX_SYNC_SNAPSHOT_BYTES, MAX_SYNC_STATE_VECTOR_BYTES, MAX_SYNC_UPDATE_BYTES,
    SYNC_DOCUMENT_SCHEMA_VERSION, SYNC_PROTOCOL_VERSION, SYNC_RECONCILE_PROTOCOL_VERSION,
    SyncEvent, SyncPullRequest, SyncPullResponse, SyncPushRequest, SyncReconcileRequest,
    SyncReconcileResponse,
};
use thiserror::Error;
use tokio::sync::broadcast;

pub mod billing;
pub mod dodo;
pub mod http;
pub mod metrics;
pub mod postgres;
pub mod workos;

#[derive(Debug, Error)]
pub enum SyncStoreError {
    #[error("unsupported sync protocol version {0}")]
    UnsupportedProtocol(u32),
    #[error("unsupported sync reconcile protocol version {0}")]
    UnsupportedReconcileProtocol(u32),
    #[error("unsupported sync document schema version {0}")]
    UnsupportedDocumentSchema(u32),
    #[error("invalid Yrs update: {0}")]
    InvalidUpdate(String),
    #[error("invalid Yrs state vector: {0}")]
    InvalidStateVector(String),
    #[error("sync payload is too large")]
    PayloadTooLarge,
    #[error("mutation id was reused with a different update")]
    MutationIdReused,
    #[error("account is not authorized for this space")]
    SpaceAccessDenied,
    #[error("sync store lock was poisoned")]
    LockPoisoned,
}

struct StoreInner {
    documents: Mutex<HashMap<(String, EntityId), SpaceDoc>>,
    mutations: Mutex<HashMap<(String, EntityId, String), StoredMutation>>,
    events: broadcast::Sender<DeliveryEvent>,
    next_event_id: AtomicU64,
}

#[derive(Clone, Debug)]
struct StoredMutation {
    device_id: String,
    local_generation: u64,
    state_vector: EncodedUpdate,
    update: EncodedUpdate,
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
        validate_document_schema(request.document_schema_version)?;
        let update = request
            .update
            .to_bytes()
            .map_err(|error| SyncStoreError::InvalidUpdate(error.to_string()))?;
        if request.mutation_id.trim().is_empty() || request.mutation_id.len() > 128 {
            return Err(SyncStoreError::InvalidUpdate(
                "mutation id must be between 1 and 128 characters".to_owned(),
            ));
        }
        if update.len() > MAX_SYNC_UPDATE_BYTES {
            return Err(SyncStoreError::PayloadTooLarge);
        }

        // Keep the same mutation-then-document lock order as reconcile so a
        // concurrent retry cannot deadlock while claiming a mutation.
        let mut mutations = self
            .inner
            .mutations
            .lock()
            .map_err(|_| SyncStoreError::LockPoisoned)?;
        if let Some(previous) = mutations.get(&(
            account_id.to_owned(),
            request.space_id,
            request.mutation_id.clone(),
        )) {
            if previous.update == request.update {
                return Ok(None);
            }
            return Err(SyncStoreError::MutationIdReused);
        }

        {
            let mut documents = self
                .inner
                .documents
                .lock()
                .map_err(|_| SyncStoreError::LockPoisoned)?;
            let Some(document) = documents.get_mut(&(account_id.to_owned(), request.space_id))
            else {
                return Err(SyncStoreError::SpaceAccessDenied);
            };
            let candidate = document.clone();
            candidate
                .apply_update(&update)
                .map_err(|error| SyncStoreError::InvalidUpdate(error.to_string()))?;
            if candidate.snapshot().len() > MAX_SYNC_SNAPSHOT_BYTES {
                return Err(SyncStoreError::PayloadTooLarge);
            }
            *document = candidate;
        }

        mutations.insert(
            (
                account_id.to_owned(),
                request.space_id,
                request.mutation_id.clone(),
            ),
            StoredMutation {
                device_id: String::new(),
                local_generation: 0,
                state_vector: EncodedUpdate::default(),
                update: request.update.clone(),
            },
        );
        drop(mutations);

        let event = SyncEvent {
            protocol_version: SYNC_PROTOCOL_VERSION,
            document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
            event_id: self.inner.next_event_id.fetch_add(1, Ordering::Relaxed),
            space_id: request.space_id,
            stable_space_id: request.stable_space_id.clone(),
            update: EncodedUpdate::from_bytes(&update),
            metadata: None,
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
        validate_document_schema(request.document_schema_version)?;
        let state_vector = request
            .state_vector
            .to_bytes()
            .map_err(|error| SyncStoreError::InvalidStateVector(error.to_string()))?;
        if state_vector.len() > MAX_SYNC_STATE_VECTOR_BYTES {
            return Err(SyncStoreError::PayloadTooLarge);
        }
        let documents = self
            .inner
            .documents
            .lock()
            .map_err(|_| SyncStoreError::LockPoisoned)?;
        let document = documents
            .get(&(account_id.to_owned(), request.space_id))
            .ok_or(SyncStoreError::SpaceAccessDenied)?;
        let update = document
            .encode_update(&state_vector)
            .map_err(|error| SyncStoreError::InvalidStateVector(error.to_string()))?;
        if update.len() > MAX_SYNC_UPDATE_BYTES {
            return Err(SyncStoreError::PayloadTooLarge);
        }

        Ok(SyncPullResponse {
            protocol_version: SYNC_PROTOCOL_VERSION,
            document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
            space_id: request.space_id,
            stable_space_id: request.stable_space_id.clone(),
            update: EncodedUpdate::from_bytes(&update),
            state_vector: EncodedUpdate::from_bytes(&document.state_vector()),
            has_more: false,
        })
    }

    /// Merge a client delta and return the server delta relative to the
    /// client's submitted state vector. This mirrors the PostgreSQL v2
    /// transaction and keeps the in-memory engine useful for fast contract
    /// tests.
    pub fn reconcile(
        &self,
        account_id: &str,
        request: &SyncReconcileRequest,
    ) -> Result<SyncReconcileResponse, SyncStoreError> {
        if request.protocol_version != SYNC_RECONCILE_PROTOCOL_VERSION {
            return Err(SyncStoreError::UnsupportedReconcileProtocol(
                request.protocol_version,
            ));
        }
        validate_document_schema(request.document_schema_version)?;
        if request.mutation_id.trim().is_empty() || request.mutation_id.len() > 128 {
            return Err(SyncStoreError::InvalidUpdate(
                "mutation id must be between 1 and 128 characters".to_owned(),
            ));
        }
        if request.device_id.trim().is_empty() || request.device_id.len() > 128 {
            return Err(SyncStoreError::InvalidUpdate(
                "device id must be between 1 and 128 characters".to_owned(),
            ));
        }
        let state_vector = request
            .state_vector
            .to_bytes()
            .map_err(|error| SyncStoreError::InvalidStateVector(error.to_string()))?;
        let update = request
            .update
            .to_bytes()
            .map_err(|error| SyncStoreError::InvalidUpdate(error.to_string()))?;
        if state_vector.len() > MAX_SYNC_STATE_VECTOR_BYTES || update.len() > MAX_SYNC_UPDATE_BYTES
        {
            return Err(SyncStoreError::PayloadTooLarge);
        }
        let key = (
            account_id.to_owned(),
            request.space_id,
            request.mutation_id.clone(),
        );
        // Hold the mutation claim while applying the document so concurrent
        // retries cannot both observe a missing mutation and emit duplicate
        // events.
        let mut mutations = self
            .inner
            .mutations
            .lock()
            .map_err(|_| SyncStoreError::LockPoisoned)?;
        let duplicate = match mutations.get(&key) {
            Some(previous)
                if previous.update == request.update
                    && previous.state_vector == request.state_vector
                    && previous.device_id == request.device_id
                    && previous.local_generation == request.local_generation =>
            {
                true
            }
            Some(_) => return Err(SyncStoreError::MutationIdReused),
            None => false,
        };
        let (server_delta, server_state_vector, event_id) = {
            let mut documents = self
                .inner
                .documents
                .lock()
                .map_err(|_| SyncStoreError::LockPoisoned)?;
            let Some(document) = documents.get_mut(&(account_id.to_owned(), request.space_id))
            else {
                return Err(SyncStoreError::SpaceAccessDenied);
            };
            let candidate = document.clone();
            let previous_state_vector = candidate.state_vector();
            if !duplicate && !update.is_empty() {
                candidate
                    .apply_update(&update)
                    .map_err(|error| SyncStoreError::InvalidUpdate(error.to_string()))?;
                if candidate.snapshot().len() > MAX_SYNC_SNAPSHOT_BYTES {
                    return Err(SyncStoreError::PayloadTooLarge);
                }
            }
            let changed = candidate.state_vector() != previous_state_vector;
            let delta = candidate
                .encode_update(&state_vector)
                .map_err(|error| SyncStoreError::InvalidStateVector(error.to_string()))?;
            if delta.len() > MAX_SYNC_UPDATE_BYTES {
                return Err(SyncStoreError::PayloadTooLarge);
            }
            let state_vector = candidate.state_vector();
            if !duplicate && changed {
                *document = candidate;
            }
            let event_id = if duplicate || !changed {
                None
            } else {
                Some(self.inner.next_event_id.fetch_add(1, Ordering::Relaxed))
            };
            if !duplicate {
                mutations.insert(
                    key,
                    StoredMutation {
                        device_id: request.device_id.clone(),
                        local_generation: request.local_generation,
                        state_vector: request.state_vector.clone(),
                        update: request.update.clone(),
                    },
                );
            }
            (delta, state_vector, event_id)
        };
        drop(mutations);
        if let Some(event_id) = event_id {
            let event = SyncEvent {
                protocol_version: SYNC_PROTOCOL_VERSION,
                document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
                event_id,
                space_id: request.space_id,
                stable_space_id: request.stable_space_id.clone(),
                update: EncodedUpdate::from_bytes(&update),
                metadata: None,
            };
            let _ = self.inner.events.send(DeliveryEvent {
                account_id: account_id.to_owned(),
                event,
            });
        }
        Ok(SyncReconcileResponse {
            protocol_version: SYNC_RECONCILE_PROTOCOL_VERSION,
            document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
            space_id: request.space_id,
            stable_space_id: request.stable_space_id.clone(),
            mutation_id: request.mutation_id.clone(),
            acknowledged_generation: request.local_generation,
            accepted: event_id.is_some() || duplicate || (!update.is_empty() && event_id.is_none()),
            event_id,
            update: EncodedUpdate::from_bytes(&server_delta),
            state_vector: EncodedUpdate::from_bytes(&server_state_vector),
            entitlement_version: 0,
            event_cursor: self
                .inner
                .next_event_id
                .load(Ordering::Acquire)
                .saturating_sub(1),
        })
    }
}

fn validate_protocol(version: u32) -> Result<(), SyncStoreError> {
    if version == SYNC_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(SyncStoreError::UnsupportedProtocol(version))
    }
}

fn validate_document_schema(version: u32) -> Result<(), SyncStoreError> {
    if version == SYNC_DOCUMENT_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(SyncStoreError::UnsupportedDocumentSchema(version))
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
            document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
            space_id: 8,
            stable_space_id: None,
            mutation_id: "device-a:1".into(),
            update,
        };
        store.push("account-a", &push).expect("push should succeed");

        let response = store
            .pull(
                "account-a",
                &SyncPullRequest {
                    protocol_version: SYNC_PROTOCOL_VERSION,
                    document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
                    space_id: 8,
                    stable_space_id: None,
                    last_server_sequence: 0,
                    state_vector,
                    local_generation: 0,
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
            document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
            space_id: 9,
            stable_space_id: None,
            mutation_id: "device-a:2".into(),
            update,
        };
        assert!(store.push("account-a", &request).unwrap().is_some());
        assert!(store.push("account-a", &request).unwrap().is_none());
    }

    #[test]
    fn reconcile_bootstraps_and_retries_idempotently() {
        let store = SyncStore::new();
        let (update, state_vector) = source_update();
        store.register_space("account-a", 10).unwrap();
        let request = SyncReconcileRequest {
            protocol_version: SYNC_RECONCILE_PROTOCOL_VERSION,
            document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
            space_id: 10,
            stable_space_id: None,
            mutation_id: "mutation-1".into(),
            device_id: "device-a".into(),
            local_generation: 1,
            last_server_sequence: 0,
            state_vector,
            update: update.clone(),
        };
        let first = store.reconcile("account-a", &request).unwrap();
        assert!(first.accepted);
        assert_eq!(first.space_id, 10);

        let retry = store.reconcile("account-a", &request).unwrap();
        assert!(retry.accepted);
        assert_eq!(retry.event_id, None);

        // The cursor is replay context, not mutation identity. A retry may
        // observe a newer SSE cursor without becoming a different effect.
        let mut cursor_changed = request.clone();
        cursor_changed.last_server_sequence = 999;
        let cursor_retry = store.reconcile("account-a", &cursor_changed).unwrap();
        assert!(cursor_retry.accepted);
        assert_eq!(cursor_retry.event_id, None);

        let mut changed_metadata = request.clone();
        changed_metadata.local_generation = 2;
        assert!(matches!(
            store.reconcile("account-a", &changed_metadata),
            Err(SyncStoreError::MutationIdReused)
        ));

        let replica = SpaceDoc::from_update(&first.update.to_bytes().unwrap()).unwrap();
        assert_eq!(replica.board().notes[0].text, "server task");
    }

    #[test]
    fn empty_reconcile_claims_mutation_id() {
        let store = SyncStore::new();
        store.register_space("account-a", 12).unwrap();
        let request = SyncReconcileRequest {
            protocol_version: SYNC_RECONCILE_PROTOCOL_VERSION,
            document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
            space_id: 12,
            stable_space_id: None,
            mutation_id: "empty-mutation".into(),
            device_id: "device-a".into(),
            local_generation: 1,
            last_server_sequence: 0,
            state_vector: EncodedUpdate::from_bytes(&SpaceDoc::new().state_vector()),
            update: EncodedUpdate::default(),
        };
        let first = store.reconcile("account-a", &request).unwrap();
        assert!(!first.accepted);

        let mut changed = request.clone();
        changed.local_generation = 2;
        assert!(matches!(
            store.reconcile("account-a", &changed),
            Err(SyncStoreError::MutationIdReused)
        ));
    }

    #[test]
    fn repeated_crdt_update_with_new_mutation_id_is_a_noop() {
        let store = SyncStore::new();
        store.register_space("account-a", 16).unwrap();
        let (update, state_vector) = source_update();
        let request = SyncReconcileRequest {
            protocol_version: SYNC_RECONCILE_PROTOCOL_VERSION,
            document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
            space_id: 16,
            stable_space_id: None,
            mutation_id: "first-noop-check".into(),
            device_id: "device-a".into(),
            local_generation: 1,
            last_server_sequence: 0,
            state_vector: state_vector.clone(),
            update: update.clone(),
        };
        let mut events = store.inner.events.subscribe();
        assert!(store.reconcile("account-a", &request).unwrap().accepted);

        let repeated = SyncReconcileRequest {
            mutation_id: "second-noop-check".into(),
            local_generation: 2,
            ..request
        };
        let response = store.reconcile("account-a", &repeated).unwrap();
        assert!(response.accepted);
        assert_eq!(response.event_id, None);
        assert!(events.try_recv().is_ok());
        assert!(events.try_recv().is_err());
    }

    #[test]
    fn account_boundaries_reject_cross_account_reads_and_writes() {
        let store = SyncStore::new();
        let (update, state_vector) = source_update();
        store.register_space("account-a", 11).unwrap();
        let request = SyncReconcileRequest {
            protocol_version: SYNC_RECONCILE_PROTOCOL_VERSION,
            document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
            space_id: 11,
            stable_space_id: None,
            mutation_id: "account-a-mutation".into(),
            device_id: "device-a".into(),
            local_generation: 1,
            last_server_sequence: 0,
            state_vector,
            update,
        };
        store.reconcile("account-a", &request).unwrap();
        let other = SyncPullRequest {
            protocol_version: SYNC_PROTOCOL_VERSION,
            document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
            space_id: 11,
            stable_space_id: None,
            last_server_sequence: 0,
            state_vector: EncodedUpdate::from_bytes(&SpaceDoc::new().state_vector()),
            local_generation: 0,
        };
        assert!(matches!(
            store.pull("account-b", &other),
            Err(SyncStoreError::SpaceAccessDenied)
        ));
    }

    #[test]
    fn unsupported_document_schema_is_rejected_before_merge() {
        let store = SyncStore::new();
        store.register_space("account-a", 14).unwrap();
        let request = SyncReconcileRequest {
            protocol_version: SYNC_RECONCILE_PROTOCOL_VERSION,
            document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION + 1,
            space_id: 14,
            stable_space_id: None,
            mutation_id: "future-schema".into(),
            device_id: "device-a".into(),
            local_generation: 1,
            last_server_sequence: 0,
            state_vector: EncodedUpdate::from_bytes(&SpaceDoc::new().state_vector()),
            update: EncodedUpdate::default(),
        };

        assert!(matches!(
            store.reconcile("account-a", &request),
            Err(SyncStoreError::UnsupportedDocumentSchema(_))
        ));
    }

    #[test]
    fn concurrent_reconcile_requests_converge_after_reordered_delivery() {
        let store = SyncStore::new();
        store.register_space("account-a", 13).unwrap();

        let base = SpaceDoc::new();
        base.import_board(&BoardData {
            notes: vec![Note {
                id: 1,
                text: "base".into(),
                ..Default::default()
            }],
            ..Default::default()
        });
        let base_state = base.state_vector();
        let bootstrap = base
            .encode_update(&SpaceDoc::empty_state_vector())
            .expect("bootstrap update should encode");
        store
            .reconcile(
                "account-a",
                &SyncReconcileRequest {
                    protocol_version: SYNC_RECONCILE_PROTOCOL_VERSION,
                    document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
                    space_id: 13,
                    stable_space_id: None,
                    mutation_id: "bootstrap-13".into(),
                    device_id: "device-bootstrap".into(),
                    local_generation: 1,
                    last_server_sequence: 0,
                    state_vector: EncodedUpdate::from_bytes(&SpaceDoc::empty_state_vector()),
                    update: EncodedUpdate::from_bytes(&bootstrap),
                },
            )
            .unwrap();

        let first = SpaceDoc::from_update(&base.snapshot()).unwrap();
        let second = SpaceDoc::from_update(&base.snapshot()).unwrap();
        first.set_note_field(1, "text", "first".into());
        second.set_note_field(1, "due_date", "2026-09-10".into());
        let first_update = first.encode_update(&base_state).unwrap();
        let second_update = second.encode_update(&base_state).unwrap();

        let first_response = store
            .reconcile(
                "account-a",
                &SyncReconcileRequest {
                    protocol_version: SYNC_RECONCILE_PROTOCOL_VERSION,
                    document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
                    space_id: 13,
                    stable_space_id: None,
                    mutation_id: "first-13".into(),
                    device_id: "device-first".into(),
                    local_generation: 2,
                    last_server_sequence: 0,
                    state_vector: EncodedUpdate::from_bytes(&base_state),
                    update: EncodedUpdate::from_bytes(&first_update),
                },
            )
            .unwrap();
        let second_response = store
            .reconcile(
                "account-a",
                &SyncReconcileRequest {
                    protocol_version: SYNC_RECONCILE_PROTOCOL_VERSION,
                    document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
                    space_id: 13,
                    stable_space_id: None,
                    mutation_id: "second-13".into(),
                    device_id: "device-second".into(),
                    local_generation: 2,
                    last_server_sequence: 0,
                    state_vector: EncodedUpdate::from_bytes(&base_state),
                    update: EncodedUpdate::from_bytes(&second_update),
                },
            )
            .unwrap();

        first
            .apply_update(&second_response.update.to_bytes().unwrap())
            .unwrap();
        second
            .apply_update(&first_response.update.to_bytes().unwrap())
            .unwrap();
        assert_eq!(first.board(), second.board());
        assert_eq!(first.board().notes[0].text, "first");
        assert_eq!(
            first.board().notes[0].due_date.as_deref(),
            Some("2026-09-10")
        );
    }

    #[test]
    fn concurrent_duplicate_reconcile_requests_emit_one_effect() {
        let store = SyncStore::new();
        store.register_space("account-a", 15).unwrap();
        let (update, state_vector) = source_update();
        let request = SyncReconcileRequest {
            protocol_version: SYNC_RECONCILE_PROTOCOL_VERSION,
            document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
            space_id: 15,
            stable_space_id: None,
            mutation_id: "concurrent-duplicate".into(),
            device_id: "device-a".into(),
            local_generation: 1,
            last_server_sequence: 0,
            state_vector,
            update,
        };
        let mut events = store.inner.events.subscribe();
        let left_store = store.clone();
        let right_store = store.clone();
        let left_request = request.clone();
        let right_request = request;

        let (left, right) = std::thread::scope(|scope| {
            let left = scope.spawn(|| left_store.reconcile("account-a", &left_request));
            let right = scope.spawn(|| right_store.reconcile("account-a", &right_request));
            (
                left.join().expect("left reconcile should not panic"),
                right.join().expect("right reconcile should not panic"),
            )
        });
        assert!(left.is_ok());
        assert!(right.is_ok());

        let mut event_count = 0;
        while events.try_recv().is_ok() {
            event_count += 1;
        }
        assert_eq!(event_count, 1);
    }
}
