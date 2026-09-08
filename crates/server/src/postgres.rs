//! PostgreSQL-backed repositories for synchronized spaces and billing.
//!
//! The database stores Yrs snapshots and opaque updates. The application only
//! projects a document to a board in the browser; it never stores provider
//! payloads or trusts browser-supplied account ids.

use std::collections::BTreeSet;
use std::sync::Arc;

use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction};
use task_core::billing::{
    BILLING_PROTOCOL_VERSION, BillingEvent, BillingEventType, Entitlement, FREE_SPACE_LIMIT,
    PRO_SPACE_LIMIT, SubscriptionPlan, SubscriptionStatus, SyncAccessMode,
};
use task_core::crdt::SpaceDoc;
use task_core::sync::{
    EncodedUpdate, MAX_SYNC_SNAPSHOT_BYTES, MAX_SYNC_STATE_VECTOR_BYTES, MAX_SYNC_UPDATE_BYTES,
    SYNC_PROTOCOL_VERSION, SYNC_RECONCILE_PROTOCOL_VERSION, SpaceMetadataOperation, SyncEvent,
    SyncMetadataEvent, SyncMetadataRequest, SyncMetadataResponse, SyncPullRequest,
    SyncPullResponse, SyncPushRequest, SyncReconcileRequest, SyncReconcileResponse,
};
use task_core::{EntityId, Space};
use thiserror::Error;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::DeliveryEvent;
use crate::billing::BillingApplyResult;

#[derive(Debug, Error)]
pub enum PostgresStoreError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("unsupported sync protocol version {0}")]
    UnsupportedSyncProtocol(u32),
    #[error("unsupported sync reconcile protocol version {0}")]
    UnsupportedSyncReconcileProtocol(u32),
    #[error("unsupported billing protocol version {0}")]
    UnsupportedBillingProtocol(u32),
    #[error("invalid Yrs update: {0}")]
    InvalidUpdate(String),
    #[error("invalid Yrs state vector: {0}")]
    InvalidStateVector(String),
    #[error("mutation id was reused with a different update")]
    MutationIdReused,
    #[error("provider event id was reused with a different event")]
    ProviderEventIdReused,
    #[error("billing event is missing {0}")]
    MissingBillingField(&'static str),
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("sync payload is too large")]
    PayloadTooLarge,
    #[error("account is not authorized for this space")]
    SpaceAccessDenied,
    #[error("provider resource maps to multiple accounts")]
    AmbiguousProviderResource,
    #[error("sync is not enabled for this account")]
    SyncNotEntitled,
    #[error("sync is paused pending payment recovery")]
    SyncPaymentPaused,
    #[error("sync space limit reached")]
    SpaceLimitReached,
    #[error("space metadata version conflict")]
    MetadataVersionConflict,
    #[error("metadata operation id was reused with a different request")]
    MetadataOperationIdReused,
    #[error("sync event cursor requires a reset")]
    EventCursorRequiresReset,
}

#[derive(Clone)]
pub struct PostgresSyncStore {
    pool: PgPool,
    events: Arc<broadcast::Sender<DeliveryEvent>>,
}

const MAX_SAFE_ENTITY_ID: u64 = (1u64 << 53) - 1;
const MAX_BILLING_FIELD_LEN: usize = 256;

impl PostgresSyncStore {
    pub fn new(pool: PgPool) -> Self {
        let (events, _) = broadcast::channel(256);
        Self {
            pool,
            events: Arc::new(events),
        }
    }

    pub async fn connect(database_url: &str) -> Result<Self, PostgresStoreError> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Self::new(pool))
    }

    pub async fn migrate(&self) -> Result<(), PostgresStoreError> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Registering is idempotent so the browser can safely retry it whenever
    /// a session starts. It never creates a space as a side effect of sync.
    pub async fn register_space(
        &self,
        account_id: &str,
        space_id: EntityId,
        name: &str,
        max_spaces: u32,
    ) -> Result<(), PostgresStoreError> {
        validate_entity_id(space_id)?;
        let snapshot = SpaceDoc::new().snapshot();
        let mut transaction = self.pool.begin().await?;
        // Serialize registrations per account so two tabs cannot both pass
        // the quota check and create a space over the entitlement limit.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(account_id)
            .execute(&mut *transaction)
            .await?;
        let already_exists = sqlx::query(
            "SELECT 1 FROM spaces WHERE account_id = $1 AND space_id = $2 AND deleted_at IS NULL",
        )
        .bind(account_id)
        .bind(to_i64(space_id))
        .fetch_optional(&mut *transaction)
        .await?
        .is_some();
        if !already_exists {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM spaces WHERE account_id = $1 AND deleted_at IS NULL",
            )
            .bind(account_id)
            .fetch_one(&mut *transaction)
            .await?;
            if count >= i64::from(max_spaces) {
                return Err(PostgresStoreError::SpaceLimitReached);
            }
        }
        sqlx::query(
            "INSERT INTO spaces (account_id, space_id, name) VALUES ($1, $2, $3)\n             ON CONFLICT (account_id, space_id) DO NOTHING",
        )
        .bind(account_id)
        .bind(to_i64(space_id))
        .bind(name)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO crdt_documents (account_id, space_id, snapshot) VALUES ($1, $2, $3)\n             ON CONFLICT (account_id, space_id) DO NOTHING",
        )
        .bind(account_id)
        .bind(to_i64(space_id))
        .bind(snapshot)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn list_spaces(&self, account_id: &str) -> Result<Vec<Space>, PostgresStoreError> {
        let rows = sqlx::query(
            "SELECT s.space_id, s.name, s.metadata_version, s.archived,\n             EXTRACT(EPOCH FROM s.created_at)::BIGINT AS created_at,\n             EXTRACT(EPOCH FROM s.updated_at)::BIGINT AS updated_at,\n             EXTRACT(EPOCH FROM s.deleted_at)::BIGINT AS deleted_at, d.snapshot\n             FROM spaces s\n             JOIN crdt_documents d ON d.account_id = s.account_id AND d.space_id = s.space_id\n             WHERE s.account_id = $1\n             ORDER BY s.created_at, s.space_id",
        )
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let snapshot: Vec<u8> = row.try_get("snapshot")?;
                if snapshot.len() > MAX_SYNC_SNAPSHOT_BYTES {
                    return Err(PostgresStoreError::PayloadTooLarge);
                }
                let document = SpaceDoc::from_update(&snapshot)
                    .map_err(|error| PostgresStoreError::InvalidUpdate(error.to_string()))?;
                Ok(Space {
                    id: to_u64(row.try_get::<i64, _>("space_id")?),
                    name: row.try_get("name")?,
                    metadata_version: to_u64(row.try_get::<i64, _>("metadata_version")?),
                    archived: row.try_get("archived")?,
                    created_at: row
                        .try_get::<Option<i64>, _>("created_at")?
                        .map(to_u64)
                        .unwrap_or_default(),
                    updated_at: row
                        .try_get::<Option<i64>, _>("updated_at")?
                        .map(to_u64)
                        .unwrap_or_default(),
                    deleted_at: row.try_get::<Option<i64>, _>("deleted_at")?.map(to_u64),
                    board: document.board(),
                })
            })
            .collect()
    }

    pub async fn apply_metadata(
        &self,
        account_id: &str,
        request: &SyncMetadataRequest,
    ) -> Result<SyncMetadataResponse, PostgresStoreError> {
        validate_entity_id(request.space_id)?;
        if request.protocol_version != SYNC_RECONCILE_PROTOCOL_VERSION {
            return Err(PostgresStoreError::UnsupportedSyncReconcileProtocol(
                request.protocol_version,
            ));
        }
        if request.operation_id.trim().is_empty() || request.operation_id.len() > 128 {
            return Err(PostgresStoreError::InvalidInput(
                "metadata operation id must be between 1 and 128 characters".to_owned(),
            ));
        }
        if request
            .name
            .as_deref()
            .is_some_and(|name| name.trim().is_empty() || name.len() > 48)
        {
            return Err(PostgresStoreError::InvalidInput(
                "space name must be between 1 and 48 characters".to_owned(),
            ));
        }
        let mut transaction = self.pool.begin().await?;
        let metadata_request_hash = Sha256::digest(
            serde_json::to_vec(&(&request.operation, &request.name, &request.expected_version))
                .map_err(|error| PostgresStoreError::InvalidInput(error.to_string()))?,
        )
        .to_vec();
        let operation_mutation_id = format!("metadata:{}", request.operation_id);
        let row = sqlx::query(
            "SELECT name, archived, metadata_version,
             EXTRACT(EPOCH FROM deleted_at)::BIGINT AS deleted_at, last_operation_id, last_operation_hash
             FROM spaces WHERE account_id = $1 AND space_id = $2 FOR UPDATE",
        )
        .bind(account_id)
        .bind(to_i64(request.space_id))
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PostgresStoreError::SpaceAccessDenied)?;
        if let Some(previous) = sqlx::query(
            "SELECT request_hash, metadata
             FROM crdt_updates
             WHERE account_id = $1 AND space_id = $2 AND mutation_id = $3",
        )
        .bind(account_id)
        .bind(to_i64(request.space_id))
        .bind(&operation_mutation_id)
        .fetch_optional(&mut *transaction)
        .await?
        {
            let previous_hash: Option<Vec<u8>> = previous.try_get("request_hash")?;
            if previous_hash.as_deref() != Some(metadata_request_hash.as_slice()) {
                return Err(PostgresStoreError::MetadataOperationIdReused);
            }
            let metadata: Option<serde_json::Value> = previous.try_get("metadata")?;
            let metadata = metadata
                .and_then(|value| serde_json::from_value::<SyncMetadataEvent>(value).ok())
                .ok_or(PostgresStoreError::MetadataOperationIdReused)?;
            transaction.commit().await?;
            return Ok(SyncMetadataResponse {
                protocol_version: SYNC_RECONCILE_PROTOCOL_VERSION,
                space_id: request.space_id,
                metadata_version: metadata.metadata_version,
                name: metadata.name,
                archived: metadata.archived,
                deleted_at: metadata.deleted_at,
            });
        }
        let current_version: i64 = row.try_get("metadata_version")?;
        let current_operation: Option<String> = row.try_get("last_operation_id")?;
        let current_operation_hash: Option<Vec<u8>> = row.try_get("last_operation_hash")?;
        let currently_deleted: Option<i64> = row.try_get("deleted_at")?;
        if current_operation.as_deref() == Some(request.operation_id.as_str()) {
            if current_operation_hash
                .as_deref()
                .is_some_and(|hash| hash != metadata_request_hash.as_slice())
            {
                return Err(PostgresStoreError::MetadataOperationIdReused);
            }
            transaction.commit().await?;
            return metadata_response_from_row(request.space_id, row);
        }
        if request
            .expected_version
            .is_some_and(|version| version != to_u64(current_version))
        {
            return Err(PostgresStoreError::MetadataVersionConflict);
        }
        if currently_deleted.is_some()
            && !matches!(&request.operation, SpaceMetadataOperation::Restore)
        {
            return Err(PostgresStoreError::MetadataVersionConflict);
        }
        let (name, archived, delete, restore) = match request.operation {
            SpaceMetadataOperation::Rename => (
                Some(
                    request
                        .name
                        .as_deref()
                        .unwrap_or_default()
                        .trim()
                        .to_owned(),
                ),
                None,
                false,
                false,
            ),
            SpaceMetadataOperation::Archive => (None, Some(true), false, false),
            SpaceMetadataOperation::Unarchive => (None, Some(false), false, false),
            SpaceMetadataOperation::Delete => (None, None, true, false),
            SpaceMetadataOperation::Restore => (None, Some(false), false, true),
        };
        let row = sqlx::query(
            "UPDATE spaces SET name = COALESCE($3, name), archived = COALESCE($4, archived),
             deleted_at = CASE WHEN $5 THEN NOW() WHEN $6 THEN NULL ELSE deleted_at END,
             metadata_version = metadata_version + 1, last_operation_id = $7,
             last_operation_hash = $8, updated_at = NOW()
             WHERE account_id = $1 AND space_id = $2
             RETURNING name, archived, metadata_version,
             EXTRACT(EPOCH FROM deleted_at)::BIGINT AS deleted_at, last_operation_id",
        )
        .bind(account_id)
        .bind(to_i64(request.space_id))
        .bind(name)
        .bind(archived)
        .bind(delete)
        .bind(restore)
        .bind(&request.operation_id)
        .bind(&metadata_request_hash)
        .fetch_one(&mut *transaction)
        .await?;
        let response = metadata_response_from_row(request.space_id, row)?;
        let metadata = SyncMetadataEvent {
            metadata_version: response.metadata_version,
            name: response.name.clone(),
            archived: response.archived,
            deleted_at: response.deleted_at,
        };
        let metadata_json = serde_json::to_value(&metadata)
            .map_err(|error| PostgresStoreError::InvalidInput(error.to_string()))?;
        let event_id: i64 = sqlx::query(
            "INSERT INTO crdt_updates (account_id, space_id, mutation_id, update, request_hash, metadata, event_kind)
             VALUES ($1, $2, $3, $4, $5, $6, 'metadata') RETURNING event_id",
        )
        .bind(account_id)
        .bind(to_i64(request.space_id))
        .bind(&operation_mutation_id)
        .bind(Vec::<u8>::new())
        .bind(&metadata_request_hash)
        .bind(metadata_json.clone())
        .fetch_one(&mut *transaction)
        .await?
        .try_get("event_id")?;
        insert_sync_event(
            &mut transaction,
            event_id,
            account_id,
            request.space_id,
            "metadata",
            &[],
            Some(metadata_json),
        )
        .await?;
        transaction.commit().await?;
        let _ = self.events.send(DeliveryEvent {
            account_id: account_id.to_owned(),
            event: SyncEvent {
                protocol_version: SYNC_PROTOCOL_VERSION,
                event_id: to_u64(event_id),
                space_id: request.space_id,
                update: EncodedUpdate::from_bytes(&[]),
                metadata: Some(metadata),
            },
        });
        Ok(response)
    }

    pub async fn push(
        &self,
        account_id: &str,
        request: &SyncPushRequest,
    ) -> Result<Option<SyncEvent>, PostgresStoreError> {
        validate_entity_id(request.space_id)?;
        validate_sync_protocol(request.protocol_version)?;
        let update = request
            .update
            .to_bytes()
            .map_err(|error| PostgresStoreError::InvalidUpdate(error.to_string()))?;
        if request.mutation_id.trim().is_empty() || request.mutation_id.len() > 128 {
            return Err(PostgresStoreError::InvalidInput(
                "mutation id must be between 1 and 128 characters".to_owned(),
            ));
        }
        if update.len() > MAX_SYNC_UPDATE_BYTES {
            return Err(PostgresStoreError::PayloadTooLarge);
        }
        let mut transaction = self.pool.begin().await?;
        let space_id = to_i64(request.space_id);
        let space_exists = sqlx::query(
            "SELECT 1 FROM spaces WHERE account_id = $1 AND space_id = $2 AND deleted_at IS NULL",
        )
        .bind(account_id)
        .bind(space_id)
        .fetch_optional(&mut *transaction)
        .await?
        .is_some();
        if !space_exists {
            return Err(PostgresStoreError::SpaceAccessDenied);
        }

        let row = sqlx::query(
            "SELECT snapshot FROM crdt_documents WHERE account_id = $1 AND space_id = $2 FOR UPDATE",
        )
        .bind(account_id)
        .bind(space_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PostgresStoreError::SpaceAccessDenied)?;
        let snapshot: Vec<u8> = row.try_get("snapshot")?;
        if snapshot.len() > MAX_SYNC_SNAPSHOT_BYTES {
            return Err(PostgresStoreError::PayloadTooLarge);
        }
        if let Some(row) = sqlx::query(
            "SELECT update FROM crdt_updates WHERE account_id = $1 AND space_id = $2 AND mutation_id = $3",
        )
        .bind(account_id)
        .bind(space_id)
        .bind(&request.mutation_id)
        .fetch_optional(&mut *transaction)
        .await?
        {
            let previous: Vec<u8> = row.try_get("update")?;
            if previous == update {
                transaction.commit().await?;
                return Ok(None);
            }
            return Err(PostgresStoreError::MutationIdReused);
        }
        let document = SpaceDoc::from_update(&snapshot)
            .map_err(|error| PostgresStoreError::InvalidUpdate(error.to_string()))?;
        document
            .apply_update(&update)
            .map_err(|error| PostgresStoreError::InvalidUpdate(error.to_string()))?;
        let next_snapshot = document.snapshot();
        if next_snapshot.len() > MAX_SYNC_SNAPSHOT_BYTES {
            return Err(PostgresStoreError::PayloadTooLarge);
        }
        let event_id: i64 = sqlx::query(
            "INSERT INTO crdt_updates (account_id, space_id, mutation_id, update)\n             VALUES ($1, $2, $3, $4) RETURNING event_id",
        )
        .bind(account_id)
        .bind(space_id)
        .bind(&request.mutation_id)
        .bind(&update)
        .fetch_one(&mut *transaction)
        .await?
        .try_get("event_id")?;
        insert_sync_event(
            &mut transaction,
            event_id,
            account_id,
            request.space_id,
            "document",
            &update,
            None,
        )
        .await?;
        sqlx::query(
            "UPDATE crdt_documents SET snapshot = $3, snapshot_event_id = $4, updated_at = NOW()\n             WHERE account_id = $1 AND space_id = $2",
        )
        .bind(account_id)
        .bind(space_id)
        .bind(next_snapshot)
        .bind(event_id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query("UPDATE spaces SET updated_at = NOW() WHERE account_id = $1 AND space_id = $2")
            .bind(account_id)
            .bind(space_id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;

        let event = SyncEvent {
            protocol_version: SYNC_PROTOCOL_VERSION,
            event_id: to_u64(event_id),
            space_id: request.space_id,
            update: EncodedUpdate::from_bytes(&update),
            metadata: None,
        };
        let _ = self.events.send(DeliveryEvent {
            account_id: account_id.to_owned(),
            event: event.clone(),
        });
        Ok(Some(event))
    }

    pub async fn pull(
        &self,
        account_id: &str,
        request: &SyncPullRequest,
    ) -> Result<SyncPullResponse, PostgresStoreError> {
        validate_entity_id(request.space_id)?;
        validate_sync_protocol(request.protocol_version)?;
        let state_vector = request
            .state_vector
            .to_bytes()
            .map_err(|error| PostgresStoreError::InvalidStateVector(error.to_string()))?;
        if state_vector.len() > MAX_SYNC_STATE_VECTOR_BYTES {
            return Err(PostgresStoreError::PayloadTooLarge);
        }
        let row = sqlx::query(
            "SELECT d.snapshot FROM crdt_documents d\n             JOIN spaces s ON s.account_id = d.account_id AND s.space_id = d.space_id\n             WHERE d.account_id = $1 AND d.space_id = $2 AND s.deleted_at IS NULL",
        )
        .bind(account_id)
        .bind(to_i64(request.space_id))
        .fetch_optional(&self.pool)
        .await?
        .ok_or(PostgresStoreError::SpaceAccessDenied)?;
        let snapshot: Vec<u8> = row.try_get("snapshot")?;
        if snapshot.len() > MAX_SYNC_SNAPSHOT_BYTES {
            return Err(PostgresStoreError::PayloadTooLarge);
        }
        let document = SpaceDoc::from_update(&snapshot)
            .map_err(|error| PostgresStoreError::InvalidStateVector(error.to_string()))?;
        let update = document
            .encode_update(&state_vector)
            .map_err(|error| PostgresStoreError::InvalidStateVector(error.to_string()))?;
        if update.len() > MAX_SYNC_UPDATE_BYTES {
            return Err(PostgresStoreError::PayloadTooLarge);
        }
        Ok(SyncPullResponse {
            protocol_version: SYNC_PROTOCOL_VERSION,
            space_id: request.space_id,
            update: EncodedUpdate::from_bytes(&update),
            has_more: false,
        })
    }

    /// Merge a client's current delta and return the server delta relative to
    /// the state vector submitted with that exact request. The document row
    /// lock makes the merge, mutation claim, snapshot, and event atomic.
    pub async fn reconcile(
        &self,
        account_id: &str,
        request: &SyncReconcileRequest,
    ) -> Result<SyncReconcileResponse, PostgresStoreError> {
        validate_entity_id(request.space_id)?;
        validate_sync_reconcile_protocol(request.protocol_version)?;
        if request.mutation_id.trim().is_empty() || request.mutation_id.len() > 128 {
            return Err(PostgresStoreError::InvalidInput(
                "mutation id must be between 1 and 128 characters".to_owned(),
            ));
        }
        if request.device_id.trim().is_empty() || request.device_id.len() > 128 {
            return Err(PostgresStoreError::InvalidInput(
                "device id must be between 1 and 128 characters".to_owned(),
            ));
        }
        let state_vector = request
            .state_vector
            .to_bytes()
            .map_err(|error| PostgresStoreError::InvalidStateVector(error.to_string()))?;
        let update = request
            .update
            .to_bytes()
            .map_err(|error| PostgresStoreError::InvalidUpdate(error.to_string()))?;
        if state_vector.len() > MAX_SYNC_STATE_VECTOR_BYTES || update.len() > MAX_SYNC_UPDATE_BYTES
        {
            return Err(PostgresStoreError::PayloadTooLarge);
        }
        let mut request_bytes =
            Vec::with_capacity(request.device_id.len() + state_vector.len() + update.len() + 32);
        request_bytes.extend_from_slice(request.device_id.as_bytes());
        request_bytes.push(0);
        request_bytes.extend_from_slice(&request.local_generation.to_be_bytes());
        request_bytes.push(0);
        request_bytes.extend_from_slice(&state_vector);
        request_bytes.push(0);
        request_bytes.extend_from_slice(&update);
        let request_hash = Sha256::digest(request_bytes).to_vec();
        let mut transaction = self.pool.begin().await?;
        let space_id = to_i64(request.space_id);
        let row = sqlx::query(
            "SELECT d.snapshot FROM crdt_documents d
             JOIN spaces s ON s.account_id = d.account_id AND s.space_id = d.space_id
             WHERE d.account_id = $1 AND d.space_id = $2 AND s.deleted_at IS NULL
             FOR UPDATE",
        )
        .bind(account_id)
        .bind(space_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(PostgresStoreError::SpaceAccessDenied)?;
        let snapshot: Vec<u8> = row.try_get("snapshot")?;
        if snapshot.len() > MAX_SYNC_SNAPSHOT_BYTES {
            return Err(PostgresStoreError::PayloadTooLarge);
        }

        let duplicate = if let Some(row) = sqlx::query(
            "SELECT update, request_hash FROM crdt_updates WHERE account_id = $1 AND space_id = $2 AND mutation_id = $3",
        )
        .bind(account_id)
        .bind(space_id)
        .bind(&request.mutation_id)
        .fetch_optional(&mut *transaction)
        .await?
        {
            let previous: Vec<u8> = row.try_get("update")?;
            let previous_hash: Option<Vec<u8>> = row.try_get("request_hash")?;
            if previous_hash.as_deref().is_some_and(|hash| hash != request_hash.as_slice())
                || (previous_hash.is_none() && previous != update)
            {
                return Err(PostgresStoreError::MutationIdReused);
            }
            true
        } else {
            false
        };

        let document = SpaceDoc::from_update(&snapshot)
            .map_err(|error| PostgresStoreError::InvalidUpdate(error.to_string()))?;
        if !duplicate && !update.is_empty() {
            document
                .apply_update(&update)
                .map_err(|error| PostgresStoreError::InvalidUpdate(error.to_string()))?;
        }
        let server_delta = document
            .encode_update(&state_vector)
            .map_err(|error| PostgresStoreError::InvalidStateVector(error.to_string()))?;
        if server_delta.len() > MAX_SYNC_UPDATE_BYTES {
            return Err(PostgresStoreError::PayloadTooLarge);
        }
        let server_state_vector = document.state_vector();
        // Claim every non-duplicate mutation, including a no-op update. This
        // prevents a later request from reusing the same id with a different
        // payload while keeping no-op claims out of the event stream.
        let mutation_event_id = if !duplicate {
            Some(
                sqlx::query(
                "INSERT INTO crdt_updates (account_id, space_id, mutation_id, update, request_hash)
                 VALUES ($1, $2, $3, $4, $5) RETURNING event_id",
                )
                .bind(account_id)
                .bind(space_id)
                .bind(&request.mutation_id)
                .bind(&update)
                .bind(&request_hash)
                .fetch_one(&mut *transaction)
                .await?
                .try_get("event_id")?,
            )
        } else {
            None
        };
        let event_id = if duplicate || update.is_empty() {
            None
        } else {
            let next_snapshot = document.snapshot();
            if next_snapshot.len() > MAX_SYNC_SNAPSHOT_BYTES {
                return Err(PostgresStoreError::PayloadTooLarge);
            }
            let event_id = mutation_event_id.expect("new non-empty mutations have an event id");
            insert_sync_event(
                &mut transaction,
                event_id,
                account_id,
                request.space_id,
                "document",
                &update,
                None,
            )
            .await?;
            sqlx::query(
                "UPDATE crdt_documents SET snapshot = $3, snapshot_event_id = $4, updated_at = NOW()
                 WHERE account_id = $1 AND space_id = $2",
            )
            .bind(account_id)
            .bind(space_id)
            .bind(next_snapshot)
            .bind(event_id)
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "UPDATE spaces SET updated_at = NOW() WHERE account_id = $1 AND space_id = $2",
            )
            .bind(account_id)
            .bind(space_id)
            .execute(&mut *transaction)
            .await?;
            Some(event_id)
        };
        transaction.commit().await?;

        if let Some(event_id) = event_id {
            let event = SyncEvent {
                protocol_version: SYNC_PROTOCOL_VERSION,
                event_id: to_u64(event_id),
                space_id: request.space_id,
                update: EncodedUpdate::from_bytes(&update),
                metadata: None,
            };
            let _ = self.events.send(DeliveryEvent {
                account_id: account_id.to_owned(),
                event,
            });
        }
        Ok(SyncReconcileResponse {
            protocol_version: SYNC_RECONCILE_PROTOCOL_VERSION,
            space_id: request.space_id,
            accepted: event_id.is_some() || duplicate,
            event_id: event_id.map(to_u64),
            update: EncodedUpdate::from_bytes(&server_delta),
            state_vector: EncodedUpdate::from_bytes(&server_state_vector),
            entitlement_version: 0,
            event_cursor: event_id.map(to_u64).unwrap_or_default(),
        })
    }

    /// Replay committed document events for an account after an SSE cursor.
    /// The update log is durable; the process-local broadcast is only a
    /// low-latency hint for clients already connected to this instance.
    pub(crate) fn subscribe(&self) -> broadcast::Receiver<DeliveryEvent> {
        self.events.subscribe()
    }

    /// Prune replay history only after snapshots are durable. Reconciliation
    /// continues to work from `crdt_documents.snapshot`; clients whose SSE
    /// cursor predates the retained event range receive an explicit reset.
    pub async fn prune_history(&self, retention_seconds: u64) -> Result<u64, PostgresStoreError> {
        let retention_seconds = i64::try_from(retention_seconds).unwrap_or(i64::MAX);
        let mut transaction = self.pool.begin().await?;
        let deleted_updates = sqlx::query(
            "DELETE FROM crdt_updates u
             WHERE u.created_at < NOW() - ($1::BIGINT * INTERVAL '1 second')
               AND NOT EXISTS (
                   SELECT 1 FROM crdt_documents d
                   WHERE d.account_id = u.account_id
                     AND d.space_id = u.space_id
                     AND d.snapshot_event_id = u.event_id
               )",
        )
        .bind(retention_seconds)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        sqlx::query(
            "DELETE FROM sync_events
             WHERE created_at < NOW() - ($1::BIGINT * INTERVAL '1 second')",
        )
        .bind(retention_seconds)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(deleted_updates)
    }

    pub async fn events_since(
        &self,
        account_id: &str,
        after_event_id: u64,
    ) -> Result<Vec<SyncEvent>, PostgresStoreError> {
        if after_event_id > 0 {
            let (oldest, newest): (Option<i64>, Option<i64>) = sqlx::query_as(
                "SELECT MIN(event_id), MAX(event_id)
                 FROM sync_events WHERE account_id = $1",
            )
            .bind(account_id)
            .fetch_one(&self.pool)
            .await?;
            // No retained rows means the account's previous cursor cannot be
            // proven complete (all history may have been pruned). Force the
            // explicit reset path instead of silently switching to live-only
            // delivery and losing future convergence hints.
            if oldest.is_none() {
                return Err(PostgresStoreError::EventCursorRequiresReset);
            }
            if oldest.is_some_and(|event_id| to_i64(after_event_id) < event_id.saturating_sub(1)) {
                return Err(PostgresStoreError::EventCursorRequiresReset);
            }
            if newest.is_some_and(|event_id| to_i64(after_event_id) > event_id) {
                // A cursor from a newer server generation is not replayable;
                // a reset lets the client recover without discarding local
                // generations.
                return Err(PostgresStoreError::EventCursorRequiresReset);
            }
        }
        let rows = sqlx::query(
            "SELECT event_id, space_id, update, metadata FROM sync_events
             WHERE account_id = $1 AND event_id > $2
            ORDER BY event_id ASC LIMIT 1001",
        )
        .bind(account_id)
        .bind(to_i64(after_event_id))
        .fetch_all(&self.pool)
        .await?;
        if rows.len() > 1000 {
            return Err(PostgresStoreError::EventCursorRequiresReset);
        }
        rows.into_iter()
            .map(|row| {
                let event_id: i64 = row.try_get("event_id")?;
                let space_id: i64 = row.try_get("space_id")?;
                let update: Vec<u8> = row.try_get("update")?;
                let metadata = row
                    .try_get::<Option<serde_json::Value>, _>("metadata")?
                    .and_then(|value| serde_json::from_value::<SyncMetadataEvent>(value).ok());
                Ok(SyncEvent {
                    protocol_version: SYNC_PROTOCOL_VERSION,
                    event_id: to_u64(event_id),
                    space_id: to_u64(space_id),
                    update: EncodedUpdate::from_bytes(&update),
                    metadata,
                })
            })
            .collect()
    }
}

#[derive(Clone)]
pub struct PostgresBillingStore {
    pool: PgPool,
}

impl PostgresBillingStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn entitlement(&self, account_id: &str) -> Result<Entitlement, PostgresStoreError> {
        let row = sqlx::query(
            "SELECT account_id, plan, status, sync_enabled, max_spaces, provider, provider_customer_id,\n             provider_subscription_id, current_period_end, cancel_at_period_end, access_mode, access_until, retention_until, access_reason, version, last_event_at, last_event_id,\n             EXTRACT(EPOCH FROM updated_at)::BIGINT AS updated_at\n             FROM billing_entitlements WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(entitlement_from_row)
            .transpose()
            .map(|entitlement| entitlement.unwrap_or_else(|| Entitlement::free(account_id)))
    }

    /// Resolve a webhook's provider resource to the server-owned account. This
    /// is used for refund/dispute payloads that legitimately omit checkout
    /// metadata; it never trusts an account id from the webhook body.
    pub async fn account_for_provider_resource(
        &self,
        provider: &str,
        provider_subscription_id: Option<&str>,
        provider_customer_id: Option<&str>,
        provider_payment_id: Option<&str>,
    ) -> Result<Option<String>, PostgresStoreError> {
        if provider.trim().is_empty() {
            return Err(PostgresStoreError::InvalidInput(
                "billing provider is required".to_owned(),
            ));
        }
        let rows = sqlx::query(
            "SELECT DISTINCT account_id
             FROM (
                 SELECT account_id
                 FROM billing_entitlements
                 WHERE provider = $1
                   AND (($2::TEXT IS NOT NULL AND provider_subscription_id = $2)
                        OR ($3::TEXT IS NOT NULL AND provider_customer_id = $3))
                 UNION ALL
                 SELECT account_id
                 FROM billing_events
                 WHERE provider = $1
                   AND $4::TEXT IS NOT NULL
                   AND provider_payment_id = $4
             ) candidates",
        )
        .bind(provider)
        .bind(provider_subscription_id)
        .bind(provider_customer_id)
        .bind(provider_payment_id)
        .fetch_all(&self.pool)
        .await?;
        let accounts = rows
            .into_iter()
            .try_fold(BTreeSet::new(), |mut accounts, row| {
                accounts.insert(row.try_get::<String, _>("account_id")?);
                Ok::<_, sqlx::Error>(accounts)
            })?;
        if accounts.len() > 1 {
            return Err(PostgresStoreError::AmbiguousProviderResource);
        }
        Ok(accounts.into_iter().next())
    }

    pub async fn reconciliation_candidates(
        &self,
        limit: i64,
        interval_seconds: u64,
    ) -> Result<Vec<(String, String)>, PostgresStoreError> {
        let interval_seconds = i64::try_from(interval_seconds).unwrap_or(i64::MAX);
        Ok(sqlx::query(
            "SELECT account_id, provider_subscription_id
             FROM billing_entitlements
             WHERE provider = 'dodo'
               AND provider_subscription_id IS NOT NULL
               AND (last_reconciled_at IS NULL
                    OR last_reconciled_at < NOW() - ($1::BIGINT * INTERVAL '1 second'))
             ORDER BY last_reconciled_at NULLS FIRST
             LIMIT $2",
        )
        .bind(interval_seconds)
        .bind(limit.max(1))
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| {
            Ok((
                row.try_get("account_id")?,
                row.try_get("provider_subscription_id")?,
            ))
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?)
    }

    pub async fn mark_reconciliation(
        &self,
        account_id: &str,
        error: Option<&str>,
    ) -> Result<(), PostgresStoreError> {
        sqlx::query(
            "UPDATE billing_entitlements
             SET last_reconciled_at = NOW(), reconciliation_error = $2
             WHERE account_id = $1",
        )
        .bind(account_id)
        .bind(error.map(|value| value.chars().take(512).collect::<String>()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn prune_billing_history(
        &self,
        retention_seconds: u64,
    ) -> Result<u64, PostgresStoreError> {
        let retention_seconds = i64::try_from(retention_seconds).unwrap_or(i64::MAX);
        let mut transaction = self.pool.begin().await?;
        let deleted_events = sqlx::query(
            "DELETE FROM billing_events
             WHERE received_at < NOW() - ($1::BIGINT * INTERVAL '1 second')",
        )
        .bind(retention_seconds)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        sqlx::query(
            "DELETE FROM billing_webhook_inbox
             WHERE state = 'processed'
               AND received_at < NOW() - ($1::BIGINT * INTERVAL '1 second')",
        )
        .bind(retention_seconds)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(deleted_events)
    }

    /// Return inbox rows left in `processing` by a crashed worker to the
    /// retryable state. Provider delivery remains at-least-once, so this is
    /// safe: the billing event table still owns the idempotency claim.
    pub async fn recover_stale_webhook_attempts(
        &self,
        stale_after_seconds: u64,
    ) -> Result<u64, PostgresStoreError> {
        let stale_after_seconds = i64::try_from(stale_after_seconds).unwrap_or(i64::MAX);
        let result = sqlx::query(
            "UPDATE billing_webhook_inbox
             SET state = 'pending', updated_at = NOW()
             WHERE state = 'processing'
               AND updated_at < NOW() - ($1::BIGINT * INTERVAL '1 second')",
        )
        .bind(stale_after_seconds)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Reserve a stable provider idempotency key for one account/interval
    /// request window. The reservation is durable, so duplicate clicks and
    /// concurrent tabs reuse the same provider request after a process restart.
    pub async fn reserve_checkout(
        &self,
        account_id: &str,
        interval: &str,
    ) -> Result<(String, Option<String>), PostgresStoreError> {
        if account_id.trim().is_empty() || !matches!(interval, "month" | "year") {
            return Err(PostgresStoreError::InvalidInput(
                "checkout account and interval are required".to_owned(),
            ));
        }
        let bucket = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_secs() / 300)
            .unwrap_or_default() as i64;
        let idempotency_key = format!(
            "task-space-checkout:{}:{}:{}",
            account_id,
            interval,
            Uuid::new_v4()
        );
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "DELETE FROM billing_checkout_idempotency
             WHERE updated_at < NOW() - INTERVAL '2 days'",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO billing_checkout_idempotency
             (account_id, interval, bucket, idempotency_key)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (account_id, interval, bucket) DO NOTHING",
        )
        .bind(account_id)
        .bind(interval)
        .bind(bucket)
        .bind(&idempotency_key)
        .execute(&mut *transaction)
        .await?;
        let row = sqlx::query(
            "SELECT idempotency_key, checkout_url
             FROM billing_checkout_idempotency
             WHERE account_id = $1 AND interval = $2 AND bucket = $3
             FOR UPDATE",
        )
        .bind(account_id)
        .bind(interval)
        .bind(bucket)
        .fetch_one(&mut *transaction)
        .await?;
        let key: String = row.try_get("idempotency_key")?;
        let url: Option<String> = row.try_get("checkout_url")?;
        transaction.commit().await?;
        Ok((key, url))
    }

    pub async fn complete_checkout(
        &self,
        idempotency_key: &str,
        checkout_url: &str,
    ) -> Result<(), PostgresStoreError> {
        if idempotency_key.trim().is_empty() || checkout_url.trim().is_empty() {
            return Err(PostgresStoreError::InvalidInput(
                "checkout idempotency key and URL are required".to_owned(),
            ));
        }
        sqlx::query(
            "UPDATE billing_checkout_idempotency
             SET checkout_url = $2, updated_at = NOW()
             WHERE idempotency_key = $1",
        )
        .bind(idempotency_key)
        .bind(checkout_url)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn apply_event(
        &self,
        event: &BillingEvent,
    ) -> Result<BillingApplyResult, PostgresStoreError> {
        validate_billing_event(event)?;
        let payload = serde_json::to_vec(event)
            .map_err(|error| PostgresStoreError::InvalidInput(error.to_string()))?;
        let payload_hash = Sha256::digest(payload).to_vec();
        let mut transaction = self.pool.begin().await?;
        // Serialize deliveries for the same provider event before checking
        // the idempotency table. Without this short advisory lock, two
        // concurrent webhook deliveries can both observe a missing row and
        // one would fail with a uniqueness error instead of returning a
        // normal duplicate result.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1 || ':' || $2))")
            .bind(&event.provider)
            .bind(&event.provider_event_id)
            .execute(&mut *transaction)
            .await?;
        // Different provider event ids for the same account must also be
        // serialized. A FOR UPDATE on the entitlement row is insufficient
        // when the account has no row yet: two first-ever events could both
        // observe the implicit free entitlement and race their upserts,
        // allowing an older event to overwrite a newer transition.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('billing-account:' || $1))")
            .bind(&event.account_id)
            .execute(&mut *transaction)
            .await?;
        if let Some(row) = sqlx::query(
            "SELECT payload_hash FROM billing_events WHERE provider = $1 AND provider_event_id = $2",
        )
        .bind(&event.provider)
        .bind(&event.provider_event_id)
        .fetch_optional(&mut *transaction)
        .await?
        {
            let previous: Vec<u8> = row.try_get("payload_hash")?;
            if previous != payload_hash {
                return Err(PostgresStoreError::ProviderEventIdReused);
            }
            let current = entitlement_in_transaction(&mut transaction, &event.account_id).await?;
            transaction.commit().await?;
            return Ok(BillingApplyResult::Duplicate(current));
        }

        let current = entitlement_in_transaction(&mut transaction, &event.account_id).await?;
        let stale = event.occurred_at < current.last_event_at
            || (event.occurred_at == current.last_event_at
                && !current.last_event_id.is_empty()
                && event.provider_event_id <= current.last_event_id);
        if !stale {
            let next = entitlement_for_event(&current, event);
            upsert_entitlement(&mut transaction, &next).await?;
            insert_billing_event(&mut transaction, event, &payload_hash).await?;
            transaction.commit().await?;
            return Ok(BillingApplyResult::Applied(next));
        }

        insert_billing_event(&mut transaction, event, &payload_hash).await?;
        transaction.commit().await?;
        Ok(BillingApplyResult::Stale(current))
    }

    /// Persist the exact verified webhook before applying its normalized
    /// billing transition. A duplicate with different bytes is rejected.
    pub async fn record_webhook_inbox(
        &self,
        provider: &str,
        webhook_id: &str,
        event_type: &str,
        payload: &[u8],
    ) -> Result<(), PostgresStoreError> {
        if provider.trim().is_empty()
            || webhook_id.trim().is_empty()
            || event_type.trim().is_empty()
        {
            return Err(PostgresStoreError::InvalidInput(
                "webhook provider, id, and type are required".to_owned(),
            ));
        }
        if provider.len() > MAX_BILLING_FIELD_LEN
            || webhook_id.len() > MAX_BILLING_FIELD_LEN
            || event_type.len() > MAX_BILLING_FIELD_LEN
        {
            return Err(PostgresStoreError::InvalidInput(
                "webhook identifiers are too long".to_owned(),
            ));
        }
        if payload.len() > 512 * 1024 {
            return Err(PostgresStoreError::PayloadTooLarge);
        }
        let payload_hash = Sha256::digest(payload).to_vec();
        let mut transaction = self.pool.begin().await?;
        // Make the check-and-claim operation deterministic when a provider
        // retries the same webhook concurrently on multiple connections.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1 || ':' || $2))")
            .bind(provider)
            .bind(webhook_id)
            .execute(&mut *transaction)
            .await?;
        if let Some(row) = sqlx::query(
            "SELECT payload_hash FROM billing_webhook_inbox WHERE provider = $1 AND webhook_id = $2 FOR UPDATE",
        )
        .bind(provider)
        .bind(webhook_id)
        .fetch_optional(&mut *transaction)
        .await?
        {
            let previous: Vec<u8> = row.try_get("payload_hash")?;
            if previous != payload_hash {
                return Err(PostgresStoreError::ProviderEventIdReused);
            }
            sqlx::query(
                "UPDATE billing_webhook_inbox
                 SET state = 'processing', attempt_count = attempt_count + 1,
                     last_error = NULL, updated_at = NOW()
                 WHERE provider = $1 AND webhook_id = $2",
            )
            .bind(provider)
            .bind(webhook_id)
            .execute(&mut *transaction)
            .await?;
        } else {
            sqlx::query(
                "INSERT INTO billing_webhook_inbox
                 (provider, webhook_id, event_type, payload, payload_hash, state, attempt_count, updated_at)
                 VALUES ($1, $2, $3, $4, $5, 'processing', 1, NOW())",
            )
            .bind(provider)
            .bind(webhook_id)
            .bind(event_type)
            .bind(payload)
            .bind(&payload_hash)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn mark_webhook_processed(
        &self,
        provider: &str,
        webhook_id: &str,
    ) -> Result<(), PostgresStoreError> {
        sqlx::query(
            "UPDATE billing_webhook_inbox
             SET state = 'processed', processed_at = NOW(), last_error = NULL, updated_at = NOW()
             WHERE provider = $1 AND webhook_id = $2",
        )
        .bind(provider)
        .bind(webhook_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_webhook_failed(
        &self,
        provider: &str,
        webhook_id: &str,
        error: &str,
    ) -> Result<(), PostgresStoreError> {
        sqlx::query(
            "UPDATE billing_webhook_inbox
             SET state = 'pending', processed_at = NULL, last_error = $3, updated_at = NOW()
             WHERE provider = $1 AND webhook_id = $2 AND state <> 'processed'",
        )
        .bind(provider)
        .bind(webhook_id)
        .bind(error.chars().take(512).collect::<String>())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Mark a verified event for provider-resource reconciliation when the
    /// webhook itself does not contain enough data to resolve an account.
    /// Keeping the raw inbox row avoids retry storms while preserving an
    /// auditable work item for a reconciliation worker.
    pub async fn mark_webhook_needs_reconciliation(
        &self,
        provider: &str,
        webhook_id: &str,
    ) -> Result<(), PostgresStoreError> {
        sqlx::query(
            "UPDATE billing_webhook_inbox
             SET state = 'needs_reconciliation', processed_at = NULL, updated_at = NOW()
             WHERE provider = $1 AND webhook_id = $2 AND state <> 'processed'",
        )
        .bind(provider)
        .bind(webhook_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

fn validate_sync_protocol(version: u32) -> Result<(), PostgresStoreError> {
    if version == SYNC_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(PostgresStoreError::UnsupportedSyncProtocol(version))
    }
}

async fn insert_sync_event(
    transaction: &mut Transaction<'_, Postgres>,
    event_id: i64,
    account_id: &str,
    space_id: EntityId,
    event_kind: &str,
    update: &[u8],
    metadata: Option<serde_json::Value>,
) -> Result<(), PostgresStoreError> {
    sqlx::query(
        "INSERT INTO sync_events (event_id, account_id, space_id, event_kind, update, metadata)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .bind(account_id)
    .bind(to_i64(space_id))
    .bind(event_kind)
    .bind(update)
    .bind(metadata)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn validate_entity_id(id: EntityId) -> Result<(), PostgresStoreError> {
    if id == 0 || id > MAX_SAFE_ENTITY_ID {
        return Err(PostgresStoreError::InvalidInput(
            "entity id must be a non-zero JavaScript-safe integer".to_owned(),
        ));
    }
    Ok(())
}

fn metadata_response_from_row(
    space_id: EntityId,
    row: sqlx::postgres::PgRow,
) -> Result<SyncMetadataResponse, PostgresStoreError> {
    Ok(SyncMetadataResponse {
        protocol_version: SYNC_RECONCILE_PROTOCOL_VERSION,
        space_id,
        metadata_version: to_u64(row.try_get::<i64, _>("metadata_version")?),
        name: row.try_get("name")?,
        archived: row.try_get("archived")?,
        deleted_at: row.try_get::<Option<i64>, _>("deleted_at")?.map(to_u64),
    })
}

fn validate_sync_reconcile_protocol(version: u32) -> Result<(), PostgresStoreError> {
    if version == SYNC_RECONCILE_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(PostgresStoreError::UnsupportedSyncReconcileProtocol(
            version,
        ))
    }
}

fn validate_billing_event(event: &BillingEvent) -> Result<(), PostgresStoreError> {
    if event.protocol_version != BILLING_PROTOCOL_VERSION {
        return Err(PostgresStoreError::UnsupportedBillingProtocol(
            event.protocol_version,
        ));
    }
    if event.provider.trim().is_empty() {
        return Err(PostgresStoreError::MissingBillingField("provider"));
    }
    if event.provider.len() > MAX_BILLING_FIELD_LEN {
        return Err(PostgresStoreError::InvalidInput(
            "billing provider is too long".to_owned(),
        ));
    }
    if event.provider_event_id.trim().is_empty() {
        return Err(PostgresStoreError::MissingBillingField("provider_event_id"));
    }
    if event.provider_event_id.len() > MAX_BILLING_FIELD_LEN {
        return Err(PostgresStoreError::InvalidInput(
            "billing provider event id is too long".to_owned(),
        ));
    }
    if event.account_id.trim().is_empty() {
        return Err(PostgresStoreError::MissingBillingField("account_id"));
    }
    if event.account_id.len() > MAX_BILLING_FIELD_LEN
        || event
            .provider_customer_id
            .as_deref()
            .is_some_and(|value| value.len() > MAX_BILLING_FIELD_LEN)
        || event
            .provider_subscription_id
            .as_deref()
            .is_some_and(|value| value.len() > MAX_BILLING_FIELD_LEN)
        || event
            .provider_payment_id
            .as_deref()
            .is_some_and(|value| value.len() > MAX_BILLING_FIELD_LEN)
    {
        return Err(PostgresStoreError::InvalidInput(
            "billing identifier is too long".to_owned(),
        ));
    }
    Ok(())
}

async fn entitlement_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    account_id: &str,
) -> Result<Entitlement, PostgresStoreError> {
    let row = sqlx::query(
        "SELECT account_id, plan, status, sync_enabled, max_spaces, provider, provider_customer_id,\n         provider_subscription_id, current_period_end, cancel_at_period_end, access_mode, access_until, retention_until, access_reason, version, last_event_at, last_event_id,\n         EXTRACT(EPOCH FROM updated_at)::BIGINT AS updated_at\n         FROM billing_entitlements WHERE account_id = $1 FOR UPDATE",
    )
    .bind(account_id)
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(entitlement_from_row)
        .transpose()
        .map(|entitlement| entitlement.unwrap_or_else(|| Entitlement::free(account_id)))
}

fn entitlement_from_row(row: sqlx::postgres::PgRow) -> Result<Entitlement, PostgresStoreError> {
    Ok(Entitlement {
        account_id: row.try_get("account_id")?,
        plan: parse_plan(row.try_get::<String, _>("plan")?.as_str())?,
        status: parse_status(row.try_get::<String, _>("status")?.as_str())?,
        sync_enabled: row.try_get("sync_enabled")?,
        max_spaces: row.try_get::<i32, _>("max_spaces")?.max(0) as u32,
        provider: row.try_get("provider")?,
        provider_customer_id: row.try_get("provider_customer_id")?,
        provider_subscription_id: row.try_get("provider_subscription_id")?,
        current_period_end: row
            .try_get::<Option<i64>, _>("current_period_end")?
            .map(to_u64),
        cancel_at_period_end: row.try_get("cancel_at_period_end")?,
        access_mode: parse_access_mode(row.try_get::<String, _>("access_mode")?.as_str())?,
        access_until: row.try_get::<Option<i64>, _>("access_until")?.map(to_u64),
        retention_until: row
            .try_get::<Option<i64>, _>("retention_until")?
            .map(to_u64),
        access_reason: row.try_get("access_reason")?,
        version: to_u64(row.try_get::<i64, _>("version")?),
        last_event_at: to_u64(row.try_get::<i64, _>("last_event_at")?),
        last_event_id: row.try_get("last_event_id")?,
        updated_at: row
            .try_get::<Option<i64>, _>("updated_at")?
            .map(to_u64)
            .unwrap_or_default(),
    })
}

fn entitlement_for_event(current: &Entitlement, event: &BillingEvent) -> Entitlement {
    let partial_refund = matches!(event.event_type, BillingEventType::RefundSucceeded)
        && event
            .refund_amount
            .zip(event.payment_amount)
            .is_some_and(|(refund, payment)| refund < payment);
    let sync_enabled = if partial_refund {
        current.sync_enabled
    } else {
        matches!(event.plan, SubscriptionPlan::Pro)
            && matches!(
                event.status,
                SubscriptionStatus::Active | SubscriptionStatus::PastDue
            )
    };
    let access_mode = if partial_refund {
        current.access_mode.clone()
    } else {
        access_mode_for_event(event)
    };
    let previous_period_end = current.current_period_end;
    let next_period_end = event.current_period_end.or(previous_period_end);
    let access_until = if partial_refund {
        current.access_until
    } else {
        event
            .current_period_end
            .or(current.access_until)
            .or(previous_period_end)
            // A payment failure can omit the provider's period end. Keep the
            // documented grace policy anchored to the event timestamp so a
            // sparse payload does not revoke access immediately.
            .or_else(|| {
                matches!(event.status, SubscriptionStatus::PastDue).then_some(event.occurred_at)
            })
    };
    let retention_until = if matches!(
        &access_mode,
        SyncAccessMode::PausedExpired | SyncAccessMode::PausedDispute
    ) {
        next_period_end
            .or(Some(event.occurred_at))
            .map(|period_end| period_end.saturating_add(90 * 24 * 60 * 60))
    } else {
        current.retention_until
    };
    Entitlement {
        account_id: event.account_id.clone(),
        plan: if partial_refund {
            current.plan.clone()
        } else {
            event.plan.clone()
        },
        status: if partial_refund {
            current.status.clone()
        } else {
            event.status.clone()
        },
        sync_enabled,
        max_spaces: if partial_refund {
            current.max_spaces
        } else if matches!(event.plan, SubscriptionPlan::Pro)
            && !matches!(event.status, SubscriptionStatus::Pending)
        {
            PRO_SPACE_LIMIT
        } else {
            FREE_SPACE_LIMIT
        },
        provider: Some(event.provider.clone()),
        provider_customer_id: event
            .provider_customer_id
            .clone()
            .or_else(|| current.provider_customer_id.clone()),
        provider_subscription_id: event
            .provider_subscription_id
            .clone()
            .or_else(|| current.provider_subscription_id.clone()),
        current_period_end: if partial_refund {
            current.current_period_end
        } else {
            next_period_end
        },
        cancel_at_period_end: if partial_refund {
            current.cancel_at_period_end
        } else {
            event.cancel_at_period_end
        },
        access_mode,
        access_until,
        retention_until,
        access_reason: Some(if partial_refund {
            "partial_refund_preserved".to_owned()
        } else {
            event_type_name(&event.event_type).to_owned()
        }),
        version: current.version.saturating_add(1),
        last_event_at: event.occurred_at,
        last_event_id: event.provider_event_id.clone(),
        updated_at: event.occurred_at,
    }
}

fn access_mode_for_event(event: &BillingEvent) -> SyncAccessMode {
    match &event.event_type {
        BillingEventType::SubscriptionPending => SyncAccessMode::PausedNotEntitled,
        BillingEventType::SubscriptionStarted
        | BillingEventType::SubscriptionRenewed
        | BillingEventType::SubscriptionChanged
        | BillingEventType::DisputeWon
        | BillingEventType::RefundFailed => {
            if matches!(event.status, SubscriptionStatus::PastDue) {
                SyncAccessMode::GraceReadWrite
            } else if matches!(event.status, SubscriptionStatus::Active) {
                SyncAccessMode::ReadWrite
            } else {
                SyncAccessMode::PausedExpired
            }
        }
        BillingEventType::SubscriptionPastDue => SyncAccessMode::GraceReadWrite,
        BillingEventType::PaymentFailed => SyncAccessMode::GraceReadWrite,
        BillingEventType::DisputeOpened => SyncAccessMode::PausedDispute,
        BillingEventType::RefundSucceeded
        | BillingEventType::DisputeLost
        | BillingEventType::SubscriptionEnded => SyncAccessMode::PausedExpired,
        BillingEventType::SubscriptionCanceled => {
            if matches!(event.status, SubscriptionStatus::Active) && event.cancel_at_period_end {
                SyncAccessMode::ReadWrite
            } else {
                SyncAccessMode::PausedExpired
            }
        }
    }
}

async fn upsert_entitlement(
    transaction: &mut Transaction<'_, Postgres>,
    entitlement: &Entitlement,
) -> Result<(), PostgresStoreError> {
    sqlx::query(
        "INSERT INTO billing_entitlements (account_id, plan, status, sync_enabled, max_spaces, provider,\n         provider_customer_id, provider_subscription_id, current_period_end, cancel_at_period_end, access_mode, access_until, retention_until, access_reason, version, last_event_at, last_event_id, updated_at)\n         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, to_timestamp($18))\n         ON CONFLICT (account_id) DO UPDATE SET plan = EXCLUDED.plan, status = EXCLUDED.status,\n         sync_enabled = EXCLUDED.sync_enabled, max_spaces = EXCLUDED.max_spaces, provider = EXCLUDED.provider,\n         provider_customer_id = EXCLUDED.provider_customer_id, provider_subscription_id = EXCLUDED.provider_subscription_id,\n         current_period_end = EXCLUDED.current_period_end, cancel_at_period_end = EXCLUDED.cancel_at_period_end,\n         access_mode = EXCLUDED.access_mode, access_until = EXCLUDED.access_until, retention_until = EXCLUDED.retention_until, access_reason = EXCLUDED.access_reason,\n         version = EXCLUDED.version, last_event_at = EXCLUDED.last_event_at, last_event_id = EXCLUDED.last_event_id, updated_at = EXCLUDED.updated_at",
    )
    .bind(&entitlement.account_id)
    .bind(plan_name(&entitlement.plan))
    .bind(status_name(&entitlement.status))
    .bind(entitlement.sync_enabled)
    .bind(entitlement.max_spaces as i32)
    .bind(&entitlement.provider)
    .bind(&entitlement.provider_customer_id)
    .bind(&entitlement.provider_subscription_id)
    .bind(entitlement.current_period_end.map(to_i64))
    .bind(entitlement.cancel_at_period_end)
    .bind(access_mode_name(&entitlement.access_mode))
    .bind(entitlement.access_until.map(to_i64))
    .bind(entitlement.retention_until.map(to_i64))
    .bind(&entitlement.access_reason)
    .bind(to_i64(entitlement.version))
    .bind(to_i64(entitlement.last_event_at))
    .bind(&entitlement.last_event_id)
    .bind(to_i64(entitlement.updated_at))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_billing_event(
    transaction: &mut Transaction<'_, Postgres>,
    event: &BillingEvent,
    payload_hash: &[u8],
) -> Result<(), PostgresStoreError> {
    sqlx::query(
        "INSERT INTO billing_events (provider, provider_event_id, account_id, event_type, occurred_at, provider_payment_id, payload_hash)\n         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(&event.provider)
    .bind(&event.provider_event_id)
    .bind(&event.account_id)
    .bind(event_type_name(&event.event_type))
    .bind(to_i64(event.occurred_at))
    .bind(&event.provider_payment_id)
    .bind(payload_hash)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn plan_name(plan: &SubscriptionPlan) -> &'static str {
    match plan {
        SubscriptionPlan::Free => "free",
        SubscriptionPlan::Pro => "pro",
    }
}

fn parse_plan(value: &str) -> Result<SubscriptionPlan, PostgresStoreError> {
    match value {
        "free" => Ok(SubscriptionPlan::Free),
        "pro" => Ok(SubscriptionPlan::Pro),
        _ => Err(PostgresStoreError::MissingBillingField("plan")),
    }
}

fn status_name(status: &SubscriptionStatus) -> &'static str {
    match status {
        SubscriptionStatus::Free => "free",
        SubscriptionStatus::Pending => "pending",
        SubscriptionStatus::Active => "active",
        SubscriptionStatus::PastDue => "past_due",
        SubscriptionStatus::Canceled => "canceled",
        SubscriptionStatus::Ended => "ended",
    }
}

fn parse_status(value: &str) -> Result<SubscriptionStatus, PostgresStoreError> {
    match value {
        "free" => Ok(SubscriptionStatus::Free),
        "pending" => Ok(SubscriptionStatus::Pending),
        "active" => Ok(SubscriptionStatus::Active),
        "past_due" => Ok(SubscriptionStatus::PastDue),
        "canceled" => Ok(SubscriptionStatus::Canceled),
        "ended" => Ok(SubscriptionStatus::Ended),
        _ => Err(PostgresStoreError::MissingBillingField("status")),
    }
}

fn access_mode_name(mode: &SyncAccessMode) -> &'static str {
    match mode {
        SyncAccessMode::ReadWrite => "read_write",
        SyncAccessMode::GraceReadWrite => "grace_read_write",
        SyncAccessMode::PausedNotEntitled => "paused_not_entitled",
        SyncAccessMode::PausedPayment => "paused_payment",
        SyncAccessMode::PausedDispute => "paused_dispute",
        SyncAccessMode::PausedExpired => "paused_expired",
    }
}

fn parse_access_mode(value: &str) -> Result<SyncAccessMode, PostgresStoreError> {
    match value {
        "read_write" => Ok(SyncAccessMode::ReadWrite),
        "grace_read_write" => Ok(SyncAccessMode::GraceReadWrite),
        "paused_not_entitled" => Ok(SyncAccessMode::PausedNotEntitled),
        "paused_payment" => Ok(SyncAccessMode::PausedPayment),
        "paused_dispute" => Ok(SyncAccessMode::PausedDispute),
        "paused_expired" => Ok(SyncAccessMode::PausedExpired),
        _ => Err(PostgresStoreError::MissingBillingField("access_mode")),
    }
}

fn event_type_name(event_type: &BillingEventType) -> &'static str {
    match event_type {
        BillingEventType::SubscriptionPending => "subscription_pending",
        BillingEventType::SubscriptionStarted => "subscription_started",
        BillingEventType::SubscriptionRenewed => "subscription_renewed",
        BillingEventType::SubscriptionChanged => "subscription_changed",
        BillingEventType::SubscriptionPastDue => "subscription_past_due",
        BillingEventType::SubscriptionCanceled => "subscription_canceled",
        BillingEventType::SubscriptionEnded => "subscription_ended",
        BillingEventType::PaymentFailed => "payment_failed",
        BillingEventType::RefundSucceeded => "refund_succeeded",
        BillingEventType::RefundFailed => "refund_failed",
        BillingEventType::DisputeOpened => "dispute_opened",
        BillingEventType::DisputeWon => "dispute_won",
        BillingEventType::DisputeLost => "dispute_lost",
    }
}

fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or_default()
}
