//! PostgreSQL-backed repositories for synchronized spaces and billing.
//!
//! The database stores Yrs snapshots and opaque updates. The application only
//! projects a document to a board in the browser; it never stores provider
//! payloads or trusts browser-supplied account ids.

use std::sync::Arc;

use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction};
use task_core::billing::{
    BILLING_PROTOCOL_VERSION, BillingEvent, BillingEventType, Entitlement, FREE_SPACE_LIMIT,
    PRO_SPACE_LIMIT, SubscriptionPlan, SubscriptionStatus,
};
use task_core::crdt::SpaceDoc;
use task_core::sync::{
    EncodedUpdate, SYNC_PROTOCOL_VERSION, SyncEvent, SyncPullRequest, SyncPullResponse,
    SyncPushRequest,
};
use task_core::{EntityId, Space};
use thiserror::Error;
use tokio::sync::broadcast;

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
    #[error("account is not authorized for this space")]
    SpaceAccessDenied,
}

#[derive(Clone)]
pub struct PostgresSyncStore {
    pool: PgPool,
    events: Arc<broadcast::Sender<DeliveryEvent>>,
}

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
    ) -> Result<(), PostgresStoreError> {
        let snapshot = SpaceDoc::new().snapshot();
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO spaces (account_id, space_id, name) VALUES ($1, $2, $3)\n             ON CONFLICT (account_id, space_id) DO UPDATE SET name = EXCLUDED.name, updated_at = NOW(), deleted_at = NULL",
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
            "SELECT s.space_id, s.name, s.archived,\n             EXTRACT(EPOCH FROM s.created_at)::BIGINT AS created_at,\n             EXTRACT(EPOCH FROM s.updated_at)::BIGINT AS updated_at,\n             EXTRACT(EPOCH FROM s.deleted_at)::BIGINT AS deleted_at, d.snapshot\n             FROM spaces s\n             JOIN crdt_documents d ON d.account_id = s.account_id AND d.space_id = s.space_id\n             WHERE s.account_id = $1 AND s.deleted_at IS NULL\n             ORDER BY s.created_at, s.space_id",
        )
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let snapshot: Vec<u8> = row.try_get("snapshot")?;
                let document = SpaceDoc::from_update(&snapshot)
                    .map_err(|error| PostgresStoreError::InvalidUpdate(error.to_string()))?;
                Ok(Space {
                    id: to_u64(row.try_get::<i64, _>("space_id")?),
                    name: row.try_get("name")?,
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

    pub async fn push(
        &self,
        account_id: &str,
        request: &SyncPushRequest,
    ) -> Result<Option<SyncEvent>, PostgresStoreError> {
        validate_sync_protocol(request.protocol_version)?;
        let update = request
            .update
            .to_bytes()
            .map_err(|error| PostgresStoreError::InvalidUpdate(error.to_string()))?;
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
        validate_sync_protocol(request.protocol_version)?;
        let state_vector = request
            .state_vector
            .to_bytes()
            .map_err(|error| PostgresStoreError::InvalidStateVector(error.to_string()))?;
        let row = sqlx::query(
            "SELECT d.snapshot FROM crdt_documents d\n             JOIN spaces s ON s.account_id = d.account_id AND s.space_id = d.space_id\n             WHERE d.account_id = $1 AND d.space_id = $2 AND s.deleted_at IS NULL",
        )
        .bind(account_id)
        .bind(to_i64(request.space_id))
        .fetch_optional(&self.pool)
        .await?
        .ok_or(PostgresStoreError::SpaceAccessDenied)?;
        let snapshot: Vec<u8> = row.try_get("snapshot")?;
        let document = SpaceDoc::from_update(&snapshot)
            .map_err(|error| PostgresStoreError::InvalidStateVector(error.to_string()))?;
        let update = document
            .encode_update(&state_vector)
            .map_err(|error| PostgresStoreError::InvalidStateVector(error.to_string()))?;
        Ok(SyncPullResponse {
            protocol_version: SYNC_PROTOCOL_VERSION,
            space_id: request.space_id,
            update: EncodedUpdate::from_bytes(&update),
            has_more: false,
        })
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<DeliveryEvent> {
        self.events.subscribe()
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
            "SELECT account_id, plan, status, sync_enabled, max_spaces, provider, provider_customer_id,\n             provider_subscription_id, current_period_end, cancel_at_period_end, version, last_event_at,\n             EXTRACT(EPOCH FROM updated_at)::BIGINT AS updated_at\n             FROM billing_entitlements WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(entitlement_from_row)
            .transpose()
            .map(|entitlement| entitlement.unwrap_or_else(|| Entitlement::free(account_id)))
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
        let stale = event.occurred_at < current.last_event_at;
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
}

fn validate_sync_protocol(version: u32) -> Result<(), PostgresStoreError> {
    if version == SYNC_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(PostgresStoreError::UnsupportedSyncProtocol(version))
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
    if event.provider_event_id.trim().is_empty() {
        return Err(PostgresStoreError::MissingBillingField("provider_event_id"));
    }
    if event.account_id.trim().is_empty() {
        return Err(PostgresStoreError::MissingBillingField("account_id"));
    }
    Ok(())
}

async fn entitlement_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    account_id: &str,
) -> Result<Entitlement, PostgresStoreError> {
    let row = sqlx::query(
        "SELECT account_id, plan, status, sync_enabled, max_spaces, provider, provider_customer_id,\n         provider_subscription_id, current_period_end, cancel_at_period_end, version, last_event_at,\n         EXTRACT(EPOCH FROM updated_at)::BIGINT AS updated_at\n         FROM billing_entitlements WHERE account_id = $1 FOR UPDATE",
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
        version: to_u64(row.try_get::<i64, _>("version")?),
        last_event_at: to_u64(row.try_get::<i64, _>("last_event_at")?),
        updated_at: row
            .try_get::<Option<i64>, _>("updated_at")?
            .map(to_u64)
            .unwrap_or_default(),
    })
}

fn entitlement_for_event(current: &Entitlement, event: &BillingEvent) -> Entitlement {
    let sync_enabled = matches!(event.plan, SubscriptionPlan::Pro)
        && matches!(event.status, SubscriptionStatus::Active);
    Entitlement {
        account_id: event.account_id.clone(),
        plan: event.plan.clone(),
        status: event.status.clone(),
        sync_enabled,
        max_spaces: if sync_enabled {
            PRO_SPACE_LIMIT
        } else {
            FREE_SPACE_LIMIT
        },
        provider: Some(event.provider.clone()),
        provider_customer_id: event.provider_customer_id.clone(),
        provider_subscription_id: event.provider_subscription_id.clone(),
        current_period_end: event.current_period_end,
        cancel_at_period_end: event.cancel_at_period_end,
        version: current.version.saturating_add(1),
        last_event_at: event.occurred_at,
        updated_at: event.occurred_at,
    }
}

async fn upsert_entitlement(
    transaction: &mut Transaction<'_, Postgres>,
    entitlement: &Entitlement,
) -> Result<(), PostgresStoreError> {
    sqlx::query(
        "INSERT INTO billing_entitlements (account_id, plan, status, sync_enabled, max_spaces, provider,\n         provider_customer_id, provider_subscription_id, current_period_end, cancel_at_period_end, version, last_event_at, updated_at)\n         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, to_timestamp($13))\n         ON CONFLICT (account_id) DO UPDATE SET plan = EXCLUDED.plan, status = EXCLUDED.status,\n         sync_enabled = EXCLUDED.sync_enabled, max_spaces = EXCLUDED.max_spaces, provider = EXCLUDED.provider,\n         provider_customer_id = EXCLUDED.provider_customer_id, provider_subscription_id = EXCLUDED.provider_subscription_id,\n         current_period_end = EXCLUDED.current_period_end, cancel_at_period_end = EXCLUDED.cancel_at_period_end,\n         version = EXCLUDED.version, last_event_at = EXCLUDED.last_event_at, updated_at = EXCLUDED.updated_at",
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
    .bind(to_i64(entitlement.version))
    .bind(to_i64(entitlement.last_event_at))
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
        "INSERT INTO billing_events (provider, provider_event_id, account_id, event_type, occurred_at, payload_hash)\n         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(&event.provider)
    .bind(&event.provider_event_id)
    .bind(&event.account_id)
    .bind(event_type_name(&event.event_type))
    .bind(to_i64(event.occurred_at))
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
        SubscriptionStatus::Active => "active",
        SubscriptionStatus::PastDue => "past_due",
        SubscriptionStatus::Canceled => "canceled",
        SubscriptionStatus::Ended => "ended",
    }
}

fn parse_status(value: &str) -> Result<SubscriptionStatus, PostgresStoreError> {
    match value {
        "free" => Ok(SubscriptionStatus::Free),
        "active" => Ok(SubscriptionStatus::Active),
        "past_due" => Ok(SubscriptionStatus::PastDue),
        "canceled" => Ok(SubscriptionStatus::Canceled),
        "ended" => Ok(SubscriptionStatus::Ended),
        _ => Err(PostgresStoreError::MissingBillingField("status")),
    }
}

fn event_type_name(event_type: &BillingEventType) -> &'static str {
    match event_type {
        BillingEventType::SubscriptionStarted => "subscription_started",
        BillingEventType::SubscriptionRenewed => "subscription_renewed",
        BillingEventType::SubscriptionChanged => "subscription_changed",
        BillingEventType::SubscriptionPastDue => "subscription_past_due",
        BillingEventType::SubscriptionCanceled => "subscription_canceled",
        BillingEventType::SubscriptionEnded => "subscription_ended",
        BillingEventType::PaymentFailed => "payment_failed",
    }
}

fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or_default()
}
