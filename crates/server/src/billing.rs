//! Server-owned billing entitlements and webhook idempotency.
//!
//! A provider adapter is responsible for verifying a webhook signature and
//! mapping the provider customer to an internal account. This module only
//! applies the normalized event, so access checks stay independent of Dodo,
//! Stripe, or another provider.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use task_core::billing::{
    BILLING_PROTOCOL_VERSION, BillingEvent, Entitlement, FREE_SPACE_LIMIT, PRO_SPACE_LIMIT,
    SubscriptionPlan, SubscriptionStatus,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BillingStoreError {
    #[error("unsupported billing protocol version {0}")]
    UnsupportedProtocol(u32),
    #[error("billing event is missing {0}")]
    MissingField(&'static str),
    #[error("provider event id was reused with a different event")]
    ProviderEventIdReused,
    #[error("billing store lock was poisoned")]
    LockPoisoned,
}

#[derive(Clone, Debug, PartialEq)]
pub enum BillingApplyResult {
    Applied(Entitlement),
    Duplicate(Entitlement),
    Stale(Entitlement),
}

struct BillingInner {
    entitlements: HashMap<String, Entitlement>,
    processed_events: HashMap<(String, String), BillingEvent>,
}

#[derive(Clone)]
pub struct BillingStore {
    inner: Arc<Mutex<BillingInner>>,
}

impl Default for BillingStore {
    fn default() -> Self {
        Self::new()
    }
}

impl BillingStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(BillingInner {
                entitlements: HashMap::new(),
                processed_events: HashMap::new(),
            })),
        }
    }

    pub fn entitlement(&self, account_id: &str) -> Result<Entitlement, BillingStoreError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| BillingStoreError::LockPoisoned)?;
        Ok(inner
            .entitlements
            .get(account_id)
            .cloned()
            .unwrap_or_else(|| Entitlement::free(account_id)))
    }

    /// Apply a verified, normalized webhook exactly once. Older events are
    /// recorded for idempotency but cannot roll an account back to stale state.
    pub fn apply_event(
        &self,
        event: &BillingEvent,
    ) -> Result<BillingApplyResult, BillingStoreError> {
        validate_event(event)?;
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| BillingStoreError::LockPoisoned)?;
        let event_key = (event.provider.clone(), event.provider_event_id.clone());

        if let Some(previous) = inner.processed_events.get(&event_key) {
            if previous == event {
                let entitlement = inner
                    .entitlements
                    .get(&event.account_id)
                    .cloned()
                    .unwrap_or_else(|| Entitlement::free(&event.account_id));
                return Ok(BillingApplyResult::Duplicate(entitlement));
            }
            return Err(BillingStoreError::ProviderEventIdReused);
        }

        let current = inner
            .entitlements
            .entry(event.account_id.clone())
            .or_insert_with(|| Entitlement::free(&event.account_id));
        if event.occurred_at < current.last_event_at {
            let entitlement = current.clone();
            inner.processed_events.insert(event_key, event.clone());
            return Ok(BillingApplyResult::Stale(entitlement));
        }

        current.plan = event.plan.clone();
        current.status = event.status.clone();
        current.sync_enabled = matches!(event.plan, SubscriptionPlan::Pro)
            && matches!(event.status, SubscriptionStatus::Active);
        current.max_spaces = if current.sync_enabled {
            PRO_SPACE_LIMIT
        } else {
            FREE_SPACE_LIMIT
        };
        current.provider = Some(event.provider.clone());
        current.provider_customer_id = event.provider_customer_id.clone();
        current.provider_subscription_id = event.provider_subscription_id.clone();
        current.current_period_end = event.current_period_end;
        current.cancel_at_period_end = event.cancel_at_period_end;
        current.version = current.version.saturating_add(1);
        current.last_event_at = event.occurred_at;
        current.updated_at = event.occurred_at;
        let entitlement = current.clone();
        inner.processed_events.insert(event_key, event.clone());

        Ok(BillingApplyResult::Applied(entitlement))
    }
}

fn validate_event(event: &BillingEvent) -> Result<(), BillingStoreError> {
    if event.protocol_version != BILLING_PROTOCOL_VERSION {
        return Err(BillingStoreError::UnsupportedProtocol(
            event.protocol_version,
        ));
    }
    if event.provider.trim().is_empty() {
        return Err(BillingStoreError::MissingField("provider"));
    }
    if event.provider_event_id.trim().is_empty() {
        return Err(BillingStoreError::MissingField("provider_event_id"));
    }
    if event.account_id.trim().is_empty() {
        return Err(BillingStoreError::MissingField("account_id"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use task_core::billing::{BILLING_PROTOCOL_VERSION, BillingEventType};

    fn event(id: &str, occurred_at: u64, status: SubscriptionStatus) -> BillingEvent {
        BillingEvent {
            protocol_version: BILLING_PROTOCOL_VERSION,
            provider: "provider".to_owned(),
            provider_event_id: id.to_owned(),
            event_type: BillingEventType::SubscriptionStarted,
            account_id: "account-1".to_owned(),
            plan: SubscriptionPlan::Pro,
            status,
            provider_customer_id: Some("customer-1".to_owned()),
            provider_subscription_id: Some("subscription-1".to_owned()),
            current_period_end: Some(2_000),
            cancel_at_period_end: false,
            occurred_at,
        }
    }

    #[test]
    fn verified_event_creates_server_owned_sync_entitlement() {
        let store = BillingStore::new();
        let result = store
            .apply_event(&event("evt-1", 100, SubscriptionStatus::Active))
            .expect("event should apply");

        let BillingApplyResult::Applied(entitlement) = result else {
            panic!("expected an applied entitlement");
        };
        assert!(entitlement.can_sync());
        assert_eq!(entitlement.version, 1);
        assert_eq!(store.entitlement("account-1").unwrap(), entitlement);
    }

    #[test]
    fn duplicate_event_is_safe_and_stale_event_cannot_revoke_access() {
        let store = BillingStore::new();
        let active = event("evt-active", 200, SubscriptionStatus::Active);
        let duplicate = store.apply_event(&active).expect("event should apply");
        assert!(matches!(duplicate, BillingApplyResult::Applied(_)));
        assert!(matches!(
            store
                .apply_event(&active)
                .expect("duplicate should be safe"),
            BillingApplyResult::Duplicate(_)
        ));

        let stale = event("evt-stale", 100, SubscriptionStatus::Ended);
        let result = store
            .apply_event(&stale)
            .expect("stale event should be recorded");
        let BillingApplyResult::Stale(entitlement) = result else {
            panic!("expected stale event result");
        };
        assert!(entitlement.can_sync());
    }

    #[test]
    fn reusing_provider_event_id_with_changed_data_is_rejected() {
        let store = BillingStore::new();
        store
            .apply_event(&event("evt-1", 100, SubscriptionStatus::Active))
            .expect("event should apply");

        let mut changed = event("evt-1", 101, SubscriptionStatus::Ended);
        changed.cancel_at_period_end = true;
        assert!(matches!(
            store.apply_event(&changed),
            Err(BillingStoreError::ProviderEventIdReused)
        ));
    }
}
