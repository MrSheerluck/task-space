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
    SubscriptionPlan, SubscriptionStatus, SyncAccessMode,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BillingStoreError {
    #[error("unsupported billing protocol version {0}")]
    UnsupportedProtocol(u32),
    #[error("billing event is missing {0}")]
    MissingField(&'static str),
    #[error("billing event has an invalid {0}")]
    InvalidField(&'static str),
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

const MAX_BILLING_FIELD_LEN: usize = 256;

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
        let stale = event.occurred_at < current.last_event_at
            || (event.occurred_at == current.last_event_at
                && !current.last_event_id.is_empty()
                && event.provider_event_id <= current.last_event_id);
        if stale {
            let entitlement = current.clone();
            inner.processed_events.insert(event_key, event.clone());
            return Ok(BillingApplyResult::Stale(entitlement));
        }

        let partial_refund = matches!(
            event.event_type,
            task_core::billing::BillingEventType::RefundSucceeded
        ) && event
            .refund_amount
            .zip(event.payment_amount)
            .is_some_and(|(refund, payment)| refund < payment);
        current.plan = if partial_refund {
            current.plan.clone()
        } else {
            event.plan.clone()
        };
        current.status = if partial_refund {
            current.status.clone()
        } else {
            event.status.clone()
        };
        current.sync_enabled = if partial_refund {
            current.sync_enabled
        } else {
            matches!(event.plan, SubscriptionPlan::Pro)
                && matches!(
                    event.status,
                    SubscriptionStatus::Active | SubscriptionStatus::PastDue
                )
        };
        current.max_spaces = if partial_refund {
            current.max_spaces
        } else if matches!(&current.plan, SubscriptionPlan::Pro)
            && !matches!(event.status, SubscriptionStatus::Pending)
        {
            PRO_SPACE_LIMIT
        } else {
            FREE_SPACE_LIMIT
        };
        current.provider = Some(event.provider.clone());
        if event.provider_customer_id.is_some() {
            current.provider_customer_id = event.provider_customer_id.clone();
        }
        if event.provider_subscription_id.is_some() {
            current.provider_subscription_id = event.provider_subscription_id.clone();
        }
        let previous_period_end = current.current_period_end;
        let next_period_end = event.current_period_end.or(previous_period_end);
        current.current_period_end = if partial_refund {
            current.current_period_end
        } else {
            next_period_end
        };
        current.cancel_at_period_end = if partial_refund {
            current.cancel_at_period_end
        } else {
            event.cancel_at_period_end
        };
        current.access_mode = if partial_refund {
            current.access_mode.clone()
        } else {
            access_mode_for_event(event)
        };
        current.access_until = if partial_refund {
            current.access_until
        } else {
            event
                .current_period_end
                .or(current.access_until)
                .or(previous_period_end)
                // Providers may omit the period end on a payment failure.
                // Anchor the bounded grace window to the verified event time
                // rather than accidentally pausing access immediately.
                .or_else(|| {
                    matches!(event.status, SubscriptionStatus::PastDue).then_some(event.occurred_at)
                })
        };
        current.retention_until = if matches!(
            &current.access_mode,
            SyncAccessMode::PausedExpired | SyncAccessMode::PausedDispute
        ) {
            next_period_end
                .or(Some(event.occurred_at))
                .map(|period_end| period_end.saturating_add(90 * 24 * 60 * 60))
        } else {
            current.retention_until
        };
        current.access_reason = Some(if partial_refund {
            "partial_refund_preserved".to_owned()
        } else {
            format!("{:?}", event.event_type)
        });
        current.version = current.version.saturating_add(1);
        current.last_event_at = event.occurred_at;
        current.last_event_id = event.provider_event_id.clone();
        current.updated_at = event.occurred_at;
        let entitlement = current.clone();
        inner.processed_events.insert(event_key, event.clone());

        Ok(BillingApplyResult::Applied(entitlement))
    }
}

fn access_mode_for_event(event: &BillingEvent) -> SyncAccessMode {
    match &event.event_type {
        task_core::billing::BillingEventType::SubscriptionPending => {
            SyncAccessMode::PausedNotEntitled
        }
        task_core::billing::BillingEventType::SubscriptionStarted
        | task_core::billing::BillingEventType::SubscriptionRenewed
        | task_core::billing::BillingEventType::SubscriptionChanged
        | task_core::billing::BillingEventType::DisputeWon
        | task_core::billing::BillingEventType::RefundFailed => {
            if matches!(event.status, SubscriptionStatus::PastDue) {
                SyncAccessMode::GraceReadWrite
            } else if matches!(event.status, SubscriptionStatus::Active) {
                SyncAccessMode::ReadWrite
            } else {
                SyncAccessMode::PausedExpired
            }
        }
        task_core::billing::BillingEventType::SubscriptionPastDue => SyncAccessMode::GraceReadWrite,
        task_core::billing::BillingEventType::PaymentFailed => SyncAccessMode::GraceReadWrite,
        task_core::billing::BillingEventType::DisputeOpened => SyncAccessMode::PausedDispute,
        task_core::billing::BillingEventType::RefundSucceeded
        | task_core::billing::BillingEventType::DisputeLost
        | task_core::billing::BillingEventType::SubscriptionEnded => SyncAccessMode::PausedExpired,
        task_core::billing::BillingEventType::SubscriptionCanceled => {
            if matches!(event.status, SubscriptionStatus::Active) && event.cancel_at_period_end {
                SyncAccessMode::ReadWrite
            } else {
                SyncAccessMode::PausedExpired
            }
        }
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
    if event.provider.len() > MAX_BILLING_FIELD_LEN {
        return Err(BillingStoreError::InvalidField("provider"));
    }
    if event.provider_event_id.trim().is_empty() {
        return Err(BillingStoreError::MissingField("provider_event_id"));
    }
    if event.provider_event_id.len() > MAX_BILLING_FIELD_LEN {
        return Err(BillingStoreError::InvalidField("provider_event_id"));
    }
    if event.account_id.trim().is_empty() {
        return Err(BillingStoreError::MissingField("account_id"));
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
        return Err(BillingStoreError::InvalidField("billing identifier"));
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
            provider_payment_id: None,
            refund_amount: None,
            payment_amount: None,
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
    fn pending_subscription_does_not_grant_sync_or_pro_quota() {
        let store = BillingStore::new();
        let mut pending = event("evt-pending", 100, SubscriptionStatus::Pending);
        pending.event_type = BillingEventType::SubscriptionPending;
        let BillingApplyResult::Applied(entitlement) = store
            .apply_event(&pending)
            .expect("pending event should apply")
        else {
            panic!("expected an applied entitlement");
        };
        assert!(!entitlement.can_sync());
        assert_eq!(entitlement.max_spaces, FREE_SPACE_LIMIT);
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

    #[test]
    fn partial_refund_preserves_existing_sync_access() {
        let store = BillingStore::new();
        let active = event("evt-active", 100, SubscriptionStatus::Active);
        let _ = store
            .apply_event(&active)
            .expect("active event should apply");
        let mut refund = event("evt-refund", 101, SubscriptionStatus::Ended);
        refund.event_type = BillingEventType::RefundSucceeded;
        refund.refund_amount = Some(500);
        refund.payment_amount = Some(1_000);

        let BillingApplyResult::Applied(entitlement) = store
            .apply_event(&refund)
            .expect("partial refund should apply")
        else {
            panic!("expected an applied entitlement");
        };
        assert!(entitlement.can_sync());
        assert_eq!(
            entitlement.access_reason.as_deref(),
            Some("partial_refund_preserved")
        );
    }

    #[test]
    fn past_due_access_expires_after_the_server_grace_window() {
        let store = BillingStore::new();
        let entitlement = match store
            .apply_event(&event("evt-past-due", 100, SubscriptionStatus::PastDue))
            .expect("event should apply")
        {
            BillingApplyResult::Applied(entitlement) => entitlement,
            other => panic!("expected applied entitlement, got {other:?}"),
        };
        assert!(!entitlement.can_sync());
        assert!(entitlement.can_sync_at(2_000 + task_core::billing::PAYMENT_GRACE_SECONDS));
        assert!(!entitlement.can_sync_at(2_001 + task_core::billing::PAYMENT_GRACE_SECONDS));
    }

    #[test]
    fn payment_failure_without_period_metadata_keeps_the_last_grace_deadline() {
        let store = BillingStore::new();
        store
            .apply_event(&event("evt-active", 100, SubscriptionStatus::Active))
            .expect("active event should apply");
        let mut failed = event("evt-failed", 101, SubscriptionStatus::PastDue);
        failed.event_type = BillingEventType::PaymentFailed;
        failed.current_period_end = None;

        let BillingApplyResult::Applied(entitlement) = store
            .apply_event(&failed)
            .expect("payment failure should apply")
        else {
            panic!("expected an applied entitlement");
        };
        assert_eq!(entitlement.current_period_end, Some(2_000));
        assert!(entitlement.can_sync_at(2_000 + task_core::billing::PAYMENT_GRACE_SECONDS));
    }

    #[test]
    fn payment_failure_without_any_period_metadata_starts_grace_at_event_time() {
        let store = BillingStore::new();
        let mut failed = event("evt-failed-no-period", 101, SubscriptionStatus::PastDue);
        failed.event_type = BillingEventType::PaymentFailed;
        failed.current_period_end = None;

        let BillingApplyResult::Applied(entitlement) = store
            .apply_event(&failed)
            .expect("payment failure should apply")
        else {
            panic!("expected an applied entitlement");
        };
        assert_eq!(entitlement.access_until, Some(101));
        assert!(entitlement.can_sync_at(101 + task_core::billing::PAYMENT_GRACE_SECONDS));
        assert!(!entitlement.can_sync_at(102 + task_core::billing::PAYMENT_GRACE_SECONDS));
    }

    #[test]
    fn sparse_provider_events_do_not_erase_resource_identity() {
        let store = BillingStore::new();
        store
            .apply_event(&event("evt-active", 100, SubscriptionStatus::Active))
            .expect("active event should apply");
        let mut failed = event("evt-failed", 101, SubscriptionStatus::PastDue);
        failed.event_type = BillingEventType::PaymentFailed;
        failed.provider_customer_id = None;
        failed.provider_subscription_id = None;

        let BillingApplyResult::Applied(entitlement) = store
            .apply_event(&failed)
            .expect("sparse payment failure should apply")
        else {
            panic!("expected an applied entitlement");
        };
        assert_eq!(
            entitlement.provider_customer_id.as_deref(),
            Some("customer-1")
        );
        assert_eq!(
            entitlement.provider_subscription_id.as_deref(),
            Some("subscription-1")
        );
    }

    #[test]
    fn equal_timestamp_events_use_provider_id_as_a_stable_tie_breaker() {
        let mut later_id = event("evt-z", 100, SubscriptionStatus::Active);
        let mut earlier_id = event("evt-a", 100, SubscriptionStatus::Ended);
        later_id.event_type = BillingEventType::SubscriptionChanged;
        earlier_id.event_type = BillingEventType::SubscriptionChanged;

        let first = BillingStore::new();
        first
            .apply_event(&earlier_id)
            .expect("first event should apply");
        first.apply_event(&later_id).expect("later id should apply");

        let second = BillingStore::new();
        second
            .apply_event(&later_id)
            .expect("first event should apply");
        let result = second
            .apply_event(&earlier_id)
            .expect("lower tie-break id should be stale");
        assert!(matches!(result, BillingApplyResult::Stale(_)));

        assert_eq!(
            first.entitlement("account-1").unwrap().last_event_id,
            "evt-z"
        );
        assert_eq!(
            second.entitlement("account-1").unwrap().last_event_id,
            "evt-z"
        );
        assert!(first.entitlement("account-1").unwrap().can_sync());
        assert!(second.entitlement("account-1").unwrap().can_sync());
    }

    #[test]
    fn oversized_provider_identifiers_are_rejected_before_state_change() {
        let store = BillingStore::new();
        let mut oversized = event("evt-oversized", 100, SubscriptionStatus::Active);
        oversized.provider_event_id = "x".repeat(MAX_BILLING_FIELD_LEN + 1);
        assert!(matches!(
            store.apply_event(&oversized),
            Err(BillingStoreError::InvalidField("provider_event_id"))
        ));
        assert_eq!(
            store.entitlement("account-1").unwrap(),
            Entitlement::free("account-1")
        );
    }
}
