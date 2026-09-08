//! Provider-neutral billing contracts.
//!
//! Payment providers should be translated into these contracts at the server
//! boundary. The browser consumes an entitlement, never provider webhook
//! payloads or client-controlled plan flags.

use serde::{Deserialize, Serialize};

pub const BILLING_PROTOCOL_VERSION: u32 = 1;
pub const FREE_SPACE_LIMIT: u32 = 3;
pub const PRO_SPACE_LIMIT: u32 = 100;
pub const PAYMENT_GRACE_SECONDS: u64 = 7 * 24 * 60 * 60;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionPlan {
    Free,
    Pro,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionStatus {
    Free,
    Pending,
    Active,
    PastDue,
    Canceled,
    Ended,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncAccessMode {
    ReadWrite,
    GraceReadWrite,
    #[default]
    PausedNotEntitled,
    PausedPayment,
    PausedDispute,
    PausedExpired,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Entitlement {
    pub account_id: String,
    pub plan: SubscriptionPlan,
    pub status: SubscriptionStatus,
    pub sync_enabled: bool,
    pub max_spaces: u32,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub provider_customer_id: Option<String>,
    #[serde(default)]
    pub provider_subscription_id: Option<String>,
    #[serde(default)]
    pub current_period_end: Option<u64>,
    #[serde(default)]
    pub cancel_at_period_end: bool,
    #[serde(default)]
    pub access_mode: SyncAccessMode,
    #[serde(default)]
    pub access_until: Option<u64>,
    #[serde(default)]
    pub retention_until: Option<u64>,
    #[serde(default)]
    pub access_reason: Option<String>,
    #[serde(default)]
    pub version: u64,
    #[serde(default)]
    pub last_event_at: u64,
    /// Provider event id used as a deterministic tie-break when two events
    /// have the same provider timestamp and arrive out of order.
    #[serde(default)]
    pub last_event_id: String,
    #[serde(default)]
    pub updated_at: u64,
}

impl Entitlement {
    pub fn free(account_id: impl Into<String>) -> Self {
        Self {
            account_id: account_id.into(),
            plan: SubscriptionPlan::Free,
            status: SubscriptionStatus::Free,
            sync_enabled: false,
            max_spaces: FREE_SPACE_LIMIT,
            provider: None,
            provider_customer_id: None,
            provider_subscription_id: None,
            current_period_end: None,
            cancel_at_period_end: false,
            access_mode: SyncAccessMode::PausedNotEntitled,
            access_until: None,
            retention_until: None,
            access_reason: None,
            version: 0,
            last_event_at: 0,
            last_event_id: String::new(),
            updated_at: 0,
        }
    }

    pub fn can_sync(&self) -> bool {
        self.sync_enabled
            && matches!(self.plan, SubscriptionPlan::Pro)
            && (matches!(self.access_mode, SyncAccessMode::ReadWrite)
                || (matches!(self.access_mode, SyncAccessMode::PausedNotEntitled)
                    && matches!(self.status, SubscriptionStatus::Active)))
    }

    /// Past-due subscriptions remain usable for a bounded recovery window.
    /// The server supplies its clock so browser clocks cannot extend access.
    pub fn can_sync_at(&self, now: u64) -> bool {
        let known_access_until = self.access_until.or(self.current_period_end);
        let active_access = self.can_sync()
            // A scheduled cancellation without a provider period end must
            // fail closed; otherwise a sparse webhook would grant access
            // indefinitely instead of waiting for reconciliation.
            && (!self.cancel_at_period_end
                || known_access_until.is_some_and(|access_until| now <= access_until));
        active_access
            || (self.sync_enabled
                && matches!(self.plan, SubscriptionPlan::Pro)
                && (matches!(self.access_mode, SyncAccessMode::GraceReadWrite)
                    || (matches!(self.access_mode, SyncAccessMode::PausedNotEntitled)
                        && matches!(self.status, SubscriptionStatus::PastDue)))
                && known_access_until.is_some_and(|period_end| {
                    now <= period_end.saturating_add(PAYMENT_GRACE_SECONDS)
                }))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BillingEventType {
    SubscriptionPending,
    SubscriptionStarted,
    SubscriptionRenewed,
    SubscriptionChanged,
    SubscriptionPastDue,
    SubscriptionCanceled,
    SubscriptionEnded,
    PaymentFailed,
    RefundSucceeded,
    RefundFailed,
    DisputeOpened,
    DisputeWon,
    DisputeLost,
}

/// A verified provider webhook after it has been normalized and mapped to an
/// internal account. Raw provider payloads stay outside the shared contract.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BillingEvent {
    pub protocol_version: u32,
    pub provider: String,
    pub provider_event_id: String,
    pub event_type: BillingEventType,
    pub account_id: String,
    pub plan: SubscriptionPlan,
    pub status: SubscriptionStatus,
    #[serde(default)]
    pub provider_customer_id: Option<String>,
    #[serde(default)]
    pub provider_subscription_id: Option<String>,
    #[serde(default)]
    pub provider_payment_id: Option<String>,
    #[serde(default)]
    pub refund_amount: Option<u64>,
    #[serde(default)]
    pub payment_amount: Option<u64>,
    #[serde(default)]
    pub current_period_end: Option<u64>,
    #[serde(default)]
    pub cancel_at_period_end: bool,
    pub occurred_at: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn free_entitlements_are_local_only() {
        let entitlement = Entitlement::free("account-1");

        assert!(!entitlement.can_sync());
        assert_eq!(entitlement.max_spaces, FREE_SPACE_LIMIT);
        assert_eq!(entitlement.version, 0);
    }

    #[test]
    fn active_pro_entitlements_can_sync() {
        let mut entitlement = Entitlement::free("account-1");
        entitlement.plan = SubscriptionPlan::Pro;
        entitlement.status = SubscriptionStatus::Active;
        entitlement.sync_enabled = true;
        entitlement.max_spaces = PRO_SPACE_LIMIT;

        assert!(entitlement.can_sync());
    }

    #[test]
    fn scheduled_cancellation_expires_at_server_period_end() {
        let mut entitlement = Entitlement::free("account-1");
        entitlement.plan = SubscriptionPlan::Pro;
        entitlement.status = SubscriptionStatus::Active;
        entitlement.sync_enabled = true;
        entitlement.access_mode = SyncAccessMode::ReadWrite;
        entitlement.cancel_at_period_end = true;
        entitlement.access_until = Some(2_000);

        assert!(entitlement.can_sync_at(2_000));
        assert!(!entitlement.can_sync_at(2_001));
    }

    #[test]
    fn scheduled_cancellation_without_period_end_fails_closed() {
        let mut entitlement = Entitlement::free("account-1");
        entitlement.plan = SubscriptionPlan::Pro;
        entitlement.status = SubscriptionStatus::Active;
        entitlement.sync_enabled = true;
        entitlement.access_mode = SyncAccessMode::ReadWrite;
        entitlement.cancel_at_period_end = true;

        assert!(!entitlement.can_sync_at(2_000));
    }

    #[test]
    fn billing_events_round_trip_without_raw_provider_payloads() {
        let event = BillingEvent {
            protocol_version: BILLING_PROTOCOL_VERSION,
            provider: "provider".to_owned(),
            provider_event_id: "evt_123".to_owned(),
            event_type: BillingEventType::SubscriptionStarted,
            account_id: "account-1".to_owned(),
            plan: SubscriptionPlan::Pro,
            status: SubscriptionStatus::Active,
            provider_customer_id: Some("cus_123".to_owned()),
            provider_subscription_id: Some("sub_123".to_owned()),
            provider_payment_id: None,
            refund_amount: None,
            payment_amount: None,
            current_period_end: Some(1_800_000_000),
            cancel_at_period_end: false,
            occurred_at: 1_700_000_000,
        };

        let raw = serde_json::to_string(&event).expect("event should serialize");
        let restored: BillingEvent = serde_json::from_str(&raw).expect("event should deserialize");

        assert_eq!(restored, event);
        assert!(!raw.contains("raw_payload"));
    }
}
