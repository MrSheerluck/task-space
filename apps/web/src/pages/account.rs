use std::cell::RefCell;

use gloo_net::http::Request;
use serde::{Deserialize, Serialize};
use task_core::billing::Entitlement;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::{JsFuture, future_to_promise};
use web_sys::{RequestCredentials, Storage};

use super::api::{api_url, send_request_with_timeout, send_with_timeout};

const AUTHENTICATED_SESSION_STORAGE_KEY: &str = "task_space_authenticated_session";
const ACTIVE_ACCOUNT_ID_STORAGE_KEY: &str = "task_space_active_account_id";

thread_local! {
    static REFRESH_IN_FLIGHT: RefCell<Option<js_sys::Promise>> = const { RefCell::new(None) };
}

#[wasm_bindgen(inline_js = r#"
const taskSpaceAuthRefreshLockKey = "task-space:auth-refresh-lock";
const taskSpaceAuthRefreshOwner = globalThis.crypto?.randomUUID?.()
  || `${Date.now()}-${Math.random()}`;
const taskSpaceAuthRefreshLeaseMs = 20_000;
const taskSpaceAuthRefreshWaitMs = taskSpaceAuthRefreshLeaseMs + 2_000;
let taskSpaceAuthRefreshLock = null;
let taskSpaceAuthRefreshRequest = null;

function taskSpaceAcquireAuthRefreshStorageLock() {
  return new Promise((resolve) => {
    let storage;
    try {
      storage = globalThis.localStorage;
      if (!storage) {
        resolve("unavailable");
        return;
      }
    } catch (_) {
      resolve("unavailable");
      return;
    }
    const deadline = Date.now() + taskSpaceAuthRefreshWaitMs;
    const attempt = () => {
      const now = Date.now();
      if (now > deadline) {
        // A live holder should finish within the request timeout. If it was
        // suspended, its expiring lease is now eligible for takeover. Keep
        // the result bounded instead of waiting on an unbounded Web Lock.
        resolve("busy");
        return;
      }
      try {
        const current = JSON.parse(storage.getItem(taskSpaceAuthRefreshLockKey) || "null");
        if (current?.owner !== taskSpaceAuthRefreshOwner && Number(current?.expiresAt) > now) {
          setTimeout(attempt, 100);
          return;
        }
        const lease = {
          owner: taskSpaceAuthRefreshOwner,
          expiresAt: now + taskSpaceAuthRefreshLeaseMs,
        };
        storage.setItem(taskSpaceAuthRefreshLockKey, JSON.stringify(lease));
        const verified = JSON.parse(storage.getItem(taskSpaceAuthRefreshLockKey) || "null");
        if (verified?.owner !== lease.owner) {
          setTimeout(attempt, 100);
          return;
        }
        taskSpaceAuthRefreshLock = { owner: lease.owner, storage: true };
        resolve("acquired");
      } catch (_) {
        resolve("unavailable");
      }
    };
    attempt();
  });
}

function taskSpaceAcquireAuthRefreshWebLock() {
  const locks = globalThis.navigator?.locks;
  if (!locks || typeof locks.request !== "function") return Promise.resolve(false);
  return new Promise((resolve) => {
    const deadline = Date.now() + taskSpaceAuthRefreshWaitMs;
    const attempt = () => {
      if (Date.now() > deadline) {
        resolve(false);
        return;
      }
      locks.request("task-space-auth-refresh", { mode: "exclusive", ifAvailable: true }, (lock) => {
        if (!lock) {
          setTimeout(attempt, 100);
          return undefined;
        }
        let release;
        const hold = new Promise((releaseLock) => { release = releaseLock; });
        taskSpaceAuthRefreshLock = { release, storage: false };
        resolve(true);
        return hold;
      }).catch(() => resolve(false));
    };
    attempt();
  });
}

export function taskSpaceAcquireAuthRefreshLock() {
  if (taskSpaceAuthRefreshLock) return Promise.resolve(true);
  if (taskSpaceAuthRefreshRequest) return taskSpaceAuthRefreshRequest;
  const promise = taskSpaceAcquireAuthRefreshStorageLock().then((result) => {
    if (result === "acquired") return true;
    if (result === "busy") return false;
    // Storage can be denied in private/restricted profiles. Native Web Locks
    // remain useful there, but ifAvailable plus a deadline prevents a frozen
    // tab from blocking authentication forever.
    if (globalThis.navigator?.locks?.request) {
      return taskSpaceAcquireAuthRefreshWebLock();
    }
    // There is no cross-tab primitive left. Preserve the old best-effort
    // behavior for this exceptional profile while keeping the normal path
    // serialized by the expiring storage lease.
    taskSpaceAuthRefreshLock = { storage: false, bestEffort: true };
    return true;
  });
  taskSpaceAuthRefreshRequest = promise;
  promise.finally(() => {
    if (taskSpaceAuthRefreshRequest === promise) taskSpaceAuthRefreshRequest = null;
  });
  return promise;
}

export function taskSpaceReleaseAuthRefreshLock() {
  const lock = taskSpaceAuthRefreshLock;
  taskSpaceAuthRefreshLock = null;
  if (lock?.storage) {
    try {
      const storage = globalThis.localStorage;
      const current = JSON.parse(storage?.getItem(taskSpaceAuthRefreshLockKey) || "null");
      if (current?.owner === lock.owner) storage?.removeItem(taskSpaceAuthRefreshLockKey);
    } catch (_) {}
  }
  if (typeof lock?.release === "function") lock.release();
}
"#)]
extern "C" {
    #[wasm_bindgen(js_name = taskSpaceAcquireAuthRefreshLock)]
    fn acquire_auth_refresh_lock() -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceReleaseAuthRefreshLock)]
    fn release_auth_refresh_lock();
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
pub enum AccountState {
    Checking,
    Guest,
    /// The browser previously had an authenticated account, but the access
    /// and refresh sessions can no longer be renewed. Keep its local
    /// namespace visible and pause cloud sync until the user signs in again.
    Expired,
    SignedIn(Entitlement),
    Unavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckoutReturnState {
    None,
    Pending,
    Failed,
}

impl AccountState {
    pub fn is_authenticated(&self) -> bool {
        matches!(self, Self::SignedIn(_))
    }

    pub fn entitlement(&self) -> Option<&Entitlement> {
        match self {
            Self::SignedIn(entitlement) => Some(entitlement),
            _ => None,
        }
    }
}

pub fn remember_authenticated_session() {
    for storage in auth_storages() {
        let _ = storage.set_item(AUTHENTICATED_SESSION_STORAGE_KEY, "true");
    }
}

pub fn forget_authenticated_session() {
    for storage in auth_storages() {
        let _ = storage.remove_item(AUTHENTICATED_SESSION_STORAGE_KEY);
        let _ = storage.remove_item(ACTIVE_ACCOUNT_ID_STORAGE_KEY);
    }
}

fn auth_storages() -> Vec<Storage> {
    let Some(window) = web_sys::window() else {
        return Vec::new();
    };
    let mut storages = Vec::new();
    if let Ok(Some(storage)) = window.local_storage() {
        storages.push(storage);
    }
    if let Ok(Some(storage)) = window.session_storage() {
        storages.push(storage);
    }
    storages
}

pub fn remember_authenticated_account(account_id: &str) {
    remember_authenticated_session();
    if account_id.trim().is_empty() {
        return;
    }
    if let Some(window) = web_sys::window()
        && let Ok(Some(storage)) = window.local_storage()
    {
        let _ = storage.set_item(ACTIVE_ACCOUNT_ID_STORAGE_KEY, account_id);
    }
}

pub fn remembered_account_principal() -> Option<String> {
    let window = web_sys::window()?;
    let authenticated = window
        .local_storage()
        .ok()
        .flatten()
        .and_then(|storage| storage.get_item(AUTHENTICATED_SESSION_STORAGE_KEY).ok())
        .flatten()
        .is_some_and(|value| value == "true");
    if !authenticated {
        return None;
    }
    window
        .local_storage()
        .ok()
        .flatten()
        .and_then(|storage| storage.get_item(ACTIVE_ACCOUNT_ID_STORAGE_KEY).ok())
        .flatten()
        .filter(|account_id| !account_id.trim().is_empty())
        .map(|account_id| format!("account:{account_id}"))
}

fn state_after_session_expiry() -> AccountState {
    state_after_session_expiry_for(remembered_account_principal().is_some())
}

fn state_after_session_expiry_for(has_remembered_account: bool) -> AccountState {
    if has_remembered_account {
        AccountState::Expired
    } else {
        AccountState::Guest
    }
}

pub async fn load_account_state() -> AccountState {
    let mut refresh_attempted = false;
    loop {
        let Ok(session) = auth_session().await else {
            return AccountState::Unavailable;
        };
        if session.status() == 401 {
            if !refresh_attempted && refresh_session_once().await {
                refresh_attempted = true;
                continue;
            }
            return state_after_session_expiry();
        }
        if !(200..300).contains(&session.status()) {
            return AccountState::Unavailable;
        }

        let Ok(response) = account_entitlement().await else {
            return AccountState::Unavailable;
        };
        if response.status() == 401 && !refresh_attempted && refresh_session_once().await {
            refresh_attempted = true;
            continue;
        }
        return match response.status() {
            200..=299 => response
                .json::<Entitlement>()
                .await
                .map(|entitlement| {
                    remember_authenticated_account(&entitlement.account_id);
                    AccountState::SignedIn(entitlement)
                })
                .unwrap_or(AccountState::Unavailable),
            401 => state_after_session_expiry(),
            _ => AccountState::Unavailable,
        };
    }
}

/// Refresh the cookie session once, sharing a single in-flight rotation among
/// concurrent callers so refresh-token rotation cannot race.
pub async fn refresh_session_once() -> bool {
    if let Some(existing) = REFRESH_IN_FLIGHT.with(|flight| flight.borrow().clone()) {
        return JsFuture::from(existing)
            .await
            .ok()
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
    }
    let promise = future_to_promise(async {
        let lock_acquired = JsFuture::from(acquire_auth_refresh_lock())
            .await
            .ok()
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        if !lock_acquired {
            return Ok(JsValue::from_bool(false));
        }
        // Another tab may have completed the rotation while this tab was
        // waiting for the shared lock. Re-check the access cookie before
        // rotating again; this avoids invalidating a fresh refresh token.
        let already_authenticated = auth_session()
            .await
            .is_ok_and(|response| (200..300).contains(&response.status()));
        let success = if already_authenticated {
            true
        } else {
            send_with_timeout(
                Request::post(&api_url("/auth/refresh")).credentials(RequestCredentials::Include),
            )
            .await
            .is_ok_and(|response| (200..300).contains(&response.status()))
        };
        release_auth_refresh_lock();
        Ok(JsValue::from_bool(success))
    });
    REFRESH_IN_FLIGHT.with(|flight| flight.replace(Some(promise.clone())));
    let result = JsFuture::from(promise)
        .await
        .ok()
        .and_then(|value| value.as_bool());
    REFRESH_IN_FLIGHT.with(|flight| {
        flight.borrow_mut().take();
    });
    result.unwrap_or(false)
}

async fn account_entitlement() -> Result<gloo_net::http::Response, gloo_net::Error> {
    // A guest 401 must not be reused after a successful sign-in on the same
    // browser. The entitlement is session state, so it should never be cached.
    let url = format!(
        "{}?check={}",
        api_url("/account/entitlement"),
        js_sys::Date::now()
    );
    send_with_timeout(Request::get(&url).credentials(RequestCredentials::Include)).await
}

async fn auth_session() -> Result<gloo_net::http::Response, gloo_net::Error> {
    let url = format!("{}?check={}", api_url("/auth/session"), js_sys::Date::now());
    send_with_timeout(Request::get(&url).credentials(RequestCredentials::Include)).await
}

#[derive(Debug, Deserialize)]
struct CheckoutResponse {
    checkout_url: String,
}

#[derive(Debug, Deserialize)]
struct BillingPortalResponse {
    portal_url: String,
}

#[derive(Debug, Serialize)]
struct CheckoutRequest<'a> {
    interval: &'a str,
}

pub async fn start_checkout(interval: &'static str) -> Result<String, String> {
    let mut refreshed = false;
    let response = loop {
        let request = Request::post(&api_url("/billing/checkout"))
            .credentials(RequestCredentials::Include)
            .json(&CheckoutRequest { interval })
            .map_err(|_| "the checkout request could not be prepared".to_owned())?;
        let response = send_request_with_timeout(request)
            .await
            .map_err(|_| "the billing service could not be reached".to_owned())?;
        if response.status() == 401 && !refreshed && refresh_session_once().await {
            refreshed = true;
            continue;
        }
        break response;
    };
    if !(200..300).contains(&response.status()) {
        let status = response.status();
        if status == 401 {
            return Err("your session expired; please sign in again before upgrading".to_owned());
        }
        let detail = response.text().await.unwrap_or_default();
        let detail = detail.trim();
        return Err(if detail.is_empty() {
            format!("checkout could not start (HTTP {status}); please try again")
        } else {
            format!("checkout could not start: {detail}")
        });
    }
    response
        .json::<CheckoutResponse>()
        .await
        .map(|checkout| checkout.checkout_url)
        .map_err(|_| "the billing service returned an unexpected response".to_owned())
}

pub async fn start_billing_portal() -> Result<String, String> {
    let mut refreshed = false;
    let response = loop {
        let response = send_with_timeout(
            Request::post(&api_url("/billing/portal")).credentials(RequestCredentials::Include),
        )
        .await
        .map_err(|_| "the billing service could not be reached".to_owned())?;
        if response.status() == 401 && !refreshed && refresh_session_once().await {
            refreshed = true;
            continue;
        }
        break response;
    };
    if !(200..300).contains(&response.status()) {
        let status = response.status();
        if status == 401 {
            return Err("your session expired; please sign in again".to_owned());
        }
        let detail = response.text().await.unwrap_or_default();
        let detail = detail.trim();
        return Err(if detail.is_empty() {
            format!("billing portal could not open (HTTP {status}); please try again")
        } else {
            format!("billing portal could not open: {detail}")
        });
    }
    response
        .json::<BillingPortalResponse>()
        .await
        .map(|portal| portal.portal_url)
        .map_err(|_| "the billing service returned an unexpected response".to_owned())
}

fn checkout_return_state_from_search(search: &str) -> CheckoutReturnState {
    let mut has_checkout_parameter = false;
    let mut returned_status = None;

    for parameter in search.trim_start_matches('?').split('&') {
        let Some((name, value)) = parameter.split_once('=') else {
            continue;
        };
        if matches!(name, "payment_id" | "subscription_id" | "status") {
            has_checkout_parameter = true;
        }
        if name == "status" {
            returned_status = Some(value.to_ascii_lowercase());
        }
    }

    if returned_status
        .as_deref()
        .is_some_and(|status| matches!(status, "failed" | "cancelled" | "canceled" | "expired"))
    {
        CheckoutReturnState::Failed
    } else if has_checkout_parameter {
        CheckoutReturnState::Pending
    } else {
        CheckoutReturnState::None
    }
}

pub fn checkout_return_state() -> CheckoutReturnState {
    let Some(window) = web_sys::window() else {
        return CheckoutReturnState::None;
    };
    let Ok(search) = window.location().search() else {
        return CheckoutReturnState::None;
    };
    checkout_return_state_from_search(&search)
}

async fn wait_ms(milliseconds: i32) {
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        let Some(window) = web_sys::window() else {
            let _ = resolve.call0(&wasm_bindgen::JsValue::NULL);
            return;
        };
        let callback = Closure::once_into_js(move || {
            let _ = resolve.call0(&wasm_bindgen::JsValue::NULL);
        });
        let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
            callback.unchecked_ref(),
            milliseconds,
        );
    });
    let _ = JsFuture::from(promise).await;
}

/// A successful checkout redirects back before the webhook is guaranteed to
/// have reached us. Give the server a short window to apply the verified
/// subscription event before showing the upgrade gate again.
pub async fn load_account_state_after_checkout() -> AccountState {
    let mut state = load_account_state().await;
    if checkout_return_state() != CheckoutReturnState::Pending
        || matches!(
            state,
            AccountState::Guest | AccountState::Expired | AccountState::Unavailable
        )
        || state.entitlement().is_some_and(Entitlement::can_sync)
    {
        return state;
    }

    for _ in 0..15 {
        wait_ms(2_000).await;
        state = load_account_state().await;
        if state.entitlement().is_some_and(Entitlement::can_sync) {
            break;
        }
    }
    state
}

pub async fn sign_out() {
    forget_authenticated_session();
    let _ = send_with_timeout(
        Request::post(&api_url("/auth/logout")).credentials(RequestCredentials::Include),
    )
    .await;
    if let Some(window) = web_sys::window() {
        let _ = window.location().set_href("/");
    }
}

#[cfg(test)]
mod tests {
    use super::{CheckoutReturnState, checkout_return_state_from_search};

    #[test]
    fn recognizes_failed_checkout_returns() {
        assert_eq!(
            checkout_return_state_from_search("?subscription_id=sub_123&status=failed"),
            CheckoutReturnState::Failed
        );
        assert_eq!(
            checkout_return_state_from_search("?status=cancelled"),
            CheckoutReturnState::Failed
        );
    }

    #[test]
    fn recognizes_checkout_returns_that_may_be_waiting_for_a_webhook() {
        assert_eq!(
            checkout_return_state_from_search("?payment_id=pay_123&status=succeeded"),
            CheckoutReturnState::Pending
        );
        assert_eq!(
            checkout_return_state_from_search("?subscription_id=sub_123"),
            CheckoutReturnState::Pending
        );
        assert_eq!(
            checkout_return_state_from_search("?unrelated=value"),
            CheckoutReturnState::None
        );
    }

    #[test]
    fn expired_sessions_keep_a_known_account_namespace_distinct_from_guests() {
        assert_eq!(
            super::state_after_session_expiry_for(true),
            super::AccountState::Expired
        );
        assert_eq!(
            super::state_after_session_expiry_for(false),
            super::AccountState::Guest
        );
    }
}
