#![allow(clippy::too_many_arguments)]

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(target_arch = "wasm32"))]
use std::time::{SystemTime, UNIX_EPOCH};

use super::account::{
    AccountState, CheckoutReturnState, checkout_return_state, load_account_state,
    load_account_state_after_checkout, refresh_session_once, remembered_account_principal,
    sign_out, start_billing_portal, start_checkout,
};
use super::api::{api_url, send_request_with_timeout, send_with_timeout};
use gloo_net::http::{Request, Response};
use js_sys::Array;
use leptos::ev::{Event, KeyboardEvent, MouseEvent, PointerEvent, WheelEvent};
use leptos::leptos_dom::helpers::{window_event_listener, window_event_listener_untyped};
use leptos::prelude::*;
use leptos::task::spawn_local;
use serde::{Deserialize, Serialize};
use task_core::crdt::SpaceDoc;
use task_core::sync::EncodedUpdate;
use task_core::sync::{
    MAX_SYNC_SNAPSHOT_BYTES, MAX_SYNC_STATE_VECTOR_BYTES, MAX_SYNC_UPDATE_BYTES,
    SYNC_DOCUMENT_SCHEMA_VERSION, SYNC_PROTOCOL_VERSION, SYNC_RECONCILE_PROTOCOL_VERSION,
    SpaceMetadataOperation, SyncEvent, SyncMetadataRequest, SyncMetadataResponse, SyncPullRequest,
    SyncPullResponse, SyncReconcileRequest, SyncReconcileResponse,
};
use task_core::{
    BoardData, CURRENT_SCHEMA_VERSION, Group, Note, NoteColor, NoteStatus, Space,
    SpaceManifestEntry, Tombstone, TombstoneKind, WorkspaceData, is_valid_stable_id,
    legacy_entity_stable_id,
};
use wasm_bindgen::{JsCast, JsValue, closure::Closure, prelude::wasm_bindgen};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    Blob, Element, FileReader, HtmlAnchorElement, HtmlInputElement, HtmlSelectElement,
    HtmlTextAreaElement, RequestCredentials, Url,
};

const STORAGE_KEY: &str = "task-space.board.v2";
const LEGACY_STORAGE_KEY: &str = "task-space.board.v1";
const LEGACY_WORKSPACE_STORAGE_KEY: &str = "task-space.workspace.v1";
#[cfg(target_arch = "wasm32")]
const DEVICE_ID_STORAGE_KEY: &str = "task-space.device-id.v1";
const GUEST_PRINCIPAL_STORAGE_KEY: &str = "task-space.guest-principal.v1";
const GUEST_LEGACY_MIGRATED_STORAGE_KEY: &str = "task-space.guest-legacy-migrated.v1";
const VIEW_STORAGE_KEY_PREFIX: &str = "task-space.view.v2.";
const LEGACY_VIEW_STORAGE_KEY: &str = "task-space.view.v1";
const MAX_HISTORY: usize = 100;
const NOTE_WIDTH: f64 = 208.0;
const NOTE_HEIGHT: f64 = 200.0;
const HORIZONTAL_PADDING: f64 = 24.0;
const TOP_PADDING: f64 = 52.0;
const BOTTOM_PADDING: f64 = 24.0;
const MIN_GROUP_WIDTH: f64 = NOTE_WIDTH + HORIZONTAL_PADDING * 2.0;
const MIN_GROUP_HEIGHT: f64 = NOTE_HEIGHT + TOP_PADDING + BOTTOM_PADDING;

#[derive(Clone, Copy)]
struct SyncRuntime {
    run_generation: u64,
    next_space_id: RwSignal<u64>,
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
    status: RwSignal<SyncStatus>,
    pending_count: RwSignal<usize>,
    last_server_ack_at: RwSignal<Option<u64>>,
    last_request_id: RwSignal<Option<String>>,
    last_error: RwSignal<Option<String>>,
    local_diagnostics: RwSignal<Option<LocalSyncDiagnostics>>,
    transport_diagnostics: RwSignal<SyncTransportDiagnostics>,
}

#[derive(Clone, Copy)]
struct RemoteSyncEventRuntime {
    run_generation: u64,
    next_space_id: RwSignal<u64>,
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct LocalSyncDiagnostics {
    db_version: u64,
    local_generation: u64,
    acknowledged_generation: u64,
    outbox_count: usize,
    outbox_bytes: usize,
    metadata_count: usize,
    inbox_count: usize,
    oldest_pending_at: Option<u64>,
    last_error: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct SyncTransportDiagnostics {
    connected: bool,
    connections: u64,
    reconnects: u64,
    resets: u64,
    last_event_id: Option<String>,
    last_event_at: Option<u64>,
    last_open_at: Option<u64>,
    last_error_at: Option<u64>,
}

#[derive(Clone, Debug, Default)]
struct PendingCrdtUpdates {
    updates: Vec<Vec<u8>>,
    inbox_keys: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexedCrdtOutbox {
    #[serde(default)]
    updates: Vec<String>,
    #[serde(default)]
    inbox_keys: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SyncStatus {
    Disabled,
    Checking,
    Syncing,
    Synced,
    Offline,
    AuthPaused,
    BillingPaused,
    Error,
}

impl SyncStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Disabled => "local only",
            Self::Checking => "checking account…",
            Self::Syncing => "syncing…",
            Self::Synced => "synced",
            Self::Offline => "offline — changes saved locally",
            Self::AuthPaused => "sign-in expired — changes saved locally",
            Self::BillingPaused => "sync paused — changes saved locally",
            Self::Error => "sync error — retry",
        }
    }
}

thread_local! {
    static SYNC_PRINCIPAL: RefCell<String> = RefCell::new("guest".to_owned());
    static SYNC_ACKED_STATE_VECTORS: RefCell<HashMap<(String, u64), Vec<u8>>> = RefCell::new(HashMap::new());
    static SYNC_ACTIVE: Cell<bool> = const { Cell::new(false) };
    static SYNC_DRAIN_IN_FLIGHT: Cell<bool> = const { Cell::new(false) };
    static SYNC_DRAIN_PENDING: Cell<bool> = const { Cell::new(false) };
    static SYNC_SERVER_CONTACT: Cell<bool> = const { Cell::new(false) };
    static SYNC_RUN_GENERATION: Cell<u64> = const { Cell::new(0) };
    static SYNC_RUNTIME: RefCell<Option<SyncRuntime>> = const { RefCell::new(None) };
    static STORAGE_SCHEMA_BLOCKED: Cell<bool> = const { Cell::new(false) };
    static STORAGE_REPAIR_REQUIRED: Cell<bool> = const { Cell::new(false) };
    static LOCAL_GENERATIONS: RefCell<HashMap<(String, u64), u64>> = RefCell::new(HashMap::new());
    static REMOTE_CRDT_PROJECTION_GENERATION: Cell<u64> = const { Cell::new(0) };
    static LOCAL_TAB_UPDATE_QUEUE: RefCell<VecDeque<LocalSyncUpdate>> =
        const { RefCell::new(VecDeque::new()) };
    static LOCAL_TAB_UPDATE_RUNNING: Cell<bool> = const { Cell::new(false) };
    static LOCAL_TAB_PENDING_MESSAGES: RefCell<VecDeque<LocalSyncUpdate>> =
        const { RefCell::new(VecDeque::new()) };
    static LOCAL_METADATA_WATERMARKS: RefCell<HashMap<(String, u64), String>> =
        RefCell::new(HashMap::new());
    static SYNC_SPACE_NETWORK_IN_FLIGHT: RefCell<HashSet<(String, u64)>> =
        RefCell::new(HashSet::new());
    static STORAGE_WRITE_GENERATION: Cell<u64> = const { Cell::new(0) };
    static STORAGE_ERROR_GENERATION: Cell<u64> = const { Cell::new(0) };
    static PENDING_COUNT_REFRESH_GENERATION: Cell<u64> = const { Cell::new(0) };
    static SYNC_PENDING_COUNT_SIGNAL: RefCell<Option<RwSignal<usize>>> =
        const { RefCell::new(None) };
    static REMOTE_SYNC_EVENT_QUEUE: RefCell<VecDeque<(String, RemoteSyncEventRuntime)>> =
        const { RefCell::new(VecDeque::new()) };
    static REMOTE_SYNC_EVENT_RUNNING: Cell<bool> = const { Cell::new(false) };
}

#[wasm_bindgen(inline_js = r#"
// 2026-09-08 static snippet cache-bust for the ngrok Wasm/SRI fix.
let taskSpaceSyncPrincipal = "guest";
const taskSpaceGuestLegacyMigratedKey = "task-space.guest-legacy-migrated.v1";
const taskSpaceMaxOutboxEntries = 128;
const taskSpaceMaxOutboxBytes = 8 * 1024 * 1024;
// The server accepts at most 2 MiB of decoded reconcile update. A URL-safe
// base64 envelope for that payload is about 2.8 MiB, so never coalesce a
// larger local snapshot into a recovery mutation that the server must reject.
const taskSpaceMaxRecoveryEnvelopeChars = Math.ceil((2 * 1024 * 1024) / 3) * 4;
const taskSpaceLocalBroadcastProtocolVersion = 1;
const taskSpaceLocalStorageSyncKeyBase = "task-space.local-sync.v1";
const taskSpaceTabId = (() => {
  const key = "task-space.local-tab-id.v1";
  try {
    const existing = globalThis.sessionStorage?.getItem(key);
    if (existing) return existing;
    const generated = globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random()}`;
    globalThis.sessionStorage?.setItem(key, generated);
    return generated;
  } catch (_) {
    // Private/restricted profiles may deny sessionStorage. The fallback still
    // remains tab-local because this inline module is evaluated per document.
    return globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random()}`;
  }
})();
let taskSpaceLocalBroadcast = null;
let taskSpaceLocalStorageListener = null;
let taskSpaceLocalBroadcastOnUpdate = null;
let taskSpaceLocalBroadcastPrincipal = "guest";
let taskSpaceLocalStorageSyncKey = `${taskSpaceLocalStorageSyncKeyBase}:guest`;

function taskSpaceIsGuestPrincipal(principal) {
  const value = String(principal || "guest");
  return value === "guest" || value.startsWith("guest:");
}

function taskSpaceCanReadLegacyGuest() {
  if (!taskSpaceIsGuestPrincipal(taskSpaceSyncPrincipal)) return false;
  try {
    return globalThis.localStorage?.getItem(taskSpaceGuestLegacyMigratedKey) !== "1";
  } catch (_) {
    return true;
  }
}

function taskSpaceMarkLegacyGuestMigrated(principal) {
  if (!taskSpaceIsGuestPrincipal(principal)) return;
  try {
    globalThis.localStorage?.setItem(taskSpaceGuestLegacyMigratedKey, "1");
  } catch (_) {}
}

function taskSpaceKey(key) {
  return taskSpaceKeyFor(taskSpaceSyncPrincipal, key);
}

function taskSpaceKeyFor(principal, key) {
  return `${String(principal || "guest")}:${key}`;
}

export function taskSpaceSetSyncPrincipal(principal) {
  taskSpaceSyncPrincipal = String(principal || "guest");
  return taskSpaceSyncPrincipal;
}

function taskSpaceEnsureStores(db) {
  if (!db.objectStoreNames.contains("workspace")) db.createObjectStore("workspace");
  if (!db.objectStoreNames.contains("crdt")) db.createObjectStore("crdt");
  if (!db.objectStoreNames.contains("crdt-updates")) db.createObjectStore("crdt-updates");
  if (!db.objectStoreNames.contains("sync-state")) db.createObjectStore("sync-state");
  if (!db.objectStoreNames.contains("metadata-updates")) db.createObjectStore("metadata-updates");
  // One record per principal/space keeps the snapshot, acknowledged vector,
  // generations, and outbox in one transaction. The older stores remain as
  // migration fallbacks until all clients have opened version 7.
  if (!db.objectStoreNames.contains("sync-records")) db.createObjectStore("sync-records");
  if (!db.objectStoreNames.contains("crdt-inbox")) db.createObjectStore("crdt-inbox");
}

function taskSpaceConfigureDb(db) {
  // Let a newer app version upgrade the database even when this page is
  // backgrounded. Without this handler, every open connection can keep an
  // older schema alive indefinitely and leave the upgrade request blocked.
  db.onversionchange = () => db.close();
  return db;
}

function taskSpaceBackupKeyBelongsToPrincipal(principal, key) {
  if (typeof key === "bigint" || typeof key === "number") {
    return taskSpaceIsGuestPrincipal(principal);
  }
  if (typeof key !== "string") return false;
  if (key.startsWith(`${principal}:`)) return true;
  // Guest migration retained a few pre-namespace keys so an interrupted
  // upgrade can still be recovered. Include those only in a guest backup;
  // never mix another account's namespaced records into the export.
  return taskSpaceIsGuestPrincipal(principal)
    && (key === "current" || key.startsWith("space:") || key.startsWith("space-"));
}

export function taskSpaceExportIndexedDbBackup(principal) {
  const requestedPrincipal = String(principal || taskSpaceSyncPrincipal || "guest");
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space");
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      const stores = Array.from(db.objectStoreNames);
      const backup = {
        format: "task-space-indexeddb-backup",
        formatVersion: 1,
        exportedAt: Date.now(),
        principal: requestedPrincipal,
        database: { name: db.name, version: db.version },
        stores: {},
        localStorage: [],
      };

      try {
        for (let index = 0; index < localStorage.length; index += 1) {
          const key = localStorage.key(index);
          if (key && key.startsWith("task-space.")) {
            backup.localStorage.push({ key, value: localStorage.getItem(key) });
          }
        }
      } catch (_) {
        // Storage access can be denied in private/restricted profiles. The
        // IndexedDB portion remains useful, so keep the export best-effort.
      }

      let storeIndex = 0;
      const fail = (error) => {
        try { db.close(); } catch (_) {}
        reject(error || new Error("Could not read IndexedDB backup"));
      };
      const readNextStore = () => {
        if (storeIndex >= stores.length) {
          try {
            const raw = JSON.stringify(backup, (_key, value) =>
              typeof value === "bigint" ? value.toString() : value,
            );
            db.close();
            resolve(raw);
          } catch (error) {
            fail(error);
          }
          return;
        }
        const storeName = stores[storeIndex++];
        const entries = [];
        backup.stores[storeName] = entries;
        let cursorRequest;
        try {
          cursorRequest = db.transaction(storeName, "readonly")
            .objectStore(storeName)
            .openCursor();
        } catch (error) {
          fail(error);
          return;
        }
        cursorRequest.onerror = () => fail(cursorRequest.error);
        cursorRequest.onsuccess = () => {
          const cursor = cursorRequest.result;
          if (!cursor) {
            readNextStore();
            return;
          }
          if (taskSpaceBackupKeyBelongsToPrincipal(requestedPrincipal, cursor.key)) {
            entries.push({ key: cursor.key, value: cursor.value });
          }
          cursor.continue();
        };
      };
      readNextStore();
    };
  });
}

function taskSpaceEmptySyncRecord(principal, spaceId) {
  return {
    principal: String(principal || "guest"),
    spaceId: Number(spaceId),
    snapshot: null,
    acknowledgedStateVector: null,
    localGeneration: 0,
    acknowledgedGeneration: 0,
    outbox: [],
    lastError: null,
  };
}

function taskSpaceSyncRecordKey(principal, spaceId) {
  return taskSpaceKeyFor(principal, `space:${spaceId}`);
}

function taskSpaceSafeCounter(value) {
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= 0 ? number : 0;
}

function taskSpaceOutboxBytes(entries) {
  return entries.reduce((total, item) => total
    + String(item?.stateVector || "").length
    + String(item?.update || "").length, 0);
}

function taskSpaceRecoveryMutationId() {
  return globalThis.crypto?.randomUUID?.()
    || `recovery-${Date.now()}-${Math.floor(Math.random() * 1_000_000_000)}`;
}

function taskSpaceWorkspaceValue(value) {
  if (value && typeof value === "object" && typeof value.raw === "string") {
    return value.raw;
  }
  return value;
}

function taskSpaceWorkspaceObject(value) {
  const raw = taskSpaceWorkspaceValue(value);
  if (typeof raw !== "string") return null;
  try {
    const parsed = JSON.parse(raw);
    return parsed && Array.isArray(parsed.spaces) ? parsed : null;
  } catch (_error) {
    return null;
  }
}

function taskSpaceWorkspaceSpaceKey(space) {
  const stableId = typeof space?.stable_id === "string" ? space.stable_id.trim() : "";
  return stableId ? `stable:${stableId}` : `id:${Number(space?.id)}`;
}

function taskSpaceWorkspaceNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? number : 0;
}

function taskSpaceWorkspaceHasBoardProjection(space) {
  return (space?.board?.notes || []).length > 0
    || (space?.board?.groups || []).length > 0
    || (space?.board?.tombstones || []).length > 0;
}

function taskSpaceWorkspaceBoardVersion(space) {
  let version = 0;
  for (const note of space?.board?.notes || []) {
    version = Math.max(version, taskSpaceWorkspaceNumber(note?.updated_at), taskSpaceWorkspaceNumber(note?.deleted_at));
  }
  for (const group of space?.board?.groups || []) {
    version = Math.max(version, taskSpaceWorkspaceNumber(group?.updated_at), taskSpaceWorkspaceNumber(group?.deleted_at));
  }
  for (const tombstone of space?.board?.tombstones || []) {
    version = Math.max(version, taskSpaceWorkspaceNumber(tombstone?.deleted_at));
  }
  return version || taskSpaceWorkspaceNumber(space?.updated_at);
}

function taskSpaceWorkspaceMetadataKey(space) {
  return JSON.stringify({
    name: String(space?.name || ""),
    archived: Boolean(space?.archived),
    deleted_at: space?.deleted_at == null ? null : taskSpaceWorkspaceNumber(space.deleted_at),
  });
}

function taskSpaceWorkspacePick(existing, incoming) {
  const existingMetadataVersion = taskSpaceSafeCounter(existing?.metadata_version);
  const incomingMetadataVersion = taskSpaceSafeCounter(incoming?.metadata_version);
  let metadata = incoming;
  if (existingMetadataVersion > incomingMetadataVersion) {
    metadata = existing;
  } else if (existingMetadataVersion === incomingMetadataVersion) {
    const existingUpdatedAt = taskSpaceWorkspaceNumber(existing?.updated_at);
    const incomingUpdatedAt = taskSpaceWorkspaceNumber(incoming?.updated_at);
    const existingKey = taskSpaceWorkspaceMetadataKey(existing);
    const incomingKey = taskSpaceWorkspaceMetadataKey(incoming);
    if (existingUpdatedAt > incomingUpdatedAt
      || (existingUpdatedAt === incomingUpdatedAt && existingKey > incomingKey)) {
      metadata = existing;
    }
  }

  const existingBoardVersion = taskSpaceWorkspaceBoardVersion(existing);
  const incomingBoardVersion = taskSpaceWorkspaceBoardVersion(incoming);
  const existingHasBoard = taskSpaceWorkspaceHasBoardProjection(existing);
  const incomingHasBoard = taskSpaceWorkspaceHasBoardProjection(incoming);
  let board = incoming?.board;
  if (existingHasBoard && !incomingHasBoard) {
    board = existing?.board;
  } else if (!existingHasBoard && incomingHasBoard) {
    board = incoming?.board;
  } else if (existingBoardVersion > incomingBoardVersion) {
    board = existing?.board;
  } else if (existingBoardVersion === incomingBoardVersion) {
    const existingBoardKey = JSON.stringify(existing?.board || {});
    const incomingBoardKey = JSON.stringify(incoming?.board || {});
    if (existingBoardKey > incomingBoardKey) board = existing?.board;
  }

  const createdAt = Math.min(
    taskSpaceWorkspaceNumber(existing?.created_at) || Number.MAX_SAFE_INTEGER,
    taskSpaceWorkspaceNumber(incoming?.created_at) || Number.MAX_SAFE_INTEGER,
  );
  return {
    ...metadata,
    stable_id: String(incoming?.stable_id || existing?.stable_id || ""),
    id: Number(incoming?.id ?? existing?.id),
    created_at: createdAt === Number.MAX_SAFE_INTEGER ? 0 : createdAt,
    updated_at: Math.max(
      taskSpaceWorkspaceNumber(existing?.updated_at),
      taskSpaceWorkspaceNumber(incoming?.updated_at),
    ),
    board: board || { schema_version: 3, notes: [], groups: [], tombstones: [] },
  };
}

// Workspace JSON is a reload projection, not the CRDT merge point. Still,
// every writer must preserve sibling spaces and reject a late stale board
// projection. This makes direct projection saves safe during cross-tab races;
// canonical CRDT snapshots and outbox rows are committed separately.
function taskSpaceMergeWorkspaceRaw(existingValue, incomingRaw) {
  const incoming = taskSpaceWorkspaceObject(incomingRaw);
  if (!incoming) return null;
  const existing = taskSpaceWorkspaceObject(existingValue);
  if (!existing) return incomingRaw;

  const spaces = new Map();
  for (const space of existing.spaces) {
    if (space && typeof space === "object") spaces.set(taskSpaceWorkspaceSpaceKey(space), space);
  }
  for (const space of incoming.spaces) {
    if (!space || typeof space !== "object") continue;
    const key = taskSpaceWorkspaceSpaceKey(space);
    const previous = spaces.get(key);
    spaces.set(key, previous ? taskSpaceWorkspacePick(previous, space) : space);
  }
  const mergedSpaces = Array.from(spaces.values()).sort((left, right) => {
    const created = taskSpaceWorkspaceNumber(left?.created_at) - taskSpaceWorkspaceNumber(right?.created_at);
    if (created !== 0) return created;
    return taskSpaceWorkspaceSpaceKey(left).localeCompare(taskSpaceWorkspaceSpaceKey(right));
  });

  const tombstones = new Map();
  for (const tombstone of [...(existing.tombstones || []), ...(incoming.tombstones || [])]) {
    if (!tombstone || typeof tombstone !== "object") continue;
    const key = `${String(tombstone.kind || "")}:${String(tombstone.stable_id || "")}:${Number(tombstone.id)}`;
    const previous = tombstones.get(key);
    if (!previous || taskSpaceWorkspaceNumber(tombstone.deleted_at) >= taskSpaceWorkspaceNumber(previous.deleted_at)) {
      tombstones.set(key, tombstone);
    }
  }
  const liveSpaceKeys = new Set(
    mergedSpaces
      .filter((space) => space?.deleted_at == null)
      .map(taskSpaceWorkspaceSpaceKey),
  );
  const mergedTombstones = Array.from(tombstones.values()).filter((tombstone) => {
    if (tombstone.kind !== "Space") return true;
    return !liveSpaceKeys.has(
      typeof tombstone.stable_id === "string" && tombstone.stable_id
        ? `stable:${tombstone.stable_id}`
        : `id:${Number(tombstone.id)}`,
    );
  });
  const activeSpaceId = Number(incoming.active_space_id);
  const fallbackActiveSpaceId = Number(existing.active_space_id);
  const activeExists = mergedSpaces.some((space) => Number(space?.id) === activeSpaceId);
  return JSON.stringify({
    ...existing,
    ...incoming,
    schema_version: Math.max(
      taskSpaceSafeCounter(existing.schema_version),
      taskSpaceSafeCounter(incoming.schema_version),
    ),
    spaces: mergedSpaces,
    tombstones: mergedTombstones,
    active_space_id: activeExists ? activeSpaceId : fallbackActiveSpaceId,
  });
}

export function taskSpaceLoadWorkspace() {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 7);
    request.onupgradeneeded = () => {
      taskSpaceEnsureStores(request.result);
      if (!request.result.objectStoreNames.contains("workspace")) {
        request.result.createObjectStore("workspace");
      }
      if (!request.result.objectStoreNames.contains("crdt")) {
        request.result.createObjectStore("crdt");
      }
      if (!request.result.objectStoreNames.contains("crdt-updates")) {
        request.result.createObjectStore("crdt-updates");
      }
      if (!request.result.objectStoreNames.contains("sync-state")) {
        request.result.createObjectStore("sync-state");
      }
      if (!request.result.objectStoreNames.contains("metadata-updates")) {
        request.result.createObjectStore("metadata-updates");
      }
    };
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      const storeName = Array.from(db.objectStoreNames).includes("workspace")
        ? "workspace"
        : db.objectStoreNames[0];
      if (!storeName) {
        resolve(null);
        return;
      }
      const transaction = db.transaction(storeName, "readonly");
      const store = transaction.objectStore(storeName);
      const read = store.get(taskSpaceKey("current"));
      read.onerror = () => reject(read.error || new Error("Could not read workspace"));
      read.onsuccess = () => {
        if (read.result != null) {
          resolve(taskSpaceWorkspaceValue(read.result));
          return;
        }
        if (!taskSpaceCanReadLegacyGuest()) {
          resolve(null);
          return;
        }
        const legacy = store.get("current");
        legacy.onerror = () => reject(legacy.error || new Error("Could not read legacy workspace"));
        legacy.onsuccess = () => {
          if (legacy.result != null) {
            resolve(taskSpaceWorkspaceValue(legacy.result));
            return;
          }
          resolve(null);
        };
      };
    };
  });
}

export function taskSpaceSaveWorkspace(raw, principal) {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 7);
    request.onupgradeneeded = () => {
      taskSpaceEnsureStores(request.result);
      if (!request.result.objectStoreNames.contains("workspace")) {
        request.result.createObjectStore("workspace");
      }
      if (!request.result.objectStoreNames.contains("crdt")) {
        request.result.createObjectStore("crdt");
      }
      if (!request.result.objectStoreNames.contains("crdt-updates")) {
        request.result.createObjectStore("crdt-updates");
      }
      if (!request.result.objectStoreNames.contains("sync-state")) {
        request.result.createObjectStore("sync-state");
      }
      if (!request.result.objectStoreNames.contains("metadata-updates")) {
        request.result.createObjectStore("metadata-updates");
      }
    };
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      const transaction = db.transaction("workspace", "readwrite");
      const store = transaction.objectStore("workspace");
      const key = taskSpaceKeyFor(principal, "current");
      const read = store.get(key);
      read.onerror = () => reject(read.error || new Error("Could not read workspace"));
      read.onsuccess = () => {
        // Allocate the sequence inside the readwrite transaction. IndexedDB
        // serializes these transactions across tabs; a per-tab counter would
        // let two tabs both write sequence 1 and let a stale physical write
        // replace a newer workspace projection.
        const previousSequence = taskSpaceSafeCounter(read.result?.writeSequence);
        const writeSequence = Math.min(Number.MAX_SAFE_INTEGER, previousSequence + 1);
        if (writeSequence >= previousSequence) {
          const mergedRaw = taskSpaceMergeWorkspaceRaw(read.result, raw) || raw;
          store.put({ raw: mergedRaw, writeSequence }, key);
        }
      };
      transaction.onerror = () => reject(transaction.error || new Error("Could not save workspace"));
      transaction.oncomplete = () => {
        taskSpaceMarkLegacyGuestMigrated(principal);
        resolve(true);
      };
    };
  });
}

export function taskSpaceLoadCrdt(spaceId) {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 7);
    request.onupgradeneeded = () => {
      taskSpaceEnsureStores(request.result);
      if (!request.result.objectStoreNames.contains("workspace")) {
        request.result.createObjectStore("workspace");
      }
      if (!request.result.objectStoreNames.contains("crdt")) {
        request.result.createObjectStore("crdt");
      }
      if (!request.result.objectStoreNames.contains("crdt-updates")) {
        request.result.createObjectStore("crdt-updates");
      }
      if (!request.result.objectStoreNames.contains("sync-state")) {
        request.result.createObjectStore("sync-state");
      }
      if (!request.result.objectStoreNames.contains("metadata-updates")) {
        request.result.createObjectStore("metadata-updates");
      }
    };
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      const transaction = db.transaction(["sync-records", "crdt"], "readonly");
      const recordRead = transaction.objectStore("sync-records").get(
        taskSpaceSyncRecordKey(taskSpaceSyncPrincipal, spaceId),
      );
      recordRead.onerror = () => reject(recordRead.error || new Error("Could not read sync record"));
      recordRead.onsuccess = () => {
        if (recordRead.result?.snapshot != null) {
          resolve(recordRead.result.snapshot);
          return;
        }
        const read = transaction.objectStore("crdt").get(taskSpaceKey(`space:${spaceId}`));
        read.onerror = () => reject(read.error || new Error("Could not read CRDT document"));
        read.onsuccess = () => {
          if (read.result != null || !taskSpaceCanReadLegacyGuest()) {
            resolve(read.result ?? null);
            return;
          }
          const legacy = transaction.objectStore("crdt").get(`space:${spaceId}`);
          legacy.onerror = () => reject(legacy.error || new Error("Could not read legacy CRDT document"));
          legacy.onsuccess = () => resolve(legacy.result ?? null);
        };
      };
    };
  });
}

// The snapshot and outbox are written atomically, but a concurrent tab can
// still leave the snapshot one generation behind while its delta remains in
// the durable outbox. Return those deltas separately so the Rust side can
// replay them into the snapshot before rendering an offline workspace.
export function taskSpaceLoadCrdtOutbox(spaceId) {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 7);
    request.onupgradeneeded = () => taskSpaceEnsureStores(request.result);
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      const transaction = db.transaction(["sync-records", "crdt-updates", "crdt-inbox"], "readonly");
      const recordRead = transaction.objectStore("sync-records").get(
        taskSpaceSyncRecordKey(taskSpaceSyncPrincipal, spaceId),
      );
      const legacyRead = transaction.objectStore("crdt-updates").getAll();
      const inboxRead = transaction.objectStore("crdt-inbox").getAll();
      let record;
      let legacy;
      let inbox;
      let recordReady = false;
      let legacyReady = false;
      let inboxReady = false;
      const finish = () => {
        if (!recordReady || !legacyReady || !inboxReady) return;
        const current = Array.isArray(record?.outbox)
          ? record.outbox
              .filter((item) => item && typeof item.update === "string")
              .map((item) => item.update)
          : [];
        const fallback = (legacy || [])
          .filter((item) => item
            && Number(item.spaceId) === Number(spaceId)
            && typeof item.update === "string"
            && (item.principal === taskSpaceSyncPrincipal
            || (taskSpaceIsGuestPrincipal(taskSpaceSyncPrincipal) && item.principal == null)))
          .map((item) => item.update);
        const seen = new Set(current);
        const merged = current.concat(fallback.filter((update) => {
          if (seen.has(update)) return false;
          seen.add(update);
          return true;
        }));
        const inboxKeys = [];
        for (const item of (inbox || [])) {
          if (item?.principal !== taskSpaceSyncPrincipal
            || Number(item.spaceId) !== Number(spaceId)
            || typeof item.update !== "string") continue;
          if (typeof item.key === "string") inboxKeys.push(item.key);
          if (seen.has(item.update)) continue;
          seen.add(item.update);
          merged.push(item.update);
        }
        resolve({ updates: merged, inboxKeys });
      };
      recordRead.onerror = () => reject(recordRead.error || new Error("Could not read sync record"));
      legacyRead.onerror = () => reject(legacyRead.error || new Error("Could not read CRDT outbox"));
      inboxRead.onerror = () => reject(inboxRead.error || new Error("Could not read CRDT inbox"));
      recordRead.onsuccess = () => { record = recordRead.result; recordReady = true; finish(); };
      legacyRead.onsuccess = () => { legacy = legacyRead.result; legacyReady = true; finish(); };
      inboxRead.onsuccess = () => { inbox = inboxRead.result; inboxReady = true; finish(); };
    };
  });
}

function taskSpaceIncomingCrdtKey(principal, spaceId, originDeviceId, localGeneration) {
  const source = String(originDeviceId || "unknown");
  const generation = taskSpaceSafeCounter(localGeneration);
  return taskSpaceKeyFor(
    principal,
    `inbox:${Number(spaceId)}:${source}:${generation}`,
  );
}

function taskSpaceDeleteIncomingCrdtKeys(transaction, principal, keys) {
  const inbox = transaction.objectStore("crdt-inbox");
  const prefix = taskSpaceKeyFor(principal, "inbox:");
  for (const key of Array.isArray(keys) ? keys : []) {
    if (typeof key === "string" && key.startsWith(prefix)) inbox.delete(key);
  }
}

// A sibling tab's CRDT delta is acknowledged by the local projection only
// after this row is durable. The inbox is not an upload queue: it is replayed
// when hydrating the canonical document and only the exact row covered by a
// successful merged snapshot transaction may be removed.
export function taskSpaceQueueIncomingCrdtUpdate(
  principal,
  spaceId,
  originDeviceId,
  localGeneration,
  encodedUpdate,
) {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 7);
    request.onupgradeneeded = () => taskSpaceEnsureStores(request.result);
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      const transaction = db.transaction("crdt-inbox", "readwrite");
      const source = String(originDeviceId || "unknown");
      const generation = taskSpaceSafeCounter(localGeneration);
      const key = taskSpaceIncomingCrdtKey(principal, spaceId, source, generation);
      transaction.objectStore("crdt-inbox").put({
        key,
        principal: String(principal || "guest"),
        spaceId: Number(spaceId),
        originDeviceId: source,
        localGeneration: generation,
        update: String(encodedUpdate || ""),
      }, key);
      transaction.onerror = () => reject(transaction.error || new Error("Could not queue CRDT inbox update"));
      transaction.oncomplete = () => resolve(true);
    };
  });
}

export function taskSpaceSaveCrdt(
  principal,
  spaceId,
  encodedSnapshot,
  snapshotGeneration = 0,
  originDeviceId = "",
  originGeneration = 0,
) {
  const writeSequence = taskSpaceNextCrdtWriteSequence(principal, spaceId);
  const suppliedGeneration = taskSpaceSafeCounter(snapshotGeneration);
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 7);
    request.onupgradeneeded = () => {
      taskSpaceEnsureStores(request.result);
      if (!request.result.objectStoreNames.contains("workspace")) {
        request.result.createObjectStore("workspace");
      }
      if (!request.result.objectStoreNames.contains("crdt")) {
        request.result.createObjectStore("crdt");
      }
      if (!request.result.objectStoreNames.contains("crdt-updates")) {
        request.result.createObjectStore("crdt-updates");
      }
      if (!request.result.objectStoreNames.contains("sync-state")) {
        request.result.createObjectStore("sync-state");
      }
      if (!request.result.objectStoreNames.contains("metadata-updates")) {
        request.result.createObjectStore("metadata-updates");
      }
    };
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      const transaction = db.transaction(["sync-records", "crdt", "crdt-inbox"], "readwrite");
      const records = transaction.objectStore("sync-records");
      const inbox = transaction.objectStore("crdt-inbox");
      const key = taskSpaceSyncRecordKey(principal, spaceId);
      const read = records.get(key);
      read.onerror = () => reject(read.error || new Error("Could not read sync record"));
      read.onsuccess = () => {
        const record = read.result || taskSpaceEmptySyncRecord(principal, spaceId);
        const currentGeneration = taskSpaceSafeCounter(record.localGeneration);
        const previousSequence = taskSpaceSafeCounter(record.snapshotWriteSequence);
        // A direct snapshot mirror is allowed to replace the record only
        // when its caller supplies the shared generation from the originating
        // atomic transaction. Equal generations are allowed because a sibling
        // tab may have merged an inbox delta into the same-generation
        // snapshot; the inbox transaction below makes that merge durable.
        // Unversioned writes are retained only for first-time initialization;
        // normal local edits use taskSpaceQueueCrdtUpdate as the authority.
        const canWriteSnapshot = suppliedGeneration > 0
          ? suppliedGeneration >= currentGeneration
          : record.snapshot == null && currentGeneration === 0;
        if (canWriteSnapshot && (suppliedGeneration > 0 || writeSequence >= previousSequence)) {
          record.snapshot = encodedSnapshot;
          record.snapshotWriteSequence = writeSequence;
          record.localGeneration = Math.max(currentGeneration, suppliedGeneration);
          // This snapshot may have been built from one sibling message. Do
          // not clear the whole space inbox: unrelated messages can already
          // be durable there but not yet merged into this snapshot.
          const origin = String(originDeviceId || "").trim();
          const generation = taskSpaceSafeCounter(originGeneration);
          if (origin && generation > 0) {
            inbox.delete(taskSpaceIncomingCrdtKey(principal, spaceId, origin, generation));
          }
        }
        records.put(record, key);
        // Keep the legacy mirror aligned with the accepted atomic record. An
        // older async write may arrive after a newer snapshot; writing the
        // incoming value unconditionally would reintroduce a stale fallback
        // for older clients/readers.
        if (record.snapshot != null) {
          transaction.objectStore("crdt").put(record.snapshot, taskSpaceKeyFor(principal, `space:${spaceId}`));
        }
      };
      transaction.onerror = () => reject(transaction.error || new Error("Could not save CRDT document"));
      transaction.oncomplete = () => resolve(true);
    };
  });
}

export function taskSpaceQueueCrdtUpdate(principal, spaceId, mutationId, deviceId, lastServerSequence, stateVector, encodedUpdate, encodedSnapshot, localGeneration, workspaceRaw) {
  const writeSequence = taskSpaceNextCrdtWriteSequence(principal, spaceId);
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 7);
    request.onupgradeneeded = () => {
      taskSpaceEnsureStores(request.result);
      if (!request.result.objectStoreNames.contains("workspace")) {
        request.result.createObjectStore("workspace");
      }
      if (!request.result.objectStoreNames.contains("crdt")) {
        request.result.createObjectStore("crdt");
      }
      if (!request.result.objectStoreNames.contains("crdt-updates")) {
        request.result.createObjectStore("crdt-updates");
      }
      if (!request.result.objectStoreNames.contains("sync-state")) {
        request.result.createObjectStore("sync-state");
      }
      if (!request.result.objectStoreNames.contains("metadata-updates")) {
        request.result.createObjectStore("metadata-updates");
      }
    };
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      const transaction = db.transaction(["sync-records", "crdt-updates", "sync-state", "crdt", "workspace"], "readwrite");
      const generationKey = taskSpaceKeyFor(principal, `generation:${spaceId}`);
      const generationStore = transaction.objectStore("sync-state");
      const records = transaction.objectStore("sync-records");
      const workspaceStore = transaction.objectStore("workspace");
      const recordKey = taskSpaceSyncRecordKey(principal, spaceId);
      const recordRead = records.get(recordKey);
      const generationRead = generationStore.get(generationKey);
      const workspaceRead = workspaceRaw == null
        ? null
        : workspaceStore.get(taskSpaceKeyFor(principal, "current"));
      let recordReady = false;
      let generationReady = false;
      let workspaceReady = workspaceRead == null;
      let committedGeneration = 0;
      recordRead.onerror = () => reject(recordRead.error || new Error("Could not read sync record"));
      generationRead.onerror = () => reject(generationRead.error || new Error("Could not read local generation"));
      if (workspaceRead) {
        workspaceRead.onerror = () => reject(workspaceRead.error || new Error("Could not read workspace"));
        workspaceRead.onsuccess = () => { workspaceReady = true; maybeWrite(); };
      }
      const maybeWrite = () => {
        if (!recordReady || !generationReady || !workspaceReady) return;
        const record = recordRead.result || taskSpaceEmptySyncRecord(principal, spaceId);
        const current = Math.max(
          taskSpaceSafeCounter(record.localGeneration),
          taskSpaceSafeCounter(generationRead.result),
        );
        const suppliedGeneration = taskSpaceSafeCounter(localGeneration);
        // Every queued local mutation receives a strictly newer generation in
        // the atomic record. This also covers two tabs that race before their
        // localStorage counters become visible to one another.
        const next = Math.min(
          Number.MAX_SAFE_INTEGER,
          Math.max(current + 1, suppliedGeneration, 1),
        );
        committedGeneration = next;
        const outbox = Array.isArray(record.outbox) ? record.outbox : [];
        const createdAt = Date.now();
        const entry = {
          mutationId,
          deviceId: String(deviceId || ""),
          lastServerSequence: taskSpaceSafeCounter(lastServerSequence),
          stateVector,
          update: encodedUpdate,
          localGeneration: next,
          createdAt,
        };
        const nextOutbox = outbox.filter((item) => item?.mutationId !== mutationId).concat(entry);
        const outboxStore = transaction.objectStore("crdt-updates");
        let persistedOutbox = nextOutbox;
        const recoveryRequested = nextOutbox.length > taskSpaceMaxOutboxEntries
          || taskSpaceOutboxBytes(nextOutbox) > taskSpaceMaxOutboxBytes;
        const recoveryFitsServer = String(encodedSnapshot || "").length
          <= taskSpaceMaxRecoveryEnvelopeChars;
        if (recoveryRequested && recoveryFitsServer) {
          // A full snapshot is a safe recovery envelope: applying it to the
          // server's Yrs document merges every local operation without
          // resurrecting server-only operations. Replace the individual
          // mutations with one fresh id so an already-committed request can
          // never be retried under a different payload.
          const recoveryMutationId = taskSpaceRecoveryMutationId();
          const queuedTimestamps = nextOutbox
            .map((item) => Number(item?.createdAt))
            .filter((value) => Number.isSafeInteger(value) && value > 0);
          const recoveryEntry = {
            principal: String(principal || "guest"),
            spaceId,
            mutationId: recoveryMutationId,
            deviceId: String(deviceId || ""),
            lastServerSequence: taskSpaceSafeCounter(lastServerSequence),
            stateVector: "",
            update: encodedSnapshot,
            localGeneration: next,
            createdAt: queuedTimestamps.length ? Math.min(...queuedTimestamps) : createdAt,
          };
          for (const item of outbox) {
            if (item?.mutationId) {
              outboxStore.delete(taskSpaceKeyFor(principal, `${spaceId}:${item.mutationId}`));
              // Remove the pre-v6 unscoped copy as well. The fallback reader
              // still exposes that key until an acknowledged mutation clears
              // it, and retaining it here could replay superseded deltas
              // alongside the recovery snapshot.
              outboxStore.delete(`${spaceId}:${item.mutationId}`);
            }
          }
          outboxStore.put(
            recoveryEntry,
            taskSpaceKeyFor(principal, `${spaceId}:${recoveryMutationId}`),
          );
          persistedOutbox = [recoveryEntry];
        }
        record.outbox = persistedOutbox;
        // Queue writes can complete out of order when several local edits
        // are being persisted at once. An older edit may still be retained
        // in the outbox, but it must never replace the newer atomic snapshot.
        if (suppliedGeneration > current || current === 0) {
          record.snapshot = encodedSnapshot;
          record.snapshotWriteSequence = writeSequence;
        }
        record.localGeneration = next;
        // Keep local edits durable even when the document is too large for a
        // single server recovery mutation. The explicit marker prevents this
        // condition from looking like a successful bounded coalesce while
        // preserving every original outbox row for export/repair tooling.
        record.lastError = recoveryRequested && !recoveryFitsServer
          ? "recovery_snapshot_too_large"
          : null;
        records.put(record, recordKey);
        generationStore.put(next, generationKey);
        if (persistedOutbox === nextOutbox) {
          outboxStore.put(
            { principal: String(principal || "guest"), spaceId, mutationId, deviceId: String(deviceId || ""), lastServerSequence: taskSpaceSafeCounter(lastServerSequence), stateVector, update: encodedUpdate, localGeneration: next, createdAt, snapshot: encodedSnapshot },
            taskSpaceKeyFor(principal, `${spaceId}:${mutationId}`),
          );
        }
        if (record.snapshot != null) {
          transaction.objectStore("crdt").put(record.snapshot, taskSpaceKeyFor(principal, `space:${spaceId}`));
        }
        if (workspaceRaw != null) {
          // Do not let a tab that queued an older local generation replace a
          // sibling's newer workspace projection. The CRDT record/outbox is
          // still committed regardless; the projection will be regenerated
          // when the newer generation is observed or reconciled.
          if (suppliedGeneration > current || current === 0) {
            const previousWorkspaceSequence = taskSpaceSafeCounter(workspaceRead?.result?.writeSequence);
            const workspaceWriteSequence = Math.min(
              Number.MAX_SAFE_INTEGER,
              previousWorkspaceSequence + 1,
            );
            workspaceStore.put(
              {
                raw: taskSpaceMergeWorkspaceRaw(workspaceRead?.result, workspaceRaw) || workspaceRaw,
                writeSequence: workspaceWriteSequence,
              },
              taskSpaceKeyFor(principal, "current"),
            );
          }
        }
      };
      recordRead.onsuccess = () => { recordReady = true; maybeWrite(); };
      generationRead.onsuccess = () => { generationReady = true; maybeWrite(); };
      transaction.onerror = () => reject(transaction.error || new Error("Could not queue CRDT update"));
      transaction.oncomplete = () => resolve(committedGeneration);
    };
  });
}

// Commit a pull response as one local transaction. The acknowledged server
// vector and the crash-recovery snapshot must advance together; otherwise a
// crash between two writes can make a later tab diff against the wrong
// server state. A local generation created while the request was in flight
// keeps the newer snapshot and leaves its outbox entry recoverable.
export function taskSpaceCommitCrdtPull(
  principal,
  spaceId,
  encodedSnapshot,
  stateVector,
  localGeneration,
  inboxKeysJson = "[]",
) {
  const writeSequence = taskSpaceNextCrdtWriteSequence(principal, spaceId);
  let inboxKeys = [];
  try {
    const parsed = JSON.parse(String(inboxKeysJson || "[]"));
    if (Array.isArray(parsed)) inboxKeys = parsed;
  } catch (_error) {}
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 7);
    request.onupgradeneeded = () => taskSpaceEnsureStores(request.result);
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      const transaction = db.transaction(["sync-records", "sync-state", "crdt", "crdt-inbox"], "readwrite");
      const records = transaction.objectStore("sync-records");
      const key = taskSpaceSyncRecordKey(principal, spaceId);
      const read = records.get(key);
      read.onerror = () => reject(read.error || new Error("Could not read sync record"));
      read.onsuccess = () => {
        const record = read.result || taskSpaceEmptySyncRecord(principal, spaceId);
        const currentGeneration = taskSpaceSafeCounter(record.localGeneration);
        const requestedGeneration = taskSpaceSafeCounter(localGeneration);
        const previousSequence = taskSpaceSafeCounter(record.snapshotWriteSequence);
        const keepNewerLocalSnapshot = currentGeneration > requestedGeneration
          || writeSequence < previousSequence;
        const snapshot = keepNewerLocalSnapshot && record.snapshot != null
          ? record.snapshot
          : encodedSnapshot;
        record.snapshot = snapshot;
        if (!keepNewerLocalSnapshot) record.snapshotWriteSequence = writeSequence;
        record.acknowledgedStateVector = stateVector;
        record.acknowledgedGeneration = Math.max(
          taskSpaceSafeCounter(record.acknowledgedGeneration),
          Math.min(requestedGeneration, currentGeneration),
        );
        record.lastError = null;
        records.put(record, key);
        transaction.objectStore("crdt").put(snapshot, taskSpaceKeyFor(principal, `space:${spaceId}`));
        transaction.objectStore("sync-state").put(stateVector, taskSpaceKeyFor(principal, `space:${spaceId}`));
        if (snapshot === encodedSnapshot) {
          taskSpaceDeleteIncomingCrdtKeys(transaction, principal, inboxKeys);
        }
      };
      transaction.onerror = () => reject(transaction.error || new Error("Could not commit pull"));
      transaction.oncomplete = () => resolve(true);
    };
  });
}

export function taskSpaceSaveSyncState(principal, spaceId, stateVector) {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 7);
    request.onupgradeneeded = () => taskSpaceEnsureStores(request.result);
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      const transaction = db.transaction(["sync-records", "sync-state"], "readwrite");
      const records = transaction.objectStore("sync-records");
      const key = taskSpaceSyncRecordKey(principal, spaceId);
      const read = records.get(key);
      read.onerror = () => reject(read.error || new Error("Could not read sync record"));
      read.onsuccess = () => {
        const record = read.result || taskSpaceEmptySyncRecord(principal, spaceId);
        record.acknowledgedStateVector = stateVector;
        records.put(record, key);
        transaction.objectStore("sync-state").put(stateVector, taskSpaceKeyFor(principal, `space:${spaceId}`));
      };
      transaction.onerror = () => reject(transaction.error || new Error("Could not save sync state"));
      transaction.oncomplete = () => resolve(true);
    };
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
  });
}

export function taskSpaceLoadSyncState(principal, spaceId) {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 7);
    request.onupgradeneeded = () => taskSpaceEnsureStores(request.result);
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      const transaction = db.transaction(["sync-records", "sync-state"], "readonly");
      const recordRead = transaction.objectStore("sync-records").get(
        taskSpaceSyncRecordKey(principal, spaceId),
      );
      recordRead.onerror = () => reject(recordRead.error || new Error("Could not read sync record"));
      recordRead.onsuccess = () => {
        if (recordRead.result?.acknowledgedStateVector != null) {
          resolve(recordRead.result.acknowledgedStateVector);
          return;
        }
        const read = transaction.objectStore("sync-state").get(taskSpaceKeyFor(principal, `space:${spaceId}`));
        read.onerror = () => reject(read.error || new Error("Could not read sync state"));
        read.onsuccess = () => resolve(read.result ?? null);
      };
    };
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
  });
}

export function taskSpaceLoadCrdtUpdates() {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 7);
    request.onupgradeneeded = () => taskSpaceEnsureStores(request.result);
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      const transaction = db.transaction(["sync-records", "crdt-updates"], "readonly");
      const recordsRead = transaction.objectStore("sync-records").getAll();
      const legacyRead = transaction.objectStore("crdt-updates").getAll();
      let records;
      let legacy;
      let recordsReady = false;
      let legacyReady = false;
      const finish = () => {
        if (!recordsReady || !legacyReady) return;
        const current = (records || [])
          .filter((record) => record?.principal === taskSpaceSyncPrincipal)
          .flatMap((record) => {
            const outbox = Array.isArray(record.outbox)
              ? record.outbox.map((item) => ({
                principal: record.principal,
                spaceId: record.spaceId,
                ...item,
              }))
              : [];
            if (outbox.length > 0) return outbox;
            const localGeneration = taskSpaceSafeCounter(record.localGeneration);
            const acknowledgedGeneration = taskSpaceSafeCounter(record.acknowledgedGeneration);
            if (localGeneration <= acknowledgedGeneration) return [];
            // The snapshot is the self-healing source of truth when an older
            // per-mutation outbox row was lost. A stable recovery mutation id
            // makes retries idempotent without a localStorage counter.
            return [{
              principal: record.principal,
              spaceId: record.spaceId,
              mutationId: "recovery:" + String(record.spaceId) + ":" + String(localGeneration),
              stateVector: record.acknowledgedStateVector ?? null,
              update: record.snapshot,
              localGeneration,
            }];
          });
        const currentKeys = new Set(current.map((item) => `${item.spaceId}:${item.mutationId}`));
        const fallback = (legacy || []).filter((item) =>
          item
          && typeof item.spaceId === "number"
          && typeof item.mutationId === "string"
          && !currentKeys.has(`${item.spaceId}:${item.mutationId}`)
          && (item.principal === taskSpaceSyncPrincipal
            // Legacy outbox rows remain readable until they are explicitly
            // acknowledged under the namespaced key. A workspace migration
            // marker must not hide a queue entry before its first retry.
            || (taskSpaceIsGuestPrincipal(taskSpaceSyncPrincipal) && item.principal == null)),
        );
        resolve(current.concat(fallback));
      };
      recordsRead.onerror = () => reject(recordsRead.error || new Error("Could not read sync records"));
      recordsRead.onsuccess = () => { records = recordsRead.result; recordsReady = true; finish(); };
      legacyRead.onerror = () => reject(legacyRead.error || new Error("Could not read CRDT update queue"));
      legacyRead.onsuccess = () => { legacy = legacyRead.result; legacyReady = true; finish(); };
    };
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
  });
}

export function taskSpaceLoadSyncErrors() {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 7);
    request.onupgradeneeded = () => taskSpaceEnsureStores(request.result);
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      const read = db.transaction("sync-records", "readonly")
        .objectStore("sync-records")
        .getAll();
      read.onerror = () => reject(read.error || new Error("Could not read sync errors"));
      read.onsuccess = () => resolve((read.result || [])
        .filter((record) => record?.principal === taskSpaceSyncPrincipal
          && typeof record.lastError === "string"
          && record.lastError.length > 0)
        .map((record) => ({
          spaceId: Number(record.spaceId),
          error: record.lastError,
        })));
    };
  });
}

export function taskSpaceLoadSyncDiagnostics(principal, spaceId) {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const principalValue = String(principal || taskSpaceSyncPrincipal || "guest");
    const numericSpaceId = Number(spaceId);
    const request = indexedDB.open("task-space", 7);
    request.onupgradeneeded = () => taskSpaceEnsureStores(request.result);
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      const transaction = db.transaction(
        ["sync-records", "metadata-updates", "crdt-inbox", "crdt-updates"],
        "readonly",
      );
      const recordRead = transaction.objectStore("sync-records").get(
        taskSpaceSyncRecordKey(principalValue, numericSpaceId),
      );
      const metadataRead = transaction.objectStore("metadata-updates").getAll();
      const inboxRead = transaction.objectStore("crdt-inbox").getAll();
      const legacyRead = transaction.objectStore("crdt-updates").getAll();
      let record;
      let metadata;
      let inbox;
      let legacy;
      let ready = 0;
      const finish = () => {
        if (ready !== 4) return;
        const recordOutbox = Array.isArray(record?.outbox)
          ? record.outbox.filter((item) => item && typeof item.update === "string")
          : [];
        const legacyOutbox = (legacy || []).filter((item) => item
          && Number(item.spaceId) === numericSpaceId
          && typeof item.update === "string"
          && (item.principal === principalValue
            || (taskSpaceIsGuestPrincipal(principalValue) && item.principal == null)));
        const metadataQueue = (metadata || []).filter((item) => item
          && item.principal === principalValue
          && Number(item.spaceId) === numericSpaceId);
        const inboxQueue = (inbox || []).filter((item) => item
          && item.principal === principalValue
          && Number(item.spaceId) === numericSpaceId
          && typeof item.update === "string");
        const queued = recordOutbox.concat(legacyOutbox, metadataQueue);
        const timestamps = queued
          .map((item) => Number(item.createdAt))
          .filter((value) => Number.isSafeInteger(value) && value > 0);
        resolve({
          dbVersion: Number(db.version) || 0,
          localGeneration: taskSpaceSafeCounter(record?.localGeneration),
          acknowledgedGeneration: taskSpaceSafeCounter(record?.acknowledgedGeneration),
          outboxCount: recordOutbox.length + legacyOutbox.length,
          outboxBytes: recordOutbox.concat(legacyOutbox).reduce((total, item) => total
            + String(item.stateVector || "").length
            + String(item.update || "").length, 0),
          metadataCount: metadataQueue.length,
          inboxCount: inboxQueue.length,
          oldestPendingAt: timestamps.length ? Math.min(...timestamps) : null,
          lastError: typeof record?.lastError === "string" ? record.lastError : null,
        });
      };
      const fail = (read) => reject(read.error || new Error("Could not read sync diagnostics"));
      for (const read of [recordRead, metadataRead, inboxRead, legacyRead]) {
        read.onerror = () => fail(read);
      }
      recordRead.onsuccess = () => { record = recordRead.result; ready += 1; finish(); };
      metadataRead.onsuccess = () => { metadata = metadataRead.result; ready += 1; finish(); };
      inboxRead.onsuccess = () => { inbox = inboxRead.result; ready += 1; finish(); };
      legacyRead.onsuccess = () => { legacy = legacyRead.result; ready += 1; finish(); };
    };
  });
}

export function taskSpaceAckCrdtUpdate(principal, spaceId, mutationId) {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 7);
    request.onupgradeneeded = () => taskSpaceEnsureStores(request.result);
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      const transaction = db.transaction(["sync-records", "crdt-updates"], "readwrite");
      const records = transaction.objectStore("sync-records");
      const key = taskSpaceSyncRecordKey(principal, spaceId);
      const read = records.get(key);
      read.onerror = () => reject(read.error || new Error("Could not read sync record"));
      read.onsuccess = () => {
        if (read.result) {
          const record = read.result;
          record.outbox = (Array.isArray(record.outbox) ? record.outbox : [])
            .filter((item) => item?.mutationId !== mutationId);
          records.put(record, key);
        }
        const updates = transaction.objectStore("crdt-updates");
        updates.delete(taskSpaceKeyFor(principal, `${spaceId}:${mutationId}`));
        // Older versions used an unscoped `space:mutation` key. Remove that
        // legacy copy only after the server has acknowledged the namespaced
        // request, otherwise it would be replayed forever on every drain.
        // This applies to authenticated principals too; the old key did not
        // include a principal namespace.
        updates.delete(`${spaceId}:${mutationId}`);
      };
      transaction.onerror = () => reject(transaction.error || new Error("Could not acknowledge CRDT update"));
      transaction.oncomplete = () => resolve(true);
    };
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
  });
}

export function taskSpaceCommitCrdtReconcile(
  principal,
  spaceId,
  mutationId,
  encodedSnapshot,
  stateVector,
  acknowledgedGeneration,
  inboxKeysJson = "[]",
) {
  const writeSequence = taskSpaceNextCrdtWriteSequence(principal, spaceId);
  let inboxKeys = [];
  try {
    const parsed = JSON.parse(String(inboxKeysJson || "[]"));
    if (Array.isArray(parsed)) inboxKeys = parsed;
  } catch (_error) {}
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 7);
    request.onupgradeneeded = () => taskSpaceEnsureStores(request.result);
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      const transaction = db.transaction(["sync-records", "crdt-updates", "sync-state", "crdt", "crdt-inbox"], "readwrite");
      const records = transaction.objectStore("sync-records");
      const key = taskSpaceSyncRecordKey(principal, spaceId);
      const read = records.get(key);
      read.onerror = () => reject(read.error || new Error("Could not read sync record"));
      read.onsuccess = () => {
        const record = read.result || taskSpaceEmptySyncRecord(principal, spaceId);
        const localGeneration = taskSpaceSafeCounter(record.localGeneration);
        const requestedAcknowledgedGeneration = taskSpaceSafeCounter(acknowledgedGeneration);
        const previousSequence = taskSpaceSafeCounter(record.snapshotWriteSequence);
        // A local edit may commit while the network response is in flight.
        // Its atomic record snapshot is newer than the response snapshot, so
        // never let the older response clobber it. The server only
        // acknowledged the generation that was sent in this request.
        const snapshot = (localGeneration > requestedAcknowledgedGeneration
          || writeSequence < previousSequence)
          && record.snapshot != null
          ? record.snapshot
          : encodedSnapshot;
        record.snapshot = snapshot;
        if (writeSequence >= previousSequence) {
          record.snapshotWriteSequence = writeSequence;
        }
        record.acknowledgedStateVector = stateVector;
        record.acknowledgedGeneration = Math.max(
          taskSpaceSafeCounter(record.acknowledgedGeneration),
          Math.min(requestedAcknowledgedGeneration, localGeneration),
        );
        record.outbox = (Array.isArray(record.outbox) ? record.outbox : [])
          .filter((item) => item?.mutationId !== mutationId);
        record.lastError = null;
        records.put(record, key);
        transaction.objectStore("crdt").put(snapshot, taskSpaceKeyFor(principal, `space:${spaceId}`));
        transaction.objectStore("sync-state").put(stateVector, taskSpaceKeyFor(principal, `space:${spaceId}`));
        transaction.objectStore("crdt-updates").delete(taskSpaceKeyFor(principal, `${spaceId}:${mutationId}`));
        // Also clear the pre-v6 unscoped row after this mutation has been
        // durably acknowledged. Otherwise an authenticated legacy queue row
        // can be rediscovered forever by the migration fallback.
        transaction.objectStore("crdt-updates").delete(`${spaceId}:${mutationId}`);
        if (snapshot === encodedSnapshot) {
          taskSpaceDeleteIncomingCrdtKeys(transaction, principal, inboxKeys);
        }
      };
      transaction.onerror = () => reject(transaction.error || new Error("Could not commit reconciliation"));
      transaction.oncomplete = () => resolve(true);
    };
  });
}

export function taskSpaceQueueMetadataUpdate(principal, spaceId, operationId, operation, name, expectedVersion, workspaceRaw) {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 7);
    request.onupgradeneeded = () => taskSpaceEnsureStores(request.result);
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      if (!db.objectStoreNames.contains("metadata-updates")) {
        resolve(false);
        return;
      }
      const stores = workspaceRaw == null
        ? ["metadata-updates"]
        : ["metadata-updates", "workspace"];
      const transaction = db.transaction(stores, "readwrite");
      transaction.objectStore("metadata-updates").put(
        { principal: String(principal || "guest"), spaceId, operationId, operation, name, expectedVersion, createdAt: Date.now() },
        taskSpaceKeyFor(principal, `${spaceId}:${operationId}`),
      );
      if (workspaceRaw != null) {
        const workspace = transaction.objectStore("workspace");
        const read = workspace.get(taskSpaceKeyFor(principal, "current"));
        read.onerror = () => reject(read.error || new Error("Could not read workspace"));
        read.onsuccess = () => {
          const previousSequence = taskSpaceSafeCounter(read.result?.writeSequence);
          const workspaceWriteSequence = Math.min(
            Number.MAX_SAFE_INTEGER,
            previousSequence + 1,
          );
          workspace.put(
            {
              raw: taskSpaceMergeWorkspaceRaw(read.result, workspaceRaw) || workspaceRaw,
              writeSequence: workspaceWriteSequence,
            },
            taskSpaceKeyFor(principal, "current"),
          );
        };
      }
      transaction.onerror = () => reject(transaction.error || new Error("Could not queue metadata update"));
      transaction.oncomplete = () => resolve(true);
    };
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
  });
}

export function taskSpaceLoadMetadataUpdates() {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 7);
    request.onupgradeneeded = () => taskSpaceEnsureStores(request.result);
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      if (!db.objectStoreNames.contains("metadata-updates")) {
        resolve([]);
        return;
      }
      const read = db.transaction("metadata-updates", "readonly").objectStore("metadata-updates").getAll();
      read.onerror = () => reject(read.error || new Error("Could not read metadata update queue"));
      read.onsuccess = () => resolve((read.result ?? []).filter((item) =>
        item && item.principal === taskSpaceSyncPrincipal,
      ));
    };
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
  });
}

export function taskSpaceAckMetadataUpdate(principal, spaceId, operationId) {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 7);
    request.onupgradeneeded = () => taskSpaceEnsureStores(request.result);
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      if (!db.objectStoreNames.contains("metadata-updates")) {
        resolve(false);
        return;
      }
      const transaction = db.transaction("metadata-updates", "readwrite");
      transaction.objectStore("metadata-updates").delete(taskSpaceKeyFor(principal, `${spaceId}:${operationId}`));
      transaction.onerror = () => reject(transaction.error || new Error("Could not acknowledge metadata update"));
      transaction.oncomplete = () => resolve(true);
    };
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
  });
}

const taskSpaceSyncSources = new Map();
const taskSpaceSyncResetTimers = new Map();
const taskSpaceSyncTransport = {
  connected: false,
  connections: 0,
  reconnects: 0,
  resets: 0,
  lastEventId: null,
  lastEventAt: null,
  lastOpenAt: null,
  lastErrorAt: null,
};
const taskSpaceSafetyTimers = new Map();
const taskSpaceLeaseTimers = new Map();
const taskSpaceCrdtWriteSequences = new Map();
const taskSpaceLeaseOwner = globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random()}`;
let taskSpaceBroadcast = null;
let taskSpaceWebLock = null;
let taskSpaceWebLockRequest = null;
const taskSpaceLeaseTokens = new Map();

function taskSpaceNextCrdtWriteSequence(principal, spaceId) {
  const key = taskSpaceSyncRecordKey(principal, spaceId);
  const next = (Number(taskSpaceCrdtWriteSequences.get(key)) || 0) + 1;
  taskSpaceCrdtWriteSequences.set(key, next);
  return next;
}

function taskSpaceAcquireLocalStorageLease(principal) {
  return new Promise((resolve) => {
    const key = taskSpaceKeyFor(principal, "sync-lease");
    const now = Date.now();
    let current = null;
    try {
      current = JSON.parse(globalThis.localStorage?.getItem(key) || "null");
    } catch (_) {}
    if (current && current.expiresAt > now && current.owner !== taskSpaceLeaseOwner) {
      resolve(false);
      return;
    }
    const currentEpoch = Number(current?.epoch) || 0;
    const previousToken = taskSpaceLeaseTokens.get(key);
    const currentLeaseIsLive = current?.owner === taskSpaceLeaseOwner
      && Number(current?.expiresAt) > now
      && currentEpoch > 0;
    const epoch = currentLeaseIsLive
      ? currentEpoch
      : Math.max(currentEpoch, Number(previousToken?.epoch) || 0) + 1;
    const lease = {
      owner: taskSpaceLeaseOwner,
      epoch,
      expiresAt: now + 12_000,
    };
    try {
      globalThis.localStorage?.setItem(key, JSON.stringify(lease));
      const verified = JSON.parse(globalThis.localStorage?.getItem(key) || "null");
      if (verified?.owner !== lease.owner || Number(verified?.epoch) !== lease.epoch) {
        resolve(false);
        return;
      }
      taskSpaceLeaseTokens.set(key, lease);
      if (!taskSpaceLeaseTimers.has(key)) {
        const timer = setInterval(() => taskSpaceAcquireSyncLease(principal), 4_000);
        taskSpaceLeaseTimers.set(key, timer);
      }
      resolve(true);
    } catch (_) {
      // Private browsing/storage-disabled environments must still sync; the
      // server-side mutation idempotency guard is the fallback coordinator.
      taskSpaceLeaseTokens.set(key, {
        owner: taskSpaceLeaseOwner,
        epoch,
        expiresAt: Number.POSITIVE_INFINITY,
        storageDisabled: true,
      });
      resolve(true);
    }
  });
}

function taskSpaceWebLockIsCurrent(principal) {
  const normalizedPrincipal = String(principal || "guest");
  if (taskSpaceWebLock?.principal !== normalizedPrincipal) return false;
  const key = taskSpaceKeyFor(normalizedPrincipal, "sync-lease");
  const token = taskSpaceLeaseTokens.get(key);
  // When storage is unavailable, the Web Lock and server-side mutation
  // idempotency are the only available fence. In storage-capable profiles,
  // the expiring lease below is what lets another tab recover from a
  // backgrounded/suspended lock holder.
  if (token?.storageDisabled) return true;
  try {
    const current = JSON.parse(globalThis.localStorage?.getItem(key) || "null");
    return current?.owner === token?.owner
      && Number(current?.epoch) === Number(token?.epoch)
      && Number(current?.expiresAt) > Date.now();
  } catch (_) {
    return true;
  }
}

function taskSpaceReleaseWebLock(principal) {
  if (taskSpaceWebLock?.principal !== String(principal || "guest")) return;
  const release = taskSpaceWebLock.release;
  taskSpaceWebLock = null;
  if (typeof release === "function") release();
}

function taskSpaceAcquireWebLock(principal) {
  const locks = globalThis.navigator?.locks;
  if (!locks || typeof locks.request !== "function") return null;
  const normalizedPrincipal = String(principal || "guest");
  if (taskSpaceWebLock?.principal === normalizedPrincipal) {
    // Renew the expiring lease even when the Web Lock is already held. If a
    // different tab acquired the lease after this tab was suspended, this
    // resolves false and the stale holder releases its Web Lock below.
    return taskSpaceAcquireLocalStorageLease(normalizedPrincipal).then((owned) => {
      if (!owned) taskSpaceReleaseWebLock(normalizedPrincipal);
      return owned;
    });
  }
  if (taskSpaceWebLockRequest?.principal === normalizedPrincipal) {
    return taskSpaceWebLockRequest.promise;
  }

  let settle;
  const promise = new Promise((resolve) => { settle = resolve; });
  taskSpaceWebLockRequest = { principal: normalizedPrincipal, promise };
  try {
    locks.request(
      `task-space-sync:${normalizedPrincipal}`,
      { mode: "exclusive", ifAvailable: true },
      (lock) => {
        if (!lock) {
          settle(false);
          return undefined;
        }
        let release;
        const hold = new Promise((resolve) => { release = resolve; });
        taskSpaceWebLock = { principal: normalizedPrincipal, release };
        // A Web Lock by itself has no expiry: a suspended document can keep
        // it forever. Pair it with the renewable local-storage lease so a
        // different tab can take over after the heartbeat expires.
        taskSpaceAcquireLocalStorageLease(normalizedPrincipal).then((owned) => {
          if (!owned) taskSpaceReleaseWebLock(normalizedPrincipal);
          settle(owned);
        });
        return hold;
      },
    ).catch(() => {
      if (taskSpaceWebLockRequest?.promise === promise) {
        taskSpaceWebLockRequest = null;
        taskSpaceAcquireLocalStorageLease(normalizedPrincipal).then(settle);
      }
    });
  } catch (_) {
    if (taskSpaceWebLockRequest?.promise === promise) {
      taskSpaceWebLockRequest = null;
      taskSpaceAcquireLocalStorageLease(normalizedPrincipal).then(settle);
    }
  }
  promise.then(() => {
    if (taskSpaceWebLockRequest?.promise === promise) taskSpaceWebLockRequest = null;
  });
  return promise;
}

export function taskSpaceAcquireSyncLease(principal) {
  const webLock = taskSpaceAcquireWebLock(principal);
  if (!webLock) return taskSpaceAcquireLocalStorageLease(principal);
  // `ifAvailable` can report no Web Lock while a suspended tab still holds
  // it. The expiring storage lease remains the liveness fallback in that
  // case; temporary dual coordinators are harmless because mutation claims
  // make server effects idempotent.
  return webLock.then((owned) => owned || taskSpaceAcquireLocalStorageLease(principal));
}

export function taskSpaceReleaseSyncLease(principal) {
  const key = taskSpaceKeyFor(principal, "sync-lease");
  taskSpaceLeaseTokens.delete(key);
  const timer = taskSpaceLeaseTimers.get(key);
  if (timer) {
    clearInterval(timer);
    taskSpaceLeaseTimers.delete(key);
  }
  taskSpaceReleaseWebLock(principal);
  try {
    const current = JSON.parse(globalThis.localStorage?.getItem(key) || "null");
    if (current?.owner === taskSpaceLeaseOwner) globalThis.localStorage?.removeItem(key);
  } catch (_) {}
}

export function taskSpaceIsSyncLeaseOwner(principal) {
  const normalizedPrincipal = String(principal || "guest");
  if (taskSpaceWebLock?.principal === normalizedPrincipal) {
    if (taskSpaceWebLockIsCurrent(normalizedPrincipal)) return true;
    // A stale Web Lock holder must release the browser lock before another
    // tab can acquire it. The local lease check below intentionally remains
    // false until this tab successfully renews ownership.
    taskSpaceReleaseWebLock(normalizedPrincipal);
  }
  const key = taskSpaceKeyFor(normalizedPrincipal, "sync-lease");
  const token = taskSpaceLeaseTokens.get(key);
  if (!token) return false;
  if (token.storageDisabled) return true;
  try {
    const current = JSON.parse(globalThis.localStorage?.getItem(key) || "null");
    return current?.owner === token.owner
      && Number(current?.epoch) === Number(token.epoch)
      && Number(current?.expiresAt) > Date.now();
  } catch (_) {
    // Storage-disabled environments use the server's idempotency guard as
    // the last-resort coordinator. Do not disable sync solely because the
    // fencing record cannot be read.
    return true;
  }
}

export function taskSpaceSyncLeaseEpoch(principal) {
  const normalizedPrincipal = String(principal || "guest");
  if (taskSpaceWebLock?.principal === normalizedPrincipal) return 0;
  const token = taskSpaceLeaseTokens.get(taskSpaceKeyFor(normalizedPrincipal, "sync-lease"));
  return Number(token?.epoch) || 0;
}

export function taskSpaceSyncLeaseMode(principal) {
  const normalizedPrincipal = String(principal || "guest");
  if (taskSpaceWebLock?.principal === normalizedPrincipal) return "web-lock";
  const token = taskSpaceLeaseTokens.get(taskSpaceKeyFor(normalizedPrincipal, "sync-lease"));
  if (token?.storageDisabled) return "server-idempotency-fallback";
  return token ? "local-storage-epoch" : "none";
}

export function taskSpaceCurrentSyncCursor(principal) {
  const key = `task-space:sync-cursor:${String(principal || "guest")}`;
  try {
    return globalThis.localStorage?.getItem(key) || "0";
  } catch (_) {
    return "0";
  }
}

export function taskSpaceRebaseMetadataUpdate(principal, spaceId, operationId, expectedVersion) {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 7);
    request.onupgradeneeded = () => taskSpaceEnsureStores(request.result);
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
    request.onsuccess = () => {
      const db = taskSpaceConfigureDb(request.result);
      if (!db.objectStoreNames.contains("metadata-updates")) {
        resolve(false);
        return;
      }
      const transaction = db.transaction("metadata-updates", "readwrite");
      const key = taskSpaceKeyFor(principal, `${spaceId}:${operationId}`);
      const store = transaction.objectStore("metadata-updates");
      const read = store.get(key);
      read.onerror = () => reject(read.error || new Error("Could not read metadata update"));
      read.onsuccess = () => {
        if (!read.result) {
          resolve(false);
          return;
        }
        read.result.expectedVersion = expectedVersion == null ? null : Number(expectedVersion);
        store.put(read.result, key);
      };
      transaction.onerror = () => reject(transaction.error || new Error("Could not rebase metadata update"));
      transaction.oncomplete = () => resolve(true);
    };
  });
}

export function taskSpaceStartSyncBroadcast(principal, onHint) {
  if (typeof globalThis.BroadcastChannel !== "function") return false;
  try {
    if (taskSpaceBroadcast) taskSpaceBroadcast.close();
    taskSpaceBroadcast = new BroadcastChannel(`task-space-sync:${String(principal || "guest")}`);
    taskSpaceBroadcast.onmessage = (event) => {
      if (event.data === "sync") onHint();
    };
    return true;
  } catch (_) {
    taskSpaceBroadcast = null;
    return false;
  }
}

function taskSpaceLocalChannelName(principal) {
  return `${taskSpaceLocalBroadcastProtocolVersion}:${taskSpaceLocalBroadcastPrincipal}:${String(principal || "guest")}`;
}

function taskSpaceLocalStorageKey(principal) {
  return `${taskSpaceLocalStorageSyncKeyBase}:${String(principal || "guest")}`;
}

function taskSpaceInstallLocalStorageListener(onUpdate) {
  if (typeof globalThis.addEventListener !== "function") return false;
  taskSpaceLocalStorageListener = (event) => {
    if (event.key === taskSpaceLocalStorageSyncKey && event.newValue) {
      onUpdate(event.newValue);
    }
  };
  globalThis.addEventListener("storage", taskSpaceLocalStorageListener);
  return true;
}

function taskSpaceOpenLocalBroadcast(onUpdate) {
  if (taskSpaceLocalBroadcast) taskSpaceLocalBroadcast.close();
  taskSpaceLocalBroadcast = null;
  if (typeof globalThis.BroadcastChannel === "function") {
    try {
      taskSpaceLocalBroadcast = new BroadcastChannel(
        taskSpaceLocalChannelName(taskSpaceLocalBroadcastPrincipal),
      );
      taskSpaceLocalBroadcast.onmessage = (event) => {
        onUpdate(event.data);
      };
    } catch (_) {
      taskSpaceLocalBroadcast = null;
    }
  }
}

// Local tab replication is deliberately independent from authentication and
// SSE. IndexedDB and BroadcastChannel are the local-first layer: an offline
// tab must receive the actual CRDT update bytes from another tab instead of
// receiving only a request to contact the server. The transport is scoped to
// the current principal so an account switch cannot expose old-account
// payloads to a new-account tab on the same origin.
export function taskSpaceStartLocalBroadcast(principal, onUpdate) {
  taskSpaceLocalBroadcastPrincipal = String(principal || "guest");
  taskSpaceLocalStorageSyncKey = taskSpaceLocalStorageKey(taskSpaceLocalBroadcastPrincipal);
  taskSpaceLocalBroadcastOnUpdate = onUpdate;
  if (taskSpaceLocalStorageListener) {
    globalThis.removeEventListener?.("storage", taskSpaceLocalStorageListener);
    taskSpaceLocalStorageListener = null;
  }
  taskSpaceOpenLocalBroadcast(onUpdate);
  // Keep the storage-event fallback active even when BroadcastChannel exists.
  // Some browser profiles expose the API but suppress delivery in private,
  // suspended, or partitioned contexts. Duplicate delivery is harmless: the
  // CRDT update is idempotent and metadata application is state-idempotent.
  const hasStorageListener = taskSpaceInstallLocalStorageListener(onUpdate);
  return Boolean(taskSpaceLocalBroadcast || hasStorageListener);
}

export function taskSpaceSetLocalBroadcastPrincipal(principal) {
  if (!taskSpaceLocalBroadcastOnUpdate) return false;
  taskSpaceLocalBroadcastPrincipal = String(principal || "guest");
  taskSpaceLocalStorageSyncKey = taskSpaceLocalStorageKey(taskSpaceLocalBroadcastPrincipal);
  if (taskSpaceLocalStorageListener) {
    globalThis.removeEventListener?.("storage", taskSpaceLocalStorageListener);
    taskSpaceLocalStorageListener = null;
  }
  taskSpaceOpenLocalBroadcast(taskSpaceLocalBroadcastOnUpdate);
  const hasStorageListener = taskSpaceInstallLocalStorageListener(taskSpaceLocalBroadcastOnUpdate);
  return Boolean(taskSpaceLocalBroadcast || hasStorageListener);
}

function taskSpaceSendLocalPayload(payload) {
  if (taskSpaceLocalBroadcast) {
    taskSpaceLocalBroadcast.postMessage(payload);
  }
  try {
    globalThis.localStorage?.setItem(taskSpaceLocalStorageSyncKey, payload);
  } catch (_) {
    // BroadcastChannel remains available when localStorage is blocked in
    // private/restricted browser modes.
  }
}

export function taskSpacePublishLocalUpdate(principal, spaceId, localGeneration, encodedUpdate, originDeviceId) {
  if ((!taskSpaceLocalBroadcast && !taskSpaceLocalStorageListener) || !encodedUpdate) return;
  taskSpaceSendLocalPayload(JSON.stringify({
    protocolVersion: taskSpaceLocalBroadcastProtocolVersion,
    principal: String(principal || "guest"),
    spaceId: Number(spaceId),
    localGeneration: Number(localGeneration) || 0,
    originDeviceId: String(originDeviceId || ""),
    originTabId: taskSpaceTabId,
    update: String(encodedUpdate),
  }));
}

export function taskSpacePublishLocalMetadata(principal, spaceId, operation, name, operationId, originDeviceId) {
  if (!taskSpaceLocalBroadcast && !taskSpaceLocalStorageListener) return;
  taskSpaceSendLocalPayload(JSON.stringify({
    protocolVersion: taskSpaceLocalBroadcastProtocolVersion,
    kind: "metadata",
    principal: String(principal || "guest"),
    spaceId: Number(spaceId),
    operation: String(operation || ""),
    name: name == null ? null : String(name),
    operationId: operationId == null ? null : String(operationId),
    originDeviceId: String(originDeviceId || ""),
    originTabId: taskSpaceTabId,
  }));
}

export function taskSpacePublishLocalSpace(principal, spaceId, name, stableSpaceId, originDeviceId) {
  if (!taskSpaceLocalBroadcast && !taskSpaceLocalStorageListener) return;
  taskSpaceSendLocalPayload(JSON.stringify({
    protocolVersion: taskSpaceLocalBroadcastProtocolVersion,
    kind: "space",
    principal: String(principal || "guest"),
    spaceId: Number(spaceId),
    operation: "create",
    name: name == null ? null : String(name),
    stableSpaceId: stableSpaceId == null ? null : String(stableSpaceId),
    originDeviceId: String(originDeviceId || ""),
    originTabId: taskSpaceTabId,
  }));
}

export function taskSpaceCurrentTabId() {
  return taskSpaceTabId;
}

export function taskSpaceStopLocalBroadcast() {
  if (taskSpaceLocalBroadcast) {
    taskSpaceLocalBroadcast.close();
    taskSpaceLocalBroadcast = null;
  }
  if (taskSpaceLocalStorageListener) {
    globalThis.removeEventListener?.("storage", taskSpaceLocalStorageListener);
    taskSpaceLocalStorageListener = null;
  }
  taskSpaceLocalBroadcastOnUpdate = null;
}

export function taskSpacePublishSyncHint() {
  if (taskSpaceBroadcast) taskSpaceBroadcast.postMessage("sync");
}

export function taskSpaceStopSyncBroadcast() {
  if (!taskSpaceBroadcast) return;
  taskSpaceBroadcast.close();
  taskSpaceBroadcast = null;
}

export function taskSpaceStartSyncSafetyTimer(onTick) {
  // SSE is a latency optimization, not the correctness path. Keep a bounded
  // coordinator-only server reconciliation timer so a proxy can silently
  // buffer/drop an EventSource stream without leaving another browser stale.
  const id = setInterval(onTick, 15_000);
  taskSpaceSafetyTimers.set(id, onTick);
  return id;
}

export function taskSpaceStopSyncSafetyTimer(id) {
  clearInterval(id);
  taskSpaceSafetyTimers.delete(id);
}

export function taskSpaceStartLocalRefresh(onTick) {
  return setInterval(onTick, 10_000);
}

export function taskSpaceStopLocalRefresh(id) {
  clearInterval(id);
}

export function taskSpaceStartAccountRefresh(onTick) {
  return setInterval(onTick, 60_000);
}

export function taskSpaceStopAccountRefresh(id) {
  clearInterval(id);
}

export function taskSpaceStartSyncEvents(url, onUpdate, onOpen, onError) {
  taskSpaceStopSyncEvents(url);
  const cursorKey = `task-space:sync-cursor:${taskSpaceSyncPrincipal}`;
  let cursor = null;
  try {
    cursor = globalThis.localStorage?.getItem(cursorKey);
  } catch (_) {}
  const eventUrl = cursor
    ? `${url}${url.includes("?") ? "&" : "?"}after_event_id=${encodeURIComponent(cursor)}`
    : url;
  taskSpaceSyncTransport.connections += 1;
  if (cursor || taskSpaceSyncTransport.connections > 1) {
    taskSpaceSyncTransport.reconnects += 1;
  }
  let source;
  try {
    source = new EventSource(eventUrl, { withCredentials: true });
  } catch (_) {
    taskSpaceSyncTransport.connected = false;
    taskSpaceSyncTransport.lastErrorAt = Date.now();
    onError();
    return false;
  }
  const update = (event) => {
    taskSpaceSyncTransport.lastEventId = event.lastEventId || taskSpaceSyncTransport.lastEventId;
    taskSpaceSyncTransport.lastEventAt = Date.now();
    onUpdate(event.data);
  };
  const reset = () => {
    taskSpaceSyncTransport.connected = false;
    taskSpaceSyncTransport.resets += 1;
    try {
      globalThis.localStorage?.removeItem(cursorKey);
    } catch (_) {}
    // EventSource keeps its own Last-Event-ID across reconnects. Closing and
    // recreating the source is required; merely clearing localStorage would
    // otherwise let the browser send the expired cursor again after the next
    // network flap and repeatedly trigger the reset path.
    const current = taskSpaceSyncSources.get(url);
    if (current?.source === source) taskSpaceSyncSources.delete(url);
    source.close();
    onOpen();
    const timer = globalThis.setTimeout(() => {
      if (taskSpaceSyncResetTimers.get(url) !== timer) return;
      taskSpaceSyncResetTimers.delete(url);
      if (!taskSpaceSyncSources.has(url)) {
        taskSpaceStartSyncEvents(url, onUpdate, onOpen, onError);
      }
    }, 0);
    taskSpaceSyncResetTimers.set(url, timer);
  };
  source.addEventListener("space-update", update);
  source.addEventListener("sync-reset", reset);
  source.onopen = () => {
    taskSpaceSyncTransport.connected = true;
    taskSpaceSyncTransport.lastOpenAt = Date.now();
    onOpen();
  };
  source.onerror = () => {
    taskSpaceSyncTransport.connected = false;
    taskSpaceSyncTransport.lastErrorAt = Date.now();
    // Keep the cursor across transient disconnects. The server emits an
    // explicit sync-reset event when retention has made a cursor unreplayable.
    const now = Date.now();
    if (!source.__taskSpaceLastErrorAt || now - source.__taskSpaceLastErrorAt > 5_000) {
      source.__taskSpaceLastErrorAt = now;
      onError();
    }
  };
  taskSpaceSyncSources.set(url, { source, update, reset });
  return true;
}

export function taskSpaceStopSyncEvents(url) {
  const resetTimer = taskSpaceSyncResetTimers.get(url);
  if (resetTimer != null) {
    globalThis.clearTimeout(resetTimer);
    taskSpaceSyncResetTimers.delete(url);
  }
  const existing = taskSpaceSyncSources.get(url);
  if (!existing) return;
  existing.source.removeEventListener("space-update", existing.update);
  existing.source.removeEventListener("sync-reset", existing.reset);
  existing.source.close();
  taskSpaceSyncTransport.connected = false;
  taskSpaceSyncSources.delete(url);
}

export function taskSpaceSyncTransportDiagnostics() {
  return { ...taskSpaceSyncTransport };
}

export function taskSpaceSaveSyncCursor(cursor) {
  if (!cursor) return;
  try {
    const key = `task-space:sync-cursor:${taskSpaceSyncPrincipal}`;
    const next = Number(cursor);
    const current = Number(globalThis.localStorage?.getItem(key));
    if (Number.isFinite(next) && (!Number.isFinite(current) || next > current)) {
      globalThis.localStorage?.setItem(key, String(next));
    }
  } catch (_) {}
}
"#)]
unsafe extern "C" {
    #[wasm_bindgen(js_name = taskSpaceSetSyncPrincipal)]
    fn indexed_db_set_sync_principal(principal: &str) -> String;

    #[wasm_bindgen(js_name = taskSpaceLoadWorkspace)]
    fn indexed_db_load_workspace() -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceSaveWorkspace)]
    fn indexed_db_save_workspace(raw: &str, principal: &str) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceExportIndexedDbBackup)]
    fn indexed_db_export_backup(principal: &str) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceLoadCrdt)]
    fn indexed_db_load_crdt(space_id: u64) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceLoadCrdtOutbox)]
    fn indexed_db_load_crdt_outbox(space_id: u64) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceQueueIncomingCrdtUpdate)]
    fn indexed_db_queue_incoming_crdt_update(
        principal: &str,
        space_id: u64,
        origin_device_id: &str,
        local_generation: u64,
        encoded_update: &str,
    ) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceSaveCrdt)]
    fn indexed_db_save_crdt(
        principal: &str,
        space_id: u64,
        encoded_snapshot: &str,
        snapshot_generation: u64,
        origin_device_id: &str,
        origin_generation: u64,
    ) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceQueueCrdtUpdate)]
    fn indexed_db_queue_crdt_update(
        principal: &str,
        space_id: u64,
        mutation_id: &str,
        device_id: &str,
        last_server_sequence: u64,
        state_vector: &str,
        encoded_update: &str,
        encoded_snapshot: &str,
        local_generation: u64,
        workspace_raw: Option<&str>,
    ) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceSaveSyncState)]
    fn indexed_db_save_sync_state(
        principal: &str,
        space_id: u64,
        state_vector: &str,
    ) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceCommitCrdtPull)]
    fn indexed_db_commit_crdt_pull(
        principal: &str,
        space_id: u64,
        encoded_snapshot: &str,
        state_vector: &str,
        local_generation: u64,
        inbox_keys_json: &str,
    ) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceLoadSyncState)]
    fn indexed_db_load_sync_state(principal: &str, space_id: u64) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceLoadCrdtUpdates)]
    fn indexed_db_load_crdt_updates() -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceLoadSyncErrors)]
    fn indexed_db_load_sync_errors() -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceLoadSyncDiagnostics)]
    fn indexed_db_load_sync_diagnostics(principal: &str, space_id: u64) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceAckCrdtUpdate)]
    fn indexed_db_ack_crdt_update(
        principal: &str,
        space_id: u64,
        mutation_id: &str,
    ) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceCommitCrdtReconcile)]
    fn indexed_db_commit_crdt_reconcile(
        principal: &str,
        space_id: u64,
        mutation_id: &str,
        encoded_snapshot: &str,
        state_vector: &str,
        acknowledged_generation: u64,
        inbox_keys_json: &str,
    ) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceQueueMetadataUpdate)]
    fn indexed_db_queue_metadata_update(
        principal: &str,
        space_id: u64,
        operation_id: &str,
        operation: &str,
        name: Option<&str>,
        expected_version: Option<u64>,
        workspace_raw: Option<&str>,
    ) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceLoadMetadataUpdates)]
    fn indexed_db_load_metadata_updates() -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceAckMetadataUpdate)]
    fn indexed_db_ack_metadata_update(
        principal: &str,
        space_id: u64,
        operation_id: &str,
    ) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceRebaseMetadataUpdate)]
    fn indexed_db_rebase_metadata_update(
        principal: &str,
        space_id: u64,
        operation_id: &str,
        expected_version: Option<u64>,
    ) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceStartSyncEvents)]
    fn start_sync_events(
        url: &str,
        on_update: &js_sys::Function,
        on_open: &js_sys::Function,
        on_error: &js_sys::Function,
    ) -> bool;

    #[wasm_bindgen(js_name = taskSpaceStopSyncEvents)]
    fn stop_sync_events(url: &str);

    #[wasm_bindgen(js_name = taskSpaceSaveSyncCursor)]
    fn save_sync_cursor(cursor: u64);

    #[wasm_bindgen(js_name = taskSpaceSyncTransportDiagnostics)]
    fn sync_transport_diagnostics() -> JsValue;

    #[wasm_bindgen(js_name = taskSpaceAcquireSyncLease)]
    fn acquire_sync_lease(principal: &str) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceIsSyncLeaseOwner)]
    fn is_sync_lease_owner(principal: &str) -> bool;

    #[wasm_bindgen(js_name = taskSpaceSyncLeaseEpoch)]
    fn sync_lease_epoch(principal: &str) -> f64;

    #[wasm_bindgen(js_name = taskSpaceSyncLeaseMode)]
    fn sync_lease_mode(principal: &str) -> String;

    #[wasm_bindgen(js_name = taskSpaceCurrentSyncCursor)]
    fn current_sync_cursor(principal: &str) -> String;

    #[wasm_bindgen(js_name = taskSpaceReleaseSyncLease)]
    fn release_sync_lease(principal: &str);

    #[wasm_bindgen(js_name = taskSpaceStartSyncBroadcast)]
    fn start_sync_broadcast(principal: &str, on_hint: &js_sys::Function) -> bool;

    #[wasm_bindgen(js_name = taskSpaceStartLocalBroadcast)]
    fn start_local_broadcast(principal: &str, on_update: &js_sys::Function) -> bool;

    #[wasm_bindgen(js_name = taskSpaceSetLocalBroadcastPrincipal)]
    fn set_local_broadcast_principal(principal: &str) -> bool;

    #[wasm_bindgen(js_name = taskSpacePublishLocalUpdate)]
    fn publish_local_update(
        principal: &str,
        space_id: u64,
        local_generation: u64,
        encoded_update: &str,
        origin_device_id: &str,
    );

    #[wasm_bindgen(js_name = taskSpacePublishLocalMetadata)]
    fn publish_local_metadata(
        principal: &str,
        space_id: u64,
        operation: &str,
        name: Option<&str>,
        operation_id: &str,
        origin_device_id: &str,
    );

    #[wasm_bindgen(js_name = taskSpacePublishLocalSpace)]
    fn publish_local_space(
        principal: &str,
        space_id: u64,
        name: &str,
        stable_space_id: &str,
        origin_device_id: &str,
    );

    #[wasm_bindgen(js_name = taskSpaceCurrentTabId)]
    fn current_tab_id() -> String;

    #[wasm_bindgen(js_name = taskSpaceStopLocalBroadcast)]
    fn stop_local_broadcast();

    #[wasm_bindgen(js_name = taskSpacePublishSyncHint)]
    fn publish_sync_hint();

    #[wasm_bindgen(js_name = taskSpaceStopSyncBroadcast)]
    fn stop_sync_broadcast();

    #[wasm_bindgen(js_name = taskSpaceStartSyncSafetyTimer)]
    fn start_sync_safety_timer(on_tick: &js_sys::Function) -> i32;

    #[wasm_bindgen(js_name = taskSpaceStopSyncSafetyTimer)]
    fn stop_sync_safety_timer(timer_id: i32);

    #[wasm_bindgen(js_name = taskSpaceStartLocalRefresh)]
    fn start_local_refresh(on_tick: &js_sys::Function) -> i32;

    #[wasm_bindgen(js_name = taskSpaceStopLocalRefresh)]
    fn stop_local_refresh(timer_id: i32);

    #[wasm_bindgen(js_name = taskSpaceStartAccountRefresh)]
    fn start_account_refresh(on_tick: &js_sys::Function) -> i32;

    #[wasm_bindgen(js_name = taskSpaceStopAccountRefresh)]
    fn stop_account_refresh(timer_id: i32);
}

fn note_color_background(color: NoteColor) -> &'static str {
    match color {
        NoteColor::Yellow => "var(--color-note-yellow)",
        NoteColor::Pink => "var(--color-note-pink)",
        NoteColor::Blue => "var(--color-note-blue)",
        NoteColor::Green => "var(--color-note-green)",
        NoteColor::Lavender => "var(--color-note-lav)",
    }
}

fn note_color_ink(color: NoteColor) -> &'static str {
    match color {
        NoteColor::Yellow => "var(--color-note-ink-yellow)",
        NoteColor::Pink => "var(--color-note-ink-pink)",
        NoteColor::Blue => "var(--color-note-ink-blue)",
        NoteColor::Green => "var(--color-note-ink-green)",
        NoteColor::Lavender => "var(--color-note-ink-lav)",
    }
}

fn due_date_label(due_date: Option<&str>, overdue: bool) -> String {
    let Some(date) = due_date else {
        return "add due".into();
    };
    let mut parts = date.split('-');
    let (Some(_year), Some(month), Some(day)) = (parts.next(), parts.next(), parts.next()) else {
        return date.into();
    };
    let month = match month {
        "01" => "Jan",
        "02" => "Feb",
        "03" => "Mar",
        "04" => "Apr",
        "05" => "May",
        "06" => "Jun",
        "07" => "Jul",
        "08" => "Aug",
        "09" => "Sep",
        "10" => "Oct",
        "11" => "Nov",
        "12" => "Dec",
        _ => return date.into(),
    };
    if overdue {
        format!("overdue · {month} {day}")
    } else {
        format!("due {month} {day}")
    }
}

fn today_date() -> String {
    let today = js_sys::Date::new_0();
    format!(
        "{:04}-{:02}-{:02}",
        today.get_full_year(),
        today.get_month() + 1,
        today.get_date()
    )
}

fn is_overdue(due_date: Option<&str>, status: NoteStatus) -> bool {
    status != NoteStatus::Done
        && due_date
            .is_some_and(|date| parse_due_date(date).is_some() && date < today_date().as_str())
}

fn parse_due_date(due_date: &str) -> Option<(i32, u32, u32)> {
    let mut parts = due_date.split('-');
    Some((
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
    ))
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

fn first_weekday(year: i32, month: u32) -> u32 {
    // Sakamoto's algorithm; Sunday is zero, matching the calendar headings.
    let month_offsets = [0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4];
    let adjusted_year = year - i32::from(month < 3);
    ((adjusted_year + adjusted_year / 4 - adjusted_year / 100
        + adjusted_year / 400
        + month_offsets[(month - 1) as usize]
        + 1)
        % 7) as u32
}

fn calendar_days(year: i32, month: u32) -> Vec<Option<u32>> {
    let mut days = vec![None; first_weekday(year, month) as usize];
    days.extend((1..=days_in_month(year, month)).map(Some));
    days
}

fn month_name(month: u32) -> &'static str {
    match month {
        1 => "January",
        2 => "February",
        3 => "March",
        4 => "April",
        5 => "May",
        6 => "June",
        7 => "July",
        8 => "August",
        9 => "September",
        10 => "October",
        11 => "November",
        12 => "December",
        _ => "Month",
    }
}

fn shift_month(year: i32, month: u32, delta: i32) -> (i32, u32) {
    let index = year * 12 + month as i32 - 1 + delta;
    (index.div_euclid(12), index.rem_euclid(12) as u32 + 1)
}

fn set_note_due_date(
    id: u64,
    due_date: Option<String>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    history: RwSignal<History>,
    editing: RwSignal<Option<u64>>,
    edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
) {
    commit_pending_edit(notes, groups, history, editing, edit_snapshot);
    mutate_notes(notes, groups, history, |items| {
        if let Some(note) = items.iter_mut().find(|note| note.id == id) {
            note.due_date = due_date;
        }
    });
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum ContextMenuTarget {
    Board,
    Note(u64),
    Group(u64),
    Space(u64),
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct ContextMenuState {
    target: ContextMenuTarget,
    x: i32,
    y: i32,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct ViewState {
    pan: (f64, f64),
    zoom: f64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum StorageStatus {
    Saved,
    Saving,
    Error,
}

#[derive(Clone, Default)]
struct History {
    undo: Vec<BoardData>,
    redo: Vec<BoardData>,
}

fn note_position(index: usize) -> (f64, f64) {
    let column = index % 5;
    let row = index / 5;
    let x = (column as f64 - 2.0) * 220.0;
    let y = (row as f64 - 2.0) * 190.0;
    (x, y)
}

fn viewport_note_position(pan: (f64, f64), zoom: f64) -> (f64, f64) {
    // Notes use their top-left corner as the world-space anchor. Place the
    // next note at the camera centre, with a small adjustment to centre the
    // card itself in the viewport.
    let zoom = zoom.max(0.01);
    (
        -pan.0 / zoom - NOTE_WIDTH / 2.0 / zoom,
        -pan.1 / zoom - NOTE_HEIGHT / 2.0 / zoom,
    )
}

fn parse_board(raw: &str) -> Option<BoardData> {
    let value = serde_json::from_str::<serde_json::Value>(raw).ok()?;
    if value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .is_some_and(|version| version > u64::from(CURRENT_SCHEMA_VERSION))
    {
        // Preserve the raw record for a newer client rather than silently
        // reinterpreting fields under an older schema.
        return None;
    }
    let mut board = serde_json::from_value::<BoardData>(value.clone())
        .ok()
        .or_else(|| {
            serde_json::from_value::<Vec<Note>>(value.clone())
                .ok()
                .map(|notes| BoardData {
                    schema_version: CURRENT_SCHEMA_VERSION,
                    notes,
                    groups: Vec::new(),
                    tombstones: Vec::new(),
                })
        })?;

    // v2 stored a boolean `done` field. Preserve completed notes when loading
    // that format while new saves use the three-state status field.
    let saved_notes = value
        .get("notes")
        .and_then(serde_json::Value::as_array)
        .or_else(|| value.as_array());
    if let Some(saved_notes) = saved_notes {
        for (note, saved) in board.notes.iter_mut().zip(saved_notes) {
            if saved.get("status").is_none()
                && saved.get("done").and_then(serde_json::Value::as_bool) == Some(true)
            {
                note.status = NoteStatus::Done;
            }
        }
    }

    Some(board)
}

fn parse_workspace(raw: &str) -> Option<WorkspaceData> {
    let value = serde_json::from_str::<serde_json::Value>(raw).ok()?;
    if value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .is_some_and(|version| version > u64::from(CURRENT_SCHEMA_VERSION))
    {
        return None;
    }
    let workspace = serde_json::from_value::<WorkspaceData>(value).ok()?;
    (!workspace.spaces.is_empty()).then(|| normalize_workspace(workspace))
}

fn workspace_from_board(board: BoardData) -> WorkspaceData {
    let now = now_millis();
    let (initial_space_id, initial_stable_id) = initial_space_identity();
    WorkspaceData {
        schema_version: CURRENT_SCHEMA_VERSION,
        device_id: load_device_id(),
        tombstones: Vec::new(),
        spaces: vec![Space {
            id: initial_space_id,
            stable_id: initial_stable_id,
            name: "my space".into(),
            metadata_version: 0,
            archived: false,
            created_at: now,
            updated_at: now,
            deleted_at: None,
            board,
        }],
        active_space_id: 0,
    }
}

fn parse_indexed_db_value(value: JsValue) -> Option<WorkspaceData> {
    let raw = value.as_string().or_else(|| {
        js_sys::JSON::stringify(&value)
            .ok()
            .and_then(|json| json.as_string())
    })?;
    mark_future_schema(&raw);
    parse_workspace(&raw)
        .or_else(|| {
            parse_board(&raw)
                .filter(|board| !board.notes.is_empty() || !board.groups.is_empty())
                .map(workspace_from_board)
        })
        .or_else(|| parse_indexed_db_json(&raw))
}

fn mark_future_schema(raw: &str) {
    let is_future = serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .and_then(|value| {
            value
                .get("schema_version")
                .and_then(serde_json::Value::as_u64)
        })
        .is_some_and(|version| version > u64::from(CURRENT_SCHEMA_VERSION));
    if is_future {
        STORAGE_SCHEMA_BLOCKED.with(|blocked| blocked.set(true));
    }
}

fn storage_schema_blocked() -> bool {
    STORAGE_SCHEMA_BLOCKED.with(Cell::get)
}

fn clear_storage_schema_blocked() {
    STORAGE_SCHEMA_BLOCKED.with(|blocked| blocked.set(false));
}

fn storage_repair_required() -> bool {
    STORAGE_REPAIR_REQUIRED.with(Cell::get)
}

fn storage_writes_blocked() -> bool {
    storage_schema_blocked() || storage_repair_required()
}

fn mark_storage_repair_required() {
    STORAGE_REPAIR_REQUIRED.with(|required| required.set(true));
}

fn clear_storage_repair_required() {
    STORAGE_REPAIR_REQUIRED.with(|required| required.set(false));
}

fn surface_storage_repair(
    storage_status: RwSignal<StorageStatus>,
    restore_message: RwSignal<Option<String>>,
) {
    mark_storage_repair_required();
    storage_status.set(StorageStatus::Error);
    restore_message.set(Some(
        "local data needs repair — export a backup, then restore a valid workspace backup".into(),
    ));
}

fn decode_indexed_crdt_value(value: JsValue) -> Result<Option<Vec<u8>>, ()> {
    let Some(encoded) = value.as_string() else {
        return Ok(None);
    };
    let encoded = EncodedUpdate::from_base64(encoded).map_err(|_| ())?;
    let bytes = encoded.to_bytes().map_err(|_| ())?;
    if bytes.len() > MAX_SYNC_SNAPSHOT_BYTES {
        return Err(());
    }
    Ok(Some(bytes))
}

async fn load_pending_crdt_updates(space_id: u64) -> Result<PendingCrdtUpdates, ()> {
    let value = JsFuture::from(indexed_db_load_crdt_outbox(space_id))
        .await
        .map_err(|_| ())?;
    let raw = js_sys::JSON::stringify(&value)
        .map_err(|_| ())?
        .as_string()
        .ok_or(())?;
    // Older clients returned a bare array. Accept it during the additive
    // rollout, while current clients also return the exact inbox keys that
    // were included in this load so a later snapshot commit can acknowledge
    // only those rows.
    let value = serde_json::from_str::<serde_json::Value>(&raw).map_err(|_| ())?;
    let loaded = if value.is_array() {
        IndexedCrdtOutbox {
            updates: serde_json::from_value(value).map_err(|_| ())?,
            inbox_keys: Vec::new(),
        }
    } else {
        serde_json::from_value::<IndexedCrdtOutbox>(value).map_err(|_| ())?
    };
    let updates = loaded
        .updates
        .into_iter()
        .map(|encoded| {
            let encoded = EncodedUpdate::from_base64(encoded).map_err(|_| ())?;
            let bytes = encoded.to_bytes().map_err(|_| ())?;
            if bytes.len() > MAX_SYNC_UPDATE_BYTES {
                return Err(());
            }
            Ok(bytes)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PendingCrdtUpdates {
        updates,
        inbox_keys: loaded.inbox_keys,
    })
}

fn replay_pending_crdt_updates(doc: &SpaceDoc, updates: &[Vec<u8>]) -> bool {
    updates
        .iter()
        .all(|update| doc.apply_update(update).is_ok())
}

/// Rehydrate a space before applying a reconciliation response. A response
/// is a delta relative to the request's state vector, not necessarily a full
/// snapshot; applying it to a blank document would create a partial local
/// projection for spaces that are dirty but not currently visible in this
/// tab.
async fn ensure_sync_runtime_doc(runtime: SyncRuntime, space_id: u64) -> Result<Vec<String>, ()> {
    if !sync_runtime_is_current(runtime) {
        return Err(());
    }
    let existing_doc = runtime.crdt_docs.get_untracked().get(&space_id).cloned();
    let loaded_snapshot = match JsFuture::from(indexed_db_load_crdt(space_id)).await {
        Ok(value) if value.is_null() || value.is_undefined() => None,
        Ok(value) => match decode_indexed_crdt_value(value) {
            Ok(snapshot) => snapshot,
            Err(()) => {
                mark_storage_repair_required();
                return Err(());
            }
        },
        Err(_) => {
            mark_storage_repair_required();
            return Err(());
        }
    };
    if !sync_runtime_is_current(runtime) {
        return Err(());
    }
    let doc = if let Some(existing_doc) = existing_doc {
        if let Some(snapshot) = loaded_snapshot {
            existing_doc.apply_snapshot(&snapshot).map_err(|_| {
                mark_storage_repair_required();
            })?;
        }
        existing_doc
    } else if let Some(snapshot) = loaded_snapshot {
        SpaceDoc::from_update(&snapshot).map_err(|_| {
            mark_storage_repair_required();
        })?
    } else {
        let doc = SpaceDoc::new();
        if let Some(space) = runtime
            .spaces
            .get_untracked()
            .iter()
            .find(|space| space.id == space_id)
        {
            doc.import_board(&space.board);
        }
        doc
    };
    let pending_updates = match load_pending_crdt_updates(space_id).await {
        Ok(updates) => updates,
        Err(()) => {
            mark_storage_repair_required();
            return Err(());
        }
    };
    if !sync_runtime_is_current(runtime) {
        return Err(());
    }
    if !replay_pending_crdt_updates(&doc, &pending_updates.updates) {
        mark_storage_repair_required();
        return Err(());
    }
    runtime.crdt_docs.update(|items| {
        items.insert(space_id, doc);
    });
    Ok(pending_updates.inbox_keys)
}

fn parse_indexed_db_json(raw: &str) -> Option<WorkspaceData> {
    let value = serde_json::from_str::<serde_json::Value>(raw).ok()?;
    parse_indexed_db_json_value(value)
}

fn parse_indexed_db_json_value(value: serde_json::Value) -> Option<WorkspaceData> {
    if value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .is_some_and(|version| version > u64::from(CURRENT_SCHEMA_VERSION))
    {
        STORAGE_SCHEMA_BLOCKED.with(|blocked| blocked.set(true));
    }
    if let Some(raw) = value.as_str() {
        return parse_indexed_db_json(raw);
    }
    if let Some(values) = value.as_array() {
        return values.iter().cloned().find_map(parse_indexed_db_json_value);
    }
    if let Ok(workspace) = serde_json::from_value::<WorkspaceData>(value.clone())
        && !workspace.spaces.is_empty()
    {
        return Some(normalize_workspace(workspace));
    }
    if let Ok(board) = serde_json::from_value::<BoardData>(value.clone())
        && (!board.notes.is_empty() || !board.groups.is_empty())
    {
        return Some(workspace_from_board(board));
    }
    let object = value.as_object()?;
    ["value", "data", "workspace", "board"]
        .into_iter()
        .find_map(|key| {
            object
                .get(key)
                .cloned()
                .and_then(parse_indexed_db_json_value)
        })
}

fn load_board() -> BoardData {
    let storage = web_sys::window().and_then(|window| window.local_storage().ok().flatten());
    let can_read_legacy = storage.as_ref().is_some_and(|storage| {
        storage
            .get_item(GUEST_LEGACY_MIGRATED_STORAGE_KEY)
            .ok()
            .flatten()
            .as_deref()
            != Some("1")
    });
    if can_read_legacy {
        for key in [STORAGE_KEY, LEGACY_STORAGE_KEY] {
            if let Some(raw) = storage
                .as_ref()
                .and_then(|storage| storage.get_item(key).ok().flatten())
            {
                mark_future_schema(&raw);
            }
        }
    }
    let current = can_read_legacy
        .then(|| {
            storage
                .as_ref()
                .and_then(|storage| storage.get_item(STORAGE_KEY).ok().flatten())
        })
        .flatten()
        .and_then(|raw| parse_board(&raw));
    let mut board = current
        .or_else(|| {
            can_read_legacy
                .then(|| {
                    storage
                        .as_ref()
                        .and_then(|storage| storage.get_item(LEGACY_STORAGE_KEY).ok().flatten())
                        .and_then(|raw| parse_board(&raw))
                        .map(|mut board| {
                            // v1 stored positions as percentages. Put those notes around
                            // the new canvas origin during the one-time migration.
                            for note in &mut board.notes {
                                note.x = note.x * 10.0 - 500.0;
                                note.y = note.y * 8.0 - 400.0;
                            }
                            board
                        })
                })
                .flatten()
        })
        .unwrap_or(BoardData {
            schema_version: CURRENT_SCHEMA_VERSION,
            notes: Vec::new(),
            groups: Vec::new(),
            tombstones: Vec::new(),
        });

    for index in 1..board.notes.len() {
        let (previous, current) = board.notes.split_at_mut(index);
        if previous.iter().any(|other| {
            (other.x - current[0].x).abs() < 1.0 && (other.y - current[0].y).abs() < 1.0
        }) {
            let (x, y) = note_position(index);
            current[0].x = x;
            current[0].y = y;
        }
    }

    for group in &mut board.groups {
        if group.origin.is_none() {
            group.origin = group_origin(group.id, &board.notes);
        }
    }

    board
}

fn empty_board() -> BoardData {
    BoardData {
        schema_version: CURRENT_SCHEMA_VERSION,
        notes: Vec::new(),
        groups: Vec::new(),
        tombstones: Vec::new(),
    }
}

fn now_millis() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        js_sys::Date::now().max(0.0) as u64
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
            .unwrap_or_default()
    }
}

fn is_guest_principal(principal: &str) -> bool {
    if principal == "guest" {
        return true;
    }
    let Some(identifier) = principal.strip_prefix("guest:") else {
        return false;
    };
    !identifier.is_empty()
        && identifier.len() <= 96
        && identifier
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn guest_principal() -> String {
    let storage = web_sys::window().and_then(|window| window.local_storage().ok().flatten());
    let stored_principal = storage
        .as_ref()
        .and_then(|storage| storage.get_item(GUEST_PRINCIPAL_STORAGE_KEY).ok().flatten())
        .or_else(|| {
            web_sys::window()
                .and_then(|window| window.session_storage().ok().flatten())
                .and_then(|storage| storage.get_item(GUEST_PRINCIPAL_STORAGE_KEY).ok().flatten())
        })
        .or_else(|| browser_cookie(GUEST_PRINCIPAL_STORAGE_KEY));
    if let Some(principal) =
        stored_principal.filter(|principal| is_guest_principal(principal) && principal != "guest")
    {
        return principal;
    }

    let identifier = web_sys::window()
        .and_then(|window| window.crypto().ok())
        .map(|crypto| crypto.random_uuid())
        .unwrap_or_else(|| {
            format!(
                "{}-{}",
                now_millis(),
                (js_sys::Math::random() * 1_000_000_000.0) as u64
            )
        });
    let principal = format!("guest:{identifier}");
    let persisted_in_local = storage.as_ref().is_some_and(|storage| {
        storage
            .set_item(GUEST_PRINCIPAL_STORAGE_KEY, &principal)
            .is_ok()
    });
    if !persisted_in_local {
        let persisted_in_session = web_sys::window()
            .and_then(|window| window.session_storage().ok().flatten())
            .is_some_and(|storage| {
                storage
                    .set_item(GUEST_PRINCIPAL_STORAGE_KEY, &principal)
                    .is_ok()
            });
        if !persisted_in_session {
            // The cookie is the final fallback for privacy modes that disable
            // both Web Storage implementations.
            set_browser_cookie(GUEST_PRINCIPAL_STORAGE_KEY, &principal);
        }
    }
    principal
}

fn browser_cookie(name: &str) -> Option<String> {
    let document = web_sys::window()?
        .document()?
        .dyn_into::<web_sys::HtmlDocument>()
        .ok()?;
    document.cookie().ok()?.split(';').find_map(|cookie| {
        let (cookie_name, value) = cookie.trim().split_once('=')?;
        (cookie_name == name && !value.trim().is_empty()).then(|| value.trim().to_owned())
    })
}

fn set_browser_cookie(name: &str, value: &str) {
    if let Some(document) = web_sys::window()
        .and_then(|window| window.document())
        .and_then(|document| document.dyn_into::<web_sys::HtmlDocument>().ok())
    {
        let _ = document.set_cookie(&format!(
            "{name}={value}; Path=/; Max-Age=31536000; SameSite=Lax"
        ));
    }
}

fn is_safe_local_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn load_device_id() -> String {
    #[cfg(not(target_arch = "wasm32"))]
    {
        "device-native-test".into()
    }
    #[cfg(target_arch = "wasm32")]
    {
        let Some(storage) =
            web_sys::window().and_then(|window| window.local_storage().ok().flatten())
        else {
            return "device-local".into();
        };
        if let Ok(Some(device_id)) = storage.get_item(DEVICE_ID_STORAGE_KEY)
            && is_safe_local_identifier(&device_id)
        {
            return device_id;
        }
        let device_id = web_sys::window()
            .and_then(|window| window.crypto().ok())
            .map(|crypto| format!("device-{}", crypto.random_uuid()))
            .unwrap_or_else(|| {
                format!(
                    "device-{}-{}",
                    now_millis(),
                    (js_sys::Math::random() * 1_000_000_000.0) as u64
                )
            });
        let _ = storage.set_item(DEVICE_ID_STORAGE_KEY, &device_id);
        device_id
    }
}

fn new_stable_entity_id() -> String {
    #[cfg(not(target_arch = "wasm32"))]
    {
        format!(
            "00000000-0000-4000-8000-{:012x}",
            ((now_millis() as u128) << 32 | u128::from(fallback_random_u64()))
                & 0x0000_FFFF_FFFF_FFFF
        )
    }
    #[cfg(target_arch = "wasm32")]
    {
        web_sys::window()
            .and_then(|window| window.crypto().ok())
            .map(|crypto| crypto.random_uuid())
            .unwrap_or_else(|| {
                format!(
                    "00000000-0000-4000-8000-{:012x}",
                    ((now_millis() as u128) << 32 | u128::from(fallback_random_u64()))
                        & 0x0000_FFFF_FFFF_FFFF
                )
            })
    }
}

#[cfg(not(target_arch = "wasm32"))]
static NATIVE_RANDOM_COUNTER: AtomicU64 = AtomicU64::new(1);

fn fallback_random_u64() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        (js_sys::Math::random() * (u64::MAX as f64)) as u64
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos() as u64)
            .unwrap_or_default();
        timestamp ^ NATIVE_RANDOM_COUNTER.fetch_add(1, Ordering::Relaxed)
    }
}

/// Generate the legacy numeric compatibility alias. Canonical entity identity
/// is the UUID in `stable_id`; this alias remains only for the current UI,
/// export, and route contract while those surfaces are migrated.
fn local_entity_seed() -> u64 {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let mut hash = 0xcbf29ce484222325u64;
        hash ^= now_millis();
        hash = hash.wrapping_mul(0x100000001b3);
        hash ^= fallback_random_u64();
        (hash & ((1u64 << 53) - 1)).max(1)
    }
    #[cfg(target_arch = "wasm32")]
    {
        let mut hash = 0xcbf29ce484222325u64;
        let random_uuid = web_sys::window()
            .and_then(|window| window.crypto().ok())
            .map(|crypto| crypto.random_uuid())
            .unwrap_or_default();
        for byte in random_uuid.bytes().chain(load_device_id().bytes()) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        if random_uuid.is_empty() {
            hash ^= now_millis();
            hash = hash.wrapping_mul(0x100000001b3);
            hash ^= fallback_random_u64();
        }
        // Keep the result below 2^53 so it survives JavaScript Number JSON
        // round-trips without losing integer precision.
        let candidate = hash & ((1u64 << 53) - 1);
        candidate.max(1)
    }
}

/// The first local space must be identical in every tab that opens a fresh
/// principal at the same time. Later entities still use random compatibility
/// aliases, but a random bootstrap space lets concurrent tabs fork the local
/// workspace before IndexedDB hydration has established the canonical record.
fn initial_space_identity() -> (u64, String) {
    let principal = current_sync_principal();
    let mut hash = 0xcbf29ce484222325u64;
    for byte in principal.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let id = (hash & ((1u64 << 53) - 1)).max(1);
    (id, legacy_entity_stable_id("principal-space", id))
}

/// Generate every new local numeric compatibility alias independently. The
/// canonical UUID is generated separately and is the CRDT map identity; this
/// alias is rejected if it is already visible in the local namespace.
fn fresh_entity_id(existing: impl IntoIterator<Item = u64>) -> u64 {
    let used = existing.into_iter().collect::<HashSet<_>>();
    loop {
        let candidate = local_entity_seed();
        if !used.contains(&candidate) {
            return candidate;
        }
    }
}

fn next_space_id_for(spaces: &[Space]) -> u64 {
    fresh_entity_id(spaces.iter().map(|space| space.id))
}

fn workspace_snapshot(
    spaces: Vec<Space>,
    active_space_id: u64,
    tombstones: Vec<Tombstone>,
) -> WorkspaceData {
    WorkspaceData {
        schema_version: CURRENT_SCHEMA_VERSION,
        device_id: load_device_id(),
        tombstones,
        spaces,
        active_space_id,
    }
}

fn workspace_has_local_data(workspace: &WorkspaceData) -> bool {
    workspace.spaces.iter().any(|space| {
        !space.board.notes.is_empty()
            || !space.board.groups.is_empty()
            || !space.board.tombstones.is_empty()
            || space.archived
            || space.deleted_at.is_some()
    }) || !workspace.tombstones.is_empty()
}

fn rekey_workspace_for_account(mut workspace: WorkspaceData) -> WorkspaceData {
    let mut used = HashSet::new();
    let mut next_id = || {
        let mut candidate = local_entity_seed();
        while !used.insert(candidate) {
            candidate = candidate.saturating_add(1);
        }
        candidate
    };
    let mut space_ids = HashMap::new();
    for space in &mut workspace.spaces {
        let new_id = next_id();
        space_ids.insert(space.id, new_id);
        space.id = new_id;
    }
    if let Some(new_active) = space_ids.get(&workspace.active_space_id).copied() {
        workspace.active_space_id = new_active;
    }
    for tombstone in &mut workspace.tombstones {
        if matches!(tombstone.kind, TombstoneKind::Space) {
            let old_id = tombstone.id;
            let new_id = if let Some(new_id) = space_ids.get(&old_id).copied() {
                new_id
            } else {
                let new_id = next_id();
                space_ids.insert(old_id, new_id);
                new_id
            };
            tombstone.id = new_id;
        }
    }
    for space in &mut workspace.spaces {
        let mut note_ids = HashMap::new();
        for note in &mut space.board.notes {
            let new_id = next_id();
            note_ids.insert(note.id, new_id);
            note.id = new_id;
        }
        let mut group_ids = HashMap::new();
        for group in &mut space.board.groups {
            let new_id = next_id();
            group_ids.insert(group.id, new_id);
            group.id = new_id;
        }
        for tombstone in &space.board.tombstones {
            match tombstone.kind {
                TombstoneKind::Note => {
                    note_ids.entry(tombstone.id).or_insert_with(&mut next_id);
                }
                TombstoneKind::Group => {
                    group_ids.entry(tombstone.id).or_insert_with(&mut next_id);
                }
                TombstoneKind::Space => {}
            }
        }
        for note in &mut space.board.notes {
            note.group_id = note.group_id.and_then(|id| group_ids.get(&id).copied());
        }
        for tombstone in &mut space.board.tombstones {
            match tombstone.kind {
                TombstoneKind::Note => {
                    if let Some(new_id) = note_ids.get(&tombstone.id).copied() {
                        tombstone.id = new_id;
                    }
                }
                TombstoneKind::Group => {
                    if let Some(new_id) = group_ids.get(&tombstone.id).copied() {
                        tombstone.id = new_id;
                    }
                }
                TombstoneKind::Space => {}
            }
        }
    }
    workspace
}

fn install_workspace(
    workspace: WorkspaceData,
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
    next_space_id: RwSignal<u64>,
    next_id: RwSignal<u64>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
) -> WorkspaceData {
    let workspace = normalize_workspace(workspace);
    let active_id = workspace.active_space_id;
    let board = workspace
        .spaces
        .iter()
        .find(|space| space.id == active_id)
        .map(|space| space.board.clone())
        .unwrap_or_else(empty_board);
    next_space_id.set(next_space_id_for(&workspace.spaces));
    next_id.set(next_note_id(&board));
    spaces.set(workspace.spaces.clone());
    active_space_id.set(active_id);
    notes.set(board.notes);
    groups.set(board.groups);
    workspace_tombstones.set(workspace.tombstones.clone());
    crdt_docs.update(|items| items.clear());
    workspace
}

fn confirm_workspace_adoption() -> bool {
    web_sys::window()
        .and_then(|window| {
            window
                .confirm_with_message(
                    "Use this browser's local workspace for your signed-in account?",
                )
                .ok()
        })
        .unwrap_or(false)
}

fn account_namespace_is_current(account_state: RwSignal<AccountState>, account_id: &str) -> bool {
    current_sync_principal() == format!("account:{account_id}")
        && matches!(
            account_state.get_untracked(),
            AccountState::SignedIn(ref entitlement) if entitlement.account_id == account_id
        )
}

async fn prepare_account_workspace(
    account_id: String,
    account_state: RwSignal<AccountState>,
    allow_guest_adoption: bool,
    guest_workspace_override: Option<WorkspaceData>,
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
    next_space_id: RwSignal<u64>,
    next_id: RwSignal<u64>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
    storage_status: RwSignal<StorageStatus>,
    restore_message: RwSignal<Option<String>>,
) -> bool {
    let guest_workspace = guest_workspace_override.unwrap_or_else(|| {
        let mut workspace = workspace_snapshot(
            spaces.get_untracked(),
            active_space_id.get_untracked(),
            workspace_tombstones.get_untracked(),
        );
        if let Some(space) = workspace
            .spaces
            .iter_mut()
            .find(|space| space.id == workspace.active_space_id)
        {
            space.board.notes = notes.get_untracked();
            space.board.groups = groups.get_untracked();
        }
        normalize_workspace(workspace)
    });
    let had_guest_data = workspace_has_local_data(&guest_workspace);

    let principal = format!("account:{account_id}");
    select_sync_principal(&principal);
    if !account_namespace_is_current(account_state, &account_id) {
        return false;
    }
    let account_workspace = match JsFuture::from(indexed_db_load_workspace()).await {
        Ok(value) if value.is_null() || value.is_undefined() => None,
        Ok(value) => match parse_indexed_db_value(value) {
            Some(workspace) => Some(workspace),
            None => {
                surface_storage_repair(storage_status, restore_message);
                return false;
            }
        },
        Err(_) => {
            surface_storage_repair(storage_status, restore_message);
            return false;
        }
    };
    if !account_namespace_is_current(account_state, &account_id) {
        return false;
    }
    let adopted_guest = allow_guest_adoption
        && account_workspace.is_none()
        && had_guest_data
        && confirm_workspace_adoption();
    let workspace = account_workspace.unwrap_or_else(|| {
        if adopted_guest {
            rekey_workspace_for_account(guest_workspace.clone())
        } else {
            workspace_from_board(empty_board())
        }
    });
    let workspace = install_workspace(
        workspace,
        spaces,
        active_space_id,
        notes,
        groups,
        workspace_tombstones,
        next_space_id,
        next_id,
        crdt_docs,
    );
    let active_id = workspace.active_space_id;
    let current_board = workspace
        .spaces
        .iter()
        .find(|space| space.id == active_id)
        .map(|space| space.board.clone())
        .unwrap_or_else(empty_board);
    let loaded_sync_state =
        match JsFuture::from(indexed_db_load_sync_state(&principal, active_id)).await {
            Ok(value) if value.is_null() || value.is_undefined() => SpaceDoc::empty_state_vector(),
            Ok(value) => {
                let Some(encoded) = value
                    .as_string()
                    .and_then(|encoded| EncodedUpdate::from_base64(encoded).ok())
                    .and_then(|encoded| encoded.to_bytes().ok())
                else {
                    surface_storage_repair(storage_status, restore_message);
                    return false;
                };
                encoded
            }
            Err(_) => {
                surface_storage_repair(storage_status, restore_message);
                return false;
            }
        };
    if !account_namespace_is_current(account_state, &account_id) {
        return false;
    }
    SYNC_ACKED_STATE_VECTORS.with(|states| {
        states
            .borrow_mut()
            .insert((principal.clone(), active_id), loaded_sync_state);
    });
    let loaded_snapshot = match JsFuture::from(indexed_db_load_crdt(active_id)).await {
        Ok(value) if value.is_null() || value.is_undefined() => None,
        Ok(value) => match decode_indexed_crdt_value(value) {
            Ok(snapshot) => snapshot,
            Err(()) => {
                surface_storage_repair(storage_status, restore_message);
                return false;
            }
        },
        Err(_) => {
            surface_storage_repair(storage_status, restore_message);
            return false;
        }
    };
    if !account_namespace_is_current(account_state, &account_id) {
        return false;
    }
    let loaded_doc = loaded_snapshot
        .as_deref()
        .and_then(|bytes| SpaceDoc::from_update(bytes).ok());
    if loaded_snapshot.is_some() && loaded_doc.is_none() {
        // Preserve an unknown/corrupt CRDT snapshot for export and upgrade
        // tooling; never silently replace it with the JSON projection.
        surface_storage_repair(storage_status, restore_message);
        return false;
    }
    let pending_updates = match load_pending_crdt_updates(active_id).await {
        Ok(updates) => updates,
        Err(()) => {
            surface_storage_repair(storage_status, restore_message);
            return false;
        }
    };
    if !account_namespace_is_current(account_state, &account_id) {
        return false;
    }
    let had_loaded_doc = loaded_doc.is_some();
    let (doc, bootstrap_state_vector) = if let Some(doc) = loaded_doc {
        (doc, None)
    } else {
        let doc = SpaceDoc::new();
        let state_vector = doc.state_vector();
        doc.import_board(&current_board);
        (doc, Some(state_vector))
    };
    if !replay_pending_crdt_updates(&doc, &pending_updates.updates) {
        surface_storage_repair(storage_status, restore_message);
        return false;
    }
    if !account_namespace_is_current(account_state, &account_id) {
        return false;
    }
    if had_loaded_doc {
        let loaded_board = doc.board();
        notes.set(loaded_board.notes);
        groups.set(loaded_board.groups);
    }
    let encoded_snapshot = EncodedUpdate::from_bytes(&doc.snapshot());
    crdt_docs.update(|items| {
        items.insert(active_id, doc);
    });
    queue_indexed_db_crdt_save(
        active_id,
        encoded_snapshot.as_str().to_owned(),
        0,
        String::new(),
        0,
    );
    if bootstrap_state_vector.is_some() && workspace_has_local_data(&workspace) {
        let space_identity = workspace
            .spaces
            .iter()
            .find(|space| space.id == active_id)
            .map(|space| (space.name.clone(), space.stable_id.clone()));
        queue_crdt_update(
            active_id,
            EncodedUpdate::from_bytes(
                &bootstrap_state_vector.unwrap_or_else(SpaceDoc::empty_state_vector),
            )
            .as_str()
            .to_owned(),
            encoded_snapshot.as_str().to_owned(),
            encoded_snapshot.as_str().to_owned(),
            space_identity.as_ref().map(|(name, _)| name.clone()),
            space_identity.map(|(_, stable_id)| stable_id),
            None,
            None,
        );
    }
    // Adoption and legacy JSON migration can contain several spaces while
    // IndexedDB has only hydrated the active one. Seed every remaining space
    // from its local projection before the first pull; otherwise only the
    // active space would ever reach the account.
    for space in workspace
        .spaces
        .iter()
        .filter(|space| space.id != active_id && space.deleted_at.is_none())
    {
        if !account_namespace_is_current(account_state, &account_id) {
            return false;
        }
        let existing_snapshot = match JsFuture::from(indexed_db_load_crdt(space.id)).await {
            Ok(value) if value.is_null() || value.is_undefined() => None,
            Ok(value) => match decode_indexed_crdt_value(value) {
                Ok(snapshot) => snapshot,
                Err(()) => {
                    surface_storage_repair(storage_status, restore_message);
                    return false;
                }
            },
            Err(_) => {
                surface_storage_repair(storage_status, restore_message);
                return false;
            }
        };
        if !account_namespace_is_current(account_state, &account_id) {
            return false;
        }
        if existing_snapshot.is_some() {
            if SpaceDoc::from_update(existing_snapshot.as_deref().unwrap_or_default()).is_ok() {
                continue;
            }
            surface_storage_repair(storage_status, restore_message);
            return false;
        }
        if space.board.notes.is_empty()
            && space.board.groups.is_empty()
            && space.board.tombstones.is_empty()
        {
            continue;
        }
        let doc = SpaceDoc::new();
        let bootstrap_state_vector = doc.state_vector();
        doc.import_board(&space.board);
        let Ok(pending_updates) = load_pending_crdt_updates(space.id).await else {
            surface_storage_repair(storage_status, restore_message);
            return false;
        };
        if !account_namespace_is_current(account_state, &account_id) {
            return false;
        }
        if !replay_pending_crdt_updates(&doc, &pending_updates.updates) {
            surface_storage_repair(storage_status, restore_message);
            return false;
        }
        let encoded_snapshot = EncodedUpdate::from_bytes(&doc.snapshot());
        queue_indexed_db_crdt_save(
            space.id,
            encoded_snapshot.as_str().to_owned(),
            0,
            String::new(),
            0,
        );
        queue_crdt_update(
            space.id,
            EncodedUpdate::from_bytes(&bootstrap_state_vector)
                .as_str()
                .to_owned(),
            encoded_snapshot.as_str().to_owned(),
            encoded_snapshot.as_str().to_owned(),
            Some(space.name.clone()),
            Some(space.stable_id.clone()),
            None,
            None,
        );
    }
    if !account_namespace_is_current(account_state, &account_id) {
        return false;
    }
    let _ = save_workspace(&workspace, None);
    true
}

fn normalize_workspace(mut workspace: WorkspaceData) -> WorkspaceData {
    workspace.schema_version = CURRENT_SCHEMA_VERSION;
    if workspace.device_id.is_empty() {
        workspace.device_id = load_device_id();
    }
    let now = now_millis();
    for tombstone in &mut workspace.tombstones {
        if !is_valid_stable_id(&tombstone.stable_id) {
            tombstone.stable_id = legacy_entity_stable_id(
                match tombstone.kind {
                    TombstoneKind::Note => "note",
                    TombstoneKind::Group => "group",
                    TombstoneKind::Space => "space",
                },
                tombstone.id,
            );
        }
    }
    for space in &mut workspace.spaces {
        if !is_valid_stable_id(&space.stable_id) {
            space.stable_id = legacy_entity_stable_id("space", space.id);
        }
        if space.created_at == 0 {
            space.created_at = now;
        }
        if space.updated_at == 0 {
            space.updated_at = space.created_at;
        }
        space.board.schema_version = CURRENT_SCHEMA_VERSION;
        let group_stable_ids = space
            .board
            .groups
            .iter()
            .map(|group| {
                (
                    group.id,
                    if !is_valid_stable_id(&group.stable_id) {
                        legacy_entity_stable_id("group", group.id)
                    } else {
                        group.stable_id.clone()
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        for group in &mut space.board.groups {
            if !is_valid_stable_id(&group.stable_id) {
                group.stable_id = group_stable_ids
                    .get(&group.id)
                    .cloned()
                    .unwrap_or_else(|| legacy_entity_stable_id("group", group.id));
            }
        }
        for note in &mut space.board.notes {
            if !is_valid_stable_id(&note.stable_id) {
                note.stable_id = legacy_entity_stable_id("note", note.id);
            }
            if note
                .group_stable_id
                .as_deref()
                .is_none_or(|value| !is_valid_stable_id(value))
            {
                note.group_stable_id = note
                    .group_id
                    .and_then(|id| group_stable_ids.get(&id).cloned());
            }
            if note.created_at == 0 {
                note.created_at = now;
            }
            if note.updated_at == 0 {
                note.updated_at = note.created_at;
            }
        }
        for tombstone in &mut space.board.tombstones {
            if !is_valid_stable_id(&tombstone.stable_id) {
                tombstone.stable_id = legacy_entity_stable_id(
                    match tombstone.kind {
                        TombstoneKind::Note => "note",
                        TombstoneKind::Group => "group",
                        TombstoneKind::Space => "space",
                    },
                    tombstone.id,
                );
            }
        }
        for group in &mut space.board.groups {
            if group.created_at == 0 {
                group.created_at = now;
            }
            if group.updated_at == 0 {
                group.updated_at = group.created_at;
            }
        }
    }

    if !workspace.spaces.iter().any(|space| {
        space.id == workspace.active_space_id && !space.archived && space.deleted_at.is_none()
    }) {
        if let Some(space) = workspace
            .spaces
            .iter_mut()
            .find(|space| !space.archived && space.deleted_at.is_none())
        {
            workspace.active_space_id = space.id;
        } else if let Some(space) = workspace
            .spaces
            .iter_mut()
            .find(|space| space.deleted_at.is_none())
        {
            space.archived = false;
            workspace.active_space_id = space.id;
        }
    }

    workspace
}

fn hydrate_workspace_from_indexed_db(
    initial_workspace: WorkspaceData,
    initial_board: BoardData,
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
    next_space_id: RwSignal<u64>,
    next_id: RwSignal<u64>,
    selection: RwSignal<Vec<u64>>,
    history: RwSignal<History>,
    editing: RwSignal<Option<u64>>,
    edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
    pan: RwSignal<(f64, f64)>,
    zoom: RwSignal<f64>,
    storage_hydrated: RwSignal<bool>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
    storage_status: RwSignal<StorageStatus>,
    restore_message: RwSignal<Option<String>>,
) {
    spawn_local(async move {
        // This hydration starts in the guest namespace, but authentication can
        // switch the principal while IndexedDB is resolving. Never apply a
        // late guest read to an account workspace (or vice versa).
        let hydration_principal = current_sync_principal();
        match JsFuture::from(indexed_db_load_workspace()).await {
            Ok(value) if value.is_null() || value.is_undefined() => {}
            Ok(value) => {
                if hydration_principal != current_sync_principal() {
                    return;
                }
                let Some(imported) = parse_indexed_db_value(value) else {
                    surface_storage_repair(storage_status, restore_message);
                    storage_hydrated.set(true);
                    return;
                };
                let local_changed = spaces.get_untracked() != initial_workspace.spaces
                    || active_space_id.get_untracked() != initial_workspace.active_space_id
                    || notes.get_untracked() != initial_board.notes
                    || groups.get_untracked() != initial_board.groups;
                if !local_changed
                    && let Some(imported_space) = imported
                        .spaces
                        .iter()
                        .find(|space| space.id == imported.active_space_id)
                {
                    let imported_board = imported_space.board.clone();
                    let imported_view = load_view(imported.active_space_id);
                    spaces.set(imported.spaces);
                    active_space_id.set(imported.active_space_id);
                    workspace_tombstones.set(imported.tombstones);
                    next_space_id.set(next_space_id_for(&spaces.get_untracked()));
                    next_id.set(next_note_id(&imported_board));
                    notes.set(imported_board.notes);
                    groups.set(imported_board.groups);
                    selection.set(Vec::new());
                    history.set(History::default());
                    editing.set(None);
                    edit_snapshot.set(None);
                    pan.set(imported_view.pan);
                    zoom.set(imported_view.zoom);
                }
            }
            Err(_) => {
                surface_storage_repair(storage_status, restore_message);
                storage_hydrated.set(true);
                return;
            }
        }

        let active_id = active_space_id.get_untracked();
        let current_board = spaces
            .get_untracked()
            .iter()
            .find(|space| space.id == active_id)
            .map(|space| space.board.clone())
            .unwrap_or_else(empty_board);
        let loaded_snapshot = match JsFuture::from(indexed_db_load_crdt(active_id)).await {
            Ok(value) if value.is_null() || value.is_undefined() => None,
            Ok(value) => match decode_indexed_crdt_value(value) {
                Ok(snapshot) => snapshot,
                Err(()) => {
                    surface_storage_repair(storage_status, restore_message);
                    storage_hydrated.set(true);
                    return;
                }
            },
            Err(_) => {
                surface_storage_repair(storage_status, restore_message);
                storage_hydrated.set(true);
                return;
            }
        };
        if hydration_principal != current_sync_principal() {
            return;
        }
        let loaded_doc = loaded_snapshot
            .as_deref()
            .and_then(|bytes| SpaceDoc::from_update(bytes).ok());
        if loaded_snapshot.is_some() && loaded_doc.is_none() {
            surface_storage_repair(storage_status, restore_message);
            storage_hydrated.set(true);
            return;
        }
        let pending_updates = match load_pending_crdt_updates(active_id).await {
            Ok(updates) => updates,
            Err(()) => {
                surface_storage_repair(storage_status, restore_message);
                storage_hydrated.set(true);
                return;
            }
        };
        let had_loaded_doc = loaded_doc.is_some();
        let (doc, bootstrap_state_vector) = if let Some(doc) = loaded_doc {
            (doc, None)
        } else {
            let doc = SpaceDoc::new();
            let state_vector = doc.state_vector();
            doc.import_board(&current_board);
            (doc, Some(state_vector))
        };
        if !replay_pending_crdt_updates(&doc, &pending_updates.updates) {
            surface_storage_repair(storage_status, restore_message);
            storage_hydrated.set(true);
            return;
        }
        let loaded_board = doc.board();
        if had_loaded_doc {
            notes.set(loaded_board.notes);
            groups.set(loaded_board.groups);
        }
        let encoded_snapshot = EncodedUpdate::from_bytes(&doc.snapshot());
        if hydration_principal != current_sync_principal() {
            return;
        }
        crdt_docs.update(|items| {
            items.insert(active_id, doc);
        });
        queue_indexed_db_crdt_save(
            active_id,
            encoded_snapshot.as_str().to_owned(),
            0,
            String::new(),
            0,
        );
        if !had_loaded_doc {
            let space_identity = spaces
                .get_untracked()
                .iter()
                .find(|space| space.id == active_id)
                .map(|space| (space.name.clone(), space.stable_id.clone()));
            let bootstrap_state_vector =
                bootstrap_state_vector.unwrap_or_else(SpaceDoc::empty_state_vector);
            queue_crdt_update(
                active_id,
                EncodedUpdate::from_bytes(&bootstrap_state_vector)
                    .as_str()
                    .to_owned(),
                encoded_snapshot.as_str().to_owned(),
                encoded_snapshot.as_str().to_owned(),
                space_identity.as_ref().map(|(name, _)| name.clone()),
                space_identity.map(|(_, stable_id)| stable_id),
                None,
                None,
            );
        }
        storage_hydrated.set(true);
    });
}

fn load_workspace() -> WorkspaceData {
    let storage = web_sys::window().and_then(|window| window.local_storage().ok().flatten());
    let can_read_legacy = storage.as_ref().is_some_and(|storage| {
        storage
            .get_item(GUEST_LEGACY_MIGRATED_STORAGE_KEY)
            .ok()
            .flatten()
            .as_deref()
            != Some("1")
    });
    let raw_workspace = can_read_legacy
        .then(|| {
            storage.as_ref().and_then(|storage| {
                storage
                    .get_item(LEGACY_WORKSPACE_STORAGE_KEY)
                    .ok()
                    .flatten()
            })
        })
        .flatten();
    if let Some(raw) = raw_workspace.as_deref() {
        mark_future_schema(raw);
    }
    let (initial_space_id, initial_stable_id) = initial_space_identity();
    let workspace = raw_workspace
        .and_then(|raw| parse_workspace(&raw))
        .unwrap_or_else(|| WorkspaceData {
            schema_version: CURRENT_SCHEMA_VERSION,
            device_id: load_device_id(),
            tombstones: Vec::new(),
            spaces: vec![Space {
                id: initial_space_id,
                stable_id: initial_stable_id,
                name: "my space".into(),
                metadata_version: 0,
                archived: false,
                created_at: now_millis(),
                updated_at: now_millis(),
                deleted_at: None,
                board: load_board(),
            }],
            active_space_id: initial_space_id,
        });
    normalize_workspace(workspace)
}

fn save_workspace(
    workspace: &WorkspaceData,
    storage_status: Option<RwSignal<StorageStatus>>,
) -> bool {
    if storage_writes_blocked() {
        if let Some(storage_status) = storage_status {
            storage_status.set(StorageStatus::Error);
        }
        return false;
    }
    let storage_generation = storage_status.map(|storage_status| {
        storage_status.set(StorageStatus::Saving);
        begin_storage_write()
    });
    let workspace = normalize_workspace(workspace.clone());
    if let Ok(raw) = serde_json::to_string(&workspace) {
        queue_indexed_db_save(raw, storage_status, storage_generation);
        true
    } else {
        false
    }
}

async fn persist_workspace_before_event_cursor(runtime: RemoteSyncEventRuntime) -> bool {
    if storage_writes_blocked() {
        return false;
    }
    let principal = current_sync_principal();
    let workspace = workspace_snapshot(
        runtime.spaces.get_untracked(),
        runtime.active_space_id.get_untracked(),
        runtime.workspace_tombstones.get_untracked(),
    );
    let Ok(raw) = serde_json::to_string(&normalize_workspace(workspace)) else {
        return false;
    };
    JsFuture::from(indexed_db_save_workspace(&raw, &principal))
        .await
        .is_ok()
}

fn begin_storage_write() -> u64 {
    STORAGE_WRITE_GENERATION.with(|generation| {
        let next = generation.get().saturating_add(1).max(1);
        generation.set(next);
        STORAGE_ERROR_GENERATION.with(|error| error.set(0));
        next
    })
}

fn current_storage_write_generation() -> u64 {
    STORAGE_WRITE_GENERATION.with(Cell::get)
}

async fn load_local_pending_count() -> Option<usize> {
    let documents = JsFuture::from(indexed_db_load_crdt_updates())
        .await
        .ok()
        .and_then(|value| parse_queued_crdt_updates(value).ok())?
        .len();
    let metadata = JsFuture::from(indexed_db_load_metadata_updates())
        .await
        .ok()
        .and_then(|value| {
            let raw = js_sys::JSON::stringify(&value).ok()?.as_string()?;
            serde_json::from_str::<Vec<QueuedMetadataUpdate>>(&raw).ok()
        })?
        .len();
    Some(documents.saturating_add(metadata))
}

async fn load_local_sync_errors() -> Option<Vec<LocalSyncError>> {
    let value = JsFuture::from(indexed_db_load_sync_errors()).await.ok()?;
    let raw = js_sys::JSON::stringify(&value).ok()?.as_string()?;
    serde_json::from_str::<Vec<LocalSyncError>>(&raw).ok()
}

async fn load_local_sync_diagnostics(
    principal: &str,
    space_id: u64,
) -> Option<LocalSyncDiagnostics> {
    let value = JsFuture::from(indexed_db_load_sync_diagnostics(principal, space_id))
        .await
        .ok()?;
    let raw = js_sys::JSON::stringify(&value).ok()?.as_string()?;
    serde_json::from_str(&raw).ok()
}

fn read_sync_transport_diagnostics() -> SyncTransportDiagnostics {
    let Ok(raw) = js_sys::JSON::stringify(&sync_transport_diagnostics()) else {
        return SyncTransportDiagnostics::default();
    };
    raw.as_string()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

fn refresh_pending_count() {
    let Some(pending_count) = SYNC_PENDING_COUNT_SIGNAL.with(|signal| *signal.borrow()) else {
        return;
    };
    let refresh_generation = next_pending_count_refresh_generation();
    let principal = current_sync_principal();
    spawn_local(async move {
        let Some(count) = load_local_pending_count().await else {
            return;
        };
        if current_sync_principal() == principal
            && PENDING_COUNT_REFRESH_GENERATION.with(|generation| generation.get())
                == refresh_generation
        {
            pending_count.set(count);
        }
    });
}

fn next_pending_count_refresh_generation() -> u64 {
    PENDING_COUNT_REFRESH_GENERATION.with(|generation| {
        let next = generation.get().saturating_add(1).max(1);
        generation.set(next);
        next
    })
}

fn mark_storage_error(generation: u64) {
    if generation == current_storage_write_generation() {
        STORAGE_ERROR_GENERATION.with(|error| error.set(generation));
    }
}

fn storage_generation_has_error(generation: u64) -> bool {
    STORAGE_ERROR_GENERATION.with(|error| error.get() == generation)
}

fn current_sync_principal() -> String {
    SYNC_PRINCIPAL.with(|principal| principal.borrow().clone())
}

fn mark_remote_crdt_projection() {
    REMOTE_CRDT_PROJECTION_GENERATION.with(|generation| {
        generation.set(generation.get().saturating_add(1));
    });
}

fn remote_crdt_projection_generation() -> u64 {
    REMOTE_CRDT_PROJECTION_GENERATION.with(Cell::get)
}

fn acknowledged_state_vector(space_id: u64) -> Option<Vec<u8>> {
    let principal = current_sync_principal();
    SYNC_ACKED_STATE_VECTORS.with(|states| states.borrow().get(&(principal, space_id)).cloned())
}

fn set_acknowledged_state_vector(principal: &str, space_id: u64, state_vector: Vec<u8>) {
    SYNC_ACKED_STATE_VECTORS.with(|states| {
        states
            .borrow_mut()
            .insert((principal.to_owned(), space_id), state_vector);
    });
}

fn refresh_sync_diagnostics(runtime: SyncRuntime) {
    let principal = current_sync_principal();
    let space_id = runtime.active_space_id.get_untracked();
    let refresh_generation = next_pending_count_refresh_generation();
    spawn_local(async move {
        let local = load_local_sync_diagnostics(&principal, space_id).await;
        let pending = load_local_pending_count().await;
        let errors = load_local_sync_errors().await.unwrap_or_default();
        let transport = read_sync_transport_diagnostics();
        if principal == current_sync_principal()
            && sync_is_active()
            && sync_run_is_current(runtime.run_generation)
            && PENDING_COUNT_REFRESH_GENERATION.with(|generation| generation.get())
                == refresh_generation
        {
            if let Some(local) = local {
                runtime.local_diagnostics.set(Some(local));
            }
            runtime.transport_diagnostics.set(transport);
            let Some(pending) = pending else {
                return;
            };
            runtime.pending_count.set(pending);
            runtime.last_error.set(
                errors
                    .first()
                    .map(|error| format!("space {}: {}", error.space_id, error.error)),
            );
            if pending == 0 && SYNC_SERVER_CONTACT.with(Cell::get) {
                runtime.status.set(SyncStatus::Synced);
            }
        }
    });
}

async fn publish_completed_sync_queue_state(runtime: SyncRuntime) {
    let Some(pending) = load_local_pending_count().await else {
        return;
    };
    let refresh_generation = next_pending_count_refresh_generation();
    if !sync_is_active()
        || !sync_run_is_current(runtime.run_generation)
        || PENDING_COUNT_REFRESH_GENERATION.with(|generation| generation.get())
            != refresh_generation
    {
        return;
    }
    runtime.pending_count.set(pending);
    if pending == 0 && SYNC_SERVER_CONTACT.with(Cell::get) {
        runtime.status.set(SyncStatus::Synced);
    }
}

fn select_sync_principal(principal: &str) {
    let principal = if principal.trim().is_empty() {
        "guest"
    } else {
        principal
    };
    SYNC_PRINCIPAL.with(|current| *current.borrow_mut() = principal.to_owned());
    let _ = indexed_db_set_sync_principal(principal);
    let _ = set_local_broadcast_principal(principal);
}

fn write_workspace_exact(
    workspace: &WorkspaceData,
    storage_status: Option<RwSignal<StorageStatus>>,
) -> bool {
    if storage_writes_blocked() {
        if let Some(storage_status) = storage_status {
            storage_status.set(StorageStatus::Error);
        }
        return false;
    }
    let storage_generation = storage_status.map(|storage_status| {
        storage_status.set(StorageStatus::Saving);
        begin_storage_write()
    });
    let workspace = normalize_workspace(workspace.clone());
    let Ok(raw) = serde_json::to_string(&workspace) else {
        return false;
    };
    queue_indexed_db_save(raw, storage_status, storage_generation);
    true
}

fn queue_indexed_db_save(
    raw: String,
    storage_status: Option<RwSignal<StorageStatus>>,
    storage_generation: Option<u64>,
) {
    if storage_writes_blocked() {
        if let Some(storage_status) = storage_status {
            storage_status.set(StorageStatus::Error);
        }
        return;
    }
    let principal = current_sync_principal();
    spawn_local(async move {
        let saved = JsFuture::from(indexed_db_save_workspace(&raw, &principal))
            .await
            .is_ok();
        if let Some(storage_status) = storage_status {
            let current_generation = storage_generation
                .is_none_or(|generation| generation == current_storage_write_generation());
            if current_generation {
                if !saved {
                    mark_storage_repair_required();
                    if let Some(generation) = storage_generation {
                        mark_storage_error(generation);
                    }
                    storage_status.set(StorageStatus::Error);
                } else if !storage_generation.is_some_and(storage_generation_has_error)
                    && !storage_writes_blocked()
                {
                    storage_status.set(StorageStatus::Saved);
                } else if storage_writes_blocked() {
                    storage_status.set(StorageStatus::Error);
                }
            }
        }
        if saved
            && let Some(storage) =
                web_sys::window().and_then(|window| window.local_storage().ok().flatten())
        {
            let _ = storage.remove_item(LEGACY_WORKSPACE_STORAGE_KEY);
        }
    });
}

fn queue_indexed_db_crdt_save(
    space_id: u64,
    encoded_snapshot: String,
    snapshot_generation: u64,
    origin_device_id: String,
    origin_generation: u64,
) {
    if storage_writes_blocked() {
        return;
    }
    let principal = current_sync_principal();
    spawn_local(async move {
        let saved = JsFuture::from(indexed_db_save_crdt(
            &principal,
            space_id,
            &encoded_snapshot,
            snapshot_generation,
            &origin_device_id,
            origin_generation,
        ))
        .await
        .is_ok();
        if !saved {
            mark_storage_repair_required();
        }
    });
}

fn sync_is_active() -> bool {
    SYNC_ACTIVE.with(Cell::get)
}

fn next_sync_run_generation() -> u64 {
    SYNC_RUN_GENERATION.with(|generation| {
        let next = generation.get().saturating_add(1).max(1);
        generation.set(next);
        next
    })
}

fn sync_run_is_current(run_generation: u64) -> bool {
    SYNC_RUN_GENERATION.with(|generation| generation.get() == run_generation)
}

fn sync_runtime_is_current(runtime: SyncRuntime) -> bool {
    sync_is_active()
        && sync_run_is_current(runtime.run_generation)
        && current_sync_principal().starts_with("account:")
}

fn sync_coordinator_is_current() -> bool {
    is_sync_lease_owner(&current_sync_principal())
}

fn set_sync_active(active: bool) {
    SYNC_ACTIVE.with(|state| state.set(active));
}

fn stop_authenticated_sync() {
    next_sync_run_generation();
    set_sync_active(false);
    SYNC_SERVER_CONTACT.with(|contact| contact.set(false));
    stop_sync_events(&api_url("/sync/events"));
    let principal = current_sync_principal();
    release_sync_lease(&principal);
    stop_sync_broadcast();
}

async fn register_sync_space(space_id: u64, name: &str, stable_id: Option<&str>) -> bool {
    let mut refreshed = false;
    loop {
        let Ok(builder) = Request::post(&api_url(&format!("/sync/spaces/{space_id}")))
            .credentials(RequestCredentials::Include)
            .json(&RegisterSpacePayload { name, stable_id })
        else {
            return false;
        };
        let Ok(response) = send_request_with_timeout(builder).await else {
            return false;
        };
        if response.status() == 401 && !refreshed && refresh_session_once().await {
            refreshed = true;
            continue;
        }
        return (200..300).contains(&response.status());
    }
}

fn queue_space_metadata_operation(
    spaces: RwSignal<Vec<Space>>,
    active_space_id: u64,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
    space_id: u64,
    operation: SpaceMetadataOperation,
    name: Option<String>,
    expected_version: Option<u64>,
) {
    let principal = current_sync_principal();
    let operation_id = next_sync_mutation_id();
    LOCAL_METADATA_WATERMARKS.with(|watermarks| {
        watermarks
            .borrow_mut()
            .insert((principal.clone(), space_id), operation_id.clone());
    });
    let workspace_raw = serde_json::to_string(&workspace_snapshot(
        spaces.get_untracked(),
        active_space_id,
        workspace_tombstones.get_untracked(),
    ))
    .ok();
    spawn_local(async move {
        let queued = JsFuture::from(indexed_db_queue_metadata_update(
            &principal,
            space_id,
            &operation_id,
            metadata_operation_name(&operation),
            name.as_deref(),
            expected_version,
            workspace_raw.as_deref(),
        ))
        .await;
        if queued.is_err() {
            LOCAL_METADATA_WATERMARKS.with(|watermarks| {
                let is_latest = {
                    let current = watermarks.borrow();
                    current
                        .get(&(principal.clone(), space_id))
                        .is_some_and(|latest| latest == &operation_id)
                };
                if is_latest {
                    watermarks
                        .borrow_mut()
                        .remove(&(principal.clone(), space_id));
                }
            });
            return;
        }
        publish_local_metadata(
            &principal,
            space_id,
            metadata_operation_name(&operation),
            name.as_deref(),
            &operation_id,
            &load_device_id(),
        );
        refresh_pending_count();
        if sync_is_active() {
            schedule_sync_drain();
        }
        publish_sync_hint();
    });
}

fn metadata_operation_name(operation: &SpaceMetadataOperation) -> &'static str {
    match operation {
        SpaceMetadataOperation::Rename => "rename",
        SpaceMetadataOperation::Archive => "archive",
        SpaceMetadataOperation::Unarchive => "unarchive",
        SpaceMetadataOperation::Delete => "delete",
        SpaceMetadataOperation::Restore => "restore",
    }
}

fn queue_crdt_update(
    space_id: u64,
    state_vector: String,
    encoded_update: String,
    encoded_snapshot: String,
    space_name: Option<String>,
    space_stable_id: Option<String>,
    workspace_raw: Option<String>,
    storage_status: Option<RwSignal<StorageStatus>>,
) {
    if storage_writes_blocked() {
        return;
    }
    let principal = current_sync_principal();
    let device_id = load_device_id();
    let storage_generation = storage_status.map(|_| current_storage_write_generation());
    let local_generation = next_local_generation(space_id);
    let last_server_sequence = current_sync_cursor(&principal)
        .parse::<u64>()
        .unwrap_or_default();
    let broadcast_update = encoded_update.clone();
    spawn_local(async move {
        let mutation_id = next_sync_mutation_id();
        let queue_result = JsFuture::from(indexed_db_queue_crdt_update(
            &principal,
            space_id,
            &mutation_id,
            &device_id,
            last_server_sequence,
            &state_vector,
            &encoded_update,
            &encoded_snapshot,
            local_generation,
            workspace_raw.as_deref(),
        ))
        .await;
        let committed_generation = queue_result
            .as_ref()
            .ok()
            .and_then(|value| value.as_f64())
            .filter(|generation| generation.is_finite() && *generation >= 1.0)
            .map(|generation| generation as u64)
            .unwrap_or(local_generation);
        let queued = queue_result.is_ok();
        if !queued {
            mark_storage_repair_required();
            let current_generation = storage_generation
                .is_none_or(|generation| generation == current_storage_write_generation());
            if current_generation {
                if let Some(generation) = storage_generation {
                    mark_storage_error(generation);
                }
                if let Some(storage_status) = storage_status {
                    storage_status.set(StorageStatus::Error);
                }
            }
            return;
        }
        if let Some(storage_status) = storage_status {
            let current_generation = storage_generation
                .is_none_or(|generation| generation == current_storage_write_generation());
            if current_generation && !storage_generation.is_some_and(storage_generation_has_error) {
                storage_status.set(StorageStatus::Saved);
            }
        }

        // A space created during an active session has not gone through the
        // startup registration pass. Register it before the outbox worker
        // attempts its first push.
        if sync_is_active()
            && let Some(name) = space_name.as_deref()
        {
            let _ = register_sync_space(space_id, name, space_stable_id.as_deref()).await;
        }
        // The local tab channel carries the committed CRDT update itself.
        // This is what makes two tabs converge while offline; the server
        // hint below remains only a network wake-up for authenticated sync.
        publish_local_update(
            &principal,
            space_id,
            committed_generation,
            &broadcast_update,
            &load_device_id(),
        );
        if let Some(runtime) = SYNC_RUNTIME.with(|runtime| *runtime.borrow()) {
            refresh_sync_diagnostics(runtime);
        }
        refresh_pending_count();
        schedule_sync_drain();
        publish_sync_hint();
    });
}

fn next_sync_mutation_id() -> String {
    web_sys::window()
        .and_then(|window| window.crypto().ok())
        .map(|crypto| crypto.random_uuid())
        .unwrap_or_else(|| {
            format!(
                "{}-{}-{}",
                load_device_id(),
                now_millis(),
                (js_sys::Math::random() * 1_000_000_000.0) as u64
            )
        })
}

fn next_local_generation(space_id: u64) -> u64 {
    let principal = current_sync_principal();
    let key = format!("task-space:generation:{principal}:{space_id}");
    let persisted = web_sys::window()
        .and_then(|window| window.local_storage().ok().flatten())
        .and_then(|storage| storage.get_item(&key).ok().flatten())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_default();
    let current = LOCAL_GENERATIONS.with(|generations| {
        let mut generations = generations.borrow_mut();
        let entry = generations
            .entry((principal.clone(), space_id))
            .or_insert(persisted);
        *entry = (*entry).max(persisted);
        *entry
    });
    let next = current.saturating_add(1).max(1);
    LOCAL_GENERATIONS.with(|generations| {
        generations.borrow_mut().insert((principal, space_id), next);
    });
    if let Some(storage) =
        web_sys::window().and_then(|window| window.local_storage().ok().flatten())
    {
        let _ = storage.set_item(&key, &next.to_string());
    }
    next
}

fn current_local_generation(space_id: u64) -> u64 {
    let principal = current_sync_principal();
    let persisted = web_sys::window()
        .and_then(|window| window.local_storage().ok().flatten())
        .and_then(|storage| {
            storage
                .get_item(&format!("task-space:generation:{principal}:{space_id}"))
                .ok()
                .flatten()
        })
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_default();
    LOCAL_GENERATIONS.with(|generations| {
        generations
            .borrow()
            .get(&(principal, space_id))
            .copied()
            .unwrap_or(persisted)
            .max(persisted)
    })
}

fn persist_space_crdt(
    space_id: u64,
    previous_board: &BoardData,
    board: &BoardData,
    spaces: RwSignal<Vec<Space>>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
    workspace_raw: Option<String>,
    storage_status: RwSignal<StorageStatus>,
) -> bool {
    if storage_writes_blocked() {
        return false;
    }
    crdt_docs.update(|items| {
        items.entry(space_id).or_default();
    });
    // A space without a server acknowledgement is a first-sync bootstrap.
    // Diffing against the local vector here would produce an empty update and
    // strand a newly-created offline space after registration.
    let previous_state_vector =
        acknowledged_state_vector(space_id).unwrap_or_else(SpaceDoc::empty_state_vector);
    crdt_docs.update(|items| {
        let doc = items.entry(space_id).or_default();
        doc.apply_board_diff(previous_board, board, now_millis());
    });
    // The CRDT is the canonical merge point. A local UI projection can be
    // stale when a sibling tab commits a remote field immediately before this
    // effect runs; render the merged projection back into the UI before
    // queueing the update so the UI and durable document cannot diverge.
    let canonical_board = crdt_docs
        .get_untracked()
        .get(&space_id)
        .map(SpaceDoc::board)
        .unwrap_or_else(|| board.clone());
    if canonical_board != *board {
        mark_remote_crdt_projection();
        spaces.update(|items| {
            if let Some(space) = items.iter_mut().find(|space| space.id == space_id) {
                space.board = canonical_board.clone();
                space.updated_at = now_millis();
            }
        });
        notes.set(canonical_board.notes.clone());
        groups.set(canonical_board.groups.clone());
    }
    let snapshot = crdt_docs
        .get_untracked()
        .get(&space_id)
        .map(SpaceDoc::snapshot)
        .unwrap_or_default();
    let mut queued = false;
    if let Some(doc) = crdt_docs.get_untracked().get(&space_id)
        && let Ok(update) = doc.encode_update(&previous_state_vector)
        && !update.is_empty()
    {
        let encoded = EncodedUpdate::from_bytes(&update);
        let space_identity = spaces
            .get_untracked()
            .iter()
            .find(|space| space.id == space_id)
            .map(|space| (space.name.clone(), space.stable_id.clone()));
        queue_crdt_update(
            space_id,
            EncodedUpdate::from_bytes(&previous_state_vector)
                .as_str()
                .to_owned(),
            encoded.as_str().to_owned(),
            EncodedUpdate::from_bytes(&snapshot).as_str().to_owned(),
            space_identity.as_ref().map(|(name, _)| name.clone()),
            space_identity.map(|(_, stable_id)| stable_id),
            workspace_raw,
            Some(storage_status),
        );
        queued = true;
    }
    queued
}

#[derive(Clone, Debug, Deserialize)]
struct QueuedCrdtUpdate {
    #[serde(default)]
    principal: Option<String>,
    #[serde(rename = "spaceId")]
    space_id: u64,
    #[serde(rename = "mutationId")]
    mutation_id: String,
    #[serde(rename = "deviceId", default)]
    device_id: Option<String>,
    #[serde(rename = "lastServerSequence", default)]
    last_server_sequence: Option<u64>,
    #[serde(rename = "localGeneration", default)]
    local_generation: Option<u64>,
    #[serde(rename = "stateVector", default)]
    state_vector: Option<String>,
    update: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LocalSyncError {
    #[serde(rename = "spaceId")]
    space_id: u64,
    error: String,
}

#[derive(Clone, Debug, Deserialize)]
struct QueuedMetadataUpdate {
    #[serde(default)]
    principal: Option<String>,
    #[serde(rename = "spaceId")]
    space_id: u64,
    #[serde(rename = "operationId")]
    operation_id: String,
    operation: String,
    name: Option<String>,
    #[serde(rename = "expectedVersion")]
    expected_version: Option<u64>,
}

#[derive(Serialize)]
struct RegisterSpacePayload<'a> {
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    stable_id: Option<&'a str>,
}

async fn register_sync_spaces(spaces: RwSignal<Vec<Space>>) {
    for space in spaces.get_untracked() {
        if space.deleted_at.is_some() {
            continue;
        }
        if !register_sync_space(space.id, &space.name, Some(&space.stable_id)).await {
            return;
        }
    }
}

async fn merge_remote_spaces(
    spaces: RwSignal<Vec<Space>>,
    next_space_id: RwSignal<u64>,
    active_space_id: RwSignal<u64>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
) -> bool {
    let principal = current_sync_principal();
    let mut refreshed = false;
    let response = loop {
        let Ok(response) = send_with_timeout(
            Request::get(&api_url("/sync/spaces")).credentials(RequestCredentials::Include),
        )
        .await
        else {
            return false;
        };
        if response.status() == 401 && !refreshed && refresh_session_once().await {
            refreshed = true;
            continue;
        }
        break response;
    };
    if principal != current_sync_principal() {
        return false;
    }
    if response.status() >= 300 {
        return false;
    }
    let Ok(remote_spaces) = response.json::<Vec<SpaceManifestEntry>>().await else {
        return false;
    };
    if principal != current_sync_principal() {
        return false;
    }
    let highest_remote_id = remote_spaces.iter().map(|space| space.id).max();
    let remote_ids: Vec<u64> = remote_spaces.iter().map(|space| space.id).collect();
    workspace_tombstones.update(|items| {
        for remote in &remote_spaces {
            if let Some(deleted_at) = remote.deleted_at {
                if items.iter().all(|tombstone| {
                    tombstone.kind != TombstoneKind::Space || tombstone.id != remote.id
                }) {
                    items.push(Tombstone {
                        kind: TombstoneKind::Space,
                        id: remote.id,
                        stable_id: remote.stable_id.clone(),
                        deleted_at,
                    });
                }
            } else {
                items.retain(|tombstone| {
                    tombstone.kind != TombstoneKind::Space || tombstone.id != remote.id
                });
            }
        }
    });
    // A new account starts with a blank local placeholder so the editor can
    // render before the account namespace is known. Once the server has real
    // spaces, that placeholder must not win the active-space selection or a
    // second browser will appear empty even though the remote document exists.
    let active_is_blank_placeholder = spaces
        .get_untracked()
        .iter()
        .find(|space| space.id == active_space_id.get_untracked())
        .is_some_and(|space| {
            space.board.notes.is_empty()
                && space.board.groups.is_empty()
                && space.board.tombstones.is_empty()
                && space.name == "my space"
        });
    if active_is_blank_placeholder
        && remote_ids
            .iter()
            .any(|remote_id| *remote_id != active_space_id.get_untracked())
    {
        let placeholder_id = active_space_id.get_untracked();
        spaces.update(|local_spaces| {
            local_spaces.retain(|space| {
                space.id != placeholder_id
                    || remote_ids.contains(&space.id)
                    || !space.board.notes.is_empty()
                    || !space.board.groups.is_empty()
                    || !space.board.tombstones.is_empty()
                    || space.name != "my space"
            });
        });
    }
    spaces.update(|local_spaces| {
        for remote in remote_spaces {
            if let Some(local) = local_spaces.iter_mut().find(|local| local.id == remote.id) {
                local.stable_id = if !is_valid_stable_id(&remote.stable_id) {
                    legacy_entity_stable_id("space", remote.id)
                } else {
                    remote.stable_id.clone()
                };
                local.name = remote.name.clone();
                local.metadata_version = remote.metadata_version;
                local.archived = remote.archived || remote.deleted_at.is_some();
                local.updated_at = remote.updated_at;
                local.deleted_at = remote.deleted_at;
            } else {
                local_spaces.push(Space {
                    id: remote.id,
                    stable_id: if !is_valid_stable_id(&remote.stable_id) {
                        legacy_entity_stable_id("space", remote.id)
                    } else {
                        remote.stable_id
                    },
                    name: remote.name,
                    metadata_version: remote.metadata_version,
                    archived: remote.archived || remote.deleted_at.is_some(),
                    created_at: remote.created_at,
                    updated_at: remote.updated_at,
                    deleted_at: remote.deleted_at,
                    // The CRDT snapshot is fetched with the normal pull path
                    // when this space becomes active. Keeping this empty here
                    // avoids treating a JSON projection as CRDT state.
                    board: BoardData::default(),
                });
            }
        }
    });
    let active_space_is_remote = remote_ids.contains(&active_space_id.get_untracked());
    let active_space_is_valid = spaces.get_untracked().iter().any(|space| {
        space.id == active_space_id.get_untracked() && !space.archived && space.deleted_at.is_none()
    });
    if (!active_space_is_valid || (!active_space_is_remote && active_is_blank_placeholder))
        && let Some(space) = spaces.get_untracked().iter().find(|space| {
            remote_ids.contains(&space.id) && !space.archived && space.deleted_at.is_none()
        })
    {
        active_space_id.set(space.id);
    }
    if let Some(highest_remote_id) = highest_remote_id {
        next_space_id.update(|next| {
            *next = (*next).max(highest_remote_id.saturating_add(1));
        });
    }
    let workspace = workspace_snapshot(
        spaces.get_untracked(),
        active_space_id.get_untracked(),
        workspace_tombstones.get_untracked(),
    );
    let _ = save_workspace(&workspace, None);
    true
}

fn parse_queued_crdt_updates(value: JsValue) -> Result<Vec<QueuedCrdtUpdate>, ()> {
    let raw = js_sys::JSON::stringify(&value).map_err(|_| ())?;
    let raw = raw.as_string().ok_or(())?;
    serde_json::from_str(&raw).map_err(|_| ())
}

enum QueueDrainResult {
    Complete,
    Retry,
    RetryAfter(i32),
    CoordinatorLost,
    Paused,
    PausedAuth,
    PausedBilling,
}

fn coordinator_lease_was_lost(runtime: SyncRuntime) -> bool {
    sync_runtime_is_current(runtime) && !sync_coordinator_is_current()
}

fn response_error_code(response: &Response) -> Option<String> {
    response.headers().get("x-task-space-error-code")
}

fn retry_result_for_response(response: &Response) -> QueueDrainResult {
    let Some(value) = response.headers().get("retry-after") else {
        return QueueDrainResult::Retry;
    };
    let Ok(seconds) = value.parse::<u64>() else {
        return QueueDrainResult::Retry;
    };
    let milliseconds = seconds
        .min(60)
        .saturating_mul(1_000)
        .try_into()
        .unwrap_or(60_000);
    QueueDrainResult::RetryAfter(milliseconds.max(250))
}

fn remember_response_id(runtime: SyncRuntime, response: &Response) {
    if let Some(request_id) = response.headers().get("x-request-id") {
        runtime.last_request_id.set(Some(request_id));
    }
}

fn is_billing_pause_response(response: &Response) -> bool {
    response.status() == 403
        && response_error_code(response)
            .as_deref()
            .is_some_and(|code| matches!(code, "SYNC_NOT_ENTITLED" | "SYNC_PAYMENT_PAUSED"))
}

fn metadata_operation_from_name(name: &str) -> Option<SpaceMetadataOperation> {
    Some(match name {
        "rename" => SpaceMetadataOperation::Rename,
        "archive" => SpaceMetadataOperation::Archive,
        "unarchive" => SpaceMetadataOperation::Unarchive,
        "delete" => SpaceMetadataOperation::Delete,
        "restore" => SpaceMetadataOperation::Restore,
        _ => return None,
    })
}

async fn fetch_remote_space_metadata(space_id: u64) -> Option<SpaceManifestEntry> {
    let mut refreshed = false;
    let response = loop {
        let response = send_with_timeout(
            Request::get(&api_url("/sync/spaces")).credentials(RequestCredentials::Include),
        )
        .await
        .ok()?;
        if response.status() == 401 && !refreshed && refresh_session_once().await {
            refreshed = true;
            continue;
        }
        break response;
    };
    if response.status() >= 300 {
        return None;
    }
    response
        .json::<Vec<SpaceManifestEntry>>()
        .await
        .ok()?
        .into_iter()
        .find(|space| space.id == space_id)
}

fn metadata_operation_is_satisfied(
    operation: &SpaceMetadataOperation,
    name: Option<&str>,
    remote: &SpaceManifestEntry,
) -> bool {
    match operation {
        SpaceMetadataOperation::Rename => {
            remote.deleted_at.is_none() && name == Some(remote.name.as_str())
        }
        SpaceMetadataOperation::Archive => remote.archived || remote.deleted_at.is_some(),
        SpaceMetadataOperation::Unarchive => !remote.archived && remote.deleted_at.is_none(),
        SpaceMetadataOperation::Delete => remote.deleted_at.is_some(),
        SpaceMetadataOperation::Restore => !remote.archived && remote.deleted_at.is_none(),
    }
}

async fn drain_metadata_queue_once(
    runtime: SyncRuntime,
    auth_retry_used: &mut bool,
) -> QueueDrainResult {
    let Ok(value) = JsFuture::from(indexed_db_load_metadata_updates()).await else {
        return QueueDrainResult::Retry;
    };
    let Ok(raw) = js_sys::JSON::stringify(&value) else {
        return QueueDrainResult::Retry;
    };
    let Some(raw) = raw.as_string() else {
        return QueueDrainResult::Retry;
    };
    let Ok(queued) = serde_json::from_str::<Vec<QueuedMetadataUpdate>>(&raw) else {
        return QueueDrainResult::Paused;
    };
    if !sync_runtime_is_current(runtime) {
        return QueueDrainResult::Paused;
    }
    if coordinator_lease_was_lost(runtime) {
        return QueueDrainResult::CoordinatorLost;
    }
    'queued: for mut queued in queued {
        let principal = queued
            .principal
            .clone()
            .unwrap_or_else(current_sync_principal);
        if principal != current_sync_principal() || !sync_runtime_is_current(runtime) {
            return QueueDrainResult::Paused;
        }
        if coordinator_lease_was_lost(runtime) {
            return QueueDrainResult::CoordinatorLost;
        }
        let Some(operation) = metadata_operation_from_name(&queued.operation) else {
            return QueueDrainResult::Paused;
        };
        loop {
            if principal != current_sync_principal() || !sync_runtime_is_current(runtime) {
                return QueueDrainResult::Paused;
            }
            if coordinator_lease_was_lost(runtime) {
                return QueueDrainResult::CoordinatorLost;
            }
            let request = SyncMetadataRequest {
                protocol_version: SYNC_RECONCILE_PROTOCOL_VERSION,
                space_id: queued.space_id,
                stable_space_id: runtime
                    .spaces
                    .get_untracked()
                    .iter()
                    .find(|space| space.id == queued.space_id)
                    .map(|space| space.stable_id.clone()),
                operation_id: queued.operation_id.clone(),
                operation: operation.clone(),
                name: queued.name.clone(),
                expected_version: queued.expected_version,
            };
            let Ok(builder) = Request::post(&api_url(&format!(
                "/sync/spaces/{}/metadata",
                queued.space_id
            )))
            .credentials(RequestCredentials::Include)
            .json(&request) else {
                return QueueDrainResult::Retry;
            };
            let Ok(response) = send_request_with_timeout(builder).await else {
                return QueueDrainResult::Retry;
            };
            remember_response_id(runtime, &response);
            if principal != current_sync_principal() || !sync_runtime_is_current(runtime) {
                return QueueDrainResult::Paused;
            }
            if coordinator_lease_was_lost(runtime) {
                return QueueDrainResult::CoordinatorLost;
            }
            match response.status() {
                200..=299 => {
                    let Ok(payload) = response.json::<SyncMetadataResponse>().await else {
                        return QueueDrainResult::Retry;
                    };
                    if principal != current_sync_principal() || !sync_runtime_is_current(runtime) {
                        return QueueDrainResult::Paused;
                    }
                    if coordinator_lease_was_lost(runtime) {
                        return QueueDrainResult::CoordinatorLost;
                    }
                    if payload.protocol_version != SYNC_RECONCILE_PROTOCOL_VERSION
                        || payload.space_id != queued.space_id
                        || (request.stable_space_id.is_some()
                            && payload.stable_space_id != request.stable_space_id)
                    {
                        return QueueDrainResult::Paused;
                    }
                    runtime.spaces.update(|items| {
                        if let Some(space) =
                            items.iter_mut().find(|space| space.id == payload.space_id)
                        {
                            space.name = payload.name.clone();
                            space.archived = payload.archived;
                            space.metadata_version = payload.metadata_version;
                            space.deleted_at = payload.deleted_at;
                        }
                    });
                    if matches!(&operation, SpaceMetadataOperation::Delete) {
                        let deleted_at = payload.deleted_at.unwrap_or_else(now_millis);
                        runtime.workspace_tombstones.update(|items| {
                            if items.iter().all(|tombstone| {
                                tombstone.kind != TombstoneKind::Space
                                    || tombstone.id != payload.space_id
                            }) {
                                items.push(Tombstone {
                                    kind: TombstoneKind::Space,
                                    id: payload.space_id,
                                    stable_id: payload.stable_space_id.clone().unwrap_or_else(
                                        || legacy_entity_stable_id("space", payload.space_id),
                                    ),
                                    deleted_at,
                                });
                            }
                        });
                    } else if matches!(&operation, SpaceMetadataOperation::Restore) {
                        runtime.workspace_tombstones.update(|items| {
                            items.retain(|tombstone| {
                                tombstone.kind != TombstoneKind::Space
                                    || tombstone.id != payload.space_id
                            });
                        });
                    }
                    let workspace = workspace_snapshot(
                        runtime.spaces.get_untracked(),
                        runtime.active_space_id.get_untracked(),
                        runtime.workspace_tombstones.get_untracked(),
                    );
                    let _ = save_workspace(&workspace, None);
                    break;
                }
                408 | 425 | 429 | 500..=599 => return retry_result_for_response(&response),
                401 => {
                    if !*auth_retry_used && refresh_session_once().await {
                        *auth_retry_used = true;
                        return QueueDrainResult::Retry;
                    }
                    return QueueDrainResult::PausedAuth;
                }
                403 if is_billing_pause_response(&response) => {
                    return QueueDrainResult::PausedBilling;
                }
                403 => return QueueDrainResult::Paused,
                409 => {
                    let conflict_superseded = response_error_code(&response).as_deref()
                        == Some("METADATA_CONFLICT_SUPERSEDED");
                    let Some(remote) = fetch_remote_space_metadata(queued.space_id).await else {
                        return QueueDrainResult::Retry;
                    };
                    if principal != current_sync_principal() || !sync_runtime_is_current(runtime) {
                        return QueueDrainResult::Paused;
                    }
                    if coordinator_lease_was_lost(runtime) {
                        return QueueDrainResult::CoordinatorLost;
                    }
                    if conflict_superseded
                        || metadata_operation_is_satisfied(
                            &operation,
                            queued.name.as_deref(),
                            &remote,
                        )
                        || (remote.deleted_at.is_some()
                            && !matches!(&operation, SpaceMetadataOperation::Restore))
                    {
                        runtime.spaces.update(|items| {
                            if let Some(space) =
                                items.iter_mut().find(|space| space.id == remote.id)
                            {
                                space.name = remote.name.clone();
                                space.archived = remote.archived;
                                space.metadata_version = remote.metadata_version;
                                space.deleted_at = remote.deleted_at;
                            }
                        });
                        if matches!(&operation, SpaceMetadataOperation::Delete) {
                            let deleted_at = remote.deleted_at.unwrap_or_else(now_millis);
                            runtime.workspace_tombstones.update(|items| {
                                if items.iter().all(|tombstone| {
                                    tombstone.kind != TombstoneKind::Space
                                        || tombstone.id != remote.id
                                }) {
                                    items.push(Tombstone {
                                        kind: TombstoneKind::Space,
                                        id: remote.id,
                                        stable_id: remote.stable_id.clone(),
                                        deleted_at,
                                    });
                                }
                            });
                        } else if matches!(&operation, SpaceMetadataOperation::Restore) {
                            runtime.workspace_tombstones.update(|items| {
                                items.retain(|tombstone| {
                                    tombstone.kind != TombstoneKind::Space
                                        || tombstone.id != remote.id
                                });
                            });
                        }
                        let workspace = workspace_snapshot(
                            runtime.spaces.get_untracked(),
                            runtime.active_space_id.get_untracked(),
                            runtime.workspace_tombstones.get_untracked(),
                        );
                        let _ = save_workspace(&workspace, None);
                        let _ = JsFuture::from(indexed_db_ack_metadata_update(
                            &principal,
                            queued.space_id,
                            &queued.operation_id,
                        ))
                        .await;
                        if principal != current_sync_principal()
                            || !sync_runtime_is_current(runtime)
                        {
                            return QueueDrainResult::Paused;
                        }
                        if coordinator_lease_was_lost(runtime) {
                            return QueueDrainResult::CoordinatorLost;
                        }
                        runtime.last_server_ack_at.set(Some(now_millis()));
                        SYNC_SERVER_CONTACT.with(|contact| contact.set(true));
                        continue 'queued;
                    }
                    queued.expected_version = Some(remote.metadata_version);
                    let rebased = JsFuture::from(indexed_db_rebase_metadata_update(
                        &principal,
                        queued.space_id,
                        &queued.operation_id,
                        queued.expected_version,
                    ))
                    .await
                    .ok()
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false);
                    if !rebased {
                        return QueueDrainResult::Retry;
                    }
                }
                404 | 422 => return QueueDrainResult::Paused,
                _ => return QueueDrainResult::Paused,
            }
        }
        let _ = JsFuture::from(indexed_db_ack_metadata_update(
            &principal,
            queued.space_id,
            &queued.operation_id,
        ))
        .await;
        if principal != current_sync_principal() || !sync_runtime_is_current(runtime) {
            return QueueDrainResult::Paused;
        }
        if coordinator_lease_was_lost(runtime) {
            return QueueDrainResult::CoordinatorLost;
        }
        runtime.last_server_ack_at.set(Some(now_millis()));
        SYNC_SERVER_CONTACT.with(|contact| contact.set(true));
    }
    QueueDrainResult::Complete
}

async fn drain_sync_queue_once(
    runtime: SyncRuntime,
    auth_retry_used: &mut bool,
) -> QueueDrainResult {
    match drain_metadata_queue_once(runtime, auth_retry_used).await {
        QueueDrainResult::Complete => {}
        other => return other,
    }
    let Ok(value) = JsFuture::from(indexed_db_load_crdt_updates()).await else {
        return QueueDrainResult::Retry;
    };
    let Ok(queued_updates) = parse_queued_crdt_updates(value) else {
        return QueueDrainResult::Paused;
    };
    if !sync_runtime_is_current(runtime) {
        return QueueDrainResult::Paused;
    }
    if coordinator_lease_was_lost(runtime) {
        return QueueDrainResult::CoordinatorLost;
    }
    for queued in queued_updates {
        if !sync_runtime_is_current(runtime) {
            return QueueDrainResult::Paused;
        }
        if coordinator_lease_was_lost(runtime) {
            return QueueDrainResult::CoordinatorLost;
        }
        let inbox_keys = match ensure_sync_runtime_doc(runtime, queued.space_id).await {
            Ok(keys) => keys,
            Err(()) => return QueueDrainResult::Paused,
        };
        if !sync_runtime_is_current(runtime) {
            return QueueDrainResult::Paused;
        }
        if coordinator_lease_was_lost(runtime) {
            return QueueDrainResult::CoordinatorLost;
        }
        let Some(_space_network_guard) = begin_space_network_operation(queued.space_id) else {
            // A state-vector pull for this space is already running. Leave
            // the outbox untouched and let the drain retry after that pull
            // releases the per-space network guard.
            return QueueDrainResult::Retry;
        };
        let Ok(update) = EncodedUpdate::from_base64(queued.update) else {
            // Keep malformed records for diagnostics instead of silently
            // acknowledging and losing the only copy of the update. Stop the
            // drain so the UI remains in an actionable error state.
            return QueueDrainResult::Paused;
        };
        let state_vector = match queued.state_vector {
            Some(state_vector) => {
                let Ok(state_vector) = EncodedUpdate::from_base64(state_vector) else {
                    return QueueDrainResult::Paused;
                };
                state_vector
            }
            None => EncodedUpdate::from_bytes(&SpaceDoc::empty_state_vector()),
        };
        let device_id = queued
            .device_id
            .clone()
            .filter(|value| is_safe_local_identifier(value))
            .unwrap_or_else(load_device_id);
        let request = SyncReconcileRequest {
            protocol_version: SYNC_RECONCILE_PROTOCOL_VERSION,
            document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
            space_id: queued.space_id,
            stable_space_id: runtime
                .spaces
                .get_untracked()
                .iter()
                .find(|space| space.id == queued.space_id)
                .map(|space| space.stable_id.clone()),
            mutation_id: queued.mutation_id.clone(),
            device_id,
            local_generation: queued.local_generation.unwrap_or_default(),
            last_server_sequence: queued.last_server_sequence.unwrap_or_default(),
            state_vector,
            update,
        };
        if !sync_runtime_is_current(runtime) {
            return QueueDrainResult::Paused;
        }
        if coordinator_lease_was_lost(runtime) {
            return QueueDrainResult::CoordinatorLost;
        }
        let Ok(builder) = Request::post(&api_url(&format!(
            "/sync/v2/spaces/{}/reconcile",
            queued.space_id
        )))
        .credentials(RequestCredentials::Include)
        .json(&request) else {
            return QueueDrainResult::Retry;
        };
        let Ok(response) = send_request_with_timeout(builder).await else {
            return QueueDrainResult::Retry;
        };
        remember_response_id(runtime, &response);
        if !sync_runtime_is_current(runtime) {
            return QueueDrainResult::Paused;
        }
        if coordinator_lease_was_lost(runtime) {
            return QueueDrainResult::CoordinatorLost;
        }
        match response.status() {
            200..=299 => {
                let Ok(payload) = response.json::<SyncReconcileResponse>().await else {
                    return QueueDrainResult::Retry;
                };
                if !sync_runtime_is_current(runtime) {
                    return QueueDrainResult::Paused;
                }
                if coordinator_lease_was_lost(runtime) {
                    return QueueDrainResult::CoordinatorLost;
                }
                if payload.protocol_version != SYNC_RECONCILE_PROTOCOL_VERSION
                    || payload.document_schema_version != SYNC_DOCUMENT_SCHEMA_VERSION
                    || payload.space_id != queued.space_id
                    || payload.mutation_id != queued.mutation_id
                    || (request.stable_space_id.is_some()
                        && payload.stable_space_id != request.stable_space_id)
                    || payload.acknowledged_generation
                        != queued.local_generation.unwrap_or_default()
                {
                    return QueueDrainResult::Paused;
                }
                let Ok(remote_update) = payload.update.to_bytes() else {
                    return QueueDrainResult::Paused;
                };
                let Ok(server_state_vector) = payload.state_vector.to_bytes() else {
                    return QueueDrainResult::Paused;
                };
                if remote_update.len() > MAX_SYNC_UPDATE_BYTES
                    || server_state_vector.len() > MAX_SYNC_STATE_VECTOR_BYTES
                {
                    return QueueDrainResult::Paused;
                }
                let principal = queued
                    .principal
                    .clone()
                    .unwrap_or_else(current_sync_principal);
                if principal != current_sync_principal() || !sync_runtime_is_current(runtime) {
                    return QueueDrainResult::Paused;
                }
                if coordinator_lease_was_lost(runtime) {
                    return QueueDrainResult::CoordinatorLost;
                }
                let Some(encoded_snapshot) =
                    apply_reconcile_update(runtime, queued.space_id, &remote_update)
                else {
                    return QueueDrainResult::Retry;
                };
                let inbox_keys_json =
                    serde_json::to_string(&inbox_keys).unwrap_or_else(|_| "[]".to_owned());
                if JsFuture::from(indexed_db_commit_crdt_reconcile(
                    &principal,
                    queued.space_id,
                    &queued.mutation_id,
                    &encoded_snapshot,
                    payload.state_vector.as_str(),
                    payload.acknowledged_generation,
                    &inbox_keys_json,
                ))
                .await
                .is_err()
                {
                    return QueueDrainResult::Retry;
                }
                if principal != current_sync_principal() || !sync_runtime_is_current(runtime) {
                    return QueueDrainResult::Paused;
                }
                if coordinator_lease_was_lost(runtime) {
                    return QueueDrainResult::CoordinatorLost;
                }
                set_acknowledged_state_vector(&principal, queued.space_id, server_state_vector);
                runtime.last_server_ack_at.set(Some(now_millis()));
                SYNC_SERVER_CONTACT.with(|contact| contact.set(true));
                // `event_cursor` is only an advisory server sequence. A
                // reconcile response contains document state but not every
                // metadata event up to that sequence, so treating it as an
                // applied SSE cursor could skip a rename/archive/delete on
                // reconnect. The cursor advances only after the SSE event's
                // authenticated pull has committed below.
            }
            408 | 425 | 429 | 500..=599 => return retry_result_for_response(&response),
            401 => {
                if !*auth_retry_used && refresh_session_once().await {
                    *auth_retry_used = true;
                    return QueueDrainResult::Retry;
                }
                return QueueDrainResult::PausedAuth;
            }
            403 if is_billing_pause_response(&response) => return QueueDrainResult::PausedBilling,
            403 => return QueueDrainResult::Paused,
            404 | 409 | 422 => return QueueDrainResult::Paused,
            _ => return QueueDrainResult::Paused,
        }
        let principal = queued
            .principal
            .clone()
            .unwrap_or_else(current_sync_principal);
        if principal != current_sync_principal() || !sync_runtime_is_current(runtime) {
            return QueueDrainResult::Paused;
        }
        if coordinator_lease_was_lost(runtime) {
            return QueueDrainResult::CoordinatorLost;
        }
        let _ = JsFuture::from(indexed_db_ack_crdt_update(
            &principal,
            queued.space_id,
            &queued.mutation_id,
        ))
        .await;
        if principal != current_sync_principal() || !sync_runtime_is_current(runtime) {
            return QueueDrainResult::Paused;
        }
        if coordinator_lease_was_lost(runtime) {
            return QueueDrainResult::CoordinatorLost;
        }
    }
    QueueDrainResult::Complete
}

fn apply_reconcile_update(runtime: SyncRuntime, space_id: u64, update: &[u8]) -> Option<String> {
    if update.len() > MAX_SYNC_UPDATE_BYTES {
        return None;
    }
    let mut next_board = None;
    let mut encoded_snapshot = None;
    runtime.crdt_docs.update(|items| {
        let doc = items.entry(space_id).or_default();
        if update.is_empty() || doc.apply_update(update).is_ok() {
            next_board = Some(doc.board());
            let snapshot = doc.snapshot();
            if snapshot.len() <= MAX_SYNC_SNAPSHOT_BYTES {
                encoded_snapshot = Some(EncodedUpdate::from_bytes(&snapshot).as_str().to_owned());
            }
        }
    });
    let board = next_board?;
    // Fan out server-returned CRDT operations to sibling tabs immediately.
    // The local channel carries the actual update; the durable outbox and
    // state-vector pull remain the recovery path if a tab misses it.
    if !update.is_empty() {
        let encoded_update = EncodedUpdate::from_bytes(update).as_str().to_owned();
        publish_local_update(
            &current_sync_principal(),
            space_id,
            current_local_generation(space_id),
            &encoded_update,
            &load_device_id(),
        );
    }
    runtime.spaces.update(|items| {
        if let Some(space) = items.iter_mut().find(|space| space.id == space_id) {
            space.board = board.clone();
            space.updated_at = now_millis();
        }
    });
    if runtime.active_space_id.get_untracked() == space_id {
        mark_remote_crdt_projection();
        runtime.notes.set(board.notes);
        runtime.groups.set(board.groups);
    }
    let workspace = workspace_snapshot(
        runtime.spaces.get_untracked(),
        runtime.active_space_id.get_untracked(),
        runtime.workspace_tombstones.get_untracked(),
    );
    let _ = save_workspace(&workspace, None);
    let snapshot = encoded_snapshot?;
    Some(snapshot)
}

async fn sync_wait_ms(milliseconds: i32) {
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        let Some(window) = web_sys::window() else {
            let _ = resolve.call0(&JsValue::NULL);
            return;
        };
        let callback = Closure::once_into_js(move || {
            let _ = resolve.call0(&JsValue::NULL);
        });
        let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
            callback.unchecked_ref(),
            milliseconds,
        );
    });
    let _ = JsFuture::from(promise).await;
}

fn schedule_sync_drain() {
    if !sync_is_active() {
        return;
    }
    let already_running = SYNC_DRAIN_IN_FLIGHT.with(|running| {
        if running.get() {
            SYNC_DRAIN_PENDING.with(|pending| pending.set(true));
            true
        } else {
            running.set(true);
            false
        }
    });
    if already_running {
        return;
    }

    let Some(runtime) = SYNC_RUNTIME.with(|runtime| *runtime.borrow()) else {
        SYNC_DRAIN_IN_FLIGHT.with(|running| running.set(false));
        return;
    };

    spawn_local(async move {
        let mut retry_delay = 1_000;
        let mut auth_retry_used = false;
        loop {
            if !sync_is_active() || !sync_run_is_current(runtime.run_generation) {
                break;
            }
            let principal = current_sync_principal();
            let owns_lease = JsFuture::from(acquire_sync_lease(&principal))
                .await
                .ok()
                .and_then(|value| value.as_bool())
                .unwrap_or(true);
            if !sync_is_active() || !sync_run_is_current(runtime.run_generation) {
                break;
            }
            if !owns_lease {
                runtime.status.set(SyncStatus::Syncing);
                // Another tab may have committed the shared outbox. Refresh
                // this tab's durable count as well; otherwise a non-owner
                // tab can remain stuck on its pre-reconnect "pending" badge
                // even though the coordinator has fully acknowledged it.
                publish_completed_sync_queue_state(runtime).await;
                sync_wait_ms(2_000).await;
                continue;
            }
            if !sync_coordinator_is_current() {
                runtime.status.set(SyncStatus::Syncing);
                publish_completed_sync_queue_state(runtime).await;
                sync_wait_ms(2_000).await;
                continue;
            }
            runtime.status.set(SyncStatus::Syncing);
            match drain_sync_queue_once(runtime, &mut auth_retry_used).await {
                QueueDrainResult::Complete => {
                    retry_delay = 1_000;
                    auth_retry_used = false;
                    publish_completed_sync_queue_state(runtime).await;
                    refresh_sync_diagnostics(runtime);
                    let rerun = SYNC_DRAIN_PENDING.with(|pending| pending.take());
                    if !rerun {
                        break;
                    }
                }
                QueueDrainResult::Retry => {
                    runtime.status.set(SyncStatus::Offline);
                    let jitter =
                        (f64::from(retry_delay) * 0.25 * (js_sys::Math::random() * 2.0 - 1.0))
                            as i32;
                    sync_wait_ms((retry_delay + jitter).max(250)).await;
                    retry_delay = (retry_delay * 2).min(60_000);
                }
                QueueDrainResult::RetryAfter(delay) => {
                    runtime.status.set(SyncStatus::Offline);
                    sync_wait_ms(delay).await;
                    retry_delay = 1_000;
                }
                QueueDrainResult::CoordinatorLost => {
                    runtime.status.set(SyncStatus::Syncing);
                    sync_wait_ms(2_000).await;
                }
                QueueDrainResult::PausedAuth => {
                    stop_sync_events(&api_url("/sync/events"));
                    runtime.status.set(SyncStatus::AuthPaused);
                    break;
                }
                QueueDrainResult::PausedBilling => {
                    runtime.status.set(SyncStatus::BillingPaused);
                    break;
                }
                QueueDrainResult::Paused => {
                    runtime.status.set(SyncStatus::Error);
                    break;
                }
            }
        }
        SYNC_DRAIN_IN_FLIGHT.with(|running| running.set(false));
        if sync_is_active()
            && sync_run_is_current(runtime.run_generation)
            && SYNC_DRAIN_PENDING.with(|pending| pending.take())
        {
            schedule_sync_drain();
        }
    });
}

const LOCAL_BROADCAST_PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LocalSyncUpdate {
    protocol_version: u32,
    #[serde(default = "default_local_sync_kind")]
    kind: String,
    principal: String,
    space_id: u64,
    #[serde(default, rename = "originDeviceId")]
    _origin_device_id: Option<String>,
    #[serde(default)]
    origin_tab_id: Option<String>,
    #[serde(default, rename = "localGeneration")]
    local_generation: u64,
    #[serde(default)]
    update: Option<String>,
    #[serde(default)]
    operation: Option<String>,
    #[serde(default, rename = "operationId")]
    operation_id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default, rename = "stableSpaceId")]
    stable_space_id: Option<String>,
}

fn default_local_sync_kind() -> String {
    "document".to_owned()
}

fn is_own_local_tab_message(message: &LocalSyncUpdate) -> bool {
    message
        .origin_tab_id
        .as_deref()
        .is_some_and(|tab_id| tab_id == current_tab_id())
}

fn start_local_tab_sync(
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
    storage_hydrated: RwSignal<bool>,
) {
    let on_update = Closure::<dyn FnMut(JsValue)>::new(move |value: JsValue| {
        let Some(raw) = value.as_string() else {
            return;
        };
        let Ok(message) = serde_json::from_str::<LocalSyncUpdate>(&raw) else {
            return;
        };
        if is_own_local_tab_message(&message)
            || message.protocol_version != LOCAL_BROADCAST_PROTOCOL_VERSION
            || message.principal != current_sync_principal()
        {
            return;
        }
        if !storage_hydrated.get_untracked() {
            LOCAL_TAB_PENDING_MESSAGES.with(|pending| {
                let mut pending = pending.borrow_mut();
                if pending.len() >= 256 {
                    pending.pop_front();
                }
                pending.push_back(message);
            });
            return;
        }
        if message.kind == "metadata" {
            apply_local_tab_metadata(
                &message,
                spaces,
                active_space_id,
                notes,
                groups,
                workspace_tombstones,
            );
        } else if message.kind == "space" {
            apply_local_tab_space(&message, spaces, active_space_id, workspace_tombstones);
        } else {
            queue_local_tab_update(
                message,
                spaces,
                active_space_id,
                notes,
                groups,
                workspace_tombstones,
                crdt_docs,
            );
        }
    });
    if start_local_broadcast(
        &current_sync_principal(),
        on_update.as_ref().unchecked_ref(),
    ) {
        on_cleanup(stop_local_broadcast);
    }
    // The callback is owned by the BroadcastChannel for this page lifetime;
    // the component cleanup closes the channel.
    on_update.forget();
}

fn flush_local_tab_messages(
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
) {
    let pending: Vec<LocalSyncUpdate> =
        LOCAL_TAB_PENDING_MESSAGES.with(|messages| messages.borrow_mut().drain(..).collect());
    for message in pending {
        if is_own_local_tab_message(&message)
            || message.protocol_version != LOCAL_BROADCAST_PROTOCOL_VERSION
            || message.principal != current_sync_principal()
        {
            continue;
        }
        if message.kind == "metadata" {
            apply_local_tab_metadata(
                &message,
                spaces,
                active_space_id,
                notes,
                groups,
                workspace_tombstones,
            );
        } else if message.kind == "space" {
            apply_local_tab_space(&message, spaces, active_space_id, workspace_tombstones);
        } else {
            queue_local_tab_update(
                message,
                spaces,
                active_space_id,
                notes,
                groups,
                workspace_tombstones,
                crdt_docs,
            );
        }
    }
}

fn apply_local_tab_space(
    message: &LocalSyncUpdate,
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
) {
    if message.operation.as_deref() != Some("create") {
        return;
    }
    let Some(name) = message
        .name
        .as_deref()
        .filter(|name| !name.trim().is_empty())
    else {
        return;
    };
    let stable_id = message
        .stable_space_id
        .as_deref()
        .filter(|stable_id| is_valid_stable_id(stable_id))
        .map(str::to_owned)
        .unwrap_or_else(|| legacy_entity_stable_id("space", message.space_id));
    let mut inserted = false;
    spaces.update(|items| {
        if items.iter().any(|space| space.id == message.space_id) {
            return;
        }
        let now = now_millis();
        items.push(Space {
            id: message.space_id,
            stable_id,
            name: name.trim().to_owned(),
            metadata_version: 0,
            archived: false,
            created_at: now,
            updated_at: now,
            deleted_at: None,
            board: BoardData::default(),
        });
        inserted = true;
    });
    if !inserted {
        return;
    }
    workspace_tombstones.update(|items| {
        items.retain(|tombstone| {
            tombstone.kind != TombstoneKind::Space || tombstone.id != message.space_id
        });
    });
    let workspace = workspace_snapshot(
        spaces.get_untracked(),
        active_space_id.get_untracked(),
        workspace_tombstones.get_untracked(),
    );
    let _ = save_workspace(&workspace, None);
    // Keep the active projection untouched: creating a sibling space is a
    // workspace change, not an instruction to switch the current tab.
}

fn queue_local_tab_update(
    message: LocalSyncUpdate,
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
) {
    LOCAL_TAB_UPDATE_QUEUE.with(|queue| {
        let mut queue = queue.borrow_mut();
        // A bounded queue prevents a malformed or malicious producer from
        // exhausting the tab. The server/reconcile path remains the durable
        // recovery source if an old local message is dropped here.
        if queue.len() >= 256 {
            queue.pop_front();
        }
        queue.push_back(message);
    });
    let already_running = LOCAL_TAB_UPDATE_RUNNING.with(|running| {
        if running.get() {
            true
        } else {
            running.set(true);
            false
        }
    });
    if already_running {
        return;
    }

    spawn_local(async move {
        loop {
            let message = LOCAL_TAB_UPDATE_QUEUE.with(|queue| queue.borrow_mut().pop_front());
            let Some(message) = message else {
                LOCAL_TAB_UPDATE_RUNNING.with(|running| running.set(false));
                break;
            };
            apply_local_tab_update(
                message,
                spaces,
                active_space_id,
                notes,
                groups,
                workspace_tombstones,
                crdt_docs,
            )
            .await;
        }
    });
}

async fn apply_local_tab_update(
    message: LocalSyncUpdate,
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
) {
    if message.principal != current_sync_principal() {
        return;
    }
    let Some(update) = message.update else {
        return;
    };
    let Ok(encoded) = EncodedUpdate::from_base64(update) else {
        return;
    };
    let Ok(update) = encoded.to_bytes() else {
        return;
    };
    if update.is_empty() || update.len() > MAX_SYNC_UPDATE_BYTES {
        return;
    }

    let space_id = message.space_id;
    let doc = if let Some(doc) = crdt_docs.get_untracked().get(&space_id).cloned() {
        doc
    } else {
        let loaded_snapshot = match JsFuture::from(indexed_db_load_crdt(space_id)).await {
            Ok(value) if value.is_null() || value.is_undefined() => None,
            Ok(value) => match decode_indexed_crdt_value(value) {
                Ok(snapshot) => snapshot,
                Err(()) => {
                    mark_storage_repair_required();
                    return;
                }
            },
            Err(_) => {
                mark_storage_repair_required();
                return;
            }
        };
        if message.principal != current_sync_principal() {
            return;
        }
        if let Some(snapshot) = loaded_snapshot {
            let Ok(doc) = SpaceDoc::from_update(&snapshot) else {
                mark_storage_repair_required();
                return;
            };
            doc
        } else {
            let doc = SpaceDoc::new();
            if let Some(space) = spaces
                .get_untracked()
                .iter()
                .find(|space| space.id == space_id)
            {
                doc.import_board(&space.board);
            }
            doc
        }
    };

    let pending_updates = match load_pending_crdt_updates(space_id).await {
        Ok(updates) => updates,
        Err(()) => {
            mark_storage_repair_required();
            return;
        }
    };
    if !replay_pending_crdt_updates(&doc, &pending_updates.updates) {
        mark_storage_repair_required();
        return;
    }

    // Persist the sibling delta before applying it to the live projection.
    // The snapshot mirror below is intentionally best-effort because another
    // tab may have won the generation race; the inbox is the crash-safe copy
    // that lets the next hydration replay this exact operation.
    let origin_device_id = message
        ._origin_device_id
        .as_deref()
        .or(message.origin_tab_id.as_deref())
        .unwrap_or("unknown");
    let encoded_update = EncodedUpdate::from_bytes(&update);
    if JsFuture::from(indexed_db_queue_incoming_crdt_update(
        &message.principal,
        space_id,
        origin_device_id,
        message.local_generation,
        encoded_update.as_str(),
    ))
    .await
    .is_err()
    {
        mark_storage_repair_required();
        return;
    }
    if message.principal != current_sync_principal() {
        return;
    }

    let candidate = doc.clone();
    if candidate.apply_update(&update).is_err() {
        return;
    }
    if message.principal != current_sync_principal() {
        return;
    }
    let snapshot = candidate.snapshot();
    if snapshot.len() > MAX_SYNC_SNAPSHOT_BYTES {
        return;
    }
    let board = candidate.board();
    crdt_docs.update(|items| {
        items.insert(space_id, candidate);
    });
    spaces.update(|items| {
        if let Some(space) = items.iter_mut().find(|space| space.id == space_id) {
            space.board = board.clone();
            space.updated_at = now_millis();
        }
    });
    if active_space_id.get_untracked() == space_id {
        mark_remote_crdt_projection();
        notes.set(board.notes.clone());
        groups.set(board.groups.clone());
    }

    let principal = message.principal;
    queue_indexed_db_crdt_save(
        space_id,
        EncodedUpdate::from_bytes(&snapshot).as_str().to_owned(),
        message.local_generation,
        origin_device_id.to_owned(),
        message.local_generation,
    );
    let mut workspace = workspace_snapshot(
        spaces.get_untracked(),
        active_space_id.get_untracked(),
        workspace_tombstones.get_untracked(),
    );
    if let Some(space) = workspace
        .spaces
        .iter_mut()
        .find(|space| space.id == space_id)
    {
        space.board = board;
    }
    if principal == current_sync_principal() {
        let _ = save_workspace(&workspace, None);
    }
}

/// Recover local updates that were durably written by a sibling tab while
/// this tab was suspended, hydrating, or running without BroadcastChannel.
/// Applying a full snapshot through Yrs is safe: already-known operations are
/// ignored and newer operations merge without replacing local state.
async fn refresh_local_space_from_indexed_db(
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
    storage_status: RwSignal<StorageStatus>,
    restore_message: RwSignal<Option<String>>,
) {
    let principal = current_sync_principal();
    let space_id = active_space_id.get_untracked();
    let value = match JsFuture::from(indexed_db_load_crdt(space_id)).await {
        Ok(value) => value,
        Err(_) => {
            surface_storage_repair(storage_status, restore_message);
            return;
        }
    };
    if principal != current_sync_principal() {
        return;
    }
    let snapshot = match decode_indexed_crdt_value(value) {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => return,
        Err(()) => {
            surface_storage_repair(storage_status, restore_message);
            return;
        }
    };
    if snapshot.len() > MAX_SYNC_SNAPSHOT_BYTES {
        return;
    }
    let doc = if let Some(doc) = crdt_docs.get_untracked().get(&space_id).cloned() {
        doc
    } else if let Ok(doc) = SpaceDoc::from_update(&snapshot) {
        doc
    } else {
        surface_storage_repair(storage_status, restore_message);
        return;
    };
    let pending_updates = match load_pending_crdt_updates(space_id).await {
        Ok(updates) => updates,
        Err(()) => {
            surface_storage_repair(storage_status, restore_message);
            return;
        }
    };
    let before = doc.board();
    let candidate = doc.clone();
    if candidate.apply_update(&snapshot).is_err()
        || !replay_pending_crdt_updates(&candidate, &pending_updates.updates)
        || principal != current_sync_principal()
    {
        if principal == current_sync_principal() {
            surface_storage_repair(storage_status, restore_message);
        }
        return;
    }
    let board = candidate.board();
    if board == before {
        return;
    }
    let encoded_snapshot = candidate.snapshot();
    if encoded_snapshot.len() > MAX_SYNC_SNAPSHOT_BYTES {
        return;
    }
    crdt_docs.update(|items| {
        items.insert(space_id, candidate);
    });
    spaces.update(|items| {
        if let Some(space) = items.iter_mut().find(|space| space.id == space_id) {
            space.board = board.clone();
            space.updated_at = now_millis();
        }
    });
    if active_space_id.get_untracked() == space_id {
        mark_remote_crdt_projection();
        notes.set(board.notes.clone());
        groups.set(board.groups.clone());
    }
    let workspace = workspace_snapshot(
        spaces.get_untracked(),
        active_space_id.get_untracked(),
        workspace_tombstones.get_untracked(),
    );
    let _ = save_workspace(&workspace, None);
    schedule_sync_drain();
    publish_sync_hint();
}

fn apply_local_tab_metadata(
    message: &LocalSyncUpdate,
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
) {
    let Some(operation) = message.operation.as_deref() else {
        return;
    };
    if let Some(operation_id) = message
        .operation_id
        .as_deref()
        .filter(|operation_id| !operation_id.is_empty())
    {
        let key = (current_sync_principal(), message.space_id);
        let should_apply = LOCAL_METADATA_WATERMARKS.with(|watermarks| {
            let mut watermarks = watermarks.borrow_mut();
            let should_apply = watermarks
                .get(&key)
                .is_none_or(|current| operation_id > current.as_str());
            if should_apply {
                watermarks.insert(key, operation_id.to_owned());
            }
            should_apply
        });
        if !should_apply {
            return;
        }
    }
    let now = now_millis();
    spaces.update(|items| {
        let Some(space) = items.iter_mut().find(|space| space.id == message.space_id) else {
            return;
        };
        match operation {
            "rename" => {
                if let Some(name) = message.name.as_deref() {
                    space.name = name.to_owned();
                }
            }
            "archive" => space.archived = true,
            "unarchive" => {
                space.archived = false;
                space.deleted_at = None;
            }
            "delete" => {
                space.archived = true;
                space.deleted_at = Some(now);
                workspace_tombstones.update(|items| {
                    if items.iter().all(|tombstone| {
                        tombstone.kind != TombstoneKind::Space || tombstone.id != message.space_id
                    }) {
                        items.push(Tombstone {
                            kind: TombstoneKind::Space,
                            id: message.space_id,
                            stable_id: spaces
                                .get_untracked()
                                .iter()
                                .find(|space| space.id == message.space_id)
                                .map(|space| space.stable_id.clone())
                                .unwrap_or_else(|| {
                                    legacy_entity_stable_id("space", message.space_id)
                                }),
                            deleted_at: now,
                        });
                    }
                });
            }
            "restore" => {
                space.archived = false;
                space.deleted_at = None;
                workspace_tombstones.update(|items| {
                    items.retain(|tombstone| {
                        tombstone.kind != TombstoneKind::Space || tombstone.id != message.space_id
                    });
                });
            }
            _ => return,
        }
        space.updated_at = now;
    });
    if (operation == "delete" || operation == "archive")
        && active_space_id.get_untracked() == message.space_id
        && let Some(space) = spaces
            .get_untracked()
            .iter()
            .find(|space| !space.archived && space.deleted_at.is_none())
    {
        active_space_id.set(space.id);
        notes.set(space.board.notes.clone());
        groups.set(space.board.groups.clone());
    }
    let workspace = workspace_snapshot(
        spaces.get_untracked(),
        active_space_id.get_untracked(),
        workspace_tombstones.get_untracked(),
    );
    let _ = save_workspace(&workspace, None);
    // Metadata was already durably queued by the originating tab. This tab
    // may be the surviving coordinator, so wake its durable metadata drain
    // after applying the projection locally.
    schedule_sync_drain();
    publish_sync_hint();
}

fn apply_remote_sync_event(
    raw: String,
    run_generation: u64,
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
) {
    let next_space_id = SYNC_RUNTIME
        .with(|runtime| {
            runtime
                .borrow()
                .as_ref()
                .map(|runtime| runtime.next_space_id)
        })
        .unwrap_or_else(|| RwSignal::new(0));
    let runtime = RemoteSyncEventRuntime {
        run_generation,
        next_space_id,
        spaces,
        active_space_id,
        notes,
        groups,
        workspace_tombstones,
        crdt_docs,
    };
    REMOTE_SYNC_EVENT_QUEUE.with(|queue| queue.borrow_mut().push_back((raw, runtime)));
    let should_start = REMOTE_SYNC_EVENT_RUNNING.with(|running| {
        if running.get() {
            false
        } else {
            running.set(true);
            true
        }
    });
    if should_start {
        spawn_local(drain_remote_sync_events());
    }
}

async fn drain_remote_sync_events() {
    loop {
        let next = REMOTE_SYNC_EVENT_QUEUE.with(|queue| queue.borrow_mut().pop_front());
        let Some((raw, runtime)) = next else {
            REMOTE_SYNC_EVENT_RUNNING.with(|running| running.set(false));
            return;
        };
        process_remote_sync_event(raw, runtime).await;
    }
}

fn requeue_remote_sync_event(raw: String, runtime: RemoteSyncEventRuntime) {
    REMOTE_SYNC_EVENT_QUEUE.with(|queue| queue.borrow_mut().push_front((raw, runtime)));
}

async fn process_remote_sync_event(raw: String, runtime: RemoteSyncEventRuntime) {
    let event_runtime = runtime;
    let RemoteSyncEventRuntime {
        run_generation,
        next_space_id,
        spaces,
        active_space_id,
        notes,
        groups,
        workspace_tombstones,
        crdt_docs,
    } = runtime;
    if !sync_is_active() || !sync_run_is_current(run_generation) {
        return;
    }
    let Ok(event) = serde_json::from_str::<SyncEvent>(&raw) else {
        return;
    };
    if event.protocol_version != SYNC_PROTOCOL_VERSION
        || event.document_schema_version != SYNC_DOCUMENT_SCHEMA_VERSION
        || event.event_id == 0
    {
        return;
    }
    let known_space = spaces
        .get_untracked()
        .into_iter()
        .find(|space| space.id == event.space_id);
    if known_space.is_none()
        || (event.stable_space_id.is_some()
            && known_space.as_ref().is_some_and(|space| {
                event.stable_space_id.as_deref() != Some(space.stable_id.as_str())
            }))
    {
        // A new space, or a stable-ID mismatch, must be resolved through the
        // authenticated manifest before an event can touch local state.
        refresh_manifest_and_reconcile(
            spaces,
            next_space_id,
            active_space_id,
            notes,
            groups,
            workspace_tombstones,
            crdt_docs,
            run_generation,
        )
        .await;
        let resolved = spaces.get_untracked().iter().any(|space| {
            space.id == event.space_id
                && (event.stable_space_id.is_none()
                    || event.stable_space_id.as_deref() == Some(space.stable_id.as_str()))
        });
        if !resolved {
            requeue_remote_sync_event(raw.clone(), event_runtime);
            sync_wait_ms(1_000).await;
        }
        if !resolved {
            return;
        }
    }
    // SSE is a latency optimization, never the correctness path. The event
    // may contain a delta whose dependencies this tab did not receive, so
    // always schedule a state-vector pull as well as attempting the fast
    // local apply below.
    schedule_sync_drain();
    // SSE is a latency optimization, never the correctness path. The event
    // may contain a delta whose dependencies this tab did not receive, so a
    // document event is followed by an authenticated state-vector pull.
    if let Some(metadata) = event.metadata.as_ref() {
        let current_metadata_version = spaces
            .get_untracked()
            .iter()
            .find(|space| space.id == event.space_id)
            .map(|space| space.metadata_version)
            .unwrap_or_default();
        // Durable event delivery can be observed out of order across API
        // instances. Never let an older metadata event roll a projection back
        // after a newer version has already been rendered.
        if metadata.metadata_version >= current_metadata_version {
            let deleted = metadata.deleted_at.is_some();
            if matches!(
                metadata.operation.as_ref(),
                Some(SpaceMetadataOperation::Delete)
            ) {
                let deleted_at = metadata.deleted_at.unwrap_or_else(now_millis);
                workspace_tombstones.update(|items| {
                    if items.iter().all(|tombstone| {
                        tombstone.kind != TombstoneKind::Space || tombstone.id != event.space_id
                    }) {
                        items.push(Tombstone {
                            kind: TombstoneKind::Space,
                            id: event.space_id,
                            stable_id: event.stable_space_id.clone().unwrap_or_else(|| {
                                legacy_entity_stable_id("space", event.space_id)
                            }),
                            deleted_at,
                        });
                    }
                });
            } else if matches!(
                metadata.operation.as_ref(),
                Some(SpaceMetadataOperation::Restore)
            ) {
                workspace_tombstones.update(|items| {
                    items.retain(|tombstone| {
                        tombstone.kind != TombstoneKind::Space || tombstone.id != event.space_id
                    });
                });
            }
            spaces.update(|items| {
                if let Some(space) = items.iter_mut().find(|space| space.id == event.space_id) {
                    space.name = metadata.name.clone();
                    space.metadata_version = metadata.metadata_version;
                    space.archived = metadata.archived || deleted;
                    space.deleted_at = metadata.deleted_at;
                }
            });
            if deleted
                && active_space_id.get_untracked() == event.space_id
                && let Some(space) = spaces
                    .get_untracked()
                    .iter()
                    .find(|space| !space.archived && space.deleted_at.is_none())
            {
                active_space_id.set(space.id);
                notes.set(space.board.notes.clone());
                groups.set(space.board.groups.clone());
            }
            let workspace = workspace_snapshot(
                spaces.get_untracked(),
                active_space_id.get_untracked(),
                workspace_tombstones.get_untracked(),
            );
            let _ = save_workspace(&workspace, None);
            if event.update.as_str().is_empty() {
                if persist_workspace_before_event_cursor(event_runtime).await {
                    save_sync_cursor(event.event_id);
                } else {
                    requeue_remote_sync_event(raw, event_runtime);
                    sync_wait_ms(1_000).await;
                }
                return;
            }
        }
    }
    let Ok(update) = event.update.to_bytes() else {
        if pull_space(
            spaces,
            active_space_id,
            notes,
            groups,
            workspace_tombstones,
            crdt_docs,
            event.space_id,
            run_generation,
        )
        .await
        {
            if persist_workspace_before_event_cursor(event_runtime).await {
                save_sync_cursor(event.event_id);
            } else {
                requeue_remote_sync_event(raw, event_runtime);
            }
        } else {
            requeue_remote_sync_event(raw, event_runtime);
            sync_wait_ms(1_000).await;
        }
        return;
    };
    if update.len() > MAX_SYNC_UPDATE_BYTES {
        return;
    }
    let mut next_board = None;
    crdt_docs.update(|items| {
        // An SSE event carries one mutation, not a full document. If this tab
        // has not hydrated the space yet, let the state-vector pull bootstrap
        // it instead of rendering and persisting a partial projection.
        let Some(doc) = items.get_mut(&event.space_id) else {
            return;
        };
        let candidate = doc.clone();
        if candidate.apply_update(&update).is_ok() {
            let snapshot = candidate.snapshot();
            if snapshot.len() <= MAX_SYNC_SNAPSHOT_BYTES {
                *doc = candidate;
                next_board = Some(doc.board());
            }
        }
    });
    let Some(board) = next_board else {
        // An SSE event is only a low-latency hint. Yrs may reject a delta
        // when this replica missed one of its dependencies or when events
        // crossed an API/reconnect boundary. The old path returned here and
        // left the tab permanently stale because it never executed the
        // state-vector pull promised by the protocol. Hydrate from the
        // durable server snapshot instead, then advance the cursor only
        // after that recovery pull has committed locally.
        if pull_space(
            spaces,
            active_space_id,
            notes,
            groups,
            workspace_tombstones,
            crdt_docs,
            event.space_id,
            run_generation,
        )
        .await
        {
            if persist_workspace_before_event_cursor(event_runtime).await {
                save_sync_cursor(event.event_id);
            } else {
                requeue_remote_sync_event(raw, event_runtime);
            }
        } else {
            requeue_remote_sync_event(raw, event_runtime);
            sync_wait_ms(1_000).await;
        }
        return;
    };
    if !update.is_empty() {
        let encoded_update = EncodedUpdate::from_bytes(&update).as_str().to_owned();
        publish_local_update(
            &current_sync_principal(),
            event.space_id,
            current_local_generation(event.space_id),
            &encoded_update,
            &load_device_id(),
        );
    }
    spaces.update(|items| {
        if let Some(space) = items.iter_mut().find(|space| space.id == event.space_id) {
            space.board = board.clone();
            space.updated_at = now_millis();
        }
    });
    if active_space_id.get_untracked() == event.space_id {
        mark_remote_crdt_projection();
        notes.set(board.notes);
        groups.set(board.groups);
    }
    let workspace = workspace_snapshot(
        spaces.get_untracked(),
        active_space_id.get_untracked(),
        workspace_tombstones.get_untracked(),
    );
    let _ = save_workspace(&workspace, None);
    if !pull_space(
        spaces,
        active_space_id,
        notes,
        groups,
        workspace_tombstones,
        crdt_docs,
        event.space_id,
        run_generation,
    )
    .await
    {
        requeue_remote_sync_event(raw, event_runtime);
        sync_wait_ms(1_000).await;
        return;
    }
    if persist_workspace_before_event_cursor(event_runtime).await {
        // The queue processes SSE events serially, so every lower event ID
        // has completed its own pull or metadata persistence before this
        // cursor is made durable.
        save_sync_cursor(event.event_id);
    } else {
        requeue_remote_sync_event(raw, event_runtime);
        sync_wait_ms(1_000).await;
    }
}

struct SyncSpaceNetworkGuard {
    key: (String, u64),
}

impl Drop for SyncSpaceNetworkGuard {
    fn drop(&mut self) {
        SYNC_SPACE_NETWORK_IN_FLIGHT.with(|spaces| {
            spaces.borrow_mut().remove(&self.key);
        });
    }
}

fn begin_space_network_operation(space_id: u64) -> Option<SyncSpaceNetworkGuard> {
    let key = (current_sync_principal(), space_id);
    SYNC_SPACE_NETWORK_IN_FLIGHT.with(|spaces| {
        let mut spaces = spaces.borrow_mut();
        if !spaces.insert(key.clone()) {
            return None;
        }
        Some(SyncSpaceNetworkGuard { key })
    })
}

async fn pull_space(
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
    space_id: u64,
    run_generation: u64,
) -> bool {
    let principal = current_sync_principal();
    let Some(_space_network_guard) = begin_space_network_operation(space_id) else {
        return false;
    };
    // Load the acknowledged server vector even when the CRDT document is
    // already cached in memory. Without this, the first edit after a fast
    // account switch/reconnect could diff against the local vector and queue
    // an empty upload instead of the actual local change.
    if let Ok(value) = JsFuture::from(indexed_db_load_sync_state(&principal, space_id)).await
        && let Some(encoded) = value
            .as_string()
            .and_then(|encoded| EncodedUpdate::from_base64(encoded).ok())
        && let Ok(state_vector) = encoded.to_bytes()
    {
        set_acknowledged_state_vector(&principal, space_id, state_vector);
    } else if acknowledged_state_vector(space_id).is_none() {
        set_acknowledged_state_vector(&principal, space_id, SpaceDoc::empty_state_vector());
    }
    if principal != current_sync_principal()
        || !sync_is_active()
        || !sync_run_is_current(run_generation)
    {
        return false;
    }
    let existing_doc = crdt_docs.get_untracked().get(&space_id).cloned();
    let loaded_snapshot = match JsFuture::from(indexed_db_load_crdt(space_id)).await {
        Ok(value) => match decode_indexed_crdt_value(value) {
            Ok(snapshot) => snapshot,
            Err(()) => return false,
        },
        Err(_) => None,
    };
    if principal != current_sync_principal()
        || !sync_is_active()
        || !sync_run_is_current(run_generation)
    {
        return false;
    }
    let doc = if let Some(doc) = existing_doc {
        if let Some(snapshot) = loaded_snapshot
            && doc.apply_snapshot(&snapshot).is_err()
        {
            return false;
        }
        doc
    } else if let Some(snapshot) = loaded_snapshot {
        match SpaceDoc::from_update(&snapshot) {
            Ok(doc) => doc,
            Err(_) => return false,
        }
    } else {
        let doc = SpaceDoc::new();
        if let Some(space) = spaces
            .get_untracked()
            .into_iter()
            .find(|space| space.id == space_id)
        {
            doc.import_board(&space.board);
        }
        doc
    };
    let Ok(pending_updates) = load_pending_crdt_updates(space_id).await else {
        return false;
    };
    if principal != current_sync_principal()
        || !sync_is_active()
        || !sync_run_is_current(run_generation)
    {
        return false;
    }
    if !replay_pending_crdt_updates(&doc, &pending_updates.updates) {
        return false;
    }
    crdt_docs.update(|items| {
        items.insert(space_id, doc.clone());
    });
    let request = SyncPullRequest {
        protocol_version: SYNC_PROTOCOL_VERSION,
        document_schema_version: SYNC_DOCUMENT_SCHEMA_VERSION,
        space_id,
        stable_space_id: spaces
            .get_untracked()
            .iter()
            .find(|space| space.id == space_id)
            .map(|space| space.stable_id.clone()),
        last_server_sequence: current_sync_cursor(&principal)
            .parse::<u64>()
            .unwrap_or_default(),
        state_vector: EncodedUpdate::from_bytes(&doc.state_vector()),
        local_generation: current_local_generation(space_id),
    };
    let mut refreshed = false;
    let response = loop {
        let Ok(builder) = Request::post(&api_url("/sync/pull"))
            .credentials(RequestCredentials::Include)
            .json(&request)
        else {
            return false;
        };
        let Ok(response) = send_request_with_timeout(builder).await else {
            return false;
        };
        if response.status() == 401 && !refreshed && refresh_session_once().await {
            refreshed = true;
            continue;
        }
        break response;
    };
    if principal != current_sync_principal()
        || !sync_is_active()
        || !sync_run_is_current(run_generation)
    {
        return false;
    }
    if response.status() >= 300 {
        return false;
    }
    let Ok(payload) = response.json::<SyncPullResponse>().await else {
        return false;
    };
    if principal != current_sync_principal()
        || !sync_is_active()
        || !sync_run_is_current(run_generation)
    {
        return false;
    }
    if payload.protocol_version != SYNC_PROTOCOL_VERSION
        || payload.document_schema_version != SYNC_DOCUMENT_SCHEMA_VERSION
        || payload.space_id != space_id
        || (request.stable_space_id.is_some() && payload.stable_space_id != request.stable_space_id)
        || payload.has_more
    {
        return false;
    }
    let Ok(update) = payload.update.to_bytes() else {
        return false;
    };
    let Ok(server_state_vector) = payload.state_vector.to_bytes() else {
        return false;
    };
    if update.len() > MAX_SYNC_UPDATE_BYTES
        || server_state_vector.len() > MAX_SYNC_STATE_VECTOR_BYTES
    {
        return false;
    }
    let mut next_board = None;
    crdt_docs.update(|items| {
        if let Some(candidate) = items.get(&space_id).cloned()
            && (update.is_empty() || candidate.apply_update(&update).is_ok())
            && candidate.snapshot().len() <= MAX_SYNC_SNAPSHOT_BYTES
        {
            next_board = Some(candidate.board());
            items.insert(space_id, candidate);
        }
    });
    let Some(board) = next_board else {
        return false;
    };
    if !update.is_empty() {
        let encoded_update = EncodedUpdate::from_bytes(&update).as_str().to_owned();
        publish_local_update(
            &current_sync_principal(),
            space_id,
            current_local_generation(space_id),
            &encoded_update,
            &load_device_id(),
        );
    }
    let encoded_snapshot = EncodedUpdate::from_bytes(
        &crdt_docs
            .get_untracked()
            .get(&space_id)
            .map(SpaceDoc::snapshot)
            .unwrap_or_default(),
    );
    let inbox_keys_json =
        serde_json::to_string(&pending_updates.inbox_keys).unwrap_or_else(|_| "[]".to_owned());
    if JsFuture::from(indexed_db_commit_crdt_pull(
        &principal,
        space_id,
        encoded_snapshot.as_str(),
        payload.state_vector.as_str(),
        request.local_generation,
        &inbox_keys_json,
    ))
    .await
    .is_err()
    {
        return false;
    }
    if principal != current_sync_principal()
        || !sync_is_active()
        || !sync_run_is_current(run_generation)
    {
        return false;
    }
    set_acknowledged_state_vector(&principal, space_id, server_state_vector);
    spaces.update(|items| {
        if let Some(space) = items.iter_mut().find(|space| space.id == space_id) {
            space.board = board.clone();
            space.updated_at = now_millis();
        }
    });
    if active_space_id.get_untracked() == space_id {
        mark_remote_crdt_projection();
        notes.set(board.notes);
        groups.set(board.groups);
    }
    let workspace = workspace_snapshot(
        spaces.get_untracked(),
        active_space_id.get_untracked(),
        workspace_tombstones.get_untracked(),
    );
    let _ = save_workspace(&workspace, None);
    SYNC_SERVER_CONTACT.with(|contact| contact.set(true));
    true
}

async fn pull_active_space(
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
    run_generation: u64,
) {
    let space_id = active_space_id.get_untracked();
    pull_space(
        spaces,
        active_space_id,
        notes,
        groups,
        workspace_tombstones,
        crdt_docs,
        space_id,
        run_generation,
    )
    .await;
}

async fn pull_all_spaces(
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
    run_generation: u64,
) {
    let active = active_space_id.get_untracked();
    let ids: Vec<u64> = spaces
        .get_untracked()
        .iter()
        .filter(|space| space.deleted_at.is_none())
        .map(|space| space.id)
        .filter(|space_id| *space_id != active)
        .collect();
    for space_id in ids {
        pull_space(
            spaces,
            active_space_id,
            notes,
            groups,
            workspace_tombstones,
            crdt_docs,
            space_id,
            run_generation,
        )
        .await;
    }
}

async fn refresh_manifest_and_reconcile(
    spaces: RwSignal<Vec<Space>>,
    next_space_id: RwSignal<u64>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
    run_generation: u64,
) {
    if !sync_is_active()
        || !sync_run_is_current(run_generation)
        || current_sync_principal().starts_with("guest")
    {
        return;
    }
    if !merge_remote_spaces(spaces, next_space_id, active_space_id, workspace_tombstones).await {
        // Registration is only safe after a successful authoritative
        // manifest read. Otherwise a transient API error could turn the
        // local blank placeholder into a brand-new remote space and mask the
        // account's real spaces on the next browser.
        return;
    }
    if !sync_is_active() || !sync_run_is_current(run_generation) {
        return;
    }
    // Registration is idempotent and repairs an offline-created space before
    // its document reconcile. It is intentionally after manifest merge so a
    // blank local placeholder cannot be uploaded when the account already has
    // real remote spaces.
    register_sync_spaces(spaces).await;
    if !sync_is_active() || !sync_run_is_current(run_generation) {
        return;
    }
    pull_active_space(
        spaces,
        active_space_id,
        notes,
        groups,
        workspace_tombstones,
        crdt_docs,
        run_generation,
    )
    .await;
    pull_all_spaces(
        spaces,
        active_space_id,
        notes,
        groups,
        workspace_tombstones,
        crdt_docs,
        run_generation,
    )
    .await;
    schedule_sync_drain();
    publish_sync_hint();
}

fn start_authenticated_sync(
    spaces: RwSignal<Vec<Space>>,
    next_space_id: RwSignal<u64>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
    sync_status: RwSignal<SyncStatus>,
    pending_count: RwSignal<usize>,
    last_server_ack_at: RwSignal<Option<u64>>,
    last_request_id: RwSignal<Option<String>>,
    last_error: RwSignal<Option<String>>,
    local_diagnostics: RwSignal<Option<LocalSyncDiagnostics>>,
    transport_diagnostics: RwSignal<SyncTransportDiagnostics>,
) {
    let principal = current_sync_principal();
    let run_generation = next_sync_run_generation();
    let cleanup_principal = principal.clone();
    let online_listener = window_event_listener_untyped("online", |_| {
        schedule_sync_drain();
    });
    let visibility_listener = window_event_listener_untyped("visibilitychange", |_| {
        schedule_sync_drain();
    });
    let safety_spaces = spaces;
    let safety_active_space_id = active_space_id;
    let safety_notes = notes;
    let safety_groups = groups;
    let safety_tombstones = workspace_tombstones;
    let safety_docs = crdt_docs;
    let safety_next_space_id = next_space_id;
    let safety_tick = Closure::<dyn FnMut()>::new(move || {
        if !sync_is_active() || !sync_run_is_current(run_generation) {
            return;
        }
        if !sync_coordinator_is_current() {
            return;
        }
        spawn_local(async move {
            refresh_manifest_and_reconcile(
                safety_spaces,
                safety_next_space_id,
                safety_active_space_id,
                safety_notes,
                safety_groups,
                safety_tombstones,
                safety_docs,
                run_generation,
            )
            .await;
            schedule_sync_drain();
        });
    });
    let safety_timer = start_sync_safety_timer(safety_tick.as_ref().unchecked_ref());
    safety_tick.forget();
    on_cleanup(move || {
        if sync_run_is_current(run_generation) {
            next_sync_run_generation();
            set_sync_active(false);
            SYNC_RUNTIME.with(|runtime| runtime.replace(None));
            REMOTE_SYNC_EVENT_QUEUE.with(|queue| queue.borrow_mut().clear());
            REMOTE_SYNC_EVENT_RUNNING.with(|running| running.set(false));
            stop_sync_events(&api_url("/sync/events"));
            release_sync_lease(&cleanup_principal);
            stop_sync_broadcast();
        }
        online_listener.remove();
        visibility_listener.remove();
        stop_sync_safety_timer(safety_timer);
    });

    let on_hint = Closure::<dyn FnMut()>::new(schedule_sync_drain);
    let _ = start_sync_broadcast(&principal, on_hint.as_ref().unchecked_ref());
    on_hint.forget();

    spawn_local(async move {
        let _ =
            merge_remote_spaces(spaces, next_space_id, active_space_id, workspace_tombstones).await;
        if principal != current_sync_principal() || !sync_run_is_current(run_generation) {
            return;
        }
        SYNC_RUNTIME.with(|runtime| {
            runtime.replace(Some(SyncRuntime {
                run_generation,
                next_space_id,
                spaces,
                active_space_id,
                notes,
                groups,
                workspace_tombstones,
                crdt_docs,
                status: sync_status,
                pending_count,
                last_server_ack_at,
                last_request_id,
                last_error,
                local_diagnostics,
                transport_diagnostics,
            }));
        });
        if let Some(runtime) = SYNC_RUNTIME.with(|runtime| *runtime.borrow()) {
            refresh_sync_diagnostics(runtime);
        }
        sync_status.set(SyncStatus::Syncing);
        SYNC_SERVER_CONTACT.with(|contact| contact.set(false));
        set_sync_active(true);
        let _ = JsFuture::from(acquire_sync_lease(&principal)).await;
        if principal != current_sync_principal() || !sync_run_is_current(run_generation) {
            return;
        }
        refresh_manifest_and_reconcile(
            spaces,
            next_space_id,
            active_space_id,
            notes,
            groups,
            workspace_tombstones,
            crdt_docs,
            run_generation,
        )
        .await;
        // Refreshing the manifest first also resolves canonical space UUIDs
        // for legacy numeric rows before any pending outbox request is sent.
        schedule_sync_drain();

        let update_spaces = spaces;
        let update_active = active_space_id;
        let update_notes = notes;
        let update_groups = groups;
        let update_tombstones = workspace_tombstones;
        let update_docs = crdt_docs;
        let on_update = Closure::<dyn FnMut(String)>::new(move |raw| {
            apply_remote_sync_event(
                raw,
                run_generation,
                update_spaces,
                update_active,
                update_notes,
                update_groups,
                update_tombstones,
                update_docs,
            );
        });
        let reconnect_spaces = spaces;
        let reconnect_next_space_id = next_space_id;
        let reconnect_active = active_space_id;
        let reconnect_notes = notes;
        let reconnect_groups = groups;
        let reconnect_tombstones = workspace_tombstones;
        let reconnect_docs = crdt_docs;
        let on_open = Closure::<dyn FnMut()>::new(move || {
            // This callback also handles the durable sync-reset event. A
            // cursor reset means the event log can no longer be trusted for
            // discovery, so refresh the manifest before pulling documents.
            spawn_local(async move {
                refresh_manifest_and_reconcile(
                    reconnect_spaces,
                    reconnect_next_space_id,
                    reconnect_active,
                    reconnect_notes,
                    reconnect_groups,
                    reconnect_tombstones,
                    reconnect_docs,
                    run_generation,
                )
                .await;
                schedule_sync_drain();
            });
        });
        let on_error_status = sync_status;
        let on_error = Closure::<dyn FnMut()>::new(move || {
            spawn_local(async move {
                let online = web_sys::window().is_some_and(|window| window.navigator().on_line());
                if !online {
                    if sync_is_active() && sync_run_is_current(run_generation) {
                        on_error_status.set(SyncStatus::Offline);
                    }
                    return;
                }
                if !sync_is_active() || !sync_run_is_current(run_generation) {
                    return;
                }
                // EventSource errors are ambiguous: they include a dropped
                // TCP connection, a proxy restart, and an expired cookie.
                // Keep the browser's reconnect loop alive and let the
                // authenticated reconcile classify a real 401. Stopping SSE
                // here would turn ordinary transient outages into a false
                // AuthPaused state.
                on_error_status.set(SyncStatus::Syncing);
                schedule_sync_drain();
            });
        });
        let _ = start_sync_events(
            &api_url("/sync/events"),
            on_update.as_ref().unchecked_ref(),
            on_open.as_ref().unchecked_ref(),
            on_error.as_ref().unchecked_ref(),
        );
        on_update.forget();
        on_open.forget();
        on_error.forget();
    });
}

fn note_content_changed(before: &Note, after: &Note) -> bool {
    before.id != after.id
        || before.text != after.text
        || before.color != after.color
        || before.status != after.status
        || before.due_date != after.due_date
        || before.x != after.x
        || before.y != after.y
        || before.rotation != after.rotation
        || before.group_id != after.group_id
        || before.deleted_at != after.deleted_at
}

fn group_content_changed(before: &Group, after: &Group) -> bool {
    before.id != after.id
        || before.label != after.label
        || before.origin != after.origin
        || before.size != after.size
        || before.deleted_at != after.deleted_at
}

fn persist_space_board(
    spaces: RwSignal<Vec<Space>>,
    active_space_id: u64,
    board: BoardData,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
    storage_status: RwSignal<StorageStatus>,
    save_workspace_projection: bool,
) -> bool {
    let mut board = board;
    let now = now_millis();
    spaces.update(|items| {
        if let Some(space) = items.iter_mut().find(|space| space.id == active_space_id) {
            let previous = space.board.clone();
            board.tombstones = previous.tombstones.clone();
            board.tombstones.retain(|tombstone| match tombstone.kind {
                TombstoneKind::Note => board.notes.iter().all(|note| note.id != tombstone.id),
                TombstoneKind::Group => board.groups.iter().all(|group| group.id != tombstone.id),
                TombstoneKind::Space => false,
            });
            for old_note in &previous.notes {
                if board.notes.iter().all(|note| note.id != old_note.id)
                    && board.tombstones.iter().all(|tombstone| {
                        tombstone.kind != TombstoneKind::Note || tombstone.id != old_note.id
                    })
                {
                    board.tombstones.push(Tombstone {
                        kind: TombstoneKind::Note,
                        id: old_note.id,
                        stable_id: old_note.stable_id.clone(),
                        deleted_at: now,
                    });
                }
            }
            for old_group in &previous.groups {
                if board.groups.iter().all(|group| group.id != old_group.id)
                    && board.tombstones.iter().all(|tombstone| {
                        tombstone.kind != TombstoneKind::Group || tombstone.id != old_group.id
                    })
                {
                    board.tombstones.push(Tombstone {
                        kind: TombstoneKind::Group,
                        id: old_group.id,
                        stable_id: old_group.stable_id.clone(),
                        deleted_at: now,
                    });
                }
            }
            for note in &mut board.notes {
                if note.created_at == 0 {
                    note.created_at = now;
                }
                if previous
                    .notes
                    .iter()
                    .find(|old| old.id == note.id)
                    .is_none_or(|old| note_content_changed(old, note))
                {
                    note.updated_at = now;
                }
            }
            for group in &mut board.groups {
                if group.created_at == 0 {
                    group.created_at = now;
                }
                if previous
                    .groups
                    .iter()
                    .find(|old| old.id == group.id)
                    .is_none_or(|old| group_content_changed(old, group))
                {
                    group.updated_at = now;
                }
            }
            if previous != board {
                space.updated_at = now;
            }
            space.board = board;
        }
    });
    if save_workspace_projection {
        save_workspace(
            &workspace_snapshot(
                spaces.get_untracked(),
                active_space_id,
                workspace_tombstones.get_untracked(),
            ),
            Some(storage_status),
        )
    } else if storage_writes_blocked() {
        storage_status.set(StorageStatus::Error);
        false
    } else {
        true
    }
}

fn should_queue_crdt_projection(
    remote_projection_changed: bool,
    canonical_board: Option<&BoardData>,
    projected_board: &BoardData,
) -> bool {
    !remote_projection_changed
        || canonical_board.is_none_or(|canonical| canonical != projected_board)
}

fn next_note_id(board: &BoardData) -> u64 {
    fresh_entity_id(
        board
            .notes
            .iter()
            .map(|note| note.id)
            .chain(board.groups.iter().map(|group| group.id)),
    )
}

fn normalize_space_name(value: &str) -> String {
    let name = value.trim();
    if name.is_empty() {
        "untitled space".into()
    } else {
        name.chars().take(48).collect()
    }
}

fn board_snapshot(notes: RwSignal<Vec<Note>>, groups: RwSignal<Vec<Group>>) -> BoardData {
    BoardData {
        schema_version: CURRENT_SCHEMA_VERSION,
        notes: notes.get_untracked(),
        groups: groups.get_untracked(),
        tombstones: Vec::new(),
    }
}

fn record_snapshot(
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    history: RwSignal<History>,
    before: BoardData,
) {
    if board_snapshot(notes, groups) == before {
        return;
    }
    history.update(|history| {
        history.undo.push(before);
        if history.undo.len() > MAX_HISTORY {
            history.undo.remove(0);
        }
        history.redo.clear();
    });
}

fn mutate_notes<F>(
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    history: RwSignal<History>,
    change: F,
) where
    F: FnOnce(&mut Vec<Note>),
{
    let before = board_snapshot(notes, groups);
    notes.update(change);
    record_snapshot(notes, groups, history, before);
}

fn commit_pending_edit(
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    history: RwSignal<History>,
    editing: RwSignal<Option<u64>>,
    edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
) {
    if let Some((_, before)) = edit_snapshot.get_untracked() {
        record_snapshot(notes, groups, history, before);
    }
    edit_snapshot.set(None);
    editing.set(None);
}

fn undo_board(
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    history: RwSignal<History>,
    editing: RwSignal<Option<u64>>,
    edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
) {
    commit_pending_edit(notes, groups, history, editing, edit_snapshot);
    let current = board_snapshot(notes, groups);
    if let Some(previous) = history.get_untracked().undo.last().cloned() {
        history.update(|history| {
            history.undo.pop();
            history.redo.push(current);
        });
        notes.set(previous.notes);
        groups.set(previous.groups);
    }
}

fn redo_board(
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    history: RwSignal<History>,
    editing: RwSignal<Option<u64>>,
    edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
) {
    commit_pending_edit(notes, groups, history, editing, edit_snapshot);
    let current = board_snapshot(notes, groups);
    if let Some(next) = history.get_untracked().redo.last().cloned() {
        history.update(|history| {
            history.redo.pop();
            history.undo.push(current);
        });
        notes.set(next.notes);
        groups.set(next.groups);
    }
}

fn load_view(space_id: u64) -> ViewState {
    let storage_key = view_storage_key(space_id);
    let view = web_sys::window()
        .and_then(|window| window.local_storage().ok().flatten())
        .and_then(|storage| {
            storage
                .get_item(&storage_key)
                .ok()
                .flatten()
                .or_else(|| storage.get_item(LEGACY_VIEW_STORAGE_KEY).ok().flatten())
        })
        .and_then(|raw| serde_json::from_str::<ViewState>(&raw).ok())
        .unwrap_or(ViewState {
            pan: (0.0, 0.0),
            zoom: 1.0,
        });
    ViewState {
        pan: view.pan,
        zoom: if view.zoom.is_finite() {
            view.zoom.clamp(0.35, 2.5)
        } else {
            1.0
        },
    }
}

fn save_view(space_id: u64, view: ViewState) {
    let Some(storage) = web_sys::window().and_then(|window| window.local_storage().ok().flatten())
    else {
        return;
    };
    let storage_key = view_storage_key(space_id);
    if let Ok(raw) = serde_json::to_string(&view) {
        let _ = storage.set_item(&storage_key, &raw);
    }
}

fn view_storage_key(space_id: u64) -> String {
    format!(
        "{VIEW_STORAGE_KEY_PREFIX}{}:{space_id}",
        current_sync_principal()
    )
}

fn activate_space(
    space_id: u64,
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    next_id: RwSignal<u64>,
    selection: RwSignal<Vec<u64>>,
    history: RwSignal<History>,
    editing: RwSignal<Option<u64>>,
    edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
    group_editing: RwSignal<Option<u64>>,
    group_edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
    pan: RwSignal<(f64, f64)>,
    zoom: RwSignal<f64>,
    restore_message: RwSignal<Option<String>>,
) -> bool {
    let Some(space) = spaces
        .get_untracked()
        .into_iter()
        .find(|space| space.id == space_id && !space.archived && space.deleted_at.is_none())
    else {
        return false;
    };
    active_space_id.set(space_id);
    next_id.set(next_note_id(&space.board));
    notes.set(space.board.notes);
    groups.set(space.board.groups);
    selection.set(Vec::new());
    history.set(History::default());
    editing.set(None);
    edit_snapshot.set(None);
    group_editing.set(None);
    group_edit_snapshot.set(None);
    let view = load_view(space_id);
    pan.set(view.pan);
    zoom.set(view.zoom);
    restore_message.set(None);
    true
}

#[derive(Clone, Copy)]
struct SpaceActions {
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    next_id: RwSignal<u64>,
    selection: RwSignal<Vec<u64>>,
    history: RwSignal<History>,
    editing: RwSignal<Option<u64>>,
    edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
    group_editing: RwSignal<Option<u64>>,
    group_edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
    pan: RwSignal<(f64, f64)>,
    zoom: RwSignal<f64>,
    restore_message: RwSignal<Option<String>>,
    space_menu_open: RwSignal<bool>,
    pending_delete_space: RwSignal<Option<u64>>,
    workspace_tombstones: RwSignal<Vec<Tombstone>>,
    storage_status: RwSignal<StorageStatus>,
}

impl SpaceActions {
    fn save_workspace(self, active_space_id: u64) {
        let saved = save_workspace(
            &workspace_snapshot(
                self.spaces.get_untracked(),
                active_space_id,
                self.workspace_tombstones.get_untracked(),
            ),
            Some(self.storage_status),
        );
        if !saved {
            self.storage_status.set(StorageStatus::Error);
        }
    }

    fn create(self, next_space_id: RwSignal<u64>) {
        commit_pending_edit(
            self.notes,
            self.groups,
            self.history,
            self.editing,
            self.edit_snapshot,
        );
        commit_pending_group_edit(
            self.notes,
            self.groups,
            self.history,
            self.group_editing,
            self.group_edit_snapshot,
        );
        persist_space_board(
            self.spaces,
            self.active_space_id.get_untracked(),
            board_snapshot(self.notes, self.groups),
            self.workspace_tombstones,
            self.storage_status,
            true,
        );
        let space_id = fresh_entity_id(self.spaces.get_untracked().iter().map(|space| space.id));
        // Keep the signal populated for older callers and diagnostics, but
        // never use its value as the identity of a new offline space.
        next_space_id.set(next_space_id_for(&self.spaces.get_untracked()));
        self.spaces.update(|items| {
            items.push(Space {
                id: space_id,
                stable_id: new_stable_entity_id(),
                name: "new space".into(),
                metadata_version: 0,
                archived: false,
                created_at: now_millis(),
                updated_at: now_millis(),
                deleted_at: None,
                board: empty_board(),
            });
        });
        self.switch(space_id);
        self.save_workspace(space_id);
        if let Some(space) = self
            .spaces
            .get_untracked()
            .iter()
            .find(|space| space.id == space_id)
        {
            publish_local_space(
                &current_sync_principal(),
                space.id,
                &space.name,
                &space.stable_id,
                &load_device_id(),
            );
        }
        self.space_menu_open.set(false);
    }

    fn begin_rename(
        self,
        rename_space_id: RwSignal<Option<u64>>,
        rename_value: RwSignal<String>,
        space_id: u64,
    ) {
        if let Some(space) = self
            .spaces
            .get_untracked()
            .into_iter()
            .find(|space| space.id == space_id)
        {
            rename_value.set(space.name);
            rename_space_id.set(Some(space_id));
            self.space_menu_open.set(true);
        }
    }

    fn archive_current(self) {
        let active_count = self
            .spaces
            .get_untracked()
            .iter()
            .filter(|space| !space.archived && space.deleted_at.is_none())
            .count();
        if active_count <= 1 {
            self.restore_message.set(Some("keep one space open".into()));
            self.space_menu_open.set(false);
            return;
        }
        commit_pending_edit(
            self.notes,
            self.groups,
            self.history,
            self.editing,
            self.edit_snapshot,
        );
        commit_pending_group_edit(
            self.notes,
            self.groups,
            self.history,
            self.group_editing,
            self.group_edit_snapshot,
        );
        let current_id = self.active_space_id.get_untracked();
        let expected_version = self
            .spaces
            .get_untracked()
            .iter()
            .find(|space| space.id == current_id)
            .map(|space| space.metadata_version);
        persist_space_board(
            self.spaces,
            current_id,
            board_snapshot(self.notes, self.groups),
            self.workspace_tombstones,
            self.storage_status,
            true,
        );
        self.spaces.update(|items| {
            if let Some(space) = items.iter_mut().find(|space| space.id == current_id) {
                space.archived = true;
                space.updated_at = now_millis();
            }
        });
        if let Some(next_space_id) = self
            .spaces
            .get_untracked()
            .into_iter()
            .find(|space| !space.archived && space.deleted_at.is_none())
            .map(|space| space.id)
        {
            self.switch(next_space_id);
            self.save_workspace(next_space_id);
        }
        queue_space_metadata_operation(
            self.spaces,
            self.active_space_id.get_untracked(),
            self.workspace_tombstones,
            current_id,
            SpaceMetadataOperation::Archive,
            None,
            expected_version,
        );
        self.space_menu_open.set(false);
    }

    fn switch(self, space_id: u64) {
        if self.active_space_id.get_untracked() == space_id {
            self.space_menu_open.set(false);
            return;
        }
        commit_pending_edit(
            self.notes,
            self.groups,
            self.history,
            self.editing,
            self.edit_snapshot,
        );
        commit_pending_group_edit(
            self.notes,
            self.groups,
            self.history,
            self.group_editing,
            self.group_edit_snapshot,
        );
        persist_space_board(
            self.spaces,
            self.active_space_id.get_untracked(),
            board_snapshot(self.notes, self.groups),
            self.workspace_tombstones,
            self.storage_status,
            true,
        );
        if activate_space(
            space_id,
            self.spaces,
            self.active_space_id,
            self.notes,
            self.groups,
            self.next_id,
            self.selection,
            self.history,
            self.editing,
            self.edit_snapshot,
            self.group_editing,
            self.group_edit_snapshot,
            self.pan,
            self.zoom,
            self.restore_message,
        ) {
            self.space_menu_open.set(false);
        }
    }

    fn restore(self, space_id: u64) {
        let expected_version = self
            .spaces
            .get_untracked()
            .iter()
            .find(|space| space.id == space_id)
            .map(|space| space.metadata_version);
        self.workspace_tombstones.update(|items| {
            items.retain(|tombstone| {
                tombstone.kind != TombstoneKind::Space || tombstone.id != space_id
            });
        });
        let operation = self
            .spaces
            .get_untracked()
            .iter()
            .find(|space| space.id == space_id)
            .filter(|space| space.deleted_at.is_some())
            .map(|_| SpaceMetadataOperation::Restore)
            .unwrap_or(SpaceMetadataOperation::Unarchive);
        self.spaces.update(|items| {
            if let Some(space) = items.iter_mut().find(|space| space.id == space_id) {
                space.archived = false;
                space.deleted_at = None;
                space.updated_at = now_millis();
            }
        });
        self.pending_delete_space.set(None);
        self.save_workspace(self.active_space_id.get_untracked());
        queue_space_metadata_operation(
            self.spaces,
            self.active_space_id.get_untracked(),
            self.workspace_tombstones,
            space_id,
            operation,
            None,
            expected_version,
        );
    }

    fn save_name(self, rename_space_id: RwSignal<Option<u64>>, rename_value: RwSignal<String>) {
        let Some(id) = rename_space_id.get_untracked() else {
            return;
        };
        let name = normalize_space_name(&rename_value.get_untracked());
        let expected_version = self
            .spaces
            .get_untracked()
            .iter()
            .find(|space| space.id == id)
            .map(|space| space.metadata_version);
        self.spaces.update(|items| {
            if let Some(space) = items.iter_mut().find(|space| space.id == id) {
                space.name = name.clone();
                space.updated_at = now_millis();
            }
        });
        self.save_workspace(self.active_space_id.get_untracked());
        queue_space_metadata_operation(
            self.spaces,
            self.active_space_id.get_untracked(),
            self.workspace_tombstones,
            id,
            SpaceMetadataOperation::Rename,
            Some(name),
            expected_version,
        );
        rename_space_id.set(None);
    }

    fn cancel_name(self, rename_space_id: RwSignal<Option<u64>>) {
        rename_space_id.set(None);
    }

    fn request_delete(self, space_id: u64) {
        self.pending_delete_space.set(Some(space_id));
    }

    fn confirm_delete(self) {
        let Some(space_id) = self.pending_delete_space.get_untracked() else {
            return;
        };
        let current_id = self.active_space_id.get_untracked();
        let expected_version = self
            .spaces
            .get_untracked()
            .iter()
            .find(|space| space.id == space_id)
            .map(|space| space.metadata_version);
        if space_id == current_id
            && self
                .spaces
                .get_untracked()
                .iter()
                .filter(|space| !space.archived && space.deleted_at.is_none())
                .count()
                <= 1
        {
            self.restore_message.set(Some("keep one space open".into()));
            self.pending_delete_space.set(None);
            return;
        }
        if space_id == current_id {
            commit_pending_edit(
                self.notes,
                self.groups,
                self.history,
                self.editing,
                self.edit_snapshot,
            );
            commit_pending_group_edit(
                self.notes,
                self.groups,
                self.history,
                self.group_editing,
                self.group_edit_snapshot,
            );
            persist_space_board(
                self.spaces,
                current_id,
                board_snapshot(self.notes, self.groups),
                self.workspace_tombstones,
                self.storage_status,
                true,
            );
        }
        self.workspace_tombstones.update(|items| {
            if items
                .iter()
                .all(|tombstone| tombstone.kind != TombstoneKind::Space || tombstone.id != space_id)
            {
                items.push(Tombstone {
                    kind: TombstoneKind::Space,
                    id: space_id,
                    stable_id: self
                        .spaces
                        .get_untracked()
                        .iter()
                        .find(|space| space.id == space_id)
                        .map(|space| space.stable_id.clone())
                        .unwrap_or_else(|| legacy_entity_stable_id("space", space_id)),
                    deleted_at: now_millis(),
                });
            }
        });
        self.spaces.update(|items| {
            if let Some(space) = items.iter_mut().find(|space| space.id == space_id) {
                space.archived = true;
                space.deleted_at = Some(now_millis());
                space.updated_at = now_millis();
            }
        });
        if space_id == current_id {
            if let Some(next_space_id) = self
                .spaces
                .get_untracked()
                .into_iter()
                .find(|space| !space.archived && space.deleted_at.is_none())
                .map(|space| space.id)
            {
                self.switch(next_space_id);
                self.save_workspace(next_space_id);
            }
        } else {
            self.save_workspace(current_id);
        }
        queue_space_metadata_operation(
            self.spaces,
            self.active_space_id.get_untracked(),
            self.workspace_tombstones,
            space_id,
            SpaceMetadataOperation::Delete,
            None,
            expected_version,
        );
        self.pending_delete_space.set(None);
        self.space_menu_open.set(false);
    }
}

#[derive(Clone, Copy)]
struct BoardActions {
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    history: RwSignal<History>,
    selection: RwSignal<Vec<u64>>,
    pan: RwSignal<(f64, f64)>,
    zoom: RwSignal<f64>,
    editing: RwSignal<Option<u64>>,
    edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
    group_editing: RwSignal<Option<u64>>,
    group_edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
    due_date_request: RwSignal<Option<u64>>,
    restore_message: RwSignal<Option<String>>,
}

impl BoardActions {
    fn create_note(self) {
        commit_pending_edit(
            self.notes,
            self.groups,
            self.history,
            self.editing,
            self.edit_snapshot,
        );
        let id = fresh_entity_id(
            self.notes
                .get_untracked()
                .iter()
                .map(|note| note.id)
                .chain(self.groups.get_untracked().iter().map(|group| group.id)),
        );
        mutate_notes(self.notes, self.groups, self.history, |items| {
            let (x, y) =
                viewport_note_position(self.pan.get_untracked(), self.zoom.get_untracked());
            items.push(Note {
                id,
                stable_id: new_stable_entity_id(),
                text: String::new(),
                color: match id % 5 {
                    0 => NoteColor::Yellow,
                    1 => NoteColor::Pink,
                    2 => NoteColor::Blue,
                    3 => NoteColor::Green,
                    _ => NoteColor::Lavender,
                },
                status: NoteStatus::Todo,
                due_date: None,
                x,
                y,
                rotation: match id % 5 {
                    0 | 3 => -2,
                    1 | 4 => 2,
                    _ => 1,
                },
                group_id: None,
                group_stable_id: None,
                created_at: now_millis(),
                updated_at: now_millis(),
                deleted_at: None,
            });
        });
        self.edit_snapshot
            .set(Some((id, board_snapshot(self.notes, self.groups))));
        self.editing.set(Some(id));
        self.restore_message.set(None);
    }

    fn undo(self) {
        undo_board(
            self.notes,
            self.groups,
            self.history,
            self.editing,
            self.edit_snapshot,
        );
    }

    fn redo(self) {
        redo_board(
            self.notes,
            self.groups,
            self.history,
            self.editing,
            self.edit_snapshot,
        );
    }

    fn group_selected(self) {
        create_group(
            self.notes,
            self.groups,
            self.history,
            self.selection,
            self.editing,
            self.edit_snapshot,
            self.group_editing,
            self.group_edit_snapshot,
        );
    }

    fn ungroup_selected(self) {
        ungroup_selection(
            self.notes,
            self.groups,
            self.history,
            self.selection,
            self.editing,
            self.edit_snapshot,
            self.group_editing,
            self.group_edit_snapshot,
        );
    }

    fn delete_selected(self) {
        delete_selected_notes(
            self.notes,
            self.groups,
            self.history,
            self.selection,
            self.editing,
            self.edit_snapshot,
            self.group_editing,
            self.group_edit_snapshot,
        );
    }

    fn edit_note(self, id: u64) {
        self.selection.set(vec![id]);
        if self.editing.get_untracked() != Some(id) {
            commit_pending_edit(
                self.notes,
                self.groups,
                self.history,
                self.editing,
                self.edit_snapshot,
            );
            self.edit_snapshot
                .set(Some((id, board_snapshot(self.notes, self.groups))));
        }
        self.editing.set(Some(id));
    }

    fn cycle_status(self, id: u64) {
        commit_pending_edit(
            self.notes,
            self.groups,
            self.history,
            self.editing,
            self.edit_snapshot,
        );
        mutate_notes(self.notes, self.groups, self.history, |items| {
            if let Some(note) = items.iter_mut().find(|note| note.id == id) {
                note.status = note.status.next();
            }
        });
    }

    fn cycle_color(self, id: u64) {
        commit_pending_edit(
            self.notes,
            self.groups,
            self.history,
            self.editing,
            self.edit_snapshot,
        );
        mutate_notes(self.notes, self.groups, self.history, |items| {
            if let Some(note) = items.iter_mut().find(|note| note.id == id) {
                note.color = note.color.next();
            }
        });
    }

    fn clear_due_date(self, id: u64) {
        set_note_due_date(
            id,
            None,
            self.notes,
            self.groups,
            self.history,
            self.editing,
            self.edit_snapshot,
        );
    }

    fn open_due_date_picker(self, id: u64) {
        self.due_date_request.set(Some(id));
    }

    fn delete_note(self, id: u64) {
        commit_pending_edit(
            self.notes,
            self.groups,
            self.history,
            self.editing,
            self.edit_snapshot,
        );
        let before = board_snapshot(self.notes, self.groups);
        self.notes
            .update(|items| items.retain(|note| note.id != id));
        let used_groups = self
            .notes
            .get_untracked()
            .iter()
            .filter_map(|note| note.group_id)
            .collect::<Vec<_>>();
        self.groups
            .update(|items| items.retain(|group| used_groups.contains(&group.id)));
        self.selection
            .update(|selected| selected.retain(|selected_id| *selected_id != id));
        record_snapshot(self.notes, self.groups, self.history, before);
    }

    fn add_selection_to_group(self, group_id: u64) {
        add_selection_to_group(
            group_id,
            self.notes,
            self.groups,
            self.history,
            self.selection,
            self.editing,
            self.edit_snapshot,
            self.group_editing,
            self.group_edit_snapshot,
        );
    }

    fn rename_group(self, id: u64) {
        if self
            .groups
            .get_untracked()
            .iter()
            .any(|group| group.id == id)
        {
            commit_pending_group_edit(
                self.notes,
                self.groups,
                self.history,
                self.group_editing,
                self.group_edit_snapshot,
            );
            self.group_edit_snapshot
                .set(Some((id, board_snapshot(self.notes, self.groups))));
            self.group_editing.set(Some(id));
        }
    }

    fn ungroup_group(self, id: u64) {
        commit_pending_group_edit(
            self.notes,
            self.groups,
            self.history,
            self.group_editing,
            self.group_edit_snapshot,
        );
        let before = board_snapshot(self.notes, self.groups);
        self.notes.update(|items| {
            for note in items {
                if note.group_id == Some(id) {
                    note.group_id = None;
                }
            }
        });
        self.groups
            .update(|items| items.retain(|group| group.id != id));
        record_snapshot(self.notes, self.groups, self.history, before);
    }

    fn reset_view(self) {
        self.pan.set((0.0, 0.0));
        self.zoom.set(1.0);
    }
}

fn workspace_with_current_board(
    spaces: Vec<Space>,
    active_space_id: u64,
    notes: &[Note],
    groups: &[Group],
    tombstones: &[Tombstone],
) -> WorkspaceData {
    let mut workspace = workspace_snapshot(spaces, active_space_id, tombstones.to_vec());
    if let Some(space) = workspace
        .spaces
        .iter_mut()
        .find(|space| space.id == active_space_id)
    {
        space.board = BoardData {
            schema_version: CURRENT_SCHEMA_VERSION,
            notes: notes.to_vec(),
            groups: groups.to_vec(),
            tombstones: space.board.tombstones.clone(),
        };
    }
    workspace
}

fn download_json(raw: &str, filename: &str) {
    let parts = Array::new();
    parts.push(&JsValue::from_str(raw));
    let Ok(blob) = Blob::new_with_str_sequence(&parts) else {
        return;
    };
    let Ok(url) = Url::create_object_url_with_blob(&blob) else {
        return;
    };
    let Some(document) = web_sys::window().and_then(|window| window.document()) else {
        return;
    };
    let Ok(element) = document.create_element("a") else {
        return;
    };
    let Ok(anchor) = element.dyn_into::<HtmlAnchorElement>() else {
        return;
    };
    anchor.set_href(&url);
    anchor.set_download(filename);
    anchor.click();
    let _ = Url::revoke_object_url(&url);
}

fn export_workspace(workspace: &WorkspaceData) {
    let Ok(raw) = serde_json::to_string_pretty(workspace) else {
        return;
    };
    download_json(&raw, "task-space-workspace.json");
}

fn export_local_storage_backup(restore_message: RwSignal<Option<String>>) {
    let principal = current_sync_principal();
    spawn_local(async move {
        match JsFuture::from(indexed_db_export_backup(&principal)).await {
            Ok(value) => {
                let Some(raw) = value.as_string() else {
                    restore_message.set(Some("couldn't export local data".into()));
                    return;
                };
                download_json(&raw, "task-space-local-storage-backup.json");
                restore_message.set(Some(
                    "local backup exported — keep it before restoring or clearing this device"
                        .into(),
                ));
            }
            Err(_) => restore_message.set(Some(
                "couldn't export local data — keep this device unchanged and retry".into(),
            )),
        }
    });
}

fn export_sync_diagnostics(
    active_space_id: u64,
    status: SyncStatus,
    pending_count: usize,
    last_server_ack_at: Option<u64>,
    last_request_id: Option<String>,
    last_error: Option<String>,
    local_diagnostics: Option<LocalSyncDiagnostics>,
    transport_diagnostics: SyncTransportDiagnostics,
    restore_message: RwSignal<Option<String>>,
) {
    let principal = current_sync_principal();
    let device = load_device_id();
    let tab = current_tab_id();
    let browser_online = web_sys::window().is_some_and(|window| window.navigator().on_line());
    let coordinator = is_sync_lease_owner(&principal);
    let fence_mode = sync_lease_mode(&principal);
    let fence_epoch = sync_lease_epoch(&principal) as u64;
    let server_cursor = current_sync_cursor(&principal);
    spawn_local(async move {
        let sync_errors = load_local_sync_errors().await.unwrap_or_default();
        let payload = serde_json::json!({
            "format": "task-space-sync-diagnostics",
            "exportedAt": now_millis(),
            "schemaVersion": CURRENT_SCHEMA_VERSION,
            "principal": principal,
            "device": device,
            "tab": tab,
            "activeSpaceId": active_space_id,
            "browserOnline": browser_online,
            "coordinator": coordinator,
            "fenceMode": fence_mode,
            "fenceEpoch": fence_epoch,
            "serverCursor": server_cursor,
            "status": status.label(),
            "pendingCount": pending_count,
            "lastServerAckAt": last_server_ack_at,
            "lastRequestId": last_request_id,
            "lastError": last_error,
            "local": local_diagnostics,
            "transport": transport_diagnostics,
            "syncErrors": sync_errors,
        });
        match serde_json::to_string_pretty(&payload) {
            Ok(raw) => {
                download_json(&raw, "task-space-sync-diagnostics.json");
                restore_message.set(Some(
                    "sync diagnostics exported — document contents and credentials were excluded"
                        .into(),
                ));
            }
            Err(_) => restore_message.set(Some("couldn't export sync diagnostics".into())),
        }
    });
}

fn board_position(
    client_x: f64,
    client_y: f64,
    offset: (f64, f64),
    pan: (f64, f64),
    zoom: f64,
) -> Option<(f64, f64)> {
    let board = web_sys::window()?
        .document()?
        .get_element_by_id("task-space-board")?;
    let rect = board.get_bounding_client_rect();
    let x = (client_x - rect.left() - rect.width() / 2.0 - pan.0 - offset.0) / zoom;
    let y = (client_y - rect.top() - rect.height() / 2.0 - pan.1 - offset.1) / zoom;
    Some((x, y))
}

fn note_snapshot(notes: RwSignal<Vec<Note>>, id: u64) -> Option<Note> {
    notes.get().into_iter().find(|note| note.id == id)
}

fn group_origin(group_id: u64, notes: &[Note]) -> Option<(f64, f64)> {
    notes
        .iter()
        .filter(|note| note.group_id == Some(group_id))
        .fold(None::<(f64, f64)>, |origin, note| {
            Some((
                origin.map_or(note.x, |(x, _)| x.min(note.x)),
                origin.map_or(note.y, |(_, y)| y.min(note.y)),
            ))
        })
        .map(|(left, top)| (left - HORIZONTAL_PADDING, top - TOP_PADDING))
}

fn group_member_bounds(
    group_id: u64,
    notes: &[Note],
    excluded_ids: &[u64],
) -> Option<(f64, f64, f64, f64)> {
    let mut bounds = None;
    for note in notes
        .iter()
        .filter(|note| note.group_id == Some(group_id) && !excluded_ids.contains(&note.id))
    {
        let entry =
            bounds.get_or_insert((note.x, note.y, note.x + NOTE_WIDTH, note.y + NOTE_HEIGHT));
        entry.0 = entry.0.min(note.x);
        entry.1 = entry.1.min(note.y);
        entry.2 = entry.2.max(note.x + NOTE_WIDTH);
        entry.3 = entry.3.max(note.y + NOTE_HEIGHT);
    }
    bounds
}

fn group_bounds(group: &Group, notes: &[Note]) -> Option<(f64, f64, f64, f64)> {
    group_bounds_excluding(group, notes, &[])
}

fn group_bounds_excluding(
    group: &Group,
    notes: &[Note],
    excluded_ids: &[u64],
) -> Option<(f64, f64, f64, f64)> {
    group_member_bounds(group.id, notes, excluded_ids).map(|(left, top, right, bottom)| {
        let (frame_left, frame_top) = group
            .origin
            .unwrap_or((left - HORIZONTAL_PADDING, top - TOP_PADDING));
        let auto_width = (right - frame_left + HORIZONTAL_PADDING).max(MIN_GROUP_WIDTH);
        let auto_height = (bottom - frame_top + BOTTOM_PADDING).max(MIN_GROUP_HEIGHT);
        let (width, height) = group
            .size
            .map_or((auto_width, auto_height), |(width, height)| {
                (auto_width.max(width), auto_height.max(height))
            });
        (frame_left, frame_top, width, height)
    })
}

fn note_rect(x: f64, y: f64) -> (f64, f64, f64, f64) {
    (x, y, x + NOTE_WIDTH, y + NOTE_HEIGHT)
}

fn rects_overlap(first: (f64, f64, f64, f64), second: (f64, f64, f64, f64)) -> bool {
    first.0 < second.2 && first.2 > second.0 && first.1 < second.3 && first.3 > second.1
}

fn positions_for_group(group: &Group, notes: &[Note], moving_ids: &[u64]) -> Vec<(u64, f64, f64)> {
    let (frame_left, frame_top, frame_width, frame_height) =
        group_bounds(group, notes).unwrap_or((
            group.origin.map_or(0.0, |origin| origin.0),
            group.origin.map_or(0.0, |origin| origin.1),
            group.size.map_or(MIN_GROUP_WIDTH, |size| size.0),
            group.size.map_or(MIN_GROUP_HEIGHT, |size| size.1),
        ));
    let inner_width = (frame_width - HORIZONTAL_PADDING * 2.0).max(NOTE_WIDTH);
    let inner_height = (frame_height - TOP_PADDING - BOTTOM_PADDING).max(NOTE_HEIGHT);
    let column_step = NOTE_WIDTH + HORIZONTAL_PADDING;
    let row_step = NOTE_HEIGHT + BOTTOM_PADDING;
    let columns = ((inner_width + HORIZONTAL_PADDING) / column_step)
        .floor()
        .max(1.0) as usize;
    let rows = ((inner_height + BOTTOM_PADDING) / row_step)
        .floor()
        .max(1.0) as usize;
    let moving_ids = moving_ids.to_vec();
    let reposition_ids = notes
        .iter()
        .filter(|note| moving_ids.contains(&note.id) && note.group_id != Some(group.id))
        .map(|note| note.id)
        .collect::<Vec<_>>();
    let occupied = notes
        .iter()
        .filter(|note| note.group_id == Some(group.id) && !reposition_ids.contains(&note.id))
        .map(|note| note_rect(note.x, note.y))
        .collect::<Vec<_>>();
    let mut placed = occupied.clone();
    let mut positions = Vec::new();

    for note in notes
        .iter()
        .filter(|note| reposition_ids.contains(&note.id))
    {
        let mut slot = None;
        for index in 0..(columns * (rows + moving_ids.len() + 1)) {
            let column = index % columns;
            let row = index / columns;
            let x = frame_left + HORIZONTAL_PADDING + column as f64 * column_step;
            let y = frame_top + TOP_PADDING + row as f64 * row_step;
            let candidate = note_rect(x, y);
            if !placed.iter().any(|other| rects_overlap(candidate, *other)) {
                slot = Some((x, y, candidate));
                break;
            }
        }
        if let Some((x, y, rect)) = slot {
            placed.push(rect);
            positions.push((note.id, x, y));
        }
    }

    positions
}

fn resized_group_frame(
    initial: (f64, f64, f64, f64),
    delta: (f64, f64),
    corner: (i8, i8),
) -> (f64, f64, f64, f64) {
    let (initial_left, initial_top, initial_width, initial_height) = initial;
    let (horizontal, vertical) = corner;
    let next_width = if horizontal < 0 {
        (initial_width - delta.0).max(MIN_GROUP_WIDTH)
    } else {
        (initial_width + delta.0).max(MIN_GROUP_WIDTH)
    };
    let next_height = if vertical < 0 {
        (initial_height - delta.1).max(MIN_GROUP_HEIGHT)
    } else {
        (initial_height + delta.1).max(MIN_GROUP_HEIGHT)
    };
    let next_left = if horizontal < 0 {
        initial_left + initial_width - next_width
    } else {
        initial_left
    };
    let next_top = if vertical < 0 {
        initial_top + initial_height - next_height
    } else {
        initial_top
    };

    (next_left, next_top, next_width, next_height)
}

fn constrain_group_frame_to_cards(
    desired: (f64, f64, f64, f64),
    initial: (f64, f64, f64, f64),
    corner: (i8, i8),
    cards: (f64, f64, f64, f64),
) -> (f64, f64, f64, f64) {
    let (mut left, mut top, mut width, mut height) = desired;
    let (initial_left, initial_top, initial_width, initial_height) = initial;
    let (horizontal, vertical) = corner;
    let (cards_left, cards_top, cards_right, cards_bottom) = cards;
    let fixed_right = initial_left + initial_width;
    let fixed_bottom = initial_top + initial_height;

    if horizontal < 0 {
        left = left
            .min(cards_left - HORIZONTAL_PADDING)
            .min(fixed_right - MIN_GROUP_WIDTH);
        width = fixed_right - left;
    } else {
        width = width
            .max(cards_right + HORIZONTAL_PADDING - initial_left)
            .max(MIN_GROUP_WIDTH);
        left = initial_left;
    }
    if vertical < 0 {
        top = top
            .min(cards_top - TOP_PADDING)
            .min(fixed_bottom - MIN_GROUP_HEIGHT);
        height = fixed_bottom - top;
    } else {
        height = height
            .max(cards_bottom + BOTTOM_PADDING - initial_top)
            .max(MIN_GROUP_HEIGHT);
        top = initial_top;
    }

    (left, top, width, height)
}

fn group_at_point(
    groups: &[Group],
    notes: &[Note],
    point: (f64, f64),
    excluded_ids: &[u64],
    frozen_board: Option<&BoardData>,
) -> Option<u64> {
    groups
        .iter()
        .filter_map(|group| {
            let freeze_group = frozen_board.is_some_and(|board| {
                board
                    .notes
                    .iter()
                    .any(|note| note.group_id == Some(group.id) && excluded_ids.contains(&note.id))
            });
            let group_notes = frozen_board
                .filter(|_| freeze_group)
                .map_or(notes, |board| board.notes.as_slice());
            let excluded = if freeze_group { &[] } else { excluded_ids };
            let (x, y, width, height) = group_bounds_excluding(group, group_notes, excluded)?;
            let inside =
                point.0 >= x && point.0 <= x + width && point.1 >= y && point.1 <= y + height;
            inside.then_some((group.id, width * height))
        })
        .min_by(|(_, first_area), (_, second_area)| {
            first_area
                .partial_cmp(second_area)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(group_id, _)| group_id)
}

fn notes_in_marquee(
    notes: &[Note],
    start: (f64, f64),
    current: (f64, f64),
    pan: (f64, f64),
    zoom: f64,
) -> Vec<u64> {
    let Some(board) = web_sys::window()
        .and_then(|window| window.document())
        .and_then(|document| document.get_element_by_id("task-space-board"))
    else {
        return Vec::new();
    };
    let rect = board.get_bounding_client_rect();
    let left = start.0.min(current.0);
    let right = start.0.max(current.0);
    let top = start.1.min(current.1);
    let bottom = start.1.max(current.1);
    notes
        .iter()
        .filter_map(|note| {
            let note_left = rect.left() + rect.width() / 2.0 + pan.0 + note.x * zoom;
            let note_top = rect.top() + rect.height() / 2.0 + pan.1 + note.y * zoom;
            let note_right = note_left + NOTE_WIDTH * zoom;
            let note_bottom = note_top + NOTE_HEIGHT * zoom;
            if note_left < right && note_right > left && note_top < bottom && note_bottom > top {
                Some(note.id)
            } else {
                None
            }
        })
        .collect()
}

fn marquee_style(start: (f64, f64), current: (f64, f64)) -> String {
    let Some(board) = web_sys::window()
        .and_then(|window| window.document())
        .and_then(|document| document.get_element_by_id("task-space-board"))
    else {
        return String::new();
    };
    let rect = board.get_bounding_client_rect();
    let left = start.0.min(current.0) - rect.left();
    let top = start.1.min(current.1) - rect.top();
    let width = (start.0 - current.0).abs();
    let height = (start.1 - current.1).abs();
    format!("left:{left}px;top:{top}px;width:{width}px;height:{height}px;")
}

fn commit_pending_group_edit(
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    history: RwSignal<History>,
    editing: RwSignal<Option<u64>>,
    edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
) {
    if let Some((_, before)) = edit_snapshot.get_untracked() {
        record_snapshot(notes, groups, history, before);
    }
    edit_snapshot.set(None);
    editing.set(None);
}

fn create_group(
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    history: RwSignal<History>,
    selection: RwSignal<Vec<u64>>,
    note_editing: RwSignal<Option<u64>>,
    note_edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
    group_editing: RwSignal<Option<u64>>,
    group_edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
) {
    let selected_ids = selection.get_untracked();
    if selected_ids.len() < 2 {
        return;
    }
    commit_pending_edit(notes, groups, history, note_editing, note_edit_snapshot);
    commit_pending_group_edit(notes, groups, history, group_editing, group_edit_snapshot);
    let before = board_snapshot(notes, groups);
    let group_id = fresh_entity_id(
        groups
            .get_untracked()
            .iter()
            .map(|group| group.id)
            .chain(notes.get_untracked().iter().map(|note| note.id)),
    );
    let group_stable_id = new_stable_entity_id();
    groups.update(|items| {
        items.push(Group {
            id: group_id,
            stable_id: group_stable_id.clone(),
            label: "new group".into(),
            origin: None,
            size: None,
            created_at: now_millis(),
            updated_at: now_millis(),
            deleted_at: None,
        })
    });
    notes.update(|items| {
        for note in items {
            if selected_ids.contains(&note.id) {
                note.group_id = Some(group_id);
                note.group_stable_id = Some(group_stable_id.clone());
            }
        }
    });
    let origin = group_origin(group_id, &notes.get_untracked());
    groups.update(|items| {
        if let Some(group) = items.iter_mut().find(|group| group.id == group_id) {
            group.origin = origin;
        }
    });
    record_snapshot(notes, groups, history, before);
    group_edit_snapshot.set(Some((group_id, board_snapshot(notes, groups))));
    group_editing.set(Some(group_id));
}

fn ungroup_selection(
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    history: RwSignal<History>,
    selection: RwSignal<Vec<u64>>,
    note_editing: RwSignal<Option<u64>>,
    note_edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
    group_editing: RwSignal<Option<u64>>,
    group_edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
) {
    let selected_ids = selection.get_untracked();
    if selected_ids.is_empty() {
        return;
    }
    commit_pending_edit(notes, groups, history, note_editing, note_edit_snapshot);
    commit_pending_group_edit(notes, groups, history, group_editing, group_edit_snapshot);
    let before = board_snapshot(notes, groups);
    notes.update(|items| {
        for note in items {
            if selected_ids.contains(&note.id) {
                note.group_id = None;
            }
        }
    });
    let used_groups = notes
        .get_untracked()
        .iter()
        .filter_map(|note| note.group_id)
        .collect::<Vec<_>>();
    groups.update(|items| items.retain(|group| used_groups.contains(&group.id)));
    record_snapshot(notes, groups, history, before);
}

fn add_selection_to_group(
    group_id: u64,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    history: RwSignal<History>,
    selection: RwSignal<Vec<u64>>,
    note_editing: RwSignal<Option<u64>>,
    note_edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
    group_editing: RwSignal<Option<u64>>,
    group_edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
) {
    let selected_ids = selection.get_untracked();
    if selected_ids.is_empty()
        || !groups
            .get_untracked()
            .iter()
            .any(|group| group.id == group_id)
    {
        return;
    }
    commit_pending_edit(notes, groups, history, note_editing, note_edit_snapshot);
    commit_pending_group_edit(notes, groups, history, group_editing, group_edit_snapshot);
    let before = board_snapshot(notes, groups);
    let positions = groups
        .get_untracked()
        .into_iter()
        .find(|group| group.id == group_id)
        .map(|group| positions_for_group(&group, &notes.get_untracked(), &selected_ids))
        .unwrap_or_default();
    notes.update(|items| {
        for note in items {
            if selected_ids.contains(&note.id) {
                note.group_id = Some(group_id);
                if let Some((_, x, y)) = positions.iter().find(|(id, _, _)| *id == note.id) {
                    note.x = *x;
                    note.y = *y;
                }
            }
        }
    });
    let used_groups = notes
        .get_untracked()
        .iter()
        .filter_map(|note| note.group_id)
        .collect::<Vec<_>>();
    groups.update(|items| items.retain(|group| used_groups.contains(&group.id)));
    record_snapshot(notes, groups, history, before);
}

fn delete_selected_notes(
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    history: RwSignal<History>,
    selection: RwSignal<Vec<u64>>,
    note_editing: RwSignal<Option<u64>>,
    note_edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
    group_editing: RwSignal<Option<u64>>,
    group_edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
) {
    let selected_ids = selection.get_untracked();
    if selected_ids.is_empty() {
        return;
    }
    commit_pending_edit(notes, groups, history, note_editing, note_edit_snapshot);
    commit_pending_group_edit(notes, groups, history, group_editing, group_edit_snapshot);
    let before = board_snapshot(notes, groups);
    notes.update(|items| items.retain(|note| !selected_ids.contains(&note.id)));
    let used_groups = notes
        .get_untracked()
        .iter()
        .filter_map(|note| note.group_id)
        .collect::<Vec<_>>();
    groups.update(|items| items.retain(|group| used_groups.contains(&group.id)));
    selection.set(Vec::new());
    record_snapshot(notes, groups, history, before);
}

fn keyboard_target_is_editable(ev: &KeyboardEvent) -> bool {
    ev.target()
        .and_then(|target| target.dyn_into::<Element>().ok())
        .is_some_and(|target| {
            matches!(target.tag_name().as_str(), "INPUT" | "TEXTAREA" | "SELECT")
                || target
                    .closest("[contenteditable=\"true\"]")
                    .ok()
                    .flatten()
                    .is_some()
        })
}

#[component]
fn GroupFrame(
    id: u64,
    context_menu: RwSignal<Option<ContextMenuState>>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    history: RwSignal<History>,
    editing: RwSignal<Option<u64>>,
    edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
    group_dragging: RwSignal<Option<u64>>,
    group_drag_start: RwSignal<Option<(f64, f64)>>,
    group_drag_snapshot: RwSignal<Option<BoardData>>,
    dragged_ids: RwSignal<Vec<u64>>,
    drag_snapshot: RwSignal<Option<BoardData>>,
    group_resizing: RwSignal<Option<u64>>,
    group_resize_start: RwSignal<Option<(f64, f64)>>,
    group_resize_initial: RwSignal<Option<(f64, f64, f64, f64)>>,
    group_resize_corner: RwSignal<Option<(i8, i8)>>,
    group_resize_snapshot: RwSignal<Option<BoardData>>,
    zoom: RwSignal<f64>,
) -> impl IntoView {
    let start_edit = move |ev: MouseEvent| {
        ev.stop_propagation();
        if editing.get_untracked() != Some(id) {
            commit_pending_group_edit(notes, groups, history, editing, edit_snapshot);
            edit_snapshot.set(Some((id, board_snapshot(notes, groups))));
        }
        editing.set(Some(id));
    };
    let start_group_drag = move |ev: PointerEvent| {
        if ev.button() != 0 {
            return;
        }
        ev.stop_propagation();
        let Some(surface) = ev
            .current_target()
            .and_then(|target| target.dyn_into::<Element>().ok())
        else {
            return;
        };
        if !notes
            .get_untracked()
            .iter()
            .any(|note| note.group_id == Some(id))
        {
            return;
        }
        let _ = surface.set_pointer_capture(ev.pointer_id());
        group_dragging.set(Some(id));
        group_drag_start.set(Some((f64::from(ev.client_x()), f64::from(ev.client_y()))));
        group_drag_snapshot.set(Some(board_snapshot(notes, groups)));
    };
    let move_group_drag = move |ev: PointerEvent| {
        if group_dragging.get_untracked() != Some(id) {
            return;
        }
        ev.stop_propagation();
        ev.prevent_default();
        let (Some(start), Some(snapshot)) = (
            group_drag_start.get_untracked(),
            group_drag_snapshot.get_untracked(),
        ) else {
            return;
        };
        let zoom = zoom.get_untracked().max(0.01);
        let delta = (
            (f64::from(ev.client_x()) - start.0) / zoom,
            (f64::from(ev.client_y()) - start.1) / zoom,
        );
        let original_origin = snapshot
            .groups
            .iter()
            .find(|group| group.id == id)
            .and_then(|group| group.origin)
            .or_else(|| group_origin(id, &snapshot.notes));
        notes.update(|items| {
            for note in items {
                if note.group_id == Some(id)
                    && let Some(original) = snapshot.notes.iter().find(|item| item.id == note.id)
                {
                    note.x = original.x + delta.0;
                    note.y = original.y + delta.1;
                }
            }
        });
        if let Some((origin_x, origin_y)) = original_origin {
            groups.update(|items| {
                if let Some(group) = items.iter_mut().find(|group| group.id == id) {
                    group.origin = Some((origin_x + delta.0, origin_y + delta.1));
                }
            });
        }
    };
    let finish_group_drag = move |ev: PointerEvent| {
        if group_dragging.get_untracked() != Some(id) {
            return;
        }
        ev.stop_propagation();
        if let Some(surface) = ev
            .current_target()
            .and_then(|target| target.dyn_into::<Element>().ok())
        {
            let _ = surface.release_pointer_capture(ev.pointer_id());
        }
        if let Some(before) = group_drag_snapshot.get_untracked() {
            record_snapshot(notes, groups, history, before);
        }
        group_drag_snapshot.set(None);
        group_drag_start.set(None);
        group_dragging.set(None);
    };
    let start_group_resize = move |ev: PointerEvent| {
        if ev.button() != 0 {
            return;
        }
        ev.stop_propagation();
        ev.prevent_default();
        let Some(handle) = ev
            .current_target()
            .and_then(|target| target.dyn_into::<Element>().ok())
        else {
            return;
        };
        let Some(group) = groups
            .get_untracked()
            .into_iter()
            .find(|group| group.id == id)
        else {
            return;
        };
        let Some((left, top, width, height)) = group_bounds(&group, &notes.get_untracked()) else {
            return;
        };
        let Some(corner) =
            handle
                .get_attribute("data-resize-corner")
                .and_then(|corner| match corner.as_str() {
                    "top-left" => Some((-1, -1)),
                    "top-right" => Some((1, -1)),
                    "bottom-left" => Some((-1, 1)),
                    "bottom-right" => Some((1, 1)),
                    _ => None,
                })
        else {
            return;
        };
        let _ = handle.set_pointer_capture(ev.pointer_id());
        group_resizing.set(Some(id));
        group_resize_start.set(Some((f64::from(ev.client_x()), f64::from(ev.client_y()))));
        group_resize_initial.set(Some((left, top, width, height)));
        group_resize_corner.set(Some(corner));
        group_resize_snapshot.set(Some(board_snapshot(notes, groups)));
    };
    let move_group_resize = move |ev: PointerEvent| {
        if group_resizing.get_untracked() != Some(id) {
            return;
        }
        ev.stop_propagation();
        ev.prevent_default();
        let (
            Some(start),
            Some((initial_left, initial_top, initial_width, initial_height)),
            Some((horizontal, vertical)),
        ) = (
            group_resize_start.get_untracked(),
            group_resize_initial.get_untracked(),
            group_resize_corner.get_untracked(),
        )
        else {
            return;
        };
        let zoom = zoom.get_untracked().max(0.01);
        let initial_frame = (initial_left, initial_top, initial_width, initial_height);
        let next_frame = resized_group_frame(
            initial_frame,
            (
                (f64::from(ev.client_x()) - start.0) / zoom,
                (f64::from(ev.client_y()) - start.1) / zoom,
            ),
            (horizontal, vertical),
        );
        let next_frame =
            group_member_bounds(id, &notes.get_untracked(), &[]).map_or(next_frame, |cards| {
                constrain_group_frame_to_cards(
                    next_frame,
                    initial_frame,
                    (horizontal, vertical),
                    cards,
                )
            });
        groups.update(|items| {
            if let Some(group) = items.iter_mut().find(|group| group.id == id) {
                group.origin = Some((next_frame.0, next_frame.1));
                group.size = Some((next_frame.2, next_frame.3));
            }
        });
    };
    let finish_group_resize = move |ev: PointerEvent| {
        if group_resizing.get_untracked() != Some(id) {
            return;
        }
        ev.stop_propagation();
        if let Some(handle) = ev
            .current_target()
            .and_then(|target| target.dyn_into::<Element>().ok())
        {
            let _ = handle.release_pointer_capture(ev.pointer_id());
        }
        if let Some(before) = group_resize_snapshot.get_untracked() {
            record_snapshot(notes, groups, history, before);
        }
        group_resize_snapshot.set(None);
        group_resize_initial.set(None);
        group_resize_corner.set(None);
        group_resize_start.set(None);
        group_resizing.set(None);
    };
    let open_group_context_menu = move |ev: MouseEvent| {
        ev.prevent_default();
        ev.stop_propagation();
        context_menu.set(Some(ContextMenuState {
            target: ContextMenuTarget::Group(id),
            x: ev.client_x(),
            y: ev.client_y(),
        }));
    };
    view! {
        <div
            class="pointer-events-auto absolute rounded-md border-2 border-dashed border-ink-soft/35 bg-marker/10"
            on:pointerdown=start_group_drag
            on:pointermove=move_group_drag
            on:pointerup=finish_group_drag
            on:pointercancel=finish_group_drag
            on:contextmenu=open_group_context_menu
            style=move || {
                let snapshot = drag_snapshot.get();
                let dragged = dragged_ids.get();
                let frame_notes = snapshot
                    .as_ref()
                    .filter(|snapshot| {
                        snapshot.notes.iter().any(|note| {
                            note.group_id == Some(id) && dragged.contains(&note.id)
                        })
                    })
                    .map_or_else(|| notes.get(), |snapshot| snapshot.notes.clone());
                groups
                    .get()
                    .into_iter()
                    .find(|group| group.id == id)
                    .and_then(|group| group_bounds(&group, &frame_notes))
                    .map_or_else(String::new, |(x, y, width, height)| {
                        format!("left:{x}px;top:{y}px;width:{width}px;height:{height}px;")
                    })
            }
        >
            <div
                class="pointer-events-auto absolute left-[-7px] top-[-7px] h-4 w-4 cursor-nwse-resize rounded-sm border-2 border-paper-shelf bg-ink-soft/60 shadow-sm hover:bg-ink"
                data-resize-corner="top-left"
                aria-label="Resize group (top left)"
                title="Drag to resize group from the top-left corner"
                on:pointerdown=start_group_resize
                on:pointermove=move_group_resize
                on:pointerup=finish_group_resize
                on:pointercancel=finish_group_resize
            ></div>
            <div
                class="pointer-events-auto absolute right-[-7px] top-[-7px] h-4 w-4 cursor-nesw-resize rounded-sm border-2 border-paper-shelf bg-ink-soft/60 shadow-sm hover:bg-ink"
                data-resize-corner="top-right"
                aria-label="Resize group (top right)"
                title="Drag to resize group from the top-right corner"
                on:pointerdown=start_group_resize
                on:pointermove=move_group_resize
                on:pointerup=finish_group_resize
                on:pointercancel=finish_group_resize
            ></div>
            <div
                class="pointer-events-auto absolute bottom-[-7px] left-[-7px] h-4 w-4 cursor-nesw-resize rounded-sm border-2 border-paper-shelf bg-ink-soft/60 shadow-sm hover:bg-ink"
                data-resize-corner="bottom-left"
                aria-label="Resize group (bottom left)"
                title="Drag to resize group from the bottom-left corner"
                on:pointerdown=start_group_resize
                on:pointermove=move_group_resize
                on:pointerup=finish_group_resize
                on:pointercancel=finish_group_resize
            ></div>
            <div
                class="pointer-events-auto absolute bottom-[-7px] right-[-7px] h-4 w-4 cursor-nwse-resize rounded-sm border-2 border-paper-shelf bg-ink-soft/60 shadow-sm hover:bg-ink"
                data-resize-corner="bottom-right"
                aria-label="Resize group (bottom right)"
                title="Drag to resize group from the bottom-right corner"
                on:pointerdown=start_group_resize
                on:pointermove=move_group_resize
                on:pointerup=finish_group_resize
                on:pointercancel=finish_group_resize
            ></div>
            {move || groups.get().into_iter().find(|group| group.id == id).map(|group| {
                if editing.get() == Some(id) {
                    view! {
                        <input
                            prop:value=group.label
                            autofocus=true
                            maxlength="60"
                            aria-label="Group label"
                            on:pointerdown=move |ev: PointerEvent| ev.stop_propagation()
                            on:click=move |ev: MouseEvent| ev.stop_propagation()
                            on:input=move |ev: Event| {
                                let Some(input) = ev.target().and_then(|target| target.dyn_into::<HtmlInputElement>().ok()) else { return; };
                                let value = input.value();
                                groups.update(|items| {
                                    if let Some(group) = items.iter_mut().find(|group| group.id == id) {
                                        group.label = value;
                                    }
                                });
                            }
                            on:keydown=move |ev: KeyboardEvent| {
                                if ev.key() == "Enter" || ev.key() == "Escape" {
                                    ev.prevent_default();
                                    commit_pending_group_edit(notes, groups, history, editing, edit_snapshot);
                                }
                            }
                            class="pointer-events-auto absolute -top-4 left-3 w-52 rounded-[3px] border border-ink-soft/25 bg-marker px-2 py-1 font-handwriting text-lg leading-none text-ink outline-none focus:ring-2 focus:ring-ink/30"
                        />
                    }.into_any()
                } else {
                    view! {
                        <button
                            type="button"
                            on:dblclick=start_edit
                            on:pointerdown=move |ev: PointerEvent| ev.stop_propagation()
                            class="pointer-events-auto absolute -top-4 left-3 max-w-52 rounded-[3px] bg-marker px-2 py-1 font-handwriting text-lg leading-none text-ink shadow-sm hover:brightness-95 focus:outline-none focus:ring-2 focus:ring-ink/30"
                            title="Double-click to rename group"
                        >
                            {group.label}
                        </button>
                    }.into_any()
                }
            })}
        </div>
    }
}

#[component]
fn NoteCard(
    id: u64,
    context_menu: RwSignal<Option<ContextMenuState>>,
    due_date_request: RwSignal<Option<u64>>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    history: RwSignal<History>,
    selection: RwSignal<Vec<u64>>,
    editing: RwSignal<Option<u64>>,
    edit_snapshot: RwSignal<Option<(u64, BoardData)>>,
    dragged: RwSignal<Option<u64>>,
    dragged_ids: RwSignal<Vec<u64>>,
    drag_offset: RwSignal<Option<(f64, f64)>>,
    drag_snapshot: RwSignal<Option<BoardData>>,
    drag_origin: RwSignal<Option<(f64, f64)>>,
    pan: RwSignal<(f64, f64)>,
    zoom: RwSignal<f64>,
) -> impl IntoView {
    let due_calendar_open = RwSignal::new(false);
    let initial_calendar_month = note_snapshot(notes, id)
        .and_then(|note| note.due_date)
        .and_then(|date| parse_due_date(&date))
        .map(|(year, month, _)| (year, month))
        .unwrap_or_else(|| {
            let today = js_sys::Date::new_0();
            (today.get_full_year() as i32, today.get_month() + 1)
        });
    let calendar_month = RwSignal::new(initial_calendar_month);

    Effect::new(move |_| {
        if due_date_request.get() == Some(id) {
            if let Some((year, month, _)) = note_snapshot(notes, id)
                .and_then(|note| note.due_date)
                .and_then(|date| parse_due_date(&date))
            {
                calendar_month.set((year, month));
            }
            due_calendar_open.set(true);
            due_date_request.set(None);
        }
    });

    let cycle_status = move |ev: MouseEvent| {
        ev.stop_propagation();
        commit_pending_edit(notes, groups, history, editing, edit_snapshot);
        mutate_notes(notes, groups, history, |items| {
            if let Some(note) = items.iter_mut().find(|note| note.id == id) {
                note.status = note.status.next();
            }
        });
    };
    let clear_due_date = move |ev: MouseEvent| {
        ev.stop_propagation();
        set_note_due_date(id, None, notes, groups, history, editing, edit_snapshot);
        due_calendar_open.set(false);
    };
    let toggle_due_calendar = move |ev: MouseEvent| {
        ev.stop_propagation();
        if !due_calendar_open.get_untracked()
            && let Some((year, month, _)) = note_snapshot(notes, id)
                .and_then(|note| note.due_date)
                .and_then(|date| parse_due_date(&date))
        {
            calendar_month.set((year, month));
        }
        due_calendar_open.update(|open| *open = !*open);
    };
    let previous_month = move |ev: MouseEvent| {
        ev.stop_propagation();
        let (year, month) = calendar_month.get_untracked();
        calendar_month.set(shift_month(year, month, -1));
    };
    let next_month = move |ev: MouseEvent| {
        ev.stop_propagation();
        let (year, month) = calendar_month.get_untracked();
        calendar_month.set(shift_month(year, month, 1));
    };
    let cycle_color = move |ev: MouseEvent| {
        ev.stop_propagation();
        commit_pending_edit(notes, groups, history, editing, edit_snapshot);
        mutate_notes(notes, groups, history, |items| {
            if let Some(note) = items.iter_mut().find(|note| note.id == id) {
                note.color = note.color.next();
            }
        });
    };
    let delete_note = move |ev: MouseEvent| {
        ev.stop_propagation();
        commit_pending_edit(notes, groups, history, editing, edit_snapshot);
        let before = board_snapshot(notes, groups);
        notes.update(|items| items.retain(|note| note.id != id));
        let used_groups = notes
            .get_untracked()
            .iter()
            .filter_map(|note| note.group_id)
            .collect::<Vec<_>>();
        groups.update(|items| items.retain(|group| used_groups.contains(&group.id)));
        record_snapshot(notes, groups, history, before);
        if editing.get_untracked() == Some(id) {
            editing.set(None);
        }
    };
    let edit_note = move |ev: MouseEvent| {
        let Some(target) = ev
            .target()
            .and_then(|target| target.dyn_into::<Element>().ok())
        else {
            editing.set(Some(id));
            return;
        };
        if target.has_attribute("data-note-drag-handle") || target.has_attribute("data-note-action")
        {
            return;
        }
        due_calendar_open.set(false);
        if ev.shift_key() || ev.ctrl_key() || ev.meta_key() {
            selection.update(|selected| {
                if let Some(index) = selected.iter().position(|selected_id| *selected_id == id) {
                    selected.remove(index);
                } else {
                    selected.push(id);
                }
            });
            editing.set(None);
            return;
        }
        selection.set(vec![id]);
        if editing.get_untracked() != Some(id) {
            commit_pending_edit(notes, groups, history, editing, edit_snapshot);
            edit_snapshot.set(Some((id, board_snapshot(notes, groups))));
        }
        editing.set(Some(id));
    };
    let start_drag = move |ev: PointerEvent| {
        if ev.button() != 0 {
            return;
        }
        ev.stop_propagation();
        let Some(handle) = ev
            .current_target()
            .and_then(|target| target.dyn_into::<Element>().ok())
        else {
            return;
        };
        let Some(card) = handle.parent_element() else {
            return;
        };
        commit_pending_edit(notes, groups, history, editing, edit_snapshot);
        let selected_ids = selection.get_untracked();
        let moved_ids = if selected_ids.contains(&id) {
            selected_ids
        } else {
            vec![id]
        };
        if selection.get_untracked() != moved_ids {
            selection.set(moved_ids.clone());
        }
        let rect = card.get_bounding_client_rect();
        let _ = card.set_pointer_capture(ev.pointer_id());
        let snapshot = board_snapshot(notes, groups);
        drag_origin.set(
            snapshot
                .notes
                .iter()
                .find(|note| note.id == id)
                .map(|note| (note.x, note.y)),
        );
        drag_snapshot.set(Some(snapshot));
        dragged_ids.set(moved_ids);
        drag_offset.set(Some((
            f64::from(ev.client_x()) - rect.left(),
            f64::from(ev.client_y()) - rect.top(),
        )));
        dragged.set(Some(id));
    };
    let move_dragged_note = move |ev: PointerEvent| {
        if dragged.get_untracked() != Some(id) {
            return;
        }
        ev.stop_propagation();
        ev.prevent_default();
        if let Some(offset) = drag_offset.get_untracked()
            && let Some((x, y)) = board_position(
                f64::from(ev.client_x()),
                f64::from(ev.client_y()),
                offset,
                pan.get_untracked(),
                zoom.get_untracked(),
            )
        {
            let Some((origin_x, origin_y)) = drag_origin.get_untracked() else {
                return;
            };
            let delta = (x - origin_x, y - origin_y);
            let moved_ids = dragged_ids.get_untracked();
            let Some(snapshot) = drag_snapshot.get_untracked() else {
                return;
            };
            notes.update(|items| {
                for note in items {
                    if let Some(original) = snapshot
                        .notes
                        .iter()
                        .find(|original| original.id == note.id)
                        && moved_ids.contains(&note.id)
                    {
                        note.x = original.x + delta.0;
                        note.y = original.y + delta.1;
                    }
                }
            });
        }
    };
    let finish_drag = move |ev: PointerEvent| {
        if dragged.get_untracked() != Some(id) {
            return;
        }
        ev.stop_propagation();
        if let Some(card) = ev
            .current_target()
            .and_then(|target| target.dyn_into::<Element>().ok())
        {
            let _ = card.release_pointer_capture(ev.pointer_id());
        }
        if let Some(before) = drag_snapshot.get_untracked() {
            let moved_ids = dragged_ids.get_untracked();
            let drop_group = notes
                .get_untracked()
                .iter()
                .find(|note| note.id == id)
                .map(|note| (note.x + NOTE_WIDTH / 2.0, note.y + NOTE_HEIGHT / 2.0))
                .and_then(|center| {
                    group_at_point(
                        &groups.get_untracked(),
                        &notes.get_untracked(),
                        center,
                        &moved_ids,
                        Some(&before),
                    )
                });
            let drop_positions = drop_group
                .and_then(|group_id| {
                    groups
                        .get_untracked()
                        .into_iter()
                        .find(|group| group.id == group_id)
                        .map(|group| {
                            positions_for_group(&group, &notes.get_untracked(), &moved_ids)
                        })
                })
                .unwrap_or_default();
            notes.update(|items| {
                for note in items {
                    if moved_ids.contains(&note.id) {
                        note.group_id = drop_group;
                        if let Some((_, x, y)) =
                            drop_positions.iter().find(|(id, _, _)| *id == note.id)
                        {
                            note.x = *x;
                            note.y = *y;
                        }
                    }
                }
            });
            let used_groups = notes
                .get_untracked()
                .iter()
                .filter_map(|note| note.group_id)
                .collect::<Vec<_>>();
            groups.update(|items| items.retain(|group| used_groups.contains(&group.id)));
            record_snapshot(notes, groups, history, before);
        }
        drag_snapshot.set(None);
        drag_origin.set(None);
        dragged_ids.set(Vec::new());
        dragged.set(None);
        drag_offset.set(None);
    };
    let open_note_context_menu = move |ev: MouseEvent| {
        ev.prevent_default();
        ev.stop_propagation();
        if !selection.get_untracked().contains(&id) {
            selection.set(vec![id]);
        }
        context_menu.set(Some(ContextMenuState {
            target: ContextMenuTarget::Note(id),
            x: ev.client_x(),
            y: ev.client_y(),
        }));
    };

    view! {
        <article
            class=move || format!(
                "group task-space-note absolute w-52 min-h-40 p-3 pb-9 rounded-[3px] shadow-lg select-none touch-none transition-[transform,box-shadow] duration-100 {} {}",
                if note_snapshot(notes, id).is_some_and(|note| note.status == NoteStatus::Done) { "opacity-70" } else { "" },
                if dragged.get() == Some(id) {
                    "z-20 cursor-grabbing shadow-2xl ring-2 ring-ink/10"
                } else if selection.get().contains(&id) {
                    "ring-2 ring-ink/30 ring-offset-2 ring-offset-paper-shelf"
                } else {
                    "cursor-pointer hover:shadow-xl"
                }
            )
            style=move || {
                let Some(note) = note_snapshot(notes, id) else {
                    return String::new();
                };
                format!(
                    "left:{}px;top:{}px;background-color:{};color:{};transform:rotate({}deg) {}",
                    note.x,
                    note.y,
                    note_color_background(note.color),
                    note_color_ink(note.color),
                    note.rotation,
                    if dragged.get() == Some(id) { "scale(1.02)" } else { "scale(1)" }
                )
            }
            on:click=edit_note
            on:pointermove=move_dragged_note
            on:pointerup=finish_drag
            on:pointercancel=finish_drag
            on:contextmenu=open_note_context_menu
            aria-label=move || note_snapshot(notes, id)
                .map(|note| {
                    if note.text.trim().is_empty() {
                        "Empty task note".to_string()
                    } else {
                        note.text
                    }
                })
                .unwrap_or_else(|| "Task note".to_string())
        >
            <div
                class="absolute -top-2 left-1/2 -translate-x-1/2 w-11 h-3 cursor-grab bg-tape rotate-[-2deg]"
                data-note-drag-handle="true"
                aria-label="Drag note to move"
                on:pointerdown=start_drag
                on:pointermove=move_dragged_note
                on:pointerup=finish_drag
                on:pointercancel=finish_drag
            ></div>
            {move || if editing.get() == Some(id) {
                let text = notes
                    .get_untracked()
                    .into_iter()
                    .find(|note| note.id == id)
                    .map(|note| note.text)
                    .unwrap_or_default();
                view! {
                    <textarea
                        prop:value=text
                        autofocus=true
                        rows="4"
                        maxlength="180"
                        aria-label="Edit task"
                        on:input=move |ev: Event| {
                            let Some(input) = ev
                                .target()
                                .and_then(|target| target.dyn_into::<HtmlTextAreaElement>().ok())
                            else {
                                return;
                            };
                            let value = input.value();
                            notes.update(|items| {
                                if let Some(note) = items.iter_mut().find(|note| note.id == id) {
                                    note.text = value;
                                }
                            });
                        }
                        on:keydown=move |ev: KeyboardEvent| {
                            if ev.key() == "Escape" {
                                commit_pending_edit(notes, groups, history, editing, edit_snapshot);
                            }
                        }
                        class="w-full resize-none bg-transparent font-handwriting text-2xl leading-tight outline-none placeholder:text-current/50"
                        placeholder="write a task…"
                    ></textarea>
                    <button
                        type="button"
                        data-note-action="finish-editing"
                        on:pointerdown=move |ev: PointerEvent| ev.stop_propagation()
                        on:click=move |ev: MouseEvent| {
                            ev.stop_propagation();
                            commit_pending_edit(notes, groups, history, editing, edit_snapshot);
                        }
                        class="absolute bottom-2 left-3 z-10 text-xs font-sans underline underline-offset-2"
                    >
                        "done editing"
                    </button>
                }
                .into_any()
            } else {
                view! {
                    <button
                        type="button"
                        class=move || format!(
                            "w-full text-left font-handwriting text-2xl leading-tight {}",
                            if note_snapshot(notes, id).is_some_and(|note| note.status == NoteStatus::Done) {
                                "line-through"
                            } else {
                                ""
                            }
                        )
                    >
                        {move || note_snapshot(notes, id)
                            .map(|note| {
                                if note.text.trim().is_empty() {
                                    "click to write".to_string()
                                } else {
                                    note.text
                                }
                            })
                            .unwrap_or_default()}
                    </button>
                }
                .into_any()
            }}
            {move || if editing.get() == Some(id) {
                ().into_any()
            } else {
                view! {
                    <div class="absolute bottom-2 left-3 right-3 flex items-center justify-between gap-1 border-t border-current/10 pt-1 text-[10px] font-sans">
                        <button
                            type="button"
                            data-note-action="set-status"
                            on:click=cycle_status
                            aria-label=move || note_snapshot(notes, id)
                                .map(|note| format!("Status: {}. Click to change", note.status.label()))
                                .unwrap_or_else(|| "Status: to do. Click to change".into())
                            title="Click to change status"
                            class="flex shrink-0 items-center gap-1 whitespace-nowrap rounded-sm px-1 font-handwriting text-sm leading-none opacity-80 hover:bg-white/20 hover:opacity-100 focus:outline-none focus:ring-2 focus:ring-current/30"
                        >
                            <span class="text-base" aria-hidden="true">
                                {move || note_snapshot(notes, id)
                                    .map(|note| note.status.mark())
                                    .unwrap_or("○")}
                            </span>
                            <span>
                                {move || note_snapshot(notes, id)
                                    .map(|note| note.status.label())
                                    .unwrap_or("to do")}
                            </span>
                        </button>
                        <div class="flex shrink-0 items-center gap-1">
                            <div class="relative flex items-center">
                                <button
                                    type="button"
                                    data-note-action="set-due-date"
                                    on:click=toggle_due_calendar
                                    aria-label="Choose task due date"
                                    title=move || if note_snapshot(notes, id).is_some_and(|note| is_overdue(note.due_date.as_deref(), note.status)) {
                                        "Overdue task — choose a new due date"
                                    } else {
                                        "Choose due date"
                                    }
                                    class=move || {
                                        let Some(note) = note_snapshot(notes, id) else {
                                            return "flex h-5 items-center whitespace-nowrap rounded-sm px-1 font-sans text-[10px] leading-none opacity-75".to_string();
                                        };
                                        if note.due_date.is_some() && is_overdue(note.due_date.as_deref(), note.status) {
                                            "flex h-5 items-center whitespace-nowrap rounded-sm border border-note-ink-pink/40 bg-note-pink px-1 font-semibold text-note-ink-pink shadow-sm hover:brightness-95 focus:outline-none focus:ring-2 focus:ring-note-ink-pink/40".to_string()
                                        } else if note.due_date.is_some() {
                                            "flex h-5 items-center whitespace-nowrap rounded-sm border border-ink/15 bg-marker px-1 font-semibold text-ink shadow-sm hover:brightness-95 focus:outline-none focus:ring-2 focus:ring-ink/30".to_string()
                                        } else {
                                            "flex h-5 items-center whitespace-nowrap rounded-sm px-1 font-sans text-[10px] leading-none opacity-75 hover:bg-white/20 hover:opacity-100 focus:outline-none focus:ring-2 focus:ring-current/30".to_string()
                                        }
                                    }
                                >
                                    {move || note_snapshot(notes, id)
                                        .map(|note| due_date_label(
                                            note.due_date.as_deref(),
                                            is_overdue(note.due_date.as_deref(), note.status),
                                        ))
                                        .unwrap_or_else(|| "add due".into())}
                                </button>
                                {move || if note_snapshot(notes, id).is_some_and(|note| note.due_date.is_some()) {
                                    view! {
                                        <button
                                            type="button"
                                            data-note-action="clear-due-date"
                                            on:click=clear_due_date
                                            aria-label="Clear due date"
                                            title="Clear due date"
                                            class="relative z-10 ml-0.5 font-sans text-xs opacity-0 transition-opacity group-hover:opacity-60 hover:!opacity-100 focus:opacity-100 focus:outline-none focus:ring-2 focus:ring-current/30"
                                        >
                                            "×"
                                        </button>
                                    }.into_any()
                                } else {
                                    ().into_any()
                                }}
                                {move || if due_calendar_open.get() {
                                    let (year, month) = calendar_month.get();
                                    let selected_date = note_snapshot(notes, id)
                                        .and_then(|note| note.due_date);
                                    view! {
                                        <div
                                            class="absolute bottom-7 right-0 z-40 w-56 rounded-[4px] border border-ink/20 bg-note-yellow p-3 text-note-ink-yellow shadow-xl"
                                            on:click=move |ev: MouseEvent| ev.stop_propagation()
                                        >
                                            <div class="flex items-center justify-between gap-2 border-b border-current/20 pb-2">
                                                <button
                                                    type="button"
                                                    data-note-action="previous-month"
                                                    on:click=previous_month
                                                    aria-label="Previous month"
                                                    class="rounded-sm px-1 font-handwriting text-xl leading-none hover:bg-white/30 focus:outline-none focus:ring-2 focus:ring-current/30"
                                                >
                                                    "‹"
                                                </button>
                                                <span class="font-handwriting text-lg leading-none">
                                                    {format!("{} {}", month_name(month), year)}
                                                </span>
                                                <button
                                                    type="button"
                                                    data-note-action="next-month"
                                                    on:click=next_month
                                                    aria-label="Next month"
                                                    class="rounded-sm px-1 font-handwriting text-xl leading-none hover:bg-white/30 focus:outline-none focus:ring-2 focus:ring-current/30"
                                                >
                                                    "›"
                                                </button>
                                            </div>
                                            <div class="mt-2 grid grid-cols-7 gap-1 text-center font-sans text-[9px] uppercase opacity-60">
                                                <span>"sun"</span>
                                                <span>"mon"</span>
                                                <span>"tue"</span>
                                                <span>"wed"</span>
                                                <span>"thu"</span>
                                                <span>"fri"</span>
                                                <span>"sat"</span>
                                            </div>
                                            <div class="mt-1 grid grid-cols-7 gap-1 text-center font-sans text-xs">
                                                {calendar_days(year, month)
                                                    .into_iter()
                                                    .map(|day| match day {
                                                        Some(day) => {
                                                            let date_value = format!("{year:04}-{month:02}-{day:02}");
                                                            let aria_label = format!("Set due date to {date_value}");
                                                            let date_value_for_handler = date_value.clone();
                                                            let is_selected = selected_date.as_deref() == Some(date_value.as_str());
                                                            view! {
                                                                <button
                                                                    type="button"
                                                                    data-note-action="choose-due-date"
                                                                    on:click=move |ev: MouseEvent| {
                                                                        ev.stop_propagation();
                                                                        set_note_due_date(
                                                                            id,
                                                                            Some(date_value_for_handler.clone()),
                                                                            notes,
                                                                            groups,
                                                                            history,
                                                                            editing,
                                                                            edit_snapshot,
                                                                        );
                                                                        due_calendar_open.set(false);
                                                                    }
                                                                    aria-label=aria_label
                                                                    class=if is_selected {
                                                                        "rounded-sm bg-note-ink-yellow px-1 py-1 font-semibold text-note-yellow focus:outline-none focus:ring-2 focus:ring-current/30"
                                                                    } else {
                                                                        "rounded-sm px-1 py-1 hover:bg-white/40 focus:bg-white/40 focus:outline-none focus:ring-2 focus:ring-current/30"
                                                                    }
                                                                >
                                                                    {day}
                                                                </button>
                                                            }
                                                            .into_any()
                                                        }
                                                        None => view! { <span class="py-1"></span> }.into_any(),
                                                    })
                                                    .collect_view()}
                                            </div>
                                        </div>
                                    }
                                    .into_any()
                                } else {
                                    ().into_any()
                                }}
                            </div>
                            <button
                                type="button"
                                data-note-action="cycle-color"
                                on:click=cycle_color
                                aria-label="Change note colour"
                                title="Change note colour"
                                style=move || note_snapshot(notes, id)
                                    .map(|note| format!("background-color:{}", note_color_background(note.color)))
                                    .unwrap_or_default()
                                class="h-3 w-3 rounded-full border border-current/30 opacity-75 hover:scale-110 hover:opacity-100 focus:outline-none focus:ring-2 focus:ring-current/30"
                            ></button>
                            <button
                                type="button"
                                data-note-action="delete-note"
                                on:click=delete_note
                                aria-label="Delete task"
                                title="Delete task"
                                class="px-1 font-sans text-xs opacity-0 transition-opacity group-hover:opacity-60 hover:!opacity-100 focus:opacity-100 focus:outline-none focus:ring-2 focus:ring-current/30"
                            >
                                "×"
                            </button>
                        </div>
                    </div>
                }
                .into_any()
            }}
        </article>
    }
}

#[component]
pub fn Board() -> impl IntoView {
    // Re-open the last account-local namespace when this browser has a
    // remembered authenticated account. If its cookie has expired, the
    // account workspace stays visible while sync pauses and asks the user to
    // sign in again. A browser with no remembered account still starts in the
    // isolated guest namespace.
    let initial_principal = remembered_account_principal().unwrap_or_else(guest_principal);
    select_sync_principal(&initial_principal);
    let initial_workspace = load_workspace();
    let initial_workspace_for_hydration = initial_workspace.clone();
    let initial_space_id = initial_workspace.active_space_id;
    let initial_board = initial_workspace
        .spaces
        .iter()
        .find(|space| space.id == initial_space_id)
        .map(|space| space.board.clone())
        .unwrap_or_else(empty_board);
    let initial_board_for_hydration = initial_board.clone();
    let initial_next_id = next_note_id(&initial_board);
    let spaces = RwSignal::new(initial_workspace.spaces);
    let workspace_tombstones = RwSignal::new(initial_workspace.tombstones);
    let active_space_id = RwSignal::new(initial_space_id);
    let notes = RwSignal::new(initial_board.notes);
    let groups = RwSignal::new(initial_board.groups);
    let history = RwSignal::new(History::default());
    let editing = RwSignal::new(None::<u64>);
    let edit_snapshot = RwSignal::new(None::<(u64, BoardData)>);
    let dragged = RwSignal::new(None::<u64>);
    let dragged_ids = RwSignal::new(Vec::<u64>::new());
    let drag_offset = RwSignal::new(None::<(f64, f64)>);
    let drag_snapshot = RwSignal::new(None::<BoardData>);
    let drag_origin = RwSignal::new(None::<(f64, f64)>);
    let initial_view = load_view(initial_space_id);
    let pan = RwSignal::new(initial_view.pan);
    let zoom = RwSignal::new(initial_view.zoom);
    let pan_pointer = RwSignal::new(None::<i32>);
    let last_pan_point = RwSignal::new(None::<(f64, f64)>);
    let selection = RwSignal::new(Vec::<u64>::new());
    let marquee_start = RwSignal::new(None::<(f64, f64)>);
    let marquee_current = RwSignal::new(None::<(f64, f64)>);
    let group_editing = RwSignal::new(None::<u64>);
    let group_edit_snapshot = RwSignal::new(None::<(u64, BoardData)>);
    let group_dragging = RwSignal::new(None::<u64>);
    let group_drag_start = RwSignal::new(None::<(f64, f64)>);
    let group_drag_snapshot = RwSignal::new(None::<BoardData>);
    let group_resizing = RwSignal::new(None::<u64>);
    let group_resize_start = RwSignal::new(None::<(f64, f64)>);
    let group_resize_initial = RwSignal::new(None::<(f64, f64, f64, f64)>);
    let group_resize_corner = RwSignal::new(None::<(i8, i8)>);
    let group_resize_snapshot = RwSignal::new(None::<BoardData>);
    let restore_message = RwSignal::new(None::<String>);
    let space_menu_open = RwSignal::new(false);
    let rename_space_id = RwSignal::new(None::<u64>);
    let rename_value = RwSignal::new(String::new());
    let pending_delete_space = RwSignal::new(None::<u64>);
    let context_menu = RwSignal::new(None::<ContextMenuState>);
    let due_date_request = RwSignal::new(None::<u64>);
    let storage_status = RwSignal::new(StorageStatus::Saving);
    let storage_hydrated = RwSignal::new(false);
    if storage_schema_blocked() {
        surface_storage_repair(storage_status, restore_message);
    }
    let sync_started = RwSignal::new(false);
    let account_state = RwSignal::new(AccountState::Checking);
    let sync_entitled = RwSignal::new(false);
    let sync_status = RwSignal::new(SyncStatus::Checking);
    let pending_sync_count = RwSignal::new(0usize);
    SYNC_PENDING_COUNT_SIGNAL.with(|signal| signal.replace(Some(pending_sync_count)));
    on_cleanup(|| {
        SYNC_PENDING_COUNT_SIGNAL.with(|signal| signal.replace(None));
    });
    let last_server_ack_at = RwSignal::new(None::<u64>);
    let last_request_id = RwSignal::new(None::<String>);
    let last_sync_error = RwSignal::new(None::<String>);
    let local_sync_diagnostics = RwSignal::new(None::<LocalSyncDiagnostics>);
    let transport_sync_diagnostics = RwSignal::new(SyncTransportDiagnostics::default());
    let show_sync_diagnostics = RwSignal::new(false);
    let account_namespace_loading = RwSignal::new(false);
    let account_namespace_prepared = RwSignal::new(None::<String>);
    // Prevent the projection persistence effect from writing a temporary
    // blank workspace into either the old account or the account being
    // opened while a principal switch is in progress.
    let namespace_transitioning = RwSignal::new(false);
    let checkout_error = RwSignal::new(None::<String>);
    let checkout_pending = RwSignal::new(false);
    let crdt_docs = RwSignal::new(HashMap::<u64, SpaceDoc>::new());
    let next_space_id = RwSignal::new(next_space_id_for(&spaces.get_untracked()));
    let next_id = RwSignal::new(initial_next_id);

    // Listen before the asynchronous IndexedDB hydration begins. Messages
    // received during hydration are buffered and replayed after the local
    // namespace has been selected, so a fast sibling tab cannot create a
    // gap between its durable write and this tab's listener startup.
    start_local_tab_sync(
        spaces,
        active_space_id,
        notes,
        groups,
        workspace_tombstones,
        crdt_docs,
        storage_hydrated,
    );

    hydrate_workspace_from_indexed_db(
        initial_workspace_for_hydration,
        initial_board_for_hydration,
        spaces,
        active_space_id,
        notes,
        groups,
        workspace_tombstones,
        next_space_id,
        next_id,
        selection,
        history,
        editing,
        edit_snapshot,
        pan,
        zoom,
        storage_hydrated,
        crdt_docs,
        storage_status,
        restore_message,
    );

    // Drain messages captured before IndexedDB hydration completed. This
    // channel remains active for guest and authenticated namespaces alike,
    // including when the API is offline.
    Effect::new(move |_| {
        if storage_hydrated.get() {
            flush_local_tab_messages(
                spaces,
                active_space_id,
                notes,
                groups,
                workspace_tombstones,
                crdt_docs,
            );
            refresh_pending_count();
        }
    });

    // A backgrounded tab may miss both BroadcastChannel and storage events.
    // Rehydrate whenever the visible space changes so guest/offline tabs get
    // the same repair path as authenticated tabs before rendering that space.
    Effect::new(move |_| {
        if !storage_hydrated.get() {
            return;
        }
        let _ = active_space_id.get();
        spawn_local(refresh_local_space_from_indexed_db(
            spaces,
            active_space_id,
            notes,
            groups,
            workspace_tombstones,
            crdt_docs,
            storage_status,
            restore_message,
        ));
    });

    let local_focus_listener = window_event_listener_untyped("focus", move |_| {
        if storage_hydrated.get_untracked() {
            spawn_local(refresh_local_space_from_indexed_db(
                spaces,
                active_space_id,
                notes,
                groups,
                workspace_tombstones,
                crdt_docs,
                storage_status,
                restore_message,
            ));
        }
    });
    let local_visibility_listener = window_event_listener_untyped("visibilitychange", move |_| {
        if storage_hydrated.get_untracked() {
            spawn_local(refresh_local_space_from_indexed_db(
                spaces,
                active_space_id,
                notes,
                groups,
                workspace_tombstones,
                crdt_docs,
                storage_status,
                restore_message,
            ));
        }
    });
    let local_refresh_tick = Closure::<dyn FnMut()>::new(move || {
        if storage_hydrated.get_untracked() {
            spawn_local(refresh_local_space_from_indexed_db(
                spaces,
                active_space_id,
                notes,
                groups,
                workspace_tombstones,
                crdt_docs,
                storage_status,
                restore_message,
            ));
        }
    });
    let local_refresh_timer = start_local_refresh(local_refresh_tick.as_ref().unchecked_ref());
    local_refresh_tick.forget();
    on_cleanup(move || {
        local_focus_listener.remove();
        local_visibility_listener.remove();
        stop_local_refresh(local_refresh_timer);
    });

    // Entitlement changes arrive through verified webhooks, not the checkout
    // return URL. Refresh independently of the sync coordinator so a page
    // paused for payment can notice recovery without a reload; principal
    // checks discard late responses after logout or account switching.
    let account_refresh_tick = Closure::<dyn FnMut()>::new(move || {
        let principal = current_sync_principal();
        spawn_local(async move {
            let state = load_account_state().await;
            if current_sync_principal() == principal
                && !(matches!(state, AccountState::Unavailable)
                    && account_state.get_untracked().is_authenticated())
            {
                account_state.set(state);
            }
        });
    });
    let account_refresh_timer =
        start_account_refresh(account_refresh_tick.as_ref().unchecked_ref());
    account_refresh_tick.forget();
    on_cleanup(move || stop_account_refresh(account_refresh_timer));

    let initial_account_principal = current_sync_principal();
    spawn_local(async move {
        let state = load_account_state_after_checkout().await;
        if current_sync_principal() == initial_account_principal {
            account_state.set(state);
        }
    });

    Effect::new(move |_| {
        if !storage_hydrated.get() || account_namespace_loading.get() {
            return;
        }
        let AccountState::SignedIn(entitlement) = account_state.get() else {
            return;
        };
        let account_id = entitlement.account_id.clone();
        if account_namespace_prepared.get().as_deref() == Some(account_id.as_str()) {
            return;
        }
        if sync_started.get_untracked() {
            stop_authenticated_sync();
            sync_started.set(false);
        }
        account_namespace_loading.set(true);
        namespace_transitioning.set(true);
        let prepared_account_id = account_id.clone();
        let allow_guest_adoption = is_guest_principal(&current_sync_principal());
        let guest_workspace = allow_guest_adoption.then(|| {
            let mut workspace = workspace_snapshot(
                spaces.get_untracked(),
                active_space_id.get_untracked(),
                workspace_tombstones.get_untracked(),
            );
            if let Some(space) = workspace
                .spaces
                .iter_mut()
                .find(|space| space.id == workspace.active_space_id)
            {
                space.board.notes = notes.get_untracked();
                space.board.groups = groups.get_untracked();
            }
            normalize_workspace(workspace)
        });
        // Remove the previous namespace from the live projection immediately.
        // The old account must not remain visible while its replacement is
        // loading, and namespace_transitioning prevents this blank frame from
        // being persisted to IndexedDB.
        install_workspace(
            workspace_from_board(empty_board()),
            spaces,
            active_space_id,
            notes,
            groups,
            workspace_tombstones,
            next_space_id,
            next_id,
            crdt_docs,
        );
        spawn_local(async move {
            let prepared = prepare_account_workspace(
                account_id,
                account_state,
                allow_guest_adoption,
                guest_workspace,
                spaces,
                active_space_id,
                notes,
                groups,
                workspace_tombstones,
                next_space_id,
                next_id,
                crdt_docs,
                storage_status,
                restore_message,
            )
            .await;
            if current_sync_principal() != format!("account:{prepared_account_id}") {
                account_namespace_loading.set(false);
                namespace_transitioning.set(false);
                return;
            }
            let account_state_matches = matches!(
                account_state.get_untracked(),
                AccountState::SignedIn(ref current)
                    if current.account_id == prepared_account_id
            );
            if !account_state_matches {
                account_namespace_loading.set(false);
                namespace_transitioning.set(false);
                if matches!(account_state.get_untracked(), AccountState::Guest) {
                    select_sync_principal(&guest_principal());
                    if let Some(window) = web_sys::window() {
                        let _ = window.location().reload();
                    }
                }
                return;
            }
            account_namespace_prepared.set(prepared.then_some(prepared_account_id));
            account_namespace_loading.set(false);
            namespace_transitioning.set(false);
            if !prepared {
                surface_storage_repair(storage_status, restore_message);
                sync_status.set(SyncStatus::Error);
            }
            sync_entitled.set(prepared && entitlement.can_sync_at(now_millis() / 1_000));
        });
    });

    Effect::new(move |_| {
        // start_authenticated_sync registers its cleanup on this effect's
        // owner. Do not subscribe to sync_started here: setting it to true
        // during the first run would immediately rerun the effect, execute
        // that cleanup, and fence off the sync run before its runtime exists.
        if storage_hydrated.get() && sync_entitled.get() && !sync_started.get_untracked() {
            sync_started.set(true);
            start_authenticated_sync(
                spaces,
                next_space_id,
                active_space_id,
                notes,
                groups,
                workspace_tombstones,
                crdt_docs,
                sync_status,
                pending_sync_count,
                last_server_ack_at,
                last_request_id,
                last_sync_error,
                local_sync_diagnostics,
                transport_sync_diagnostics,
            );
        }
    });

    Effect::new(move |_| {
        if account_namespace_loading.get() {
            sync_status.set(SyncStatus::Checking);
            return;
        }
        match account_state.get() {
            AccountState::Checking => sync_status.set(SyncStatus::Checking),
            AccountState::Guest | AccountState::Unavailable => {
                sync_entitled.set(false);
                pending_sync_count.set(0);
                last_server_ack_at.set(None);
                last_request_id.set(None);
                last_sync_error.set(None);
                if sync_started.get_untracked() {
                    stop_authenticated_sync();
                    sync_started.set(false);
                }
                if matches!(account_state.get_untracked(), AccountState::Guest)
                    && current_sync_principal().starts_with("account:")
                {
                    // A session expiry/account removal is a namespace
                    // transition too. Reloading starts Board in the durable
                    // guest principal and guarantees no late account request
                    // can paint account data over the guest workspace.
                    namespace_transitioning.set(true);
                    select_sync_principal(&guest_principal());
                    if let Some(window) = web_sys::window() {
                        let _ = window.location().reload();
                    }
                    return;
                }
                sync_status.set(
                    if matches!(account_state.get_untracked(), AccountState::Guest) {
                        SyncStatus::Disabled
                    } else {
                        SyncStatus::Error
                    },
                );
            }
            AccountState::Expired => {
                sync_entitled.set(false);
                refresh_pending_count();
                if sync_started.get_untracked() {
                    stop_authenticated_sync();
                    sync_started.set(false);
                }
                // Expiration is not a namespace reset. Keep local account
                // edits visible and durable while cloud sync waits for a new
                // sign-in, otherwise a valid local workspace appears empty.
                sync_status.set(SyncStatus::AuthPaused);
            }
            AccountState::SignedIn(entitlement)
                if !entitlement.can_sync_at(now_millis() / 1_000) =>
            {
                sync_entitled.set(false);
                refresh_pending_count();
                last_request_id.set(None);
                if sync_started.get_untracked() {
                    stop_authenticated_sync();
                    sync_started.set(false);
                }
                sync_status.set(SyncStatus::BillingPaused);
            }
            AccountState::SignedIn(entitlement) => {
                if account_namespace_prepared.get_untracked().as_deref()
                    == Some(entitlement.account_id.as_str())
                {
                    sync_entitled.set(entitlement.can_sync_at(now_millis() / 1_000));
                }
            }
        }
    });

    Effect::new(move |_| {
        if !sync_started.get() {
            return;
        }
        let _ = active_space_id.get();
        spawn_local(pull_active_space(
            spaces,
            active_space_id,
            notes,
            groups,
            workspace_tombstones,
            crdt_docs,
            SYNC_RUN_GENERATION.with(Cell::get),
        ));
    });

    let board_actions = BoardActions {
        notes,
        groups,
        history,
        selection,
        pan,
        zoom,
        editing,
        edit_snapshot,
        group_editing,
        group_edit_snapshot,
        due_date_request,
        restore_message,
    };

    let begin_checkout = move |interval: &'static str| {
        checkout_error.set(None);
        checkout_pending.set(true);
        spawn_local(async move {
            match start_checkout(interval).await {
                Ok(url) => {
                    if let Some(window) = web_sys::window() {
                        let _ = window.location().set_href(&url);
                    }
                }
                Err(message) => {
                    checkout_pending.set(false);
                    checkout_error.set(Some(message));
                }
            }
        });
    };

    let begin_billing_portal = move |_| {
        checkout_error.set(None);
        checkout_pending.set(true);
        spawn_local(async move {
            match start_billing_portal().await {
                Ok(url) => {
                    if let Some(window) = web_sys::window() {
                        let _ = window.location().set_href(&url);
                    }
                }
                Err(message) => {
                    checkout_pending.set(false);
                    checkout_error.set(Some(message));
                }
            }
        });
    };

    let retry_account_check = move |_| {
        let principal = current_sync_principal();
        spawn_local(async move {
            let state = load_account_state().await;
            if current_sync_principal() == principal {
                account_state.set(state);
            }
        });
    };

    let mut last_remote_crdt_projection = remote_crdt_projection_generation();
    let last_projected_board = Rc::new(RefCell::new(None::<(u64, BoardData)>));
    Effect::new(move |_| {
        if !storage_hydrated.get() || namespace_transitioning.get() {
            return;
        }
        let remote_projection = remote_crdt_projection_generation();
        let remote_projection_changed = remote_projection != last_remote_crdt_projection;
        last_remote_crdt_projection = remote_projection;
        let active_id = active_space_id.get();
        if storage_writes_blocked() {
            storage_status.set(StorageStatus::Error);
        } else {
            storage_status.set(StorageStatus::Saving);
        }
        let board = BoardData {
            schema_version: CURRENT_SCHEMA_VERSION,
            notes: notes.get(),
            groups: groups.get(),
            tombstones: Vec::new(),
        };
        // A remote projection can be followed by a local UI edit before this
        // reactive effect runs. Compare against the live CRDT, not only the
        // projection-generation marker: remote-only work needs no new outbox
        // row, but a local edit made in the same turn must be diffed against
        // the latest CRDT so it cannot disappear from the canonical document.
        let canonical_board_before = crdt_docs
            .get_untracked()
            .get(&active_id)
            .map(SpaceDoc::board);
        let should_queue_crdt = should_queue_crdt_projection(
            remote_projection_changed,
            canonical_board_before.as_ref(),
            &board,
        );
        let previous_board = last_projected_board
            .borrow()
            .as_ref()
            .filter(|(space_id, _)| *space_id == active_id)
            .map(|(_, board)| board.clone());
        *last_projected_board.borrow_mut() = Some((active_id, board.clone()));
        let saved = persist_space_board(
            spaces,
            active_id,
            board,
            workspace_tombstones,
            storage_status,
            false,
        );
        let workspace_raw = if saved {
            serde_json::to_string(&workspace_snapshot(
                spaces.get_untracked(),
                active_id,
                workspace_tombstones.get_untracked(),
            ))
            .ok()
        } else {
            None
        };
        let mut queued_crdt = false;
        if saved
            && should_queue_crdt
            && let Some(previous_board) = canonical_board_before.or(previous_board)
            && let Some(current_board) = spaces
                .get_untracked()
                .iter()
                .find(|space| space.id == active_id)
                .map(|space| space.board.clone())
        {
            queued_crdt = persist_space_crdt(
                active_id,
                &previous_board,
                &current_board,
                spaces,
                notes,
                groups,
                crdt_docs,
                workspace_raw,
                storage_status,
            );
        }
        if !saved || storage_writes_blocked() {
            storage_status.set(StorageStatus::Error);
        } else if !queued_crdt {
            // Space switches, hydration, and remote projections may update
            // the active signals without creating a new local CRDT update.
            // They still leave the already-loaded local record in a saved
            // state; do not leave the indicator stuck at "saving".
            storage_status.set(StorageStatus::Saved);
        }
    });

    Effect::new(move |_| {
        save_view(
            active_space_id.get(),
            ViewState {
                pan: pan.get(),
                zoom: zoom.get(),
            },
        );
    });

    let start_pan = move |ev: PointerEvent| {
        if ev.button() != 0 || dragged.get_untracked().is_some() {
            return;
        }
        let Some(target) = ev
            .target()
            .and_then(|target| target.dyn_into::<Element>().ok())
        else {
            return;
        };
        if target.closest(".task-space-note").ok().flatten().is_some() {
            return;
        }
        let Some(surface) = ev
            .current_target()
            .and_then(|target| target.dyn_into::<Element>().ok())
        else {
            return;
        };
        let _ = surface.set_pointer_capture(ev.pointer_id());
        pan_pointer.set(Some(ev.pointer_id()));
        let point = (f64::from(ev.client_x()), f64::from(ev.client_y()));
        if ev.shift_key() {
            selection.set(Vec::new());
            marquee_start.set(Some(point));
            marquee_current.set(Some(point));
        } else {
            selection.set(Vec::new());
            last_pan_point.set(Some(point));
        }
    };

    let move_pan = move |ev: PointerEvent| {
        if pan_pointer.get_untracked() != Some(ev.pointer_id()) {
            return;
        }
        ev.prevent_default();
        let current = (f64::from(ev.client_x()), f64::from(ev.client_y()));
        if marquee_start.get_untracked().is_some() {
            marquee_current.set(Some(current));
            return;
        }
        if let Some(previous) = last_pan_point.get_untracked() {
            pan.update(|position| {
                position.0 += current.0 - previous.0;
                position.1 += current.1 - previous.1;
            });
        }
        last_pan_point.set(Some(current));
    };

    let finish_pan = move |ev: PointerEvent| {
        if pan_pointer.get_untracked() != Some(ev.pointer_id()) {
            return;
        }
        if let Some(surface) = ev
            .current_target()
            .and_then(|target| target.dyn_into::<Element>().ok())
        {
            let _ = surface.release_pointer_capture(ev.pointer_id());
        }
        if let (Some(start), Some(current)) = (
            marquee_start.get_untracked(),
            marquee_current.get_untracked(),
        ) {
            selection.set(notes_in_marquee(
                &notes.get_untracked(),
                start,
                current,
                pan.get_untracked(),
                zoom.get_untracked(),
            ));
        }
        marquee_start.set(None);
        marquee_current.set(None);
        pan_pointer.set(None);
        last_pan_point.set(None);
    };

    let zoom_or_pan = move |ev: WheelEvent| {
        ev.prevent_default();
        if ev.ctrl_key() || ev.meta_key() {
            let Some(board) = web_sys::window()
                .and_then(|window| window.document())
                .and_then(|document| document.get_element_by_id("task-space-board"))
            else {
                return;
            };
            let rect = board.get_bounding_client_rect();
            let cursor = (
                f64::from(ev.client_x()) - rect.left() - rect.width() / 2.0,
                f64::from(ev.client_y()) - rect.top() - rect.height() / 2.0,
            );
            let old_zoom = zoom.get_untracked();
            let next_zoom =
                (old_zoom * if ev.delta_y() < 0.0 { 1.1 } else { 0.9 }).clamp(0.35, 2.5);
            let old_pan = pan.get_untracked();
            let world_point = (
                (cursor.0 - old_pan.0) / old_zoom,
                (cursor.1 - old_pan.1) / old_zoom,
            );
            pan.set((
                cursor.0 - world_point.0 * next_zoom,
                cursor.1 - world_point.1 * next_zoom,
            ));
            zoom.set(next_zoom);
        } else {
            pan.update(|position| {
                position.0 -= ev.delta_x();
                position.1 -= ev.delta_y();
            });
        }
    };

    let zoom_in = move |_| zoom.update(|value| *value = (*value * 1.2).min(2.5));
    let zoom_out = move |_| zoom.update(|value| *value = (*value / 1.2).max(0.35));
    let reset_view = move |_| board_actions.reset_view();

    let undo = move |_| board_actions.undo();
    let redo = move |_| board_actions.redo();
    let group_selected = move |_: MouseEvent| board_actions.group_selected();
    let ungroup_selected = move |_: MouseEvent| board_actions.ungroup_selected();
    let delete_selected = move |_: MouseEvent| board_actions.delete_selected();

    let space_actions = SpaceActions {
        spaces,
        active_space_id,
        notes,
        groups,
        next_id,
        selection,
        history,
        editing,
        edit_snapshot,
        group_editing,
        group_edit_snapshot,
        pan,
        zoom,
        restore_message,
        space_menu_open,
        pending_delete_space,
        workspace_tombstones,
        storage_status,
    };

    let create_space = move |_| space_actions.create(next_space_id);
    let begin_rename_space = move |_| {
        space_actions.begin_rename(
            rename_space_id,
            rename_value,
            active_space_id.get_untracked(),
        );
    };
    let archive_current_space = move |_| space_actions.archive_current();

    let add_to_group = move |ev: Event| {
        let Some(select) = ev
            .target()
            .and_then(|target| target.dyn_into::<HtmlSelectElement>().ok())
        else {
            return;
        };
        let Ok(group_id) = select.value().parse::<u64>() else {
            return;
        };
        add_selection_to_group(
            group_id,
            notes,
            groups,
            history,
            selection,
            editing,
            edit_snapshot,
            group_editing,
            group_edit_snapshot,
        );
        select.set_value("");
    };

    let add_note = move |_| board_actions.create_note();

    let restore_file = move |ev: Event| {
        let Some(input) = ev
            .target()
            .and_then(|target| target.dyn_into::<HtmlInputElement>().ok())
        else {
            return;
        };
        let Some(file) = input.files().and_then(|files| files.get(0)) else {
            return;
        };
        commit_pending_edit(notes, groups, history, editing, edit_snapshot);
        let Ok(reader) = FileReader::new() else {
            restore_message.set(Some("couldn't open that file".into()));
            return;
        };
        let reader_for_callback = reader.clone();
        let onload = Closure::wrap(Box::new(move |_event: web_sys::ProgressEvent| {
            let result = reader_for_callback
                .result()
                .ok()
                .and_then(|value| value.as_string());
            let Some(raw) = result else {
                restore_message.set(Some("couldn't read that file".into()));
                return;
            };

            if let Some(mut imported) = parse_workspace(&raw) {
                let confirmed =
                    web_sys::window()
                        .and_then(|window| {
                            window.confirm_with_message(
                        "Replace the spaces on this device with the imported workspace?",
                    ).ok()
                        })
                        .unwrap_or(false);
                if !confirmed {
                    restore_message.set(Some("restore cancelled".into()));
                    return;
                }
                imported.device_id = load_device_id();
                imported = normalize_workspace(imported);
                let imported_space_id = imported.active_space_id;
                let imported_device_id = imported.device_id.clone();
                let imported_tombstones = imported.tombstones.clone();
                let Some(imported_space) = imported
                    .spaces
                    .iter()
                    .find(|space| space.id == imported_space_id)
                else {
                    restore_message.set(Some("that workspace has no active space".into()));
                    return;
                };
                let imported_board = imported_space.board.clone();
                let imported_view = load_view(imported_space_id);
                clear_storage_schema_blocked();
                clear_storage_repair_required();
                let imported_next_space_id = imported
                    .spaces
                    .iter()
                    .map(|space| space.id)
                    .max()
                    .unwrap_or(0)
                    .saturating_add(1);
                spaces.set(imported.spaces);
                active_space_id.set(imported_space_id);
                workspace_tombstones.set(imported_tombstones.clone());
                next_space_id.set(imported_next_space_id);
                next_id.set(next_note_id(&imported_board));
                notes.set(imported_board.notes);
                groups.set(imported_board.groups);
                selection.set(Vec::new());
                history.set(History::default());
                editing.set(None);
                edit_snapshot.set(None);
                pan.set(imported_view.pan);
                zoom.set(imported_view.zoom);
                storage_status.set(StorageStatus::Saving);
                let saved = write_workspace_exact(
                    &WorkspaceData {
                        schema_version: CURRENT_SCHEMA_VERSION,
                        device_id: imported_device_id,
                        tombstones: imported_tombstones,
                        spaces: spaces.get_untracked(),
                        active_space_id: imported_space_id,
                    },
                    Some(storage_status),
                );
                if !saved {
                    mark_storage_repair_required();
                    storage_status.set(StorageStatus::Error);
                    restore_message.set(Some(
                        "workspace restored in memory, but local saving is still unavailable"
                            .into(),
                    ));
                } else {
                    restore_message.set(Some("workspace restored".into()));
                }
            } else if let Some(restored) = parse_board(&raw) {
                clear_storage_schema_blocked();
                clear_storage_repair_required();
                let before = board_snapshot(notes, groups);
                next_id.set(next_note_id(&restored));
                notes.set(restored.notes);
                groups.set(restored.groups);
                record_snapshot(notes, groups, history, before);
                edit_snapshot.set(None);
                editing.set(None);
                restore_message.set(Some("board restored into current space".into()));
            } else {
                restore_message.set(Some("that file is not a Task Space backup".into()));
            }
        }) as Box<dyn FnMut(_)>);
        reader.set_onload(Some(onload.as_ref().unchecked_ref()));
        onload.forget();
        let _ = reader.read_as_text(&file);
        input.set_value("");
    };

    let keyboard_listener = window_event_listener(leptos::ev::keydown, move |ev: KeyboardEvent| {
        if matches!(account_state.get_untracked(), AccountState::SignedIn(entitlement) if !entitlement.can_sync_at(now_millis() / 1_000))
        {
            return;
        }
        if ev.key() == "Escape" && !keyboard_target_is_editable(&ev) {
            if pending_delete_space.get_untracked().is_some() {
                pending_delete_space.set(None);
            } else if context_menu.get_untracked().is_some() {
                context_menu.set(None);
            } else if space_menu_open.get_untracked() {
                space_menu_open.set(false);
            }
            return;
        }
        if keyboard_target_is_editable(&ev) {
            return;
        }
        let key = ev.key().to_lowercase();
        if (key == "delete" || key == "backspace")
            && !ev.ctrl_key()
            && !ev.meta_key()
            && !ev.alt_key()
        {
            ev.prevent_default();
            delete_selected_notes(
                notes,
                groups,
                history,
                selection,
                editing,
                edit_snapshot,
                group_editing,
                group_edit_snapshot,
            );
            return;
        }
        if !(ev.ctrl_key() || ev.meta_key()) {
            return;
        }
        if key == "g" {
            ev.prevent_default();
            if ev.shift_key() {
                ungroup_selection(
                    notes,
                    groups,
                    history,
                    selection,
                    editing,
                    edit_snapshot,
                    group_editing,
                    group_edit_snapshot,
                );
            } else {
                create_group(
                    notes,
                    groups,
                    history,
                    selection,
                    editing,
                    edit_snapshot,
                    group_editing,
                    group_edit_snapshot,
                );
            }
            return;
        }
        if key != "z" {
            return;
        }
        ev.prevent_default();
        if ev.shift_key() {
            redo_board(notes, groups, history, editing, edit_snapshot);
        } else {
            undo_board(notes, groups, history, editing, edit_snapshot);
        }
    });
    on_cleanup(move || keyboard_listener.remove());

    view! {
        <main
            class="relative h-[100dvh] min-h-screen overflow-hidden bg-paper"
            on:pointerdown=move |_| context_menu.set(None)
        >
            <div
                id="task-space-board"
                class=move || if pan_pointer.get().is_some() {
                    "absolute inset-0 overflow-hidden bg-paper-shelf cursor-grabbing"
                } else {
                    "absolute inset-0 overflow-hidden bg-paper-shelf cursor-grab"
                }
                style=move || {
                    let (pan_x, pan_y) = pan.get();
                    let grid_size = 24.0 * zoom.get();
                    format!(
                        "background-image: radial-gradient(color-mix(in srgb, var(--color-ink-soft) 18%, transparent) 1px, transparent 1.5px); background-size: {grid_size}px {grid_size}px; background-position: calc(50% + {pan_x}px) calc(50% + {pan_y}px);"
                    )
                }
                on:pointerdown=move |ev: PointerEvent| {
                    space_menu_open.set(false);
                    start_pan(ev);
                }
                on:pointermove=move_pan
                on:pointerup=finish_pan
                on:pointercancel=finish_pan
                on:wheel=zoom_or_pan
                on:contextmenu=move |ev: MouseEvent| {
                    ev.prevent_default();
                    ev.stop_propagation();
                    space_menu_open.set(false);
                    context_menu.set(Some(ContextMenuState {
                        target: ContextMenuTarget::Board,
                        x: ev.client_x(),
                        y: ev.client_y(),
                    }));
                }
            >
                <div class="pointer-events-none absolute inset-0 opacity-40" style="background:linear-gradient(110deg, transparent 0%, rgb(255 255 255 / .2) 47%, transparent 50%);"></div>
                {move || if notes.get().is_empty() {
                    view! {
                        <div class="absolute inset-0 grid place-items-center p-8 text-center">
                            <div class="max-w-sm rotate-[-1deg] rounded-[3px] bg-note-yellow px-8 py-7 text-note-ink-yellow shadow-lg">
                                <p class="font-handwriting text-4xl">"start with one small thing"</p>
                                <p class="mt-2 text-sm">"Put the task somewhere you can see it. The board remembers it here, even when the network disappears."</p>
                                <button
                                    type="button"
                                    on:pointerdown=move |ev: PointerEvent| ev.stop_propagation()
                                    on:click=move |ev: MouseEvent| {
                                        ev.stop_propagation();
                                        board_actions.create_note();
                                    }
                                    class="mt-5 rounded-[3px] bg-note-ink-yellow px-4 py-2 text-sm font-medium text-note-yellow hover:brightness-110 focus:outline-none focus:ring-2 focus:ring-note-ink-yellow/50"
                                >
                                    "pin the first note"
                                </button>
                            </div>
                        </div>
                    }.into_any()
                } else {
                    ().into_any()
                }}
                {move || marquee_start.get().zip(marquee_current.get()).map(|(start, current)| view! {
                    <div
                        class="pointer-events-none absolute z-30 border border-ink-soft/50 bg-marker/20"
                        style=move || marquee_style(start, current)
                    ></div>
                })}
                <div
                    class="absolute left-1/2"
                    style=move || {
                        let (pan_x, pan_y) = pan.get();
                        format!(
                            "left:50%;top:50%;transform:translate3d({pan_x}px,{pan_y}px,0) scale({}); transform-origin:0 0;",
                            zoom.get()
                        )
                    }
                >
                    <For
                        each={move || groups.get().into_iter().map(|group| group.id).collect::<Vec<_>>()}
                        key=|id| *id
                        children=move |id: u64| {
                            view! {
                                <GroupFrame
                                    id=id
                                    context_menu=context_menu
                                    notes=notes
                                    groups=groups
                                    history=history
                                    editing=group_editing
                                    edit_snapshot=group_edit_snapshot
                                    group_dragging=group_dragging
                                    group_drag_start=group_drag_start
                                    group_drag_snapshot=group_drag_snapshot
                                    dragged_ids=dragged_ids
                                    drag_snapshot=drag_snapshot
                                    group_resizing=group_resizing
                                    group_resize_start=group_resize_start
                                    group_resize_initial=group_resize_initial
                                    group_resize_corner=group_resize_corner
                                    group_resize_snapshot=group_resize_snapshot
                                    zoom=zoom
                                />
                            }
                        }
                    />
                    <For
                        each={move || notes.get().into_iter().map(|note| note.id).collect::<Vec<_>>()}
                        key=|id| *id
                        children=move |id: u64| {
                            view! {
                                <NoteCard
                                    id=id
                                    context_menu=context_menu
                                    due_date_request=due_date_request
                                    notes=notes
                                    groups=groups
                                    history=history
                                    selection=selection
                                    editing=editing
                                    edit_snapshot=edit_snapshot
                                    dragged=dragged
                                    dragged_ids=dragged_ids
                                    drag_offset=drag_offset
                                    drag_snapshot=drag_snapshot
                                    drag_origin=drag_origin
                                    pan=pan
                                    zoom=zoom
                                />
                            }
                        }
                    />
                </div>
            </div>

            {move || context_menu.get().map(|menu| {
                let menu_style = format!(
                    "left:max(.75rem,min({}px,calc(100vw - 14.75rem)));top:max(.75rem,min({}px,calc(100dvh - 29rem)));",
                    menu.x,
                    menu.y,
                );
                match menu.target {
                    ContextMenuTarget::Board => view! {
                        <div
                            role="menu"
                            aria-label="Board actions"
                            class="pointer-events-auto fixed z-[70] w-56 max-w-[calc(100vw-1.5rem)] max-h-[min(28rem,calc(100dvh-1.5rem))] overflow-y-auto rounded-md border border-ink/20 bg-paper p-2 text-ink shadow-xl"
                            style=menu_style
                            on:pointerdown=move |ev: PointerEvent| ev.stop_propagation()
                            on:click=move |ev: MouseEvent| ev.stop_propagation()
                        >
                            <div class="border-b border-ink-soft/15 px-2 pb-2 font-handwriting text-xl">"board actions"</div>
                            <div class="mt-1 space-y-0.5">
                                <button type="button" role="menuitem" on:click=move |_| { board_actions.undo(); context_menu.set(None); } disabled=move || history.get().undo.is_empty() class="context-menu-item">"undo"</button>
                                <button type="button" role="menuitem" on:click=move |_| { board_actions.redo(); context_menu.set(None); } disabled=move || history.get().redo.is_empty() class="context-menu-item">"redo"</button>
                                <div class="my-1 border-t border-ink-soft/15"></div>
                                <button type="button" role="menuitem" on:click=move |_| { board_actions.create_note(); context_menu.set(None); } class="context-menu-item context-menu-item-accent">"+ new note"</button>
                                <button type="button" role="menuitem" on:click=move |_| { board_actions.group_selected(); context_menu.set(None); } disabled=move || selection.get().len() < 2 class="context-menu-item">"group selected"</button>
                                <button type="button" role="menuitem" on:click=move |_| { board_actions.ungroup_selected(); context_menu.set(None); } disabled=move || !selection.get().iter().any(|id| notes.get().iter().any(|note| note.id == *id && note.group_id.is_some())) class="context-menu-item">"ungroup selected"</button>
                                <button type="button" role="menuitem" on:click=move |_| { board_actions.delete_selected(); context_menu.set(None); } disabled=move || selection.get().is_empty() class="context-menu-item context-menu-item-danger">"delete selected"</button>
                                {move || if groups.get().is_empty() || selection.get().is_empty() {
                                    ().into_any()
                                } else {
                                    view! {
                                        <div class="mt-1 border-t border-ink-soft/15 pt-1">
                                            <div class="px-2 py-1 text-[10px] uppercase tracking-[0.12em] text-ink-soft">"add selected to"</div>
                                            {move || groups.get().into_iter().map(|group| {
                                                let group_id = group.id;
                                                view! {
                                                    <button type="button" role="menuitem" on:click=move |_| { board_actions.add_selection_to_group(group_id); context_menu.set(None); } class="context-menu-item">{group.label}</button>
                                                }
                                            }).collect_view()}
                                        </div>
                                    }.into_any()
                                }}
                                <div class="my-1 border-t border-ink-soft/15"></div>
                                <button type="button" role="menuitem" on:click=move |_| { board_actions.reset_view(); context_menu.set(None); } class="context-menu-item">"reset view"</button>
                                <button type="button" role="menuitem" on:click=move |_| { export_workspace(&workspace_with_current_board(spaces.get_untracked(), active_space_id.get_untracked(), &notes.get_untracked(), &groups.get_untracked(), &workspace_tombstones.get_untracked())); context_menu.set(None); } class="context-menu-item">"export workspace"</button>
                            </div>
                        </div>
                    }.into_any(),
                    ContextMenuTarget::Note(id) => view! {
                        <div
                            role="menu"
                            aria-label="Note actions"
                            class="pointer-events-auto fixed z-[70] w-56 max-w-[calc(100vw-1.5rem)] max-h-[min(28rem,calc(100dvh-1.5rem))] overflow-y-auto rounded-md border border-ink/20 bg-paper p-2 text-ink shadow-xl"
                            style=menu_style
                            on:pointerdown=move |ev: PointerEvent| ev.stop_propagation()
                            on:click=move |ev: MouseEvent| ev.stop_propagation()
                        >
                            <div class="border-b border-ink-soft/15 px-2 pb-2 font-handwriting text-xl">"note actions"</div>
                            <div class="mt-1 space-y-0.5">
                                <button type="button" role="menuitem" on:click=move |_| { board_actions.edit_note(id); context_menu.set(None); } class="context-menu-item">"edit note"</button>
                                <button type="button" role="menuitem" on:click=move |_| { board_actions.cycle_status(id); context_menu.set(None); } class="context-menu-item">{move || note_snapshot(notes, id).map(|note| format!("mark {}", note.status.next().label())).unwrap_or_else(|| "change status".into())}</button>
                                <button type="button" role="menuitem" on:click=move |_| { board_actions.open_due_date_picker(id); context_menu.set(None); } class="context-menu-item">"choose due date"</button>
                                <button type="button" role="menuitem" on:click=move |_| { board_actions.clear_due_date(id); context_menu.set(None); } disabled=move || note_snapshot(notes, id).is_none_or(|note| note.due_date.is_none()) class="context-menu-item">"clear due date"</button>
                                <button type="button" role="menuitem" on:click=move |_| { board_actions.cycle_color(id); context_menu.set(None); } class="context-menu-item">"change colour"</button>
                                {move || if groups.get().is_empty() {
                                    ().into_any()
                                } else {
                                    view! {
                                        <div class="mt-1 border-t border-ink-soft/15 pt-1">
                                            <div class="px-2 py-1 text-[10px] uppercase tracking-[0.12em] text-ink-soft">"move to group"</div>
                                            {move || groups.get().into_iter().map(|group| {
                                                let group_id = group.id;
                                                view! {
                                                    <button type="button" role="menuitem" on:click=move |_| { board_actions.add_selection_to_group(group_id); context_menu.set(None); } class="context-menu-item">{group.label}</button>
                                                }
                                            }).collect_view()}
                                        </div>
                                    }.into_any()
                                }}
                                <div class="my-1 border-t border-ink-soft/15"></div>
                                <button type="button" role="menuitem" on:click=move |_| { board_actions.delete_note(id); context_menu.set(None); } class="context-menu-item context-menu-item-danger">"delete note"</button>
                            </div>
                        </div>
                    }.into_any(),
                    ContextMenuTarget::Group(id) => view! {
                        <div
                            role="menu"
                            aria-label="Group actions"
                            class="pointer-events-auto fixed z-[70] w-56 max-w-[calc(100vw-1.5rem)] max-h-[min(28rem,calc(100dvh-1.5rem))] overflow-y-auto rounded-md border border-ink/20 bg-paper p-2 text-ink shadow-xl"
                            style=menu_style
                            on:pointerdown=move |ev: PointerEvent| ev.stop_propagation()
                            on:click=move |ev: MouseEvent| ev.stop_propagation()
                        >
                            <div class="border-b border-ink-soft/15 px-2 pb-2 font-handwriting text-xl">"group actions"</div>
                            <div class="mt-1 space-y-0.5">
                                <button type="button" role="menuitem" on:click=move |_| { board_actions.rename_group(id); context_menu.set(None); } class="context-menu-item">"rename group"</button>
                                <button type="button" role="menuitem" on:click=move |_| { board_actions.ungroup_group(id); context_menu.set(None); } class="context-menu-item context-menu-item-danger">"remove group"</button>
                            </div>
                        </div>
                    }.into_any(),
                    ContextMenuTarget::Space(id) => view! {
                        <div
                            role="menu"
                            aria-label="Space actions"
                            class="pointer-events-auto fixed z-[70] w-56 max-w-[calc(100vw-1.5rem)] max-h-[min(28rem,calc(100dvh-1.5rem))] overflow-y-auto rounded-md border border-ink/20 bg-paper p-2 text-ink shadow-xl"
                            style=menu_style
                            on:pointerdown=move |ev: PointerEvent| ev.stop_propagation()
                            on:click=move |ev: MouseEvent| ev.stop_propagation()
                        >
                            <div class="border-b border-ink-soft/15 px-2 pb-2 font-handwriting text-xl">"space actions"</div>
                            <div class="mt-1 space-y-0.5">
                                <button type="button" role="menuitem" on:click=move |_| { space_actions.create(next_space_id); context_menu.set(None); } class="context-menu-item context-menu-item-accent">"+ new space"</button>
                                {move || spaces.get().into_iter().find(|space| space.id == id).map(|space| {
                                    let archived = space.archived;
                                    view! {
                                        {if archived {
                                            view! {
                                                <button type="button" role="menuitem" on:click=move |_| { space_actions.restore(id); context_menu.set(None); } class="context-menu-item">"restore space"</button>
                                                {move || if pending_delete_space.get() == Some(id) {
                                                    view! { <button type="button" role="menuitem" on:click=move |_| { space_actions.confirm_delete(); context_menu.set(None); } class="context-menu-item context-menu-item-danger">"delete forever"</button> }.into_any()
                                                } else {
                                                    view! { <button type="button" role="menuitem" on:click=move |_| space_actions.request_delete(id) class="context-menu-item context-menu-item-danger">"delete forever"</button> }.into_any()
                                                }}
                                            }.into_any()
                                        } else {
                                            view! {
                                                <button type="button" role="menuitem" on:click=move |_| { space_actions.switch(id); context_menu.set(None); } disabled=move || active_space_id.get() == id class="context-menu-item">"switch to space"</button>
                                                <button type="button" role="menuitem" on:click=move |_| { space_actions.begin_rename(rename_space_id, rename_value, id); context_menu.set(None); } class="context-menu-item">"rename space"</button>
                                                <button type="button" role="menuitem" on:click=move |_| { space_actions.archive_current(); context_menu.set(None); } disabled=move || active_space_id.get() != id class="context-menu-item context-menu-item-danger">"archive space"</button>
                                            }.into_any()
                                        }}
                                    }
                                })}
                            </div>
                        </div>
                    }.into_any(),
                }
            })}

            <header class="pointer-events-none absolute inset-x-3 top-3 z-10 flex flex-col items-stretch gap-2 sm:inset-x-5 sm:top-5 sm:flex-row sm:items-start sm:justify-between sm:gap-3">
                <div class="pointer-events-auto flex min-w-0 max-w-full flex-wrap items-center gap-2 rounded-md border border-ink-soft/15 bg-paper/90 px-2.5 py-2 shadow-md backdrop-blur-sm sm:gap-3 sm:px-3">
                    <a href="/" class="flex shrink-0 items-center gap-2 whitespace-nowrap" aria-label="Task Space home">
                        <img src="/smbl-logo.png" alt="SMBL" class="h-6 w-auto"/>
                        <span class="font-handwriting text-2xl leading-none sm:text-3xl">"Task Space"</span>
                    </a>
                    <span class="hidden h-6 w-px bg-ink-soft/20 sm:block"></span>
                    <div class="relative min-w-0">
                        <button
                            type="button"
                            on:click=move |ev: MouseEvent| {
                                ev.stop_propagation();
                                space_menu_open.update(|open| *open = !*open);
                            }
                            aria-label="Switch space"
                            aria-haspopup="menu"
                            aria-expanded=move || space_menu_open.get().to_string()
                            title="Switch space"
                            class="flex max-w-[42vw] min-w-0 items-center gap-1 rounded-[3px] px-2 py-1 font-handwriting text-lg leading-none hover:bg-white/60 focus:outline-none focus:ring-2 focus:ring-ink/30 sm:max-w-44 sm:text-xl"
                        >
                            <span class="truncate">
                                {move || spaces
                                    .get()
                                    .into_iter()
                                    .find(|space| space.id == active_space_id.get())
                                    .map(|space| space.name)
                                    .unwrap_or_else(|| "my space".into())}
                            </span>
                            <span class="font-sans text-xs opacity-60" aria-hidden="true">"⌄"</span>
                        </button>
                        {move || if space_menu_open.get() {
                            view! {
                                <div
                                    role="menu"
                                    class="absolute left-0 top-10 z-50 max-h-[calc(100dvh-5.5rem)] w-72 max-w-[calc(100vw-1.5rem)] overflow-y-auto rounded-md border border-ink/20 bg-paper p-3 text-ink shadow-xl max-sm:fixed max-sm:left-3 max-sm:right-3 max-sm:top-16 max-sm:w-auto max-sm:max-w-none"
                                    on:pointerdown=move |ev: PointerEvent| ev.stop_propagation()
                                    on:click=move |ev: MouseEvent| ev.stop_propagation()
                                >
                                    <div class="flex items-center justify-between gap-2 border-b border-ink-soft/15 pb-2">
                                        <span class="font-handwriting text-xl">"spaces"</span>
                                        <button
                                            type="button"
                                            on:click=create_space
                                            class="rounded-[3px] bg-marker px-2 py-1 text-xs font-medium hover:brightness-95 focus:outline-none focus:ring-2 focus:ring-ink/30"
                                        >
                                            "+ new space"
                                        </button>
                                    </div>
                            <div class="mt-2 space-y-1">
                                {move || spaces
                                            .get()
                                            .into_iter()
                                            .filter(|space| !space.archived && space.deleted_at.is_none())
                                            .map(|space| {
                                                let is_active = space.id == active_space_id.get();
                                                view! {
                                                    <button
                                                        type="button"
                                                        role="menuitem"
                                                        on:contextmenu=move |ev: MouseEvent| {
                                                            ev.prevent_default();
                                                            ev.stop_propagation();
                                                            context_menu.set(Some(ContextMenuState {
                                                                target: ContextMenuTarget::Space(space.id),
                                                                x: ev.client_x(),
                                                                y: ev.client_y(),
                                                            }));
                                                        }
                                                        on:click=move |ev: MouseEvent| {
                                                            ev.stop_propagation();
                                                            space_actions.switch(space.id);
                                                        }
                                                        class=if is_active {
                                                            "flex w-full items-center justify-between rounded-[3px] bg-note-yellow px-2 py-1.5 text-left text-sm text-note-ink-yellow focus:outline-none focus:ring-2 focus:ring-ink/30"
                                                        } else {
                                                            "flex w-full items-center justify-between rounded-[3px] px-2 py-1.5 text-left text-sm hover:bg-paper-shelf focus:outline-none focus:ring-2 focus:ring-ink/30"
                                                        }
                                                    >
                                                        <span class="truncate">{space.name}</span>
                                                        <span class="ml-2 shrink-0 text-[10px] opacity-60">
                                                            {format!("{}", space.board.notes.len())}
                                                        </span>
                                                    </button>
                                                }
                                            })
                                            .collect_view()}
                                    </div>
                                    <div class="mt-3 border-t border-ink-soft/15 pt-2">
                                        {move || if rename_space_id.get() == Some(active_space_id.get()) {
                                            view! {
                                                <div class="space-y-2">
                                                    <input
                                                        prop:value=rename_value.get_untracked()
                                                        maxlength="48"
                                                        autofocus=true
                                                        aria-label="Space name"
                                                        class="w-full rounded-[3px] border border-ink-soft/25 bg-blank px-2 py-1.5 text-sm outline-none focus:border-ink focus:ring-2 focus:ring-ink/30"
                                                        on:input=move |ev: Event| {
                                                            if let Some(input) = ev.target().and_then(|target| target.dyn_into::<HtmlInputElement>().ok()) {
                                                                rename_value.set(input.value());
                                                            }
                                                        }
                                                        on:keydown=move |ev: KeyboardEvent| {
                                                            if ev.key() == "Enter" {
                                                                ev.prevent_default();
                                                                space_actions.save_name(rename_space_id, rename_value);
                                                            } else if ev.key() == "Escape" {
                                                                ev.prevent_default();
                                                                space_actions.cancel_name(rename_space_id);
                                                            }
                                                        }
                                                    />
                                                    <div class="flex justify-end gap-1">
                                                        <button
                                                            type="button"
                                                            on:click=move |_| space_actions.cancel_name(rename_space_id)
                                                            class="rounded-[3px] px-2 py-1.5 text-xs text-ink-soft hover:bg-paper-shelf hover:text-ink focus:outline-none focus:ring-2 focus:ring-ink/30"
                                                        >
                                                            "cancel"
                                                        </button>
                                                        <button
                                                            type="button"
                                                            on:click=move |_| space_actions.save_name(rename_space_id, rename_value)
                                                            class="rounded-[3px] bg-note-green px-2 py-1.5 text-xs text-note-ink-green hover:brightness-95 focus:outline-none focus:ring-2 focus:ring-ink/30"
                                                        >
                                                            "save"
                                                        </button>
                                                    </div>
                                                </div>
                                            }
                                            .into_any()
                                        } else {
                                            view! {
                                                <div class="flex items-center gap-1">
                                                    <button
                                                        type="button"
                                                        on:click=begin_rename_space
                                                        class="flex-1 rounded-[3px] px-2 py-1.5 text-left text-xs text-ink-soft hover:bg-paper-shelf hover:text-ink focus:outline-none focus:ring-2 focus:ring-ink/30"
                                                    >
                                                        "rename space"
                                                    </button>
                                                    <button
                                                        type="button"
                                                        on:click=archive_current_space
                                                        class="rounded-[3px] px-2 py-1.5 text-xs text-ink-soft hover:bg-note-pink hover:text-note-ink-pink focus:outline-none focus:ring-2 focus:ring-ink/30"
                                                    >
                                                        "archive"
                                                    </button>
                                                </div>
                                            }
                                            .into_any()
                                        }}
                                    </div>
                                    {move || {
                                        let archived = spaces
                                            .get()
                                            .into_iter()
                                            .filter(|space| space.archived)
                                            .collect::<Vec<_>>();
                                        if archived.is_empty() {
                                            ().into_any()
                                        } else {
                                            view! {
                                                <div class="mt-3 border-t border-ink-soft/15 pt-2">
                                                    <span class="font-handwriting text-lg text-ink-soft">"archived"</span>
                                                    <div class="mt-1 space-y-1">
                                                        {archived.into_iter().map(|space| {
                                                            view! {
                                                            <div
                                                                class="flex min-w-0 items-center gap-1 text-xs"
                                                                on:contextmenu=move |ev: MouseEvent| {
                                                                    ev.prevent_default();
                                                                    ev.stop_propagation();
                                                                    context_menu.set(Some(ContextMenuState {
                                                                        target: ContextMenuTarget::Space(space.id),
                                                                        x: ev.client_x(),
                                                                        y: ev.client_y(),
                                                                    }));
                                                                }
                                                            >
                                                                    <span class="min-w-0 flex-1 truncate text-ink-soft">{space.name}</span>
                                                                    <button
                                                                        type="button"
                                                                        on:click=move |_| space_actions.restore(space.id)
                                                                        class="rounded-[3px] px-1.5 py-1 text-ink-soft hover:bg-note-green hover:text-note-ink-green focus:outline-none focus:ring-2 focus:ring-ink/30"
                                                                    >
                                                                        "restore"
                                                                    </button>
                                                                    {if pending_delete_space.get() == Some(space.id) {
                                                                        view! {
                                                                            <button
                                                                                type="button"
                                                                                on:click=move |_| space_actions.confirm_delete()
                                                                                class="rounded-[3px] bg-note-pink px-1.5 py-1 text-note-ink-pink focus:outline-none focus:ring-2 focus:ring-note-ink-pink/40"
                                                                            >
                                                                                "delete forever"
                                                                            </button>
                                                                        }.into_any()
                                                                    } else {
                                                                        view! {
                                                                            <button
                                                                                type="button"
                                                                                on:click=move |_| space_actions.request_delete(space.id)
                                                                                class="rounded-[3px] px-1.5 py-1 text-ink-soft hover:bg-note-pink hover:text-note-ink-pink focus:outline-none focus:ring-2 focus:ring-ink/30"
                                                                            >
                                                                                "delete"
                                                                            </button>
                                                                        }.into_any()
                                                                    }}
                                                                </div>
                                                            }
                                                        }).collect_view()}
                                                    </div>
                                                </div>
                                            }.into_any()
                                        }
                                    }}
                                </div>
                            }
                            .into_any()
                        } else {
                            ().into_any()
                        }}
                    </div>
                    <span class="hidden shrink-0 text-xs text-ink-soft sm:block">
                        {move || format!("{} {}", notes.get().len(), if notes.get().len() == 1 { "note" } else { "notes" })}
                    </span>
                </div>

                <div class="pointer-events-auto flex min-w-0 max-w-full shrink-0 items-center gap-1 overflow-x-auto whitespace-nowrap rounded-md border border-ink-soft/15 bg-paper/90 p-1 shadow-md backdrop-blur-sm sm:gap-2 sm:p-1.5">
                    <button
                        type="button"
                        on:click=undo
                        disabled=move || history.get().undo.is_empty()
                        class="rounded-[3px] px-2 py-2 text-sm text-ink-soft hover:bg-white/70 hover:text-ink disabled:cursor-not-allowed disabled:opacity-35 focus:outline-none focus:ring-2 focus:ring-ink/30"
                        aria-label="Undo"
                        title="Undo"
                    >
                        "undo"
                    </button>
                    <button
                        type="button"
                        on:click=redo
                        disabled=move || history.get().redo.is_empty()
                        class="rounded-[3px] px-2 py-2 text-sm text-ink-soft hover:bg-white/70 hover:text-ink disabled:cursor-not-allowed disabled:opacity-35 focus:outline-none focus:ring-2 focus:ring-ink/30"
                        aria-label="Redo"
                        title="Redo"
                    >
                        "redo"
                    </button>
                    <span class="mx-1 hidden h-5 w-px bg-ink-soft/20 sm:block"></span>
                    <button
                        type="button"
                        on:click=group_selected
                        disabled=move || selection.get().len() < 2
                        class="rounded-[3px] px-2 py-2 text-sm text-ink-soft hover:bg-white/70 hover:text-ink disabled:cursor-not-allowed disabled:opacity-35 focus:outline-none focus:ring-2 focus:ring-ink/30"
                        title="Group selected notes"
                    >
                        "group"
                    </button>
                    <button
                        type="button"
                        on:click=ungroup_selected
                        disabled=move || {
                            !selection.get().iter().any(|id| {
                                notes.get().iter().any(|note| note.id == *id && note.group_id.is_some())
                            })
                        }
                        class="rounded-[3px] px-2 py-2 text-sm text-ink-soft hover:bg-white/70 hover:text-ink disabled:cursor-not-allowed disabled:opacity-35 focus:outline-none focus:ring-2 focus:ring-ink/30"
                        title="Remove selected notes from their group"
                    >
                        "ungroup"
                    </button>
                    <button
                        type="button"
                        on:click=delete_selected
                        disabled=move || selection.get().is_empty()
                        class="rounded-[3px] px-2 py-2 text-sm text-ink-soft hover:bg-white/70 hover:text-ink disabled:cursor-not-allowed disabled:opacity-35 focus:outline-none focus:ring-2 focus:ring-ink/30"
                        aria-label="Delete selected notes"
                        title="Delete selected notes (Delete or Backspace)"
                    >
                        "delete"
                    </button>
                    {move || if groups.get().is_empty() {
                        ().into_any()
                    } else {
                        view! {
                            <select
                                aria-label="Add selected to group"
                                on:change=add_to_group
                                disabled=move || selection.get().is_empty()
                                class="max-w-28 rounded-[3px] bg-paper-shelf px-2 py-2 text-sm text-ink-soft outline-none hover:text-ink disabled:cursor-not-allowed disabled:opacity-35 focus:ring-2 focus:ring-ink/30"
                            >
                                <option value="">"add to…"</option>
                                {move || groups.get().into_iter().map(|group| view! {
                                    <option value=group.id.to_string()>{group.label}</option>
                                }).collect_view()}
                            </select>
                        }.into_any()
                    }}
                    <button
                        type="button"
                        on:click=add_note
                        class="rounded-[3px] bg-marker px-3 py-2 text-sm font-medium shadow-sm hover:brightness-95 focus:outline-none focus:ring-2 focus:ring-ink/40"
                    >
                        "+ new note"
                    </button>
                    <button
                        type="button"
                        on:click=move |_| {
                            if storage_status.get_untracked() == StorageStatus::Error
                                && storage_repair_required()
                            {
                                export_local_storage_backup(restore_message);
                            } else {
                                export_workspace(&workspace_with_current_board(spaces.get_untracked(), active_space_id.get_untracked(), &notes.get_untracked(), &groups.get_untracked(), &workspace_tombstones.get_untracked()));
                            }
                        }
                        class="rounded-[3px] px-2 py-2 text-sm text-ink-soft hover:bg-white/70 hover:text-ink focus:outline-none focus:ring-2 focus:ring-ink/30 sm:px-3"
                        title=move || if storage_status.get() == StorageStatus::Error
                            && storage_repair_required()
                        {
                            "Export the raw local records before repair"
                        } else {
                            "Export the current workspace"
                        }
                    >
                        {move || if storage_status.get() == StorageStatus::Error
                            && storage_repair_required()
                        {
                            "export local backup"
                        } else {
                            "export"
                        }}
                    </button>
                    <label class="cursor-pointer rounded-[3px] px-2 py-2 text-sm text-ink-soft hover:bg-white/70 hover:text-ink focus-within:ring-2 focus-within:ring-ink/30 sm:px-3">
                        "restore workspace"
                        <input type="file" accept="application/json,.json" class="sr-only" on:change=restore_file/>
                    </label>
                    <span class="hidden h-5 w-px bg-ink-soft/20 sm:block"></span>
                    {move || match account_state.get() {
                        AccountState::SignedIn(entitlement) => {
                            let has_dodo_customer = entitlement.provider.as_deref() == Some("dodo")
                                && entitlement.provider_customer_id.is_some();
                            view! {
                            <span class=if entitlement.can_sync_at(now_millis() / 1_000) {
                                "rounded-[3px] bg-note-green/70 px-2 py-2 text-xs text-note-ink-green"
                            } else {
                                "rounded-[3px] bg-note-yellow/80 px-2 py-2 text-xs text-note-ink-yellow"
                            }>
                                {if entitlement.can_sync_at(now_millis() / 1_000) { "pro" } else { "account" }}
                            </span>
                            {if !entitlement.can_sync_at(now_millis() / 1_000) {
                                view! {
                                    <a href="#account-gate" class="rounded-[3px] bg-marker px-2 py-2 text-sm font-medium text-ink hover:brightness-95 focus:outline-none focus:ring-2 focus:ring-ink/40">
                                        "upgrade"
                                    </a>
                                }.into_any()
                            } else if has_dodo_customer {
                                view! {
                                    <button
                                        type="button"
                                        disabled=move || checkout_pending.get()
                                        on:click=begin_billing_portal
                                        class="rounded-[3px] px-2 py-2 text-sm text-ink-soft hover:bg-white/70 hover:text-ink disabled:cursor-wait disabled:opacity-60 focus:outline-none focus:ring-2 focus:ring-ink/30"
                                    >
                                        {move || if checkout_pending.get() { "opening billing…" } else { "manage billing" }}
                                    </button>
                                }.into_any()
                            } else {
                                ().into_any()
                            }}
                            {move || checkout_error.get().map(|message| view! {
                                <span class="max-w-56 rounded-[3px] bg-note-pink/70 px-2 py-2 text-xs text-note-ink-pink">{message}</span>
                            })}
                            <button type="button" on:click=move |_| { stop_authenticated_sync(); spawn_local(sign_out()); } class="rounded-[3px] px-2 py-2 text-sm text-ink-soft hover:bg-white/70 hover:text-ink focus:outline-none focus:ring-2 focus:ring-ink/30">
                                "sign out"
                            </button>
                            }.into_any()
                        },
                        AccountState::Checking => view! {
                            <span class="px-2 py-2 text-xs text-ink-soft">"checking account…"</span>
                        }.into_any(),
                        AccountState::Guest => view! {
                            <a href="/signin" class="rounded-[3px] px-2 py-2 text-sm text-ink-soft hover:bg-white/70 hover:text-ink focus:outline-none focus:ring-2 focus:ring-ink/30">
                                "sign in"
                            </a>
                            <a href="/signup" class="rounded-[3px] bg-marker px-3 py-2 text-sm font-medium text-ink hover:brightness-95 focus:outline-none focus:ring-2 focus:ring-ink/40">
                                "sign up"
                            </a>
                        }.into_any(),
                        AccountState::Expired => view! {
                            <a href="/signin" class="rounded-[3px] bg-marker px-3 py-2 text-sm font-medium text-ink hover:brightness-95 focus:outline-none focus:ring-2 focus:ring-ink/40">
                                "sign in again"
                            </a>
                            <button type="button" on:click=move |_| spawn_local(sign_out()) class="rounded-[3px] px-2 py-2 text-sm text-ink-soft hover:bg-white/70 hover:text-ink focus:outline-none focus:ring-2 focus:ring-ink/30">
                                "use local version"
                            </button>
                        }.into_any(),
                        AccountState::Unavailable => view! {
                            <span class="px-2 py-2 text-xs text-ink-soft">"account check unavailable"</span>
                        }.into_any(),
                    }}
                </div>
            </header>

            <div class="pointer-events-auto absolute bottom-12 left-3 z-10 flex items-center gap-1 rounded-md border border-ink-soft/15 bg-paper/90 p-1 shadow-md backdrop-blur-sm sm:bottom-5 sm:left-5">
                <button
                    type="button"
                    on:click=zoom_out
                    class="rounded-[3px] px-2 py-1 text-lg leading-none text-ink-soft hover:bg-white/70 hover:text-ink focus:outline-none focus:ring-2 focus:ring-ink/30"
                    aria-label="Zoom out"
                >"−"</button>
                <button
                    type="button"
                    on:click=reset_view
                    class="min-w-14 rounded-[3px] px-1.5 py-1 text-xs text-ink-soft hover:bg-white/70 hover:text-ink focus:outline-none focus:ring-2 focus:ring-ink/30"
                    title="Reset view"
                >
                    {move || format!("{}%", (zoom.get() * 100.0).round() as i32)}
                </button>
                <button
                    type="button"
                    on:click=zoom_in
                    class="rounded-[3px] px-2 py-1 text-lg leading-none text-ink-soft hover:bg-white/70 hover:text-ink focus:outline-none focus:ring-2 focus:ring-ink/30"
                    aria-label="Zoom in"
                >"+"</button>
            </div>

            <div class="pointer-events-none absolute inset-x-3 bottom-3 z-10 flex items-end justify-between gap-3 text-xs text-ink-soft sm:inset-x-5 sm:bottom-5">
                <span class="rounded-[3px] border border-ink-soft/15 bg-paper/85 px-2.5 py-1.5 shadow-sm backdrop-blur-sm">
                    "shift-drag to select · shift-click to add · drag empty space to pan"
                </span>
                <div class="flex min-h-7 items-center gap-2">
                    {move || restore_message.get().map(|message| view! {
                        <span class="rounded-[3px] bg-note-green px-2.5 py-1.5 text-note-ink-green shadow-sm">{message}</span>
                    })}
                    <span
                        class=move || match storage_status.get() {
                            StorageStatus::Error => "rounded-[3px] border border-note-ink-pink/30 bg-note-pink px-2.5 py-1.5 text-note-ink-pink shadow-sm backdrop-blur-sm",
                            _ => "rounded-[3px] border border-ink-soft/15 bg-paper/85 px-2.5 py-1.5 shadow-sm backdrop-blur-sm",
                        }
                    >
                        {move || match storage_status.get() {
                            StorageStatus::Saved => "saved on this device",
                            StorageStatus::Saving => "saving locally…",
                            StorageStatus::Error if storage_repair_required() => {
                                "local data needs repair — export/restore"
                            }
                            StorageStatus::Error => "couldn't save locally",
                        }}
                    </span>
                    <button
                        type="button"
                        disabled=move || !sync_started.get()
                        on:click=move |_| {
                            sync_status.set(SyncStatus::Syncing);
                            spawn_local(async move {
                                let refreshed = load_account_state().await;
                                account_state.set(refreshed);
                                schedule_sync_drain();
                            });
                        }
                        class="rounded-[3px] border border-ink-soft/15 bg-paper/85 px-2.5 py-1.5 shadow-sm backdrop-blur-sm hover:bg-white/70 disabled:cursor-not-allowed disabled:opacity-50"
                        title="Retry synchronization now"
                    >
                        "sync now"
                    </button>
                    <span class=move || match sync_status.get() {
                        SyncStatus::AuthPaused | SyncStatus::BillingPaused | SyncStatus::Error =>
                            "rounded-[3px] border border-note-ink-yellow/30 bg-note-yellow/70 px-2.5 py-1.5 text-note-ink-yellow shadow-sm backdrop-blur-sm",
                        _ => "rounded-[3px] border border-ink-soft/15 bg-paper/85 px-2.5 py-1.5 shadow-sm backdrop-blur-sm",
                    }>
                        {move || sync_status.get().label()}
                    </span>
                    {move || (pending_sync_count.get() > 0).then(|| view! {
                        <span
                            class="rounded-[3px] border border-note-ink-yellow/30 bg-note-yellow/60 px-2.5 py-1.5 text-note-ink-yellow shadow-sm"
                            title=move || last_server_ack_at.get().map_or_else(
                                || "no server acknowledgement yet".to_owned(),
                                |timestamp| format!("last server acknowledgement: {timestamp}"),
                            )
                        >
                            {move || format!("{} pending", pending_sync_count.get())}
                        </span>
                    })}
                    <button
                        type="button"
                        on:click=move |_| show_sync_diagnostics.update(|open| *open = !*open)
                        class="rounded-[3px] border border-ink-soft/15 bg-paper/85 px-2.5 py-1.5 shadow-sm backdrop-blur-sm hover:bg-white/70 focus:outline-none focus:ring-2 focus:ring-ink/30"
                        title="Show safe synchronization diagnostics"
                    >
                        "details"
                    </button>
                </div>
            </div>

            {move || show_sync_diagnostics.get().then(|| view! {
                <section class="pointer-events-auto absolute bottom-14 right-3 z-20 w-[min(28rem,calc(100vw-1.5rem))] rounded-[3px] border border-ink-soft/20 bg-paper/95 p-3 text-xs text-ink shadow-xl backdrop-blur-sm sm:bottom-16 sm:right-5">
                    <div class="flex items-center justify-between gap-3">
                        <p class="font-medium uppercase tracking-[0.12em] text-ink-soft">"sync diagnostics"</p>
                        <span class="text-ink-soft">{format!("schema {}", CURRENT_SCHEMA_VERSION)}</span>
                    </div>
                    <dl class="mt-2 grid grid-cols-[auto_1fr] gap-x-3 gap-y-1">
                        <dt class="text-ink-soft">"principal"</dt>
                        <dd class="truncate">{current_sync_principal()}</dd>
                        <dt class="text-ink-soft">"device"</dt>
                        <dd class="truncate">{load_device_id()}</dd>
                        <dt class="text-ink-soft">"tab"</dt>
                        <dd class="truncate">{current_tab_id()}</dd>
                        <dt class="text-ink-soft">"active space"</dt>
                        <dd>{active_space_id.get()}</dd>
                        <dt class="text-ink-soft">"browser online"</dt>
                        <dd>{if web_sys::window().is_some_and(|window| window.navigator().on_line()) { "yes" } else { "no" }}</dd>
                        <dt class="text-ink-soft">"coordinator"</dt>
                        <dd>{if is_sync_lease_owner(&current_sync_principal()) { "yes" } else { "no" }}</dd>
                        <dt class="text-ink-soft">"fence"</dt>
                        <dd>{format!("{} / epoch {}", sync_lease_mode(&current_sync_principal()), sync_lease_epoch(&current_sync_principal()) as u64)}</dd>
                        <dt class="text-ink-soft">"server cursor"</dt>
                        <dd>{current_sync_cursor(&current_sync_principal())}</dd>
                        <dt class="text-ink-soft">"IndexedDB"</dt>
                        <dd>{local_sync_diagnostics.get().map_or_else(|| "unknown".to_owned(), |value| format!("v{}", value.db_version))}</dd>
                        <dt class="text-ink-soft">"local / ack generation"</dt>
                        <dd>{local_sync_diagnostics.get().map_or_else(|| "unknown".to_owned(), |value| format!("{} / {}", value.local_generation, value.acknowledged_generation))}</dd>
                        <dt class="text-ink-soft">"active outbox"</dt>
                        <dd>{local_sync_diagnostics.get().map_or_else(|| "unknown".to_owned(), |value| format!("{} entries · {} bytes", value.outbox_count, value.outbox_bytes))}</dd>
                        <dt class="text-ink-soft">"metadata queue / inbox"</dt>
                        <dd>{local_sync_diagnostics.get().map_or_else(|| "unknown".to_owned(), |value| format!("{} / {}", value.metadata_count, value.inbox_count))}</dd>
                        <dt class="text-ink-soft">"oldest pending"</dt>
                        <dd>{local_sync_diagnostics.get().and_then(|value| value.oldest_pending_at).map_or_else(|| "none".to_owned(), |value| format!("{} ms ago", now_millis().saturating_sub(value)))}</dd>
                        <dt class="text-ink-soft">"SSE"</dt>
                        <dd>{if transport_sync_diagnostics.get().connected { "connected" } else { "disconnected / reconnecting" }}</dd>
                        <dt class="text-ink-soft">"SSE connections / reconnects"</dt>
                        <dd>{move || { let value = transport_sync_diagnostics.get(); format!("{} / {}", value.connections, value.reconnects) }}</dd>
                        <dt class="text-ink-soft">"SSE resets / last event"</dt>
                        <dd>{move || { let value = transport_sync_diagnostics.get(); format!("{} / {}", value.resets, value.last_event_id.unwrap_or_else(|| "none".to_owned())) }}</dd>
                        <dt class="text-ink-soft">"SSE last event time"</dt>
                        <dd>{transport_sync_diagnostics.get().last_event_at.map_or_else(|| "never".to_owned(), |timestamp| timestamp.to_string())}</dd>
                        <dt class="text-ink-soft">"SSE open / error"</dt>
                        <dd>{move || { let value = transport_sync_diagnostics.get(); format!("{} / {}", value.last_open_at.map_or_else(|| "never".to_owned(), |timestamp| timestamp.to_string()), value.last_error_at.map_or_else(|| "never".to_owned(), |timestamp| timestamp.to_string())) }}</dd>
                        <dt class="text-ink-soft">"durable record error"</dt>
                        <dd class="truncate">{local_sync_diagnostics.get().and_then(|value| value.last_error).unwrap_or_else(|| "none".to_owned())}</dd>
                        <dt class="text-ink-soft">"pending"</dt>
                        <dd>{pending_sync_count.get()}</dd>
                        <dt class="text-ink-soft">"status"</dt>
                        <dd>{sync_status.get().label()}</dd>
                        <dt class="text-ink-soft">"last ack"</dt>
                        <dd>{last_server_ack_at.get().map_or_else(|| "never".to_owned(), |value| value.to_string())}</dd>
                        <dt class="text-ink-soft">"last request"</dt>
                        <dd class="truncate">{last_request_id.get().unwrap_or_else(|| "none".to_owned())}</dd>
                        <dt class="text-ink-soft">"last local sync error"</dt>
                        <dd class="truncate">{last_sync_error.get().unwrap_or_else(|| "none".to_owned())}</dd>
                    </dl>
                    <button
                        type="button"
                        on:click=move |_| export_sync_diagnostics(
                            active_space_id.get_untracked(),
                            sync_status.get_untracked(),
                            pending_sync_count.get_untracked(),
                            last_server_ack_at.get_untracked(),
                            last_request_id.get_untracked(),
                            last_sync_error.get_untracked(),
                            local_sync_diagnostics.get_untracked(),
                            transport_sync_diagnostics.get_untracked(),
                            restore_message,
                        )
                        class="mt-3 rounded-[3px] border border-ink-soft/15 bg-paper-shelf px-2.5 py-1.5 text-xs text-ink-soft hover:bg-white/70 hover:text-ink focus:outline-none focus:ring-2 focus:ring-ink/30"
                    >
                        "export diagnostics"
                    </button>
                    <p class="mt-3 border-t border-ink-soft/10 pt-2 text-ink-soft">"Safe diagnostics only; document contents and credentials are never shown."</p>
                </section>
            })}

            {move || match account_state.get() {
                AccountState::SignedIn(entitlement) if !entitlement.can_sync_at(now_millis() / 1_000) => {
                    let has_dodo_customer = entitlement.provider.as_deref() == Some("dodo")
                        && entitlement.provider_customer_id.is_some();
                    let status_message = match entitlement.status {
                        task_core::billing::SubscriptionStatus::Free =>
                            "your account is ready, but sync is waiting for Pro",
                        task_core::billing::SubscriptionStatus::Pending =>
                            "your payment is still processing; sync will start after confirmation",
                        task_core::billing::SubscriptionStatus::PastDue =>
                            "your payment needs attention before sync can continue",
                        task_core::billing::SubscriptionStatus::Canceled
                        | task_core::billing::SubscriptionStatus::Ended =>
                            "your Pro subscription is not active right now",
                        task_core::billing::SubscriptionStatus::Active =>
                            "this account is not enabled for sync yet",
                    };
                    // Billing suspension must pause cloud sync, not local
                    // editing. Keep the notice compact and pointer-scoped so
                    // the board and its durable outbox remain usable.
                    view! {
                        <div id="account-notice" class="pointer-events-none absolute inset-x-3 top-20 z-[40] flex justify-end p-2 sm:right-5 sm:top-24">
                            <section class="pointer-events-auto w-full max-w-md rotate-[-0.5deg] rounded-[3px] border border-note-ink-yellow/30 bg-note-yellow p-4 text-note-ink-yellow shadow-xl">
                                <p class="text-xs uppercase tracking-[0.16em] opacity-70">"signed in · local editing continues"</p>
                                <p class="mt-2 text-sm leading-relaxed">{status_message}. Changes remain saved locally and will be eligible for sync after access is restored.</p>
                                <div class="mt-3 flex flex-wrap gap-2">
                                    <button type="button" disabled=move || checkout_pending.get() on:click=move |_| begin_checkout("month") class="rounded-[3px] bg-note-ink-yellow px-3 py-2 text-sm font-medium text-note-yellow hover:brightness-110 disabled:cursor-wait disabled:opacity-60 focus:outline-none focus:ring-2 focus:ring-note-ink-yellow/50">
                                        {move || if checkout_pending.get() { "opening checkout…" } else { "Pro · $2 / month" }}
                                    </button>
                                    <button type="button" disabled=move || checkout_pending.get() on:click=move |_| begin_checkout("year") class="rounded-[3px] border border-note-ink-yellow/40 px-3 py-2 text-sm font-medium hover:bg-note-yellow/50 disabled:cursor-wait disabled:opacity-60 focus:outline-none focus:ring-2 focus:ring-note-ink-yellow/50">
                                        {move || if checkout_pending.get() { "opening checkout…" } else { "Pro · $20 / year" }}
                                    </button>
                                    {if has_dodo_customer {
                                        view! {
                                            <button
                                                type="button"
                                                disabled=move || checkout_pending.get()
                                                on:click=begin_billing_portal
                                                class="rounded-[3px] border border-note-ink-yellow/30 px-3 py-2 text-sm hover:bg-note-yellow/50 disabled:cursor-wait disabled:opacity-60 focus:outline-none focus:ring-2 focus:ring-note-ink-yellow/50"
                                            >
                                                {move || if checkout_pending.get() { "opening billing…" } else { "manage billing" }}
                                            </button>
                                        }.into_any()
                                    } else {
                                        ().into_any()
                                    }}
                                    <button type="button" on:click=move |_| spawn_local(sign_out()) class="rounded-[3px] border border-note-ink-yellow/30 px-3 py-2 text-sm hover:bg-note-yellow/50 focus:outline-none focus:ring-2 focus:ring-note-ink-yellow/50">
                                        "sign out"
                                    </button>
                                </div>
                                {match checkout_return_state() {
                                    CheckoutReturnState::Pending => view! {
                                        <p class="mt-3 rounded-[3px] border border-note-ink-yellow/30 bg-note-yellow/50 px-3 py-2 text-xs">
                                            "payment returned successfully; waiting for account confirmation…"
                                        </p>
                                    }.into_any(),
                                    CheckoutReturnState::Failed => view! {
                                        <p class="mt-3 rounded-[3px] bg-note-pink/70 px-3 py-2 text-xs text-note-ink-pink">
                                            "payment was not completed. Try again or choose another supported payment method."
                                        </p>
                                    }.into_any(),
                                    CheckoutReturnState::None => ().into_any(),
                                }}
                                {move || checkout_error.get().map(|message| view! {
                                    <p class="mt-3 rounded-[3px] bg-note-pink/70 px-3 py-2 text-xs text-note-ink-pink">{message}</p>
                                })}
                            </section>
                        </div>
                    }.into_any()
                }
                AccountState::Unavailable => view! {
                    <div class="pointer-events-auto absolute inset-0 z-[80] grid place-items-center bg-paper/75 p-6 backdrop-blur-[2px]">
                        <section class="w-full max-w-md rotate-[-0.5deg] rounded-[3px] border border-ink-soft/20 bg-paper-shelf p-6 text-ink shadow-xl">
                            <p class="text-xs uppercase tracking-[0.16em] text-ink-soft">"account check unavailable"</p>
                            <h2 class="mt-2 font-handwriting text-4xl">"let's check before opening the desk"</h2>
                            <p class="mt-2 text-sm leading-relaxed text-ink-soft">"We couldn't confirm whether this session is signed in. The board is paused until we know whether it is a local guest board or a subscribed account."</p>
                            <button type="button" on:click=retry_account_check class="mt-5 w-full rounded-[3px] bg-marker px-3 py-2 text-sm font-medium text-ink hover:brightness-95 focus:outline-none focus:ring-2 focus:ring-ink/40">
                                "check again"
                            </button>
                            <button type="button" on:click=move |_| spawn_local(sign_out()) class="mt-2 w-full rounded-[3px] border border-ink-soft/20 px-3 py-2 text-sm text-ink-soft hover:bg-paper hover:text-ink focus:outline-none focus:ring-2 focus:ring-ink/30">
                                "sign out and use local version"
                            </button>
                        </section>
                    </div>
                }.into_any(),
                AccountState::Expired => view! {
                    <div class="pointer-events-none absolute inset-x-3 top-20 z-[40] flex justify-end p-2 sm:right-5 sm:top-24">
                        <section class="pointer-events-auto w-full max-w-md rotate-[-0.5deg] rounded-[3px] border border-note-ink-yellow/30 bg-note-yellow p-4 text-note-ink-yellow shadow-xl">
                            <p class="text-xs uppercase tracking-[0.16em] opacity-70">"session expired · local editing continues"</p>
                            <p class="mt-2 text-sm leading-relaxed">"Your local workspace and pending changes are safe. Sign in again to resume cloud sync."</p>
                            <div class="mt-3 flex flex-wrap gap-2">
                                <a href="/signin" class="rounded-[3px] bg-note-ink-yellow px-3 py-2 text-sm font-medium text-note-yellow hover:brightness-110 focus:outline-none focus:ring-2 focus:ring-note-ink-yellow/50">
                                    "sign in again"
                                </a>
                                <button type="button" on:click=move |_| spawn_local(sign_out()) class="rounded-[3px] border border-note-ink-yellow/30 px-3 py-2 text-sm hover:bg-note-yellow/50 focus:outline-none focus:ring-2 focus:ring-note-ink-yellow/50">
                                    "use local version"
                                </button>
                            </div>
                        </section>
                    </div>
                }.into_any(),
                _ => ().into_any(),
            }}
        </main>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexed_db_outbox_mutation_id_uses_the_javascript_wire_name() {
        let updates: Vec<QueuedCrdtUpdate> = serde_json::from_str(
            r#"[{"principal":"account:test","spaceId":7,"mutationId":"mutation-1","update":"encoded"}]"#,
        )
        .expect("IndexedDB outbox row should deserialize");

        assert_eq!(updates[0].mutation_id, "mutation-1");
    }

    #[test]
    fn remote_projection_does_not_hide_a_local_edit_before_the_effect_runs() {
        let canonical = BoardData::default();
        let projected = BoardData {
            notes: vec![Note {
                text: "local edit".into(),
                ..Default::default()
            }],
            ..BoardData::default()
        };

        assert!(!should_queue_crdt_projection(
            true,
            Some(&canonical),
            &canonical
        ));
        assert!(should_queue_crdt_projection(
            true,
            Some(&canonical),
            &projected
        ));
        assert!(should_queue_crdt_projection(
            false,
            Some(&canonical),
            &canonical
        ));
        assert!(should_queue_crdt_projection(true, None, &projected));
    }

    #[test]
    fn guest_adoption_rekeys_compatibility_ids_but_preserves_stable_identity() {
        let space_stable_id = legacy_entity_stable_id("space", 7);
        let note_stable_id = legacy_entity_stable_id("note", 10);
        let group_stable_id = legacy_entity_stable_id("group", 20);
        let deleted_note_stable_id = legacy_entity_stable_id("note", 11);
        let workspace = WorkspaceData {
            spaces: vec![Space {
                id: 7,
                stable_id: space_stable_id.clone(),
                name: "guest space".into(),
                board: BoardData {
                    notes: vec![Note {
                        id: 10,
                        text: "adopt me".into(),
                        stable_id: note_stable_id.clone(),
                        group_id: Some(20),
                        group_stable_id: Some(group_stable_id.clone()),
                        ..Default::default()
                    }],
                    groups: vec![Group {
                        id: 20,
                        label: "adopted group".into(),
                        stable_id: group_stable_id.clone(),
                        ..Default::default()
                    }],
                    tombstones: vec![Tombstone {
                        kind: TombstoneKind::Note,
                        id: 11,
                        stable_id: deleted_note_stable_id.clone(),
                        deleted_at: 123,
                    }],
                    ..BoardData::default()
                },
                ..Default::default()
            }],
            active_space_id: 7,
            schema_version: CURRENT_SCHEMA_VERSION,
            device_id: "guest-device".into(),
            tombstones: vec![Tombstone {
                kind: TombstoneKind::Space,
                id: 7,
                stable_id: space_stable_id.clone(),
                deleted_at: 456,
            }],
        };

        let adopted = rekey_workspace_for_account(workspace);
        let space = &adopted.spaces[0];
        assert_ne!(space.id, 7);
        assert_eq!(space.stable_id, space_stable_id);
        assert_eq!(adopted.active_space_id, space.id);
        assert_eq!(adopted.tombstones[0].id, space.id);
        assert_eq!(adopted.tombstones[0].stable_id, space_stable_id);

        let note = &space.board.notes[0];
        let group = &space.board.groups[0];
        assert_ne!(note.id, 10);
        assert_ne!(group.id, 20);
        assert_eq!(note.stable_id, note_stable_id);
        assert_eq!(note.group_id, Some(group.id));
        assert_eq!(
            note.group_stable_id.as_deref(),
            Some(group_stable_id.as_str())
        );
        assert_eq!(group.stable_id, group_stable_id);
        assert_ne!(space.board.tombstones[0].id, 11);
        assert_eq!(space.board.tombstones[0].stable_id, deleted_note_stable_id);
    }

    #[test]
    fn calendar_handles_month_lengths_and_year_boundaries() {
        assert_eq!(days_in_month(2028, 2), 29);
        assert_eq!(days_in_month(2027, 2), 28);
        assert_eq!(first_weekday(2026, 9), 2);
        assert_eq!(calendar_days(2026, 9).len(), 32);
        assert_eq!(shift_month(2026, 1, -1), (2025, 12));
        assert_eq!(shift_month(2026, 12, 1), (2027, 1));
    }

    #[test]
    fn legacy_done_notes_load_as_completed() {
        let board = parse_board(
            r#"{"notes":[{"id":1,"text":"ship it","color":"Yellow","done":true,"x":0.0,"y":0.0,"rotation":0}],"groups":[]}"#,
        )
        .expect("legacy board should load");

        assert_eq!(board.notes[0].status, NoteStatus::Done);
        assert_eq!(board.notes[0].due_date, None);
    }

    #[test]
    fn status_and_due_date_round_trip() {
        let board = BoardData {
            notes: vec![Note {
                id: 1,
                text: "follow up".into(),
                color: NoteColor::Pink,
                status: NoteStatus::InProgress,
                due_date: Some("2026-09-02".into()),
                x: 0.0,
                y: 0.0,
                rotation: 0,
                group_id: None,
                ..Default::default()
            }],
            groups: Vec::new(),
            ..Default::default()
        };
        let raw = serde_json::to_string(&board).expect("board should serialize");
        let restored = parse_board(&raw).expect("board should deserialize");

        assert_eq!(restored.notes[0].status, NoteStatus::InProgress);
        assert_eq!(restored.notes[0].due_date.as_deref(), Some("2026-09-02"));
    }

    #[test]
    fn spaces_round_trip_with_name_archive_state_and_board() {
        let workspace = WorkspaceData {
            spaces: vec![Space {
                id: 7,
                stable_id: legacy_entity_stable_id("space", 7),
                name: "research".into(),
                metadata_version: 0,
                archived: true,
                board: BoardData {
                    notes: vec![Note {
                        id: 4,
                        text: "read paper".into(),
                        color: NoteColor::Blue,
                        status: NoteStatus::Todo,
                        due_date: None,
                        x: 12.0,
                        y: 24.0,
                        rotation: -1,
                        group_id: None,
                        ..Default::default()
                    }],
                    groups: Vec::new(),
                    ..Default::default()
                },
                ..Default::default()
            }],
            active_space_id: 7,
            schema_version: CURRENT_SCHEMA_VERSION,
            device_id: "test-device".into(),
            tombstones: Vec::new(),
        };

        let raw = serde_json::to_string(&workspace).expect("workspace should serialize");
        let restored: WorkspaceData =
            serde_json::from_str(&raw).expect("workspace should deserialize");

        assert_eq!(restored, workspace);
    }

    #[test]
    fn moving_a_note_to_a_group_places_it_inside_without_using_its_old_position() {
        let group = Group {
            id: 1,
            label: "Work".into(),
            origin: Some((0.0, 0.0)),
            size: Some((488.0, 276.0)),
            ..Default::default()
        };
        let notes = vec![
            Note {
                id: 1,
                text: "already here".into(),
                color: NoteColor::Yellow,
                status: NoteStatus::Todo,
                due_date: None,
                x: 24.0,
                y: 52.0,
                rotation: 0,
                group_id: Some(1),
                ..Default::default()
            },
            Note {
                id: 2,
                text: "move me".into(),
                color: NoteColor::Blue,
                status: NoteStatus::Todo,
                due_date: None,
                x: 900.0,
                y: 900.0,
                rotation: 0,
                group_id: None,
                ..Default::default()
            },
        ];

        let positions = positions_for_group(&group, &notes, &[2]);

        assert_eq!(positions, vec![(2, 256.0, 52.0)]);
    }

    #[test]
    fn moving_a_note_that_is_already_in_the_group_does_not_reposition_it() {
        let group = Group {
            id: 1,
            label: "Work".into(),
            origin: Some((0.0, 0.0)),
            size: Some((256.0, 276.0)),
            ..Default::default()
        };
        let notes = vec![Note {
            id: 1,
            text: "stay here".into(),
            color: NoteColor::Yellow,
            status: NoteStatus::Todo,
            due_date: None,
            x: 24.0,
            y: 52.0,
            rotation: 0,
            group_id: Some(1),
            ..Default::default()
        }];

        assert!(positions_for_group(&group, &notes, &[1]).is_empty());
    }

    #[test]
    fn space_names_are_trimmed_and_have_a_safe_fallback() {
        assert_eq!(normalize_space_name("  personal  "), "personal");
        assert_eq!(normalize_space_name("   "), "untitled space");
        assert_eq!(normalize_space_name(&"x".repeat(60)).len(), 48);
    }

    #[test]
    fn anchored_group_frame_moves_with_its_cards() {
        let group = Group {
            id: 1,
            label: "Work".into(),
            origin: Some((100.0, 120.0)),
            size: None,
            ..Default::default()
        };
        let notes = vec![
            Note {
                id: 1,
                text: "inside".into(),
                color: NoteColor::Yellow,
                status: NoteStatus::Todo,
                due_date: None,
                x: 124.0,
                y: 172.0,
                rotation: 0,
                group_id: Some(1),
                ..Default::default()
            },
            Note {
                id: 2,
                text: "outside".into(),
                color: NoteColor::Blue,
                status: NoteStatus::Todo,
                due_date: None,
                x: 0.0,
                y: 0.0,
                rotation: 0,
                group_id: None,
                ..Default::default()
            },
        ];

        let before = group_bounds(&group, &notes).expect("group should have a frame");
        let moved_group = Group {
            origin: Some((220.0, 200.0)),
            ..group
        };
        let moved_notes = vec![Note {
            x: 244.0,
            y: 252.0,
            ..notes[0].clone()
        }];
        let after = group_bounds(&moved_group, &moved_notes).expect("group should have a frame");

        assert_eq!(before.2, after.2);
        assert_eq!(before.3, after.3);
        assert_eq!(after.0 - before.0, 120.0);
        assert_eq!(after.1 - before.1, 80.0);
    }

    #[test]
    fn every_resize_corner_keeps_the_opposite_corner_fixed() {
        let initial = (100.0, 120.0, 400.0, 400.0);

        assert_eq!(
            resized_group_frame(initial, (-40.0, -30.0), (-1, -1)),
            (60.0, 90.0, 440.0, 430.0)
        );
        assert_eq!(
            resized_group_frame(initial, (40.0, -30.0), (1, -1)),
            (100.0, 90.0, 440.0, 430.0)
        );
        assert_eq!(
            resized_group_frame(initial, (-40.0, 30.0), (-1, 1)),
            (60.0, 120.0, 440.0, 430.0)
        );
        assert_eq!(
            resized_group_frame(initial, (40.0, 30.0), (1, 1)),
            (100.0, 120.0, 440.0, 430.0)
        );
        assert_eq!(
            resized_group_frame(initial, (500.0, 500.0), (-1, -1)),
            (244.0, 244.0, 256.0, 276.0)
        );
    }

    #[test]
    fn resizing_cannot_leave_grouped_cards_outside_the_frame() {
        let initial = (100.0, 120.0, 424.0, 424.0);
        let cards = (124.0, 172.0, 500.0, 520.0);

        assert_eq!(
            constrain_group_frame_to_cards(
                resized_group_frame(initial, (500.0, 500.0), (-1, -1)),
                initial,
                (-1, -1),
                cards,
            ),
            initial
        );
        assert_eq!(
            constrain_group_frame_to_cards(
                resized_group_frame(initial, (-500.0, -500.0), (1, 1)),
                initial,
                (1, 1),
                cards,
            ),
            initial
        );
    }
}
