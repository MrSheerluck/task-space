use std::collections::HashMap;

use super::account::{AccountState, load_account_state, sign_out, start_checkout};
use super::api::api_url;
use gloo_net::http::Request;
use js_sys::Array;
use leptos::ev::{Event, KeyboardEvent, MouseEvent, PointerEvent, WheelEvent};
use leptos::leptos_dom::helpers::window_event_listener;
use leptos::prelude::*;
use leptos::task::spawn_local;
use serde::{Deserialize, Serialize};
use task_core::billing::Entitlement;
use task_core::crdt::SpaceDoc;
use task_core::sync::EncodedUpdate;
use task_core::sync::{
    SYNC_PROTOCOL_VERSION, SyncEvent, SyncPullRequest, SyncPullResponse, SyncPushRequest,
};
use task_core::{
    BoardData, CURRENT_SCHEMA_VERSION, Group, Note, NoteColor, NoteStatus, Space, Tombstone,
    TombstoneKind, WorkspaceData,
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
const DEVICE_ID_STORAGE_KEY: &str = "task-space.device-id.v1";
const SYNC_SEQUENCE_STORAGE_KEY: &str = "task-space.sync-sequence.v1";
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

#[wasm_bindgen(inline_js = r#"
export function taskSpaceLoadWorkspace() {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 3);
    request.onupgradeneeded = () => {
      if (!request.result.objectStoreNames.contains("workspace")) {
        request.result.createObjectStore("workspace");
      }
      if (!request.result.objectStoreNames.contains("crdt")) {
        request.result.createObjectStore("crdt");
      }
      if (!request.result.objectStoreNames.contains("crdt-updates")) {
        request.result.createObjectStore("crdt-updates");
      }
    };
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
    request.onsuccess = () => {
      const db = request.result;
      const storeName = Array.from(db.objectStoreNames).includes("workspace")
        ? "workspace"
        : db.objectStoreNames[0];
      if (!storeName) {
        resolve(null);
        return;
      }
      const transaction = db.transaction(storeName, "readonly");
      const store = transaction.objectStore(storeName);
      const read = store.get("current");
      read.onerror = () => reject(read.error || new Error("Could not read workspace"));
      read.onsuccess = () => {
        if (read.result != null) {
          resolve(read.result);
          return;
        }
        const all = store.getAll();
        all.onerror = () => reject(all.error || new Error("Could not list workspace records"));
        all.onsuccess = () => resolve(all.result ?? null);
      };
    };
  });
}

export function taskSpaceSaveWorkspace(raw) {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 3);
    request.onupgradeneeded = () => {
      if (!request.result.objectStoreNames.contains("workspace")) {
        request.result.createObjectStore("workspace");
      }
      if (!request.result.objectStoreNames.contains("crdt")) {
        request.result.createObjectStore("crdt");
      }
      if (!request.result.objectStoreNames.contains("crdt-updates")) {
        request.result.createObjectStore("crdt-updates");
      }
    };
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
    request.onsuccess = () => {
      const db = request.result;
      const transaction = db.transaction("workspace", "readwrite");
      transaction.objectStore("workspace").put(raw, "current");
      transaction.onerror = () => reject(transaction.error || new Error("Could not save workspace"));
      transaction.oncomplete = () => resolve(true);
    };
  });
}

export function taskSpaceLoadCrdt(spaceId) {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 3);
    request.onupgradeneeded = () => {
      if (!request.result.objectStoreNames.contains("workspace")) {
        request.result.createObjectStore("workspace");
      }
      if (!request.result.objectStoreNames.contains("crdt")) {
        request.result.createObjectStore("crdt");
      }
      if (!request.result.objectStoreNames.contains("crdt-updates")) {
        request.result.createObjectStore("crdt-updates");
      }
    };
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
    request.onsuccess = () => {
      const db = request.result;
      const transaction = db.transaction("crdt", "readonly");
      const read = transaction.objectStore("crdt").get(`space:${spaceId}`);
      read.onerror = () => reject(read.error || new Error("Could not read CRDT document"));
      read.onsuccess = () => resolve(read.result ?? null);
    };
  });
}

export function taskSpaceSaveCrdt(spaceId, encodedSnapshot) {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 3);
    request.onupgradeneeded = () => {
      if (!request.result.objectStoreNames.contains("workspace")) {
        request.result.createObjectStore("workspace");
      }
      if (!request.result.objectStoreNames.contains("crdt")) {
        request.result.createObjectStore("crdt");
      }
      if (!request.result.objectStoreNames.contains("crdt-updates")) {
        request.result.createObjectStore("crdt-updates");
      }
    };
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
    request.onsuccess = () => {
      const db = request.result;
      const transaction = db.transaction("crdt", "readwrite");
      transaction.objectStore("crdt").put(encodedSnapshot, `space:${spaceId}`);
      transaction.onerror = () => reject(transaction.error || new Error("Could not save CRDT document"));
      transaction.oncomplete = () => resolve(true);
    };
  });
}

export function taskSpaceQueueCrdtUpdate(spaceId, mutationId, encodedUpdate) {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 3);
    request.onupgradeneeded = () => {
      if (!request.result.objectStoreNames.contains("workspace")) {
        request.result.createObjectStore("workspace");
      }
      if (!request.result.objectStoreNames.contains("crdt")) {
        request.result.createObjectStore("crdt");
      }
      if (!request.result.objectStoreNames.contains("crdt-updates")) {
        request.result.createObjectStore("crdt-updates");
      }
    };
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
    request.onsuccess = () => {
      const db = request.result;
      const transaction = db.transaction("crdt-updates", "readwrite");
      transaction.objectStore("crdt-updates").put(
        { spaceId, mutationId, update: encodedUpdate },
        `${spaceId}:${mutationId}`,
      );
      transaction.onerror = () => reject(transaction.error || new Error("Could not queue CRDT update"));
      transaction.oncomplete = () => resolve(true);
    };
  });
}

export function taskSpaceLoadCrdtUpdates() {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 3);
    request.onsuccess = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains("crdt-updates")) {
        resolve([]);
        return;
      }
      const read = db.transaction("crdt-updates", "readonly").objectStore("crdt-updates").getAll();
      read.onerror = () => reject(read.error || new Error("Could not read CRDT update queue"));
      read.onsuccess = () => resolve(read.result ?? []);
    };
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
  });
}

export function taskSpaceAckCrdtUpdate(spaceId, mutationId) {
  return new Promise((resolve, reject) => {
    if (!globalThis.indexedDB) {
      reject(new Error("IndexedDB is unavailable"));
      return;
    }
    const request = indexedDB.open("task-space", 3);
    request.onsuccess = () => {
      const db = request.result;
      const transaction = db.transaction("crdt-updates", "readwrite");
      transaction.objectStore("crdt-updates").delete(`${spaceId}:${mutationId}`);
      transaction.onerror = () => reject(transaction.error || new Error("Could not acknowledge CRDT update"));
      transaction.oncomplete = () => resolve(true);
    };
    request.onerror = () => reject(request.error || new Error("Could not open IndexedDB"));
  });
}

const taskSpaceSyncSources = new Map();

export function taskSpaceStartSyncEvents(url, onUpdate, onOpen) {
  taskSpaceStopSyncEvents(url);
  const source = new EventSource(url, { withCredentials: true });
  const update = (event) => onUpdate(event.data);
  source.addEventListener("space-update", update);
  source.onopen = () => onOpen();
  taskSpaceSyncSources.set(url, { source, update });
  return true;
}

export function taskSpaceStopSyncEvents(url) {
  const existing = taskSpaceSyncSources.get(url);
  if (!existing) return;
  existing.source.removeEventListener("space-update", existing.update);
  existing.source.close();
  taskSpaceSyncSources.delete(url);
}
"#)]
unsafe extern "C" {
    #[wasm_bindgen(js_name = taskSpaceLoadWorkspace)]
    fn indexed_db_load_workspace() -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceSaveWorkspace)]
    fn indexed_db_save_workspace(raw: &str) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceLoadCrdt)]
    fn indexed_db_load_crdt(space_id: u64) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceSaveCrdt)]
    fn indexed_db_save_crdt(space_id: u64, encoded_snapshot: &str) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceQueueCrdtUpdate)]
    fn indexed_db_queue_crdt_update(
        space_id: u64,
        mutation_id: &str,
        encoded_update: &str,
    ) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceLoadCrdtUpdates)]
    fn indexed_db_load_crdt_updates() -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceAckCrdtUpdate)]
    fn indexed_db_ack_crdt_update(space_id: u64, mutation_id: &str) -> js_sys::Promise;

    #[wasm_bindgen(js_name = taskSpaceStartSyncEvents)]
    fn start_sync_events(
        url: &str,
        on_update: &js_sys::Function,
        on_open: &js_sys::Function,
    ) -> bool;
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
    let workspace = serde_json::from_str::<WorkspaceData>(raw).ok()?;
    (!workspace.spaces.is_empty()).then(|| normalize_workspace(workspace))
}

fn workspace_from_board(board: BoardData) -> WorkspaceData {
    let now = now_millis();
    WorkspaceData {
        schema_version: CURRENT_SCHEMA_VERSION,
        device_id: load_device_id(),
        tombstones: Vec::new(),
        spaces: vec![Space {
            id: 1,
            name: "my space".into(),
            archived: false,
            created_at: now,
            updated_at: now,
            deleted_at: None,
            board,
        }],
        active_space_id: 1,
    }
}

fn parse_indexed_db_value(value: JsValue) -> Option<WorkspaceData> {
    let raw = value.as_string().or_else(|| {
        js_sys::JSON::stringify(&value)
            .ok()
            .and_then(|json| json.as_string())
    })?;
    parse_workspace(&raw)
        .or_else(|| {
            parse_board(&raw)
                .filter(|board| !board.notes.is_empty() || !board.groups.is_empty())
                .map(workspace_from_board)
        })
        .or_else(|| parse_indexed_db_json(&raw))
}

fn parse_indexed_db_json(raw: &str) -> Option<WorkspaceData> {
    let value = serde_json::from_str::<serde_json::Value>(raw).ok()?;
    parse_indexed_db_json_value(value)
}

fn parse_indexed_db_json_value(value: serde_json::Value) -> Option<WorkspaceData> {
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
    let current = storage
        .as_ref()
        .and_then(|storage| storage.get_item(STORAGE_KEY).ok().flatten())
        .and_then(|raw| parse_board(&raw));
    let mut board = current
        .or_else(|| {
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
    js_sys::Date::now().max(0.0) as u64
}

fn load_device_id() -> String {
    let Some(storage) = web_sys::window().and_then(|window| window.local_storage().ok().flatten())
    else {
        return "device-local".into();
    };
    if let Ok(Some(device_id)) = storage.get_item(DEVICE_ID_STORAGE_KEY) {
        return device_id;
    }
    let device_id = format!(
        "device-{}-{}",
        now_millis(),
        (js_sys::Math::random() * 1_000_000_000.0) as u64
    );
    let _ = storage.set_item(DEVICE_ID_STORAGE_KEY, &device_id);
    device_id
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

fn normalize_workspace(mut workspace: WorkspaceData) -> WorkspaceData {
    workspace.schema_version = CURRENT_SCHEMA_VERSION;
    if workspace.device_id.is_empty() {
        workspace.device_id = load_device_id();
    }
    let now = now_millis();
    for space in &mut workspace.spaces {
        if space.created_at == 0 {
            space.created_at = now;
        }
        if space.updated_at == 0 {
            space.updated_at = space.created_at;
        }
        space.board.schema_version = CURRENT_SCHEMA_VERSION;
        for note in &mut space.board.notes {
            if note.created_at == 0 {
                note.created_at = now;
            }
            if note.updated_at == 0 {
                note.updated_at = note.created_at;
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

    if !workspace
        .spaces
        .iter()
        .any(|space| space.id == workspace.active_space_id && !space.archived)
    {
        if let Some(space) = workspace.spaces.iter_mut().find(|space| !space.archived) {
            workspace.active_space_id = space.id;
        } else if let Some(space) = workspace.spaces.first_mut() {
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
) {
    spawn_local(async move {
        if let Ok(value) = JsFuture::from(indexed_db_load_workspace()).await {
            if let Some(imported) = parse_indexed_db_value(value) {
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
                    next_space_id.set(
                        spaces
                            .get_untracked()
                            .iter()
                            .map(|space| space.id)
                            .max()
                            .unwrap_or(0)
                            .saturating_add(1),
                    );
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
        }

        let active_id = active_space_id.get_untracked();
        let current_board = spaces
            .get_untracked()
            .iter()
            .find(|space| space.id == active_id)
            .map(|space| space.board.clone())
            .unwrap_or_else(empty_board);
        let loaded_doc = JsFuture::from(indexed_db_load_crdt(active_id))
            .await
            .ok()
            .and_then(|value| value.as_string())
            .and_then(|encoded| EncodedUpdate::from_base64(encoded).ok())
            .and_then(|encoded| encoded.to_bytes().ok())
            .and_then(|bytes| SpaceDoc::from_update(&bytes).ok());
        let had_loaded_doc = loaded_doc.is_some();
        let doc = loaded_doc.unwrap_or_else(|| {
            let doc = SpaceDoc::new();
            doc.import_board(&current_board);
            doc
        });
        let loaded_board = doc.board();
        if had_loaded_doc {
            notes.set(loaded_board.notes);
            groups.set(loaded_board.groups);
        }
        let encoded_snapshot = EncodedUpdate::from_bytes(&doc.snapshot());
        crdt_docs.update(|items| {
            items.insert(active_id, doc);
        });
        queue_indexed_db_crdt_save(active_id, encoded_snapshot.as_str().to_owned());
        storage_hydrated.set(true);
    });
}

fn load_workspace() -> WorkspaceData {
    let storage = web_sys::window().and_then(|window| window.local_storage().ok().flatten());
    let workspace = storage
        .as_ref()
        .and_then(|storage| {
            storage
                .get_item(LEGACY_WORKSPACE_STORAGE_KEY)
                .ok()
                .flatten()
        })
        .and_then(|raw| serde_json::from_str::<WorkspaceData>(&raw).ok())
        .filter(|workspace| !workspace.spaces.is_empty())
        .unwrap_or_else(|| WorkspaceData {
            schema_version: CURRENT_SCHEMA_VERSION,
            device_id: load_device_id(),
            tombstones: Vec::new(),
            spaces: vec![Space {
                id: 1,
                name: "my space".into(),
                archived: false,
                created_at: now_millis(),
                updated_at: now_millis(),
                deleted_at: None,
                board: load_board(),
            }],
            active_space_id: 1,
        });
    normalize_workspace(workspace)
}

fn save_workspace(
    workspace: &WorkspaceData,
    storage_status: Option<RwSignal<StorageStatus>>,
) -> bool {
    let workspace = normalize_workspace(workspace.clone());
    if let Ok(raw) = serde_json::to_string(&workspace) {
        queue_indexed_db_save(raw, storage_status);
        true
    } else {
        false
    }
}

fn write_workspace_exact(
    workspace: &WorkspaceData,
    storage_status: Option<RwSignal<StorageStatus>>,
) -> bool {
    let workspace = normalize_workspace(workspace.clone());
    let Ok(raw) = serde_json::to_string(&workspace) else {
        return false;
    };
    queue_indexed_db_save(raw, storage_status);
    true
}

fn queue_indexed_db_save(raw: String, storage_status: Option<RwSignal<StorageStatus>>) {
    spawn_local(async move {
        let saved = JsFuture::from(indexed_db_save_workspace(&raw))
            .await
            .is_ok();
        if let Some(storage_status) = storage_status {
            storage_status.set(if saved {
                StorageStatus::Saved
            } else {
                StorageStatus::Error
            });
        }
        if saved {
            if let Some(storage) =
                web_sys::window().and_then(|window| window.local_storage().ok().flatten())
            {
                let _ = storage.remove_item(LEGACY_WORKSPACE_STORAGE_KEY);
            }
        }
    });
}

fn queue_indexed_db_crdt_save(space_id: u64, encoded_snapshot: String) {
    spawn_local(async move {
        let _ = JsFuture::from(indexed_db_save_crdt(space_id, &encoded_snapshot)).await;
    });
}

fn next_sync_mutation_id() -> String {
    let Some(storage) = web_sys::window().and_then(|window| window.local_storage().ok().flatten())
    else {
        return format!("{}:{}", load_device_id(), now_millis());
    };
    let sequence = storage
        .get_item(SYNC_SEQUENCE_STORAGE_KEY)
        .ok()
        .flatten()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
        .saturating_add(1);
    let _ = storage.set_item(SYNC_SEQUENCE_STORAGE_KEY, &sequence.to_string());
    format!("{}:{}", load_device_id(), sequence)
}

fn persist_space_crdt(
    space_id: u64,
    board: &BoardData,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
) {
    crdt_docs.update(|items| {
        items.entry(space_id).or_default();
    });
    let previous_state_vector = crdt_docs
        .get_untracked()
        .get(&space_id)
        .map(SpaceDoc::state_vector)
        .unwrap_or_default();
    crdt_docs.update(|items| {
        let doc = items.entry(space_id).or_default();
        doc.import_board(board);
    });
    let snapshot = crdt_docs
        .get_untracked()
        .get(&space_id)
        .map(SpaceDoc::snapshot)
        .unwrap_or_default();
    queue_indexed_db_crdt_save(
        space_id,
        EncodedUpdate::from_bytes(&snapshot).as_str().to_owned(),
    );

    if let Some(doc) = crdt_docs.get_untracked().get(&space_id) {
        if let Ok(update) = doc.encode_update(&previous_state_vector)
            && !update.is_empty()
        {
            let mutation_id = next_sync_mutation_id();
            let encoded = EncodedUpdate::from_bytes(&update);
            spawn_local(async move {
                let _ = JsFuture::from(indexed_db_queue_crdt_update(
                    space_id,
                    &mutation_id,
                    encoded.as_str(),
                ))
                .await;
            });
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct QueuedCrdtUpdate {
    #[serde(rename = "spaceId")]
    space_id: u64,
    mutation_id: String,
    update: String,
}

#[derive(Serialize)]
struct RegisterSpacePayload<'a> {
    name: &'a str,
}

async fn register_sync_spaces(spaces: RwSignal<Vec<Space>>) {
    for space in spaces.get_untracked() {
        let Ok(builder) = Request::post(&api_url(&format!("/sync/spaces/{}", space.id)))
            .credentials(RequestCredentials::Include)
            .json(&RegisterSpacePayload { name: &space.name })
        else {
            return;
        };
        let Ok(response) = builder.send().await else {
            return;
        };
        if response.status() >= 300 {
            return;
        }
    }
}

async fn merge_remote_spaces(spaces: RwSignal<Vec<Space>>, next_space_id: RwSignal<u64>) {
    let Ok(response) = Request::get(&api_url("/sync/spaces"))
        .credentials(RequestCredentials::Include)
        .send()
        .await
    else {
        return;
    };
    if response.status() >= 300 {
        return;
    }
    let Ok(remote_spaces) = response.json::<Vec<Space>>().await else {
        return;
    };
    let highest_remote_id = remote_spaces.iter().map(|space| space.id).max();
    spaces.update(|local_spaces| {
        for remote in remote_spaces {
            if local_spaces.iter().all(|local| local.id != remote.id) {
                local_spaces.push(Space {
                    id: remote.id,
                    name: remote.name,
                    archived: remote.archived,
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
    if let Some(highest_remote_id) = highest_remote_id {
        next_space_id.update(|next| {
            *next = (*next).max(highest_remote_id.saturating_add(1));
        });
    }
}

fn parse_queued_crdt_updates(value: JsValue) -> Vec<QueuedCrdtUpdate> {
    let Ok(raw) = js_sys::JSON::stringify(&value) else {
        return Vec::new();
    };
    let Some(raw) = raw.as_string() else {
        return Vec::new();
    };
    serde_json::from_str(&raw).unwrap_or_default()
}

async fn drain_sync_queue() {
    let Ok(value) = JsFuture::from(indexed_db_load_crdt_updates()).await else {
        return;
    };
    for queued in parse_queued_crdt_updates(value) {
        let Ok(update) = EncodedUpdate::from_base64(queued.update) else {
            continue;
        };
        let request = SyncPushRequest {
            protocol_version: SYNC_PROTOCOL_VERSION,
            space_id: queued.space_id,
            mutation_id: queued.mutation_id.clone(),
            update,
        };
        let Ok(builder) = Request::post(&api_url("/sync/push"))
            .credentials(RequestCredentials::Include)
            .json(&request)
        else {
            return;
        };
        let Ok(response) = builder.send().await else {
            return;
        };
        if response.status() >= 300 {
            return;
        }
        let _ = JsFuture::from(indexed_db_ack_crdt_update(
            queued.space_id,
            &queued.mutation_id,
        ))
        .await;
    }
}

fn apply_remote_sync_event(
    raw: String,
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
) {
    let Ok(event) = serde_json::from_str::<SyncEvent>(&raw) else {
        return;
    };
    let Ok(update) = event.update.to_bytes() else {
        return;
    };
    let mut next_board = None;
    crdt_docs.update(|items| {
        let doc = items.entry(event.space_id).or_default();
        if doc.apply_update(&update).is_ok() {
            next_board = Some(doc.board());
        }
    });
    let Some(board) = next_board else {
        return;
    };
    spaces.update(|items| {
        if let Some(space) = items.iter_mut().find(|space| space.id == event.space_id) {
            space.board = board.clone();
            space.updated_at = now_millis();
        }
    });
    if active_space_id.get_untracked() == event.space_id {
        notes.set(board.notes);
        groups.set(board.groups);
    }
    save_crdt_doc_snapshot(event.space_id, crdt_docs);
}

fn save_crdt_doc_snapshot(space_id: u64, crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>) {
    let Some(doc) = crdt_docs.get_untracked().get(&space_id).cloned() else {
        return;
    };
    queue_indexed_db_crdt_save(
        space_id,
        EncodedUpdate::from_bytes(&doc.snapshot())
            .as_str()
            .to_owned(),
    );
}

async fn pull_active_space(
    spaces: RwSignal<Vec<Space>>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
) {
    let space_id = active_space_id.get_untracked();
    let Some(doc) = crdt_docs.get_untracked().get(&space_id).cloned() else {
        return;
    };
    let request = SyncPullRequest {
        protocol_version: SYNC_PROTOCOL_VERSION,
        space_id,
        state_vector: EncodedUpdate::from_bytes(&doc.state_vector()),
    };
    let Ok(builder) = Request::post(&api_url("/sync/pull"))
        .credentials(RequestCredentials::Include)
        .json(&request)
    else {
        return;
    };
    let Ok(response) = builder.send().await else {
        return;
    };
    if response.status() >= 300 {
        return;
    }
    let Ok(payload) = response.json::<SyncPullResponse>().await else {
        return;
    };
    let Ok(update) = payload.update.to_bytes() else {
        return;
    };
    if update.is_empty() {
        return;
    }
    let mut next_board = None;
    crdt_docs.update(|items| {
        if let Some(doc) = items.get(&space_id)
            && doc.apply_update(&update).is_ok()
        {
            next_board = Some(doc.board());
        }
    });
    let Some(board) = next_board else {
        return;
    };
    spaces.update(|items| {
        if let Some(space) = items.iter_mut().find(|space| space.id == space_id) {
            space.board = board.clone();
            space.updated_at = now_millis();
        }
    });
    notes.set(board.notes);
    groups.set(board.groups);
    save_crdt_doc_snapshot(space_id, crdt_docs);
}

fn start_authenticated_sync(
    spaces: RwSignal<Vec<Space>>,
    next_space_id: RwSignal<u64>,
    active_space_id: RwSignal<u64>,
    notes: RwSignal<Vec<Note>>,
    groups: RwSignal<Vec<Group>>,
    crdt_docs: RwSignal<HashMap<u64, SpaceDoc>>,
) {
    spawn_local(async move {
        merge_remote_spaces(spaces, next_space_id).await;
        register_sync_spaces(spaces).await;
        drain_sync_queue().await;
        pull_active_space(spaces, active_space_id, notes, groups, crdt_docs).await;

        let update_spaces = spaces;
        let update_active = active_space_id;
        let update_notes = notes;
        let update_groups = groups;
        let update_docs = crdt_docs;
        let on_update = Closure::<dyn FnMut(String)>::new(move |raw| {
            apply_remote_sync_event(
                raw,
                update_spaces,
                update_active,
                update_notes,
                update_groups,
                update_docs,
            );
        });
        let reconnect_spaces = spaces;
        let reconnect_active = active_space_id;
        let reconnect_notes = notes;
        let reconnect_groups = groups;
        let reconnect_docs = crdt_docs;
        let on_open = Closure::<dyn FnMut()>::new(move || {
            spawn_local(pull_active_space(
                reconnect_spaces,
                reconnect_active,
                reconnect_notes,
                reconnect_groups,
                reconnect_docs,
            ));
        });
        let _ = start_sync_events(
            &api_url("/sync/events"),
            on_update.as_ref().unchecked_ref(),
            on_open.as_ref().unchecked_ref(),
        );
        on_update.forget();
        on_open.forget();
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
    save_workspace(
        &workspace_snapshot(
            spaces.get_untracked(),
            active_space_id,
            workspace_tombstones.get_untracked(),
        ),
        Some(storage_status),
    )
}

fn next_note_id(board: &BoardData) -> u64 {
    board
        .notes
        .iter()
        .map(|note| note.id)
        .max()
        .unwrap_or(0)
        .saturating_add(1)
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
    let storage_key = format!("{VIEW_STORAGE_KEY_PREFIX}{space_id}");
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
    let storage_key = format!("{VIEW_STORAGE_KEY_PREFIX}{space_id}");
    if let Ok(raw) = serde_json::to_string(&view) {
        let _ = storage.set_item(&storage_key, &raw);
    }
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
        .find(|space| space.id == space_id && !space.archived)
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
        self.storage_status.set(StorageStatus::Saving);
        let saved = save_workspace(
            &workspace_snapshot(
                self.spaces.get_untracked(),
                active_space_id,
                self.workspace_tombstones.get_untracked(),
            ),
            Some(self.storage_status),
        );
        self.storage_status.set(if saved {
            StorageStatus::Saved
        } else {
            StorageStatus::Error
        });
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
        );
        let space_id = next_space_id.get_untracked();
        next_space_id.update(|next| *next = next.saturating_add(1));
        self.spaces.update(|items| {
            items.push(Space {
                id: space_id,
                name: "new space".into(),
                archived: false,
                created_at: now_millis(),
                updated_at: now_millis(),
                deleted_at: None,
                board: empty_board(),
            });
        });
        self.switch(space_id);
        self.save_workspace(space_id);
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
            .filter(|space| !space.archived)
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
        persist_space_board(
            self.spaces,
            current_id,
            board_snapshot(self.notes, self.groups),
            self.workspace_tombstones,
            self.storage_status,
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
            .find(|space| !space.archived)
            .map(|space| space.id)
        {
            self.switch(next_space_id);
            self.save_workspace(next_space_id);
        }
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
        self.workspace_tombstones.update(|items| {
            items.retain(|tombstone| {
                tombstone.kind != TombstoneKind::Space || tombstone.id != space_id
            });
        });
        self.spaces.update(|items| {
            if let Some(space) = items.iter_mut().find(|space| space.id == space_id) {
                space.archived = false;
                space.updated_at = now_millis();
            }
        });
        self.pending_delete_space.set(None);
        self.save_workspace(self.active_space_id.get_untracked());
    }

    fn save_name(self, rename_space_id: RwSignal<Option<u64>>, rename_value: RwSignal<String>) {
        let Some(id) = rename_space_id.get_untracked() else {
            return;
        };
        let name = normalize_space_name(&rename_value.get_untracked());
        self.spaces.update(|items| {
            if let Some(space) = items.iter_mut().find(|space| space.id == id) {
                space.name = name;
                space.updated_at = now_millis();
            }
        });
        self.save_workspace(self.active_space_id.get_untracked());
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
        if space_id == current_id
            && self
                .spaces
                .get_untracked()
                .iter()
                .filter(|space| !space.archived)
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
                    deleted_at: now_millis(),
                });
            }
        });
        self.spaces
            .update(|items| items.retain(|space| space.id != space_id));
        if space_id == current_id {
            if let Some(next_space_id) = self
                .spaces
                .get_untracked()
                .into_iter()
                .find(|space| !space.archived)
                .map(|space| space.id)
            {
                self.switch(next_space_id);
                self.save_workspace(next_space_id);
            }
        } else {
            self.save_workspace(current_id);
        }
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
    next_id: RwSignal<u64>,
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
        let id = self.next_id.get_untracked();
        self.next_id.update(|next| *next += 1);
        mutate_notes(self.notes, self.groups, self.history, |items| {
            let (x, y) =
                viewport_note_position(self.pan.get_untracked(), self.zoom.get_untracked());
            items.push(Note {
                id,
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
    let moving_ids = moving_ids.iter().copied().collect::<Vec<_>>();
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
    let group_id = groups
        .get_untracked()
        .iter()
        .map(|group| group.id)
        .max()
        .unwrap_or(0)
        + 1;
    groups.update(|items| {
        items.push(Group {
            id: group_id,
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
                if note.group_id == Some(id) {
                    if let Some(original) = snapshot.notes.iter().find(|item| item.id == note.id) {
                        note.x = original.x + delta.0;
                        note.y = original.y + delta.1;
                    }
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
        if !due_calendar_open.get_untracked() {
            if let Some((year, month, _)) = note_snapshot(notes, id)
                .and_then(|note| note.due_date)
                .and_then(|date| parse_due_date(&date))
            {
                calendar_month.set((year, month));
            }
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
        if let Some(offset) = drag_offset.get_untracked() {
            if let Some((x, y)) = board_position(
                f64::from(ev.client_x()),
                f64::from(ev.client_y()),
                offset,
                pan.get_untracked(),
                zoom.get_untracked(),
            ) {
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
                        {
                            if moved_ids.contains(&note.id) {
                                note.x = original.x + delta.0;
                                note.y = original.y + delta.1;
                            }
                        }
                    }
                });
            }
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
                        on:click=move |ev: MouseEvent| {
                            ev.stop_propagation();
                            commit_pending_edit(notes, groups, history, editing, edit_snapshot);
                        }
                        class="absolute bottom-2 left-3 text-xs font-sans underline underline-offset-2"
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
    let sync_started = RwSignal::new(false);
    let account_state = RwSignal::new(AccountState::Checking);
    let sync_entitled = RwSignal::new(false);
    let checkout_error = RwSignal::new(None::<String>);
    let crdt_docs = RwSignal::new(HashMap::<u64, SpaceDoc>::new());
    let next_space_id = RwSignal::new(
        spaces
            .get_untracked()
            .iter()
            .map(|space| space.id)
            .max()
            .unwrap_or(0)
            .saturating_add(1),
    );
    let next_id = RwSignal::new(initial_next_id);

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
    );

    spawn_local(async move {
        account_state.set(load_account_state().await);
    });

    Effect::new(move |_| {
        if storage_hydrated.get()
            && account_state
                .get()
                .entitlement()
                .is_some_and(Entitlement::can_sync)
        {
            sync_entitled.set(true);
        }
    });

    Effect::new(move |_| {
        if storage_hydrated.get() && sync_entitled.get() && !sync_started.get() {
            sync_started.set(true);
            start_authenticated_sync(
                spaces,
                next_space_id,
                active_space_id,
                notes,
                groups,
                crdt_docs,
            );
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
            crdt_docs,
        ));
    });

    let board_actions = BoardActions {
        notes,
        groups,
        history,
        selection,
        next_id,
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
        spawn_local(async move {
            match start_checkout(interval).await {
                Ok(url) => {
                    if let Some(window) = web_sys::window() {
                        let _ = window.location().set_href(&url);
                    }
                }
                Err(message) => checkout_error.set(Some(message)),
            }
        });
    };

    let retry_account_check = move |_| {
        spawn_local(async move {
            account_state.set(load_account_state().await);
        });
    };

    Effect::new(move |_| {
        if !storage_hydrated.get() {
            return;
        }
        let active_id = active_space_id.get();
        storage_status.set(StorageStatus::Saving);
        let board = BoardData {
            schema_version: CURRENT_SCHEMA_VERSION,
            notes: notes.get(),
            groups: groups.get(),
            tombstones: Vec::new(),
        };
        let saved = persist_space_board(
            spaces,
            active_id,
            board,
            workspace_tombstones,
            storage_status,
        );
        if saved
            && let Some(board) = spaces
                .get_untracked()
                .iter()
                .find(|space| space.id == active_id)
                .map(|space| space.board.clone())
        {
            persist_space_crdt(active_id, &board, crdt_docs);
        }
        storage_status.set(if saved {
            StorageStatus::Saved
        } else {
            StorageStatus::Error
        });
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
                storage_status.set(if saved {
                    StorageStatus::Saved
                } else {
                    StorageStatus::Error
                });
                restore_message.set(Some("workspace restored".into()));
            } else if let Some(restored) = parse_board(&raw) {
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
        if matches!(account_state.get_untracked(), AccountState::SignedIn(entitlement) if !entitlement.can_sync())
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
                                    on:click=add_note
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
                                <button type="button" role="menuitem" on:click=move |_| { board_actions.clear_due_date(id); context_menu.set(None); } disabled=move || !note_snapshot(notes, id).is_some_and(|note| note.due_date.is_some()) class="context-menu-item">"clear due date"</button>
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
                                            .filter(|space| !space.archived)
                                            .map(|space| {
                                                let space_actions = space_actions;
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
                                                            let space_actions = space_actions;
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
                            export_workspace(&workspace_with_current_board(spaces.get_untracked(), active_space_id.get_untracked(), &notes.get_untracked(), &groups.get_untracked(), &workspace_tombstones.get_untracked()))
                        }
                        class="rounded-[3px] px-2 py-2 text-sm text-ink-soft hover:bg-white/70 hover:text-ink focus:outline-none focus:ring-2 focus:ring-ink/30 sm:px-3"
                    >
                        "export"
                    </button>
                    <label class="cursor-pointer rounded-[3px] px-2 py-2 text-sm text-ink-soft hover:bg-white/70 hover:text-ink focus-within:ring-2 focus-within:ring-ink/30 sm:px-3">
                        "restore workspace"
                        <input type="file" accept="application/json,.json" class="sr-only" on:change=restore_file/>
                    </label>
                    <span class="hidden h-5 w-px bg-ink-soft/20 sm:block"></span>
                    {move || match account_state.get() {
                        AccountState::SignedIn(entitlement) => view! {
                            <span class=if entitlement.can_sync() {
                                "rounded-[3px] bg-note-green/70 px-2 py-2 text-xs text-note-ink-green"
                            } else {
                                "rounded-[3px] bg-note-yellow/80 px-2 py-2 text-xs text-note-ink-yellow"
                            }>
                                {if entitlement.can_sync() { "pro" } else { "account" }}
                            </span>
                            {if !entitlement.can_sync() {
                                view! {
                                    <a href="#account-gate" class="rounded-[3px] bg-marker px-2 py-2 text-sm font-medium text-ink hover:brightness-95 focus:outline-none focus:ring-2 focus:ring-ink/40">
                                        "upgrade"
                                    </a>
                                }.into_any()
                            } else {
                                ().into_any()
                            }}
                            <button type="button" on:click=move |_| spawn_local(sign_out()) class="rounded-[3px] px-2 py-2 text-sm text-ink-soft hover:bg-white/70 hover:text-ink focus:outline-none focus:ring-2 focus:ring-ink/30">
                                "sign out"
                            </button>
                        }.into_any(),
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
                            StorageStatus::Error => "couldn't save locally",
                        }}
                    </span>
                </div>
            </div>

            {move || match account_state.get() {
                AccountState::SignedIn(entitlement) if !entitlement.can_sync() => {
                    let status_message = match entitlement.status {
                        task_core::billing::SubscriptionStatus::Free =>
                            "your account is ready, but sync is waiting for Pro",
                        task_core::billing::SubscriptionStatus::PastDue =>
                            "your payment needs attention before sync can continue",
                        task_core::billing::SubscriptionStatus::Canceled
                        | task_core::billing::SubscriptionStatus::Ended =>
                            "your Pro subscription is not active right now",
                        task_core::billing::SubscriptionStatus::Active =>
                            "this account is not enabled for sync yet",
                    };
                    view! {
                        <div id="account-gate" class="pointer-events-auto absolute inset-0 z-[80] grid place-items-center bg-paper/75 p-6 backdrop-blur-[2px]">
                            <section class="w-full max-w-md rotate-[-0.5deg] rounded-[3px] border border-note-ink-yellow/30 bg-note-yellow p-6 text-note-ink-yellow shadow-xl">
                                <p class="text-xs uppercase tracking-[0.16em] opacity-70">"signed in · local board paused"</p>
                                <h2 class="mt-2 font-handwriting text-4xl">"one small step before sync"</h2>
                                <p class="mt-2 text-sm leading-relaxed">{status_message}. Choose a plan to unlock this account, or sign out to keep using this board locally on this device.</p>
                                <div class="mt-5 grid grid-cols-2 gap-2">
                                    <button type="button" on:click=move |_| begin_checkout("month") class="rounded-[3px] bg-note-ink-yellow px-3 py-2 text-sm font-medium text-note-yellow hover:brightness-110 focus:outline-none focus:ring-2 focus:ring-note-ink-yellow/50">
                                        "Pro · $2 / month"
                                    </button>
                                    <button type="button" on:click=move |_| begin_checkout("year") class="rounded-[3px] border border-note-ink-yellow/40 px-3 py-2 text-sm font-medium hover:bg-note-yellow/50 focus:outline-none focus:ring-2 focus:ring-note-ink-yellow/50">
                                        "Pro · $20 / year"
                                    </button>
                                </div>
                                {move || checkout_error.get().map(|message| view! {
                                    <p class="mt-3 rounded-[3px] bg-note-pink/70 px-3 py-2 text-xs text-note-ink-pink">{message}</p>
                                })}
                                <button type="button" on:click=move |_| spawn_local(sign_out()) class="mt-4 w-full rounded-[3px] border border-note-ink-yellow/30 px-3 py-2 text-sm hover:bg-note-yellow/50 focus:outline-none focus:ring-2 focus:ring-note-ink-yellow/50">
                                    "sign out and use local version"
                                </button>
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
                _ => ().into_any(),
            }}
        </main>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                name: "research".into(),
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
