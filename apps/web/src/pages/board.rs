use js_sys::Array;
use leptos::ev::{Event, KeyboardEvent, MouseEvent, PointerEvent, WheelEvent};
use leptos::leptos_dom::helpers::window_event_listener;
use leptos::prelude::*;
use serde::{Deserialize, Serialize};
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use web_sys::{
    Blob, Element, FileReader, HtmlAnchorElement, HtmlInputElement, HtmlSelectElement,
    HtmlTextAreaElement, Url,
};

const STORAGE_KEY: &str = "task-space.board.v2";
const LEGACY_STORAGE_KEY: &str = "task-space.board.v1";
const VIEW_STORAGE_KEY: &str = "task-space.view.v1";
const MAX_HISTORY: usize = 100;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct Note {
    id: u64,
    text: String,
    color: NoteColor,
    done: bool,
    x: f64,
    y: f64,
    rotation: i8,
    #[serde(default)]
    group_id: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct Group {
    id: u64,
    label: String,
    #[serde(default)]
    origin: Option<(f64, f64)>,
    #[serde(default)]
    size: Option<(f64, f64)>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct BoardData {
    notes: Vec<Note>,
    #[serde(default)]
    groups: Vec<Group>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct ViewState {
    pan: (f64, f64),
    zoom: f64,
}

#[derive(Clone, Default)]
struct History {
    undo: Vec<BoardData>,
    redo: Vec<BoardData>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
enum NoteColor {
    Yellow,
    Pink,
    Blue,
    Green,
    Lavender,
}

impl NoteColor {
    fn background(self) -> &'static str {
        match self {
            Self::Yellow => "var(--color-note-yellow)",
            Self::Pink => "var(--color-note-pink)",
            Self::Blue => "var(--color-note-blue)",
            Self::Green => "var(--color-note-green)",
            Self::Lavender => "var(--color-note-lav)",
        }
    }

    fn ink(self) -> &'static str {
        match self {
            Self::Yellow => "var(--color-note-ink-yellow)",
            Self::Pink => "var(--color-note-ink-pink)",
            Self::Blue => "var(--color-note-ink-blue)",
            Self::Green => "var(--color-note-ink-green)",
            Self::Lavender => "var(--color-note-ink-lav)",
        }
    }

    fn next(self) -> Self {
        match self {
            Self::Yellow => Self::Pink,
            Self::Pink => Self::Blue,
            Self::Blue => Self::Green,
            Self::Green => Self::Lavender,
            Self::Lavender => Self::Yellow,
        }
    }
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
    (-pan.0 / zoom - 88.0 / zoom, -pan.1 / zoom - 80.0 / zoom)
}

fn load_board() -> BoardData {
    let storage = web_sys::window().and_then(|window| window.local_storage().ok().flatten());
    let current = storage
        .as_ref()
        .and_then(|storage| storage.get_item(STORAGE_KEY).ok().flatten())
        .and_then(|raw| {
            serde_json::from_str::<BoardData>(&raw).ok().or_else(|| {
                serde_json::from_str::<Vec<Note>>(&raw)
                    .ok()
                    .map(|notes| BoardData {
                        notes,
                        groups: Vec::new(),
                    })
            })
        });
    let mut board = current
        .or_else(|| {
            storage
                .as_ref()
                .and_then(|storage| storage.get_item(LEGACY_STORAGE_KEY).ok().flatten())
                .and_then(|raw| serde_json::from_str::<Vec<Note>>(&raw).ok())
                .map(|mut notes| {
                    // v1 stored positions as percentages. Put those notes around
                    // the new canvas origin during the one-time migration.
                    for note in &mut notes {
                        note.x = note.x * 10.0 - 500.0;
                        note.y = note.y * 8.0 - 400.0;
                    }
                    BoardData {
                        notes,
                        groups: Vec::new(),
                    }
                })
        })
        .unwrap_or(BoardData {
            notes: Vec::new(),
            groups: Vec::new(),
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

fn save_board(board: &BoardData) {
    let Some(storage) = web_sys::window().and_then(|window| window.local_storage().ok().flatten())
    else {
        return;
    };
    if let Ok(raw) = serde_json::to_string(board) {
        let _ = storage.set_item(STORAGE_KEY, &raw);
    }
}

fn board_snapshot(notes: RwSignal<Vec<Note>>, groups: RwSignal<Vec<Group>>) -> BoardData {
    BoardData {
        notes: notes.get_untracked(),
        groups: groups.get_untracked(),
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

fn load_view() -> ViewState {
    let view = web_sys::window()
        .and_then(|window| window.local_storage().ok().flatten())
        .and_then(|storage| storage.get_item(VIEW_STORAGE_KEY).ok().flatten())
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

fn save_view(view: ViewState) {
    let Some(storage) = web_sys::window().and_then(|window| window.local_storage().ok().flatten())
    else {
        return;
    };
    if let Ok(raw) = serde_json::to_string(&view) {
        let _ = storage.set_item(VIEW_STORAGE_KEY, &raw);
    }
}

fn export_board(notes: &[Note], groups: &[Group]) {
    let Ok(raw) = serde_json::to_string_pretty(&BoardData {
        notes: notes.to_vec(),
        groups: groups.to_vec(),
    }) else {
        return;
    };
    let parts = Array::new();
    parts.push(&JsValue::from_str(&raw));
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
    anchor.set_download("task-space-board.json");
    anchor.click();
    let _ = Url::revoke_object_url(&url);
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
    const HORIZONTAL_PADDING: f64 = 24.0;
    const TOP_PADDING: f64 = 52.0;

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

fn group_bounds(group: &Group, notes: &[Note]) -> Option<(f64, f64, f64, f64)> {
    group_bounds_excluding(group, notes, &[])
}

fn group_bounds_excluding(
    group: &Group,
    notes: &[Note],
    excluded_ids: &[u64],
) -> Option<(f64, f64, f64, f64)> {
    const NOTE_WIDTH: f64 = 176.0;
    const NOTE_HEIGHT: f64 = 200.0;
    const HORIZONTAL_PADDING: f64 = 24.0;
    const TOP_PADDING: f64 = 52.0;
    const BOTTOM_PADDING: f64 = 24.0;

    let members = notes
        .iter()
        .filter(|note| note.group_id == Some(group.id) && !excluded_ids.contains(&note.id));
    let mut bounds = None;
    for note in members {
        let entry =
            bounds.get_or_insert((note.x, note.y, note.x + NOTE_WIDTH, note.y + NOTE_HEIGHT));
        entry.0 = entry.0.min(note.x);
        entry.1 = entry.1.min(note.y);
        entry.2 = entry.2.max(note.x + NOTE_WIDTH);
        entry.3 = entry.3.max(note.y + NOTE_HEIGHT);
    }
    bounds.map(|(left, top, right, bottom)| {
        let (frame_left, frame_top) = group
            .origin
            .unwrap_or((left - HORIZONTAL_PADDING, top - TOP_PADDING));
        let auto_width =
            (right - frame_left + HORIZONTAL_PADDING).max(NOTE_WIDTH + HORIZONTAL_PADDING * 2.0);
        let auto_height =
            (bottom - frame_top + BOTTOM_PADDING).max(NOTE_HEIGHT + TOP_PADDING + BOTTOM_PADDING);
        let (width, height) = group
            .size
            .map_or((auto_width, auto_height), |(width, height)| {
                (auto_width.max(width), auto_height.max(height))
            });
        (frame_left, frame_top, width, height)
    })
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
            let note_right = note_left + 176.0 * zoom;
            let note_bottom = note_top + 200.0 * zoom;
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
    notes.update(|items| {
        for note in items {
            if selected_ids.contains(&note.id) {
                note.group_id = Some(group_id);
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

#[component]
fn GroupFrame(
    id: u64,
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
    group_resize_initial: RwSignal<Option<(f64, f64)>>,
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
        let Some((_, _, width, height)) = group_bounds(&group, &notes.get_untracked()) else {
            return;
        };
        let _ = handle.set_pointer_capture(ev.pointer_id());
        group_resizing.set(Some(id));
        group_resize_start.set(Some((f64::from(ev.client_x()), f64::from(ev.client_y()))));
        group_resize_initial.set(Some((width, height)));
        group_resize_snapshot.set(Some(board_snapshot(notes, groups)));
    };
    let move_group_resize = move |ev: PointerEvent| {
        if group_resizing.get_untracked() != Some(id) {
            return;
        }
        ev.stop_propagation();
        ev.prevent_default();
        let (Some(start), Some(initial)) = (
            group_resize_start.get_untracked(),
            group_resize_initial.get_untracked(),
        ) else {
            return;
        };
        let zoom = zoom.get_untracked().max(0.01);
        let next_size = (
            (initial.0 + (f64::from(ev.client_x()) - start.0) / zoom).max(224.0),
            (initial.1 + (f64::from(ev.client_y()) - start.1) / zoom).max(276.0),
        );
        groups.update(|items| {
            if let Some(group) = items.iter_mut().find(|group| group.id == id) {
                group.size = Some(next_size);
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
        group_resize_start.set(None);
        group_resizing.set(None);
    };
    view! {
        <div
            class="pointer-events-auto absolute rounded-md border-2 border-dashed border-ink-soft/35 bg-marker/10"
            on:pointerdown=start_group_drag
            on:pointermove=move_group_drag
            on:pointerup=finish_group_drag
            on:pointercancel=finish_group_drag
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
                class="pointer-events-auto absolute bottom-[-7px] right-[-7px] h-4 w-4 cursor-nwse-resize rounded-sm border-2 border-paper-shelf bg-ink-soft/60 shadow-sm hover:bg-ink"
                aria-label="Resize group"
                title="Drag to resize group"
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
                            class="pointer-events-auto absolute -top-4 left-3 w-44 rounded-[3px] border border-ink-soft/25 bg-marker px-2 py-1 font-handwriting text-lg leading-none text-ink outline-none focus:ring-2 focus:ring-ink/30"
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
    let toggle_done = move |ev: MouseEvent| {
        ev.stop_propagation();
        commit_pending_edit(notes, groups, history, editing, edit_snapshot);
        mutate_notes(notes, groups, history, |items| {
            if let Some(note) = items.iter_mut().find(|note| note.id == id) {
                note.done = !note.done;
            }
        });
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
                .map(|note| (note.x + 88.0, note.y + 100.0))
                .and_then(|center| {
                    group_at_point(
                        &groups.get_untracked(),
                        &notes.get_untracked(),
                        center,
                        &moved_ids,
                        Some(&before),
                    )
                });
            notes.update(|items| {
                for note in items {
                    if moved_ids.contains(&note.id) {
                        note.group_id = drop_group;
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

    view! {
        <article
            class=move || format!(
                "task-space-note absolute w-44 min-h-40 p-3 pb-9 rounded-[3px] shadow-lg select-none touch-none transition-[transform,box-shadow] duration-100 {} {}",
                if note_snapshot(notes, id).is_some_and(|note| note.done) { "opacity-70" } else { "" },
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
                    note.color.background(),
                    note.color.ink(),
                    note.rotation,
                    if dragged.get() == Some(id) { "scale(1.02)" } else { "scale(1)" }
                )
            }
            on:click=edit_note
            on:pointermove=move_dragged_note
            on:pointerup=finish_drag
            on:pointercancel=finish_drag
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
                            if note_snapshot(notes, id).is_some_and(|note| note.done) {
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
                    <div class="absolute bottom-2 left-3 right-3 flex items-center justify-between gap-2 text-[11px] font-sans">
                        <button
                            type="button"
                            data-note-action="toggle-done"
                            on:click=toggle_done
                            class="rounded-sm border border-current/30 px-1.5 py-0.5 hover:bg-white/30 focus:outline-none focus:ring-2 focus:ring-current/30"
                            title=move || if note_snapshot(notes, id).is_some_and(|note| note.done) {
                                "Mark task open"
                            } else {
                                "Complete task"
                            }
                        >
                            {move || if note_snapshot(notes, id).is_some_and(|note| note.done) {
                                "open"
                            } else {
                                "done"
                            }}
                        </button>
                        <div class="flex items-center gap-2">
                            <button
                                type="button"
                                data-note-action="cycle-color"
                                on:click=cycle_color
                                class="hover:underline focus:outline-none focus:ring-2 focus:ring-current/30"
                                title="Change note colour"
                            >
                                "colour"
                            </button>
                            <button
                                type="button"
                                data-note-action="delete-note"
                                on:click=delete_note
                                class="hover:underline focus:outline-none focus:ring-2 focus:ring-current/30"
                                title="Delete task"
                            >
                                "delete"
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
    let initial_board = load_board();
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
    let initial_view = load_view();
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
    let group_resize_initial = RwSignal::new(None::<(f64, f64)>);
    let group_resize_snapshot = RwSignal::new(None::<BoardData>);
    let restore_message = RwSignal::new(None::<String>);
    let next_id = RwSignal::new(
        notes
            .get_untracked()
            .iter()
            .map(|note| note.id)
            .max()
            .unwrap_or(0)
            + 1,
    );

    Effect::new(move |_| {
        save_board(&BoardData {
            notes: notes.get(),
            groups: groups.get(),
        });
    });

    Effect::new(move |_| {
        save_view(ViewState {
            pan: pan.get(),
            zoom: zoom.get(),
        });
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
    let reset_view = move |_| {
        pan.set((0.0, 0.0));
        zoom.set(1.0);
    };

    let undo = move |_| undo_board(notes, groups, history, editing, edit_snapshot);
    let redo = move |_| redo_board(notes, groups, history, editing, edit_snapshot);

    let group_selected = move |_: MouseEvent| {
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
    };
    let ungroup_selected = move |_: MouseEvent| {
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
    };

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

    let add_note = move |_| {
        commit_pending_edit(notes, groups, history, editing, edit_snapshot);
        let id = next_id.get_untracked();
        next_id.update(|next| *next += 1);
        mutate_notes(notes, groups, history, |items| {
            let (x, y) = viewport_note_position(pan.get_untracked(), zoom.get_untracked());
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
                done: false,
                x,
                y,
                rotation: match id % 5 {
                    0 | 3 => -2,
                    1 | 4 => 2,
                    _ => 1,
                },
                group_id: None,
            });
        });
        edit_snapshot.set(Some((id, board_snapshot(notes, groups))));
        editing.set(Some(id));
        restore_message.set(None);
    };

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
            match result.and_then(|raw| {
                serde_json::from_str::<BoardData>(&raw).ok().or_else(|| {
                    serde_json::from_str::<Vec<Note>>(&raw)
                        .ok()
                        .map(|notes| BoardData {
                            notes,
                            groups: Vec::new(),
                        })
                })
            }) {
                Some(restored) => {
                    let before = board_snapshot(notes, groups);
                    notes.set(restored.notes);
                    groups.set(restored.groups);
                    record_snapshot(notes, groups, history, before);
                    edit_snapshot.set(None);
                    editing.set(None);
                    restore_message.set(Some("board restored".into()));
                }
                None => restore_message.set(Some("that file is not a Task Space board".into())),
            }
        }) as Box<dyn FnMut(_)>);
        reader.set_onload(Some(onload.as_ref().unchecked_ref()));
        onload.forget();
        let _ = reader.read_as_text(&file);
        input.set_value("");
    };

    let keyboard_listener = window_event_listener(leptos::ev::keydown, move |ev: KeyboardEvent| {
        if !(ev.ctrl_key() || ev.meta_key()) {
            return;
        }
        let key = ev.key().to_lowercase();
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
        <main class="relative h-[100dvh] min-h-screen overflow-hidden bg-paper">
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
                on:pointerdown=start_pan
                on:pointermove=move_pan
                on:pointerup=finish_pan
                on:pointercancel=finish_pan
                on:wheel=zoom_or_pan
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

            <header class="pointer-events-none absolute inset-x-3 top-3 z-10 flex items-start justify-between gap-3 sm:inset-x-5 sm:top-5">
                <div class="pointer-events-auto flex items-center gap-3 rounded-md border border-ink-soft/15 bg-paper/90 px-3 py-2 shadow-md backdrop-blur-sm">
                    <a href="/" class="flex items-center gap-2" aria-label="Task Space home">
                        <img src="/smbl-logo.png" alt="SMBL" class="h-6 w-auto"/>
                        <span class="font-handwriting text-3xl leading-none">"Task Space"</span>
                    </a>
                    <span class="hidden h-6 w-px bg-ink-soft/20 sm:block"></span>
                    <span class="hidden text-xs text-ink-soft sm:block">
                        {move || format!("{} {}", notes.get().len(), if notes.get().len() == 1 { "note" } else { "notes" })}
                    </span>
                </div>

                <div class="pointer-events-auto flex items-center gap-1 rounded-md border border-ink-soft/15 bg-paper/90 p-1 shadow-md backdrop-blur-sm sm:gap-2 sm:p-1.5">
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
                            export_board(&notes.get_untracked(), &groups.get_untracked())
                        }
                        class="rounded-[3px] px-2 py-2 text-sm text-ink-soft hover:bg-white/70 hover:text-ink focus:outline-none focus:ring-2 focus:ring-ink/30 sm:px-3"
                    >
                        "export"
                    </button>
                    <label class="cursor-pointer rounded-[3px] px-2 py-2 text-sm text-ink-soft hover:bg-white/70 hover:text-ink focus-within:ring-2 focus-within:ring-ink/30 sm:px-3">
                        "restore"
                        <input type="file" accept="application/json,.json" class="sr-only" on:change=restore_file/>
                    </label>
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
                    <span class="rounded-[3px] border border-ink-soft/15 bg-paper/85 px-2.5 py-1.5 shadow-sm backdrop-blur-sm">
                        "saved on this device"
                    </span>
                </div>
            </div>
        </main>
    }
}
