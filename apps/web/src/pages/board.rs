use js_sys::Array;
use leptos::ev::{Event, KeyboardEvent, MouseEvent, PointerEvent, WheelEvent};
use leptos::prelude::*;
use serde::{Deserialize, Serialize};
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use web_sys::{
    Blob, Element, FileReader, HtmlAnchorElement, HtmlInputElement, HtmlTextAreaElement, Url,
};

const STORAGE_KEY: &str = "task-space.board.v2";
const LEGACY_STORAGE_KEY: &str = "task-space.board.v1";
const VIEW_STORAGE_KEY: &str = "task-space.view.v1";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct Note {
    id: u64,
    text: String,
    color: NoteColor,
    done: bool,
    x: f64,
    y: f64,
    rotation: i8,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct ViewState {
    pan: (f64, f64),
    zoom: f64,
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

fn load_notes() -> Vec<Note> {
    let storage = web_sys::window().and_then(|window| window.local_storage().ok().flatten());
    let current = storage
        .as_ref()
        .and_then(|storage| storage.get_item(STORAGE_KEY).ok().flatten())
        .and_then(|raw| serde_json::from_str::<Vec<Note>>(&raw).ok());
    let mut notes = current
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
                    notes
                })
        })
        .unwrap_or_default();

    for index in 1..notes.len() {
        let (previous, current) = notes.split_at_mut(index);
        if previous.iter().any(|other| {
            (other.x - current[0].x).abs() < 1.0 && (other.y - current[0].y).abs() < 1.0
        }) {
            let (x, y) = note_position(index);
            current[0].x = x;
            current[0].y = y;
        }
    }

    notes
}

fn save_notes(notes: &[Note]) {
    let Some(storage) = web_sys::window().and_then(|window| window.local_storage().ok().flatten())
    else {
        return;
    };
    if let Ok(raw) = serde_json::to_string(notes) {
        let _ = storage.set_item(STORAGE_KEY, &raw);
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

fn export_notes(notes: &[Note]) {
    let Ok(raw) = serde_json::to_string_pretty(notes) else {
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
    anchor.set_download("task-space-notes.json");
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

#[component]
fn NoteCard(
    id: u64,
    notes: RwSignal<Vec<Note>>,
    editing: RwSignal<Option<u64>>,
    dragged: RwSignal<Option<u64>>,
    drag_offset: RwSignal<Option<(f64, f64)>>,
    pan: RwSignal<(f64, f64)>,
    zoom: RwSignal<f64>,
) -> impl IntoView {
    let toggle_done = move |ev: MouseEvent| {
        ev.stop_propagation();
        notes.update(|items| {
            if let Some(note) = items.iter_mut().find(|note| note.id == id) {
                note.done = !note.done;
            }
        });
    };
    let cycle_color = move |ev: MouseEvent| {
        ev.stop_propagation();
        notes.update(|items| {
            if let Some(note) = items.iter_mut().find(|note| note.id == id) {
                note.color = note.color.next();
            }
        });
    };
    let delete_note = move |ev: MouseEvent| {
        ev.stop_propagation();
        notes.update(|items| items.retain(|note| note.id != id));
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
        let rect = card.get_bounding_client_rect();
        let _ = card.set_pointer_capture(ev.pointer_id());
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
                notes.update(|items| {
                    if let Some(note) = items.iter_mut().find(|note| note.id == id) {
                        note.x = x;
                        note.y = y;
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
                                editing.set(None);
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
                            editing.set(None);
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
    let notes = RwSignal::new(load_notes());
    let editing = RwSignal::new(None::<u64>);
    let dragged = RwSignal::new(None::<u64>);
    let drag_offset = RwSignal::new(None::<(f64, f64)>);
    let initial_view = load_view();
    let pan = RwSignal::new(initial_view.pan);
    let zoom = RwSignal::new(initial_view.zoom);
    let pan_pointer = RwSignal::new(None::<i32>);
    let last_pan_point = RwSignal::new(None::<(f64, f64)>);
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
        save_notes(&notes.get());
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
        last_pan_point.set(Some((f64::from(ev.client_x()), f64::from(ev.client_y()))));
    };

    let move_pan = move |ev: PointerEvent| {
        if pan_pointer.get_untracked() != Some(ev.pointer_id()) {
            return;
        }
        ev.prevent_default();
        let current = (f64::from(ev.client_x()), f64::from(ev.client_y()));
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

    let add_note = move |_| {
        let id = next_id.get_untracked();
        next_id.update(|next| *next += 1);
        notes.update(|items| {
            let (x, y) = note_position(items.len());
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
            });
        });
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
            match result.and_then(|raw| serde_json::from_str::<Vec<Note>>(&raw).ok()) {
                Some(restored) => {
                    notes.set(restored);
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
                        each={move || notes.get().into_iter().map(|note| note.id).collect::<Vec<_>>()}
                        key=|id| *id
                        children=move |id: u64| {
                            view! {
                                <NoteCard
                                    id=id
                                    notes=notes
                                    editing=editing
                                    dragged=dragged
                                    drag_offset=drag_offset
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
                        on:click=add_note
                        class="rounded-[3px] bg-marker px-3 py-2 text-sm font-medium shadow-sm hover:brightness-95 focus:outline-none focus:ring-2 focus:ring-ink/40"
                    >
                        "+ new note"
                    </button>
                    <button
                        type="button"
                        on:click=move |_| export_notes(&notes.get_untracked())
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
                    "drag empty space to pan · scroll to move · +/- to zoom"
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
