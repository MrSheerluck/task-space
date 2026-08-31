use js_sys::Array;
use leptos::ev::{DragEvent, Event, KeyboardEvent, MouseEvent};
use leptos::prelude::*;
use serde::{Deserialize, Serialize};
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use web_sys::{
    Blob, Element, FileReader, HtmlAnchorElement, HtmlInputElement, HtmlTextAreaElement, Url,
};

const STORAGE_KEY: &str = "task-space.board.v1";

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

fn load_notes() -> Vec<Note> {
    web_sys::window()
        .and_then(|window| window.local_storage().ok().flatten())
        .and_then(|storage| storage.get_item(STORAGE_KEY).ok().flatten())
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
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

fn board_position(event: &DragEvent) -> Option<(f64, f64)> {
    let board = event.current_target()?.dyn_into::<Element>().ok()?;
    let rect = board.get_bounding_client_rect();
    let x =
        ((f64::from(event.client_x()) - rect.left()) / rect.width() * 100.0 - 5.0).clamp(2.0, 86.0);
    let y =
        ((f64::from(event.client_y()) - rect.top()) / rect.height() * 100.0 - 4.0).clamp(3.0, 88.0);
    Some((x, y))
}

#[component]
fn NoteCard(
    note: Note,
    notes: RwSignal<Vec<Note>>,
    editing: RwSignal<Option<u64>>,
    dragged: RwSignal<Option<u64>>,
) -> impl IntoView {
    let id = note.id;
    let card_style = format!(
        "left:{}%;top:{}%;background-color:{};color:{};transform:rotate({}deg)",
        note.x,
        note.y,
        note.color.background(),
        note.color.ink(),
        note.rotation
    );
    let empty = note.text.trim().is_empty();
    let aria_label = if empty {
        "Empty task note".to_string()
    } else {
        note.text.clone()
    };

    let update_text = move |ev: Event| {
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
    };

    let finish_editing = move |_| editing.set(None);
    let handle_key = move |ev: KeyboardEvent| {
        if ev.key() == "Escape" {
            editing.set(None);
        }
    };
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

    view! {
        <article
            class=move || format!(
                "absolute w-44 min-h-40 p-3 pb-9 rounded-[3px] shadow-lg select-none transition-shadow {} {}",
                if note.done { "opacity-70" } else { "" },
                if dragged.get() == Some(id) { "shadow-2xl cursor-grabbing" } else { "cursor-grab hover:shadow-xl" }
            )
            style=card_style
            draggable="true"
            on:dragstart=move |_| dragged.set(Some(id))
            on:dragend=move |_| dragged.set(None)
            aria-label=aria_label
        >
            <div class="absolute -top-2 left-1/2 -translate-x-1/2 w-11 h-3 bg-tape rotate-[-2deg]" aria-hidden="true"></div>
            {if editing.get() == Some(id) {
                view! {
                    <textarea
                        prop:value=note.text.clone()
                        autofocus=true
                        rows="4"
                        maxlength="180"
                        aria-label="Edit task"
                        on:input=update_text
                        on:keydown=handle_key
                        class="w-full resize-none bg-transparent font-handwriting text-2xl leading-tight outline-none placeholder:text-current/50"
                        placeholder="write a task…"
                    ></textarea>
                    <button
                        type="button"
                        on:click=finish_editing
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
                        on:click=move |_| editing.set(Some(id))
                        class=format!(
                            "w-full text-left font-handwriting text-2xl leading-tight {}",
                            if note.done { "line-through" } else { "" }
                        )
                    >
                        {if empty { "click to write" } else { note.text.as_str() }}
                    </button>
                }
                .into_any()
            }}
            <div class="absolute bottom-2 left-3 right-3 flex items-center justify-between gap-2 text-[11px] font-sans">
                <button
                    type="button"
                    on:click=toggle_done
                    class="rounded-sm border border-current/30 px-1.5 py-0.5 hover:bg-white/30 focus:outline-none focus:ring-2 focus:ring-current/30"
                    title=if note.done { "Mark task open" } else { "Complete task" }
                >
                    {if note.done { "open" } else { "done" }}
                </button>
                <div class="flex items-center gap-2">
                    <button
                        type="button"
                        on:click=cycle_color
                        class="hover:underline focus:outline-none focus:ring-2 focus:ring-current/30"
                        title="Change note colour"
                    >
                        "colour"
                    </button>
                    <button
                        type="button"
                        on:click=delete_note
                        class="hover:underline focus:outline-none focus:ring-2 focus:ring-current/30"
                        title="Delete task"
                    >
                        "delete"
                    </button>
                </div>
            </div>
        </article>
    }
}

#[component]
pub fn Board() -> impl IntoView {
    let notes = RwSignal::new(load_notes());
    let editing = RwSignal::new(None::<u64>);
    let dragged = RwSignal::new(None::<u64>);
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

    let add_note = move |_| {
        let id = next_id.get_untracked();
        next_id.update(|next| *next += 1);
        notes.update(|items| {
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
                x: 42.0,
                y: 42.0,
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

    let drop_note = move |ev: DragEvent| {
        ev.prevent_default();
        let Some(id) = dragged.get_untracked() else {
            return;
        };
        if let Some((x, y)) = board_position(&ev) {
            notes.update(|items| {
                if let Some(note) = items.iter_mut().find(|note| note.id == id) {
                    note.x = x;
                    note.y = y;
                }
            });
        }
        dragged.set(None);
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

    let prevent_drag = move |ev: DragEvent| ev.prevent_default();

    view! {
        <main class="relative h-[100dvh] min-h-screen overflow-hidden bg-paper">
            <div
                class="absolute inset-0 overflow-hidden bg-paper-shelf"
                style="background-image: radial-gradient(color-mix(in srgb, var(--color-ink-soft) 18%, transparent) 1px, transparent 1.5px); background-size: 24px 24px;"
                on:dragover=prevent_drag
                on:drop=drop_note
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
                    notes.get().into_iter().map(|note| view! {
                        <NoteCard note=note notes=notes editing=editing dragged=dragged/>
                    }).collect_view().into_any()
                }}
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

            <div class="pointer-events-none absolute inset-x-3 bottom-3 z-10 flex items-end justify-between gap-3 text-xs text-ink-soft sm:inset-x-5 sm:bottom-5">
                <span class="rounded-[3px] border border-ink-soft/15 bg-paper/85 px-2.5 py-1.5 shadow-sm backdrop-blur-sm">
                    "drag to move · click to edit"
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
