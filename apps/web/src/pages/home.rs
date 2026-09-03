use gloo_net::http::Request;
use leptos::prelude::*;
use leptos::task::spawn_local;

#[component]
fn MockNote(
    bg: &'static str,
    ink: &'static str,
    rot: &'static str,
    text: &'static str,
    done: bool,
    due: &'static str,
) -> impl IntoView {
    view! {
        <div
            class=format!(
                "relative font-handwriting text-xl w-24 shadow-md pt-2 px-2 {} {}",
                if done { "line-through opacity-60" } else { "" },
                if due.is_empty() { "pb-2 min-h-28" } else { "pb-8 min-h-32" }
            )
            style=format!("background-color:{bg};color:{ink};transform:rotate({rot})")
        >
            {text}
            <span
                class="block absolute -top-2 left-1/2 -translate-x-1/2 w-10 h-3 bg-tape"
                aria-hidden="true"
            ></span>
            {if done {
                view! {
                    <span
                        class="block absolute bottom-0 right-0 h-0 w-0 border-b-[12px] border-l-[12px] border-b-ink/15 border-l-transparent"
                        aria-hidden="true"
                    ></span>
                }
                .into_any()
            } else {
                ().into_any()
            }}
            {if due.is_empty() {
                ().into_any()
            } else {
                view! {
                    <span class="block absolute bottom-1 left-1/2 -translate-x-1/2 text-[10px] bg-chip rounded-[2px] px-1.5 py-px">
                        {due}
                    </span>
                }
                .into_any()
            }}
        </div>
    }
}

#[component]
fn BoardMock() -> impl IntoView {
    view! {
        <div
            class="relative w-full max-w-md aspect-[4/3] rounded-md shadow-2xl rotate-1 overflow-hidden bg-paper-shelf"
        >
            <div
                class="absolute inset-0"
                style="background-image: radial-gradient(color-mix(in srgb, var(--color-ink-soft) 14%, transparent) 1px, transparent 1.5px); background-size: 22px 22px;"
            ></div>
            <div class="absolute top-5 left-6 -rotate-3">
                <MockNote
                    bg="var(--color-note-yellow)"
                    ink="var(--color-note-ink-yellow)"
                    rot="-2deg"
                    text="review the launch plan"
                    done=false
                    due=""
                />
            </div>
            <div class="absolute top-10 right-6 rotate-2">
                <MockNote
                    bg="var(--color-note-pink)"
                    ink="var(--color-note-ink-pink)"
                    rot="2.5deg"
                    text="call the printer"
                    done=false
                    due=""
                />
            </div>
            <div class="absolute top-6 left-1/2 -translate-x-1/2 rotate-1">
                <MockNote
                    bg="var(--color-note-blue)"
                    ink="var(--color-note-ink-blue)"
                    rot="1deg"
                    text="write the blog post"
                    done=false
                    due="due today"
                />
            </div>
            <div class="absolute bottom-8 left-10 rotate-2">
                <MockNote
                    bg="var(--color-note-green)"
                    ink="var(--color-note-ink-green)"
                    rot="2deg"
                    text="finish the draft"
                    done=true
                    due=""
                />
            </div>
            <div class="absolute bottom-6 right-10 -rotate-2">
                <MockNote
                    bg="var(--color-note-lav)"
                    ink="var(--color-note-ink-lav)"
                    rot="-2deg"
                    text="water the plants"
                    done=false
                    due="sat"
                />
            </div>
        </div>
    }
}

const GITHUB_MARK: &str = "M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27s1.36.09 2 .27c1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.01 8.01 0 0 0 16 8c0-4.42-3.58-8-8-8z";

#[component]
fn GithubLink(class: &'static str) -> impl IntoView {
    view! {
        <a
            href="https://github.com/MrSheerluck/task-space"
            aria-label="Task Space source on GitHub"
            title="source on github"
            class=class
        >
            <svg viewBox="0 0 16 16" width="18" height="18" fill="currentColor" aria-hidden="true">
                <path d=GITHUB_MARK/>
            </svg>
        </a>
    }
}

#[component]
fn Header() -> impl IntoView {
    view! {
        <header class="max-w-6xl mx-auto flex flex-wrap items-center justify-between gap-x-6 gap-y-2 px-6 py-4">
            <a href="/" class="flex items-center gap-2">
                <img src="/smbl-logo.png" alt="SMBL" class="h-7 w-auto"/>
                <span class="font-handwriting text-4xl leading-none">
                    "Task Space"
                </span>
            </a>
            <nav class="flex items-center gap-5 text-ink-soft">
                <a href="#features" class="hidden sm:block hover:text-ink">
                    "features"
                </a>
                <GithubLink class="hover:text-ink"/>
                <a
                    href="#waitlist"
                    class="bg-marker text-ink rounded-[3px] px-3 py-1.5 font-medium hover:brightness-95"
                >
                    "join the waitlist"
                </a>
            </nav>
        </header>
    }
}

#[component]
fn Feature(icon: &'static str, title: &'static str, body: &'static str) -> impl IntoView {
    view! {
        <div class="bg-paper-shelf/60 rounded-md p-5 border border-ink-soft/10">
            <div class="font-handwriting text-3xl text-ink-soft">
                {icon}
            </div>
            <h3 class="mt-2 font-semibold text-lg">
                {title}
            </h3>
            <p class="mt-1 text-ink-soft text-sm leading-relaxed">
                {body}
            </p>
        </div>
    }
}

#[component]
fn WaitlistForm() -> impl IntoView {
    let email = RwSignal::new(String::new());
    let status = RwSignal::new(None::<&'static str>);

    let submit = move |ev: leptos::ev::SubmitEvent| {
        ev.prevent_default();
        let value = email.get_untracked().trim().to_string();
        if !value.contains('@') {
            status.set(Some("invalid"));
            return;
        }
        if let Some(storage) = web_sys::window().and_then(|w| w.local_storage().ok().flatten()) {
            let _ = storage.set_item("task-space.waitlist", &value);
        }
        status.set(Some("sending"));
        let url = format!("{WAITLIST_API}/waitlist");
        spawn_local(async move {
            let body = serde_json::json!({ "email": value, "source": "landing" });
            let ok = match Request::post(&url).json(&body) {
                Ok(request) => request.send().await.is_ok(),
                Err(_) => false,
            };
            status.set(if ok { Some("server") } else { Some("local") });
        });
    };

    view! {
        <div id="waitlist">
            <form
                on:submit=submit
                class="mt-6 flex flex-col sm:flex-row gap-3 max-w-md"
            >
                <input
                    type="email"
                    required
                    placeholder="you@example.com"
                    bind:value=email
                    class="flex-1 rounded-[3px] border border-ink/25 bg-blank px-4 py-2.5 placeholder:text-ink-soft/60 focus:outline-none focus:border-ink"
                />
                <button
                    type="submit"
                    disabled=move || {
                        status.get() == Some("sending") || status.get() == Some("server")
                    }
                    class=move || {
                        format!(
                            "rounded-[3px] px-5 py-2.5 font-medium shadow hover:brightness-95 {}",
                            if status.get() == Some("sending") {
                                "bg-paper-shelf text-ink-soft"
                            } else if status.get() == Some("server") {
                                "bg-note-green text-note-ink-green"
                            } else {
                                "bg-marker text-ink"
                            }
                        )
                    }
                >
                    {move || match status.get() {
                        Some("sending") => "waiting…",
                        Some("server") => "done",
                        _ => "join the waitlist",
                    }}
                </button>
            </form>
            <p class="mt-3 text-sm text-ink-soft">
                "no account needed. no spam. we write once the board is ready."
            </p>
            {move || match status.get() {
                Some("server") => {
                    view! {
                        <div class="mt-4 max-w-md rounded-[3px] bg-note-yellow text-note-ink-yellow px-4 py-2 rotate-[-0.5deg] shadow">
                            "you're on the list. we'll ping you when Task Space opens."
                        </div>
                    }
                    .into_any()
                }
                Some("local") => {
                    view! {
                        <div class="mt-4 max-w-md rounded-[3px] bg-note-pink text-note-ink-pink px-4 py-2 rotate-[-0.5deg] shadow">
                            "couldn't reach the waitlist. your email is saved on this device for now."
                        </div>
                    }
                    .into_any()
                }
                Some("invalid") => {
                    view! {
                        <div class="mt-4 max-w-md rounded-[3px] bg-note-pink text-note-ink-pink px-4 py-2 rotate-[-0.5deg] shadow">
                            "that doesn't look like an email. try you@example.com"
                        </div>
                    }
                    .into_any()
                }
                _ => ().into_any(),
            }}
        </div>
    }
}

const WAITLIST_API: &str = match option_env!("WAITLIST_API") {
    Some(url) => url,
    None => "https://task-space-waitlist.mrsheerluck003.workers.dev",
};

#[component]
fn Footer() -> impl IntoView {
    view! {
        <footer class="border-t border-ink-soft/15 mt-16">
            <div class="max-w-6xl mx-auto px-6 py-8 flex flex-col sm:flex-row items-center justify-between gap-3 text-sm text-ink-soft">
                <span class="flex items-center gap-2">
                    <img src="/smbl-logo.png" alt="SMBL" class="h-6 w-auto"/>
                    <span class="font-handwriting text-2xl text-ink">
                        "Task Space"
                    </span>
                </span>
                <span>
                    "by SMBL · made with paper, leptos & a love for sticky notes"
                </span>
                <span class="flex items-center gap-4">
                    <GithubLink class="text-ink-soft hover:text-ink"/>
                    <a
                        href="https://github.com/MrSheerluck/task-space/blob/main/LICENSE"
                        class="hover:text-ink"
                    >
                        "MIT"
                    </a>
                </span>
            </div>
        </footer>
    }
}

#[component]
pub fn Home() -> impl IntoView {
    view! {
        <div class="min-h-screen flex flex-col">
            <Header/>
            <main class="flex-1">
                <section class="max-w-6xl mx-auto px-6 pt-10 pb-8 grid lg:grid-cols-2 gap-10 items-center">
                    <div>
                        <h1 class="font-handwriting text-5xl sm:text-6xl xl:text-7xl leading-tight">
                            "your day," <br/> "pinned down."
                        </h1>
                        <p class="mt-4 text-ink-soft text-lg">
                            "An infinite canvas of sticky notes for tasks and plans. Local-first:
                            works offline, data stays in your browser, sync across devices
                            is optional, and only when you want it."
                        </p>
                        <span class="sr-only" id="waitlist-heading">
                            "join the waitlist"
                        </span>
                        <WaitlistForm/>
                    </div>
                    <div class="flex justify-center">
                        <BoardMock/>
                    </div>
                </section>

                <section class="max-w-6xl mx-auto px-6 py-10" id="features">
                    <h2 class="font-handwriting text-5xl text-center">
                        "a desk, not a database"
                    </h2>
                    <div class="mt-8 grid sm:grid-cols-2 lg:grid-cols-4 gap-4">
                        <Feature
                            icon="offline ✎"
                            title="Local-first"
                            body="Your spaces live in your browser. Work offline forever, export a file, don't lose anything when the network does."
                        />
                        <Feature
                            icon="paper"
                            title="A paper canvas"
                            body="Pan, zoom and drag notes around one warm paper board. Tasks look like the sticky notes you already use."
                        />
                        <Feature
                            icon="sync ⇄"
                            title="Sync, only if you want"
                            body="Multi-device sync is a paid option (Pro). No account, no tracking for the free local mode, ever."
                        />
                        <Feature
                            icon="keys"
                            title="Keyboard-first"
                            body="Create, move, complete and organise notes without ever touching the mouse. Tailwind-fast board, steady desk."
                        />
                    </div>
                </section>

                <section class="max-w-3xl mx-auto px-6 py-10">
                    <h2 class="font-handwriting text-5xl text-center">
                        "paper costs nothing"
                    </h2>
                    <div class="mt-8 grid sm:grid-cols-2 gap-4">
                        <div class="rounded-md border border-ink-soft/10 p-6 bg-paper-shelf/60">
                            <h3 class="text-lg font-semibold">
                                "free"
                            </h3>
                            <p class="font-handwriting text-5xl mt-1">
                                "€0, for ever"
                            </p>
                            <ul class="mt-3 space-y-1 text-sm text-ink-soft">
                                <li>"unlimited local spaces"</li>
                                <li>"offline-first, data stays in your browser"</li>
                                <li>"JSON export & restore"</li>
                            </ul>
                        </div>
                        <div class="rounded-md border border-ink/20 p-6 rotate-[-0.5deg] shadow-md bg-note-yellow text-note-ink-yellow">
                            <h3 class="text-lg font-semibold">
                                "pro"
                            </h3>
                            <p class="font-handwriting text-5xl mt-1">
                                "€3 / mo · tbd"
                            </p>
                            <ul class="mt-3 space-y-1 text-sm">
                                <li>"sync your spaces across any browser (web app, installable)"</li>
                                <li>"same local files, nothing converted"</li>
                                <li>"runs offline everywhere, syncs when it can"</li>
                            </ul>
                            <a
                                class="mt-5 inline-block rounded-[3px] bg-ink text-paper px-4 py-2 text-sm opacity-70 pointer-events-none"
                                aria-disabled="true"
                            >
                                "coming soon"
                            </a>
                        </div>
                    </div>
                </section>
            </main>
            <Footer/>
        </div>
    }
}
