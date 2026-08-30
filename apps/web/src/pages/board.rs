use leptos::prelude::*;

#[component]
pub fn Board() -> impl IntoView {
    view! {
        <main class="min-h-screen grid place-items-center px-6">
            <div class="text-center">
                <h1 class="font-handwriting text-7xl">
                    "your paper board"
                </h1>
                <p class="mt-3 text-ink-soft">
                    "the board itself is the next task on the pile, M0.5"
                </p>
            </div>
        </main>
    }
}
