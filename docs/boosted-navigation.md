# Boosted Navigation & the Persistent Audio Player

Vecto uses [htmx](https://htmx.org/) (v2.0.8, vendored) to turn every in-app navigation into an AJAX partial swap instead of a full page load. The point of the whole system is **uninterrupted audio**: the global now-playing bar and its hidden `<audio>` engine live *outside* the swapped region, so starting an episode and then navigating anywhere — breadcrumbs, dashboard filters, Creator Settings tabs, another episode's page — never stops or restarts playback.

This is not cosmetic plumbing. It changes the runtime contract for **every inline script in every template**: scripts re-execute on each navigation, listeners can stack, and async work can outlive its page. The rules below exist because each one was a real bug first.

---

## Architecture

`base.html` is split into three nested zones. The nesting is the whole design — each level exists because putting it anywhere else broke something:

1. **Persistent zone** (lives for the browser tab's lifetime): everything in `<head>` (Bootstrap, htmx, quill.js, diff.min.js), the floating player bar `#floatingPlayer` + `#vGlobalAudio` + the `VectoPlayer`/`VectoPage` script near the top of `<body>`, and the impersonation banner. **Order matters**: the player block sits *before* the boosted region so `window.VectoPlayer`/`window.VectoPage` exist when page scripts execute at parse time.

2. **The boost wrapper** — an unstyled `<div>` that encloses *both* the nav and the swap region, and carries the boost:

   ```html
   <div hx-boost="true" hx-target="#boosted-region" hx-select="#boosted-region"
        hx-swap="outerHTML show:window:top" hx-push-url="true">
   ```

   These attributes are **not** on `#boosted-region`, and moving them there is a regression. The nav must stay *outside* the swapped region (so it persists, and so an HX fragment carries no navbar), but it must stay *inside* the boost (or nav links would hard-navigate and destroy the out-of-region player). Only a wrapper enclosing both satisfies both constraints. Boosted links inside a swapped-in region inherit these attributes from here.

3. **The swap region** — `<div id="boosted-region" hx-history-elt>`, holding the `.container`, flash messages, `{% block content %}`, and the approval badge. This is the only thing that is ever swapped.

### The nav persists — push nav state explicitly

Because the nav now lives outside the region, **nothing re-renders it after a swap**. Any nav-visible state that can change must be pushed by hand. The avatar picker is the live example: `update_avatar_preference` returns the new URL and the profile page assigns it to `#navbarAvatar`. Changing a nav-visible value server-side and expecting it to appear is a silent no-op. (An htmx OOB swap is the other option.)

### Base-swap: the server renders only the region on HX requests

The server is **not** htmx-agnostic (it was until the base-swap rollout; that sentence used to live here and is now false). On a boosted request it renders the fragment alone — roughly 40 KB of chrome shed from every boosted nav across every view.

- `HtmxBaseTemplateMiddleware` (`pod_manager/middleware.py`) sets `request.base_template` to `base_htmx.html` when `HX-Request` is present, else `base.html`. The `htmx` context processor exposes it as `base_template`.
- **Every** template extends `{% extends base_template|default:'pod_manager/base.html' %}`. The `|default` is what let the rollout land one view at a time.
- `base_htmx.html` emits *only* the region. Both bases build that region from `snippets/_boosted_region_open.html` / `_close.html`, so the two can never drift.
- **`Vary: HX-Request` is on every response** and is non-negotiable: without it any shared/browser/proxy cache can serve a fragment to a full-page request or vice versa.

Two constraints this puts on new templates:

- **Only fill `{% block content %}`.** `base.html`'s `navbar_extra` / `navbar_extra_mobile` blocks render outside the region, so the skinny base drops them silently. Check with `grep -o "{% block [a-z_]* %}" <template>` before converting anything.
- **In-content `<script>`/`<link>` ride along** precisely because they sit inside the content block. That's deliberate, and `LogViewerBaseSwapTests` / `AdminConsoleBaseSwapTests` guard it — losing such a script yields a dead-but-rendered page, not an obvious break.

Supporting config and safety nets (all in `base.html`):

- `<meta name="htmx-config" content='{"historyCacheSize": 0}'>` — back/forward always refetches from the server. This is also what makes base-swap safe for history: a restore is a normal, non-HX request, so it gets the full page.
- **`hx-history-elt` is on the region in *both* bases**, unconditionally. htmx's `getHistoryElement()` is `querySelector('[hx-history-elt]') || document.body`, and a boosted nav *replaces* the region. If the swapped-in fragment's region lacked the attribute, the live region would lose it after one swap, the history element would fall back to `<body>`, and the next browser-back would overwrite everything — wiping nav and player.
- A fallback handler on `htmx:sendError` / `htmx:responseError` / `htmx:swapError` degrades to a real navigation, so a network error, 500, or cross-origin login redirect never leaves a dead click. **The custom 404 is deliberately outside the swap contract**: htmx never swaps a non-2xx, so this handler forces a real navigation, which arrives non-HX and renders the full page.
- The approval badge renders *inside* the region (it's `position: fixed`, DOM placement is irrelevant) so its count refreshes on every navigation.

### The audio engine

`window.VectoPlayer` is the **only** audio engine. Everything plays through `VectoPlayer.play({url, title, show, cover, chapters, link, seek})`; pages never create their own `<audio>`/Plyr instances. The episode page's inline player card (`.js-episode-remote`) is a *remote control* — it mirrors and drives the global engine, and only while `VectoPlayer.current().url` matches its own `data-audio`.

Two Plyr behaviors shape the API and must not be "simplified" away:

- **Plyr destroys and recreates the media element on every source change**, dropping its `id`. Never look up `#vGlobalAudio` from page scripts and never bind listeners to the element. Mirror playback via the `vecto:playerupdate` CustomEvent on `document` (dispatched by VectoPlayer on playing/pause/ended/timeupdate/loadedmetadata) and read state from `window._mainPlyr`.
- **Plyr's unloader loads `blankVideo` into the *discarded* element**, whose events still proxy through the player container. That's why `play()`'s seek logic ignores `canplay` while the live media is still unloaded, and why `blankVideo` points at the vendored `pod_manager/audio/blank.wav` (never Plyr's cdn.plyr.io default).

Playback state (`vectoPlayerState` in localStorage, throttled) survives true full loads: after F5 or a deep link, the bar reappears paused at the saved position and one click resumes (browser autoplay policy makes silent auto-resume impossible).

---

## Rules for page scripts

Inline scripts inside the boosted region **re-execute on every visit** to their page. On a first, full page load they run at parse time; after a boosted swap htmx re-evaluates them. Every rule below fails *silently on the second visit* if violated — always browser-test by visiting the same page twice.

1. **Wrap every inline script in an IIFE.** Top-level `let`/`const`/`class` in a classic script live in global lexical scope; re-execution throws `SyntaxError: redeclaration` and kills the whole script. Never use bare top-level declarations, and never `DOMContentLoaded` (it fires once per real load, so wrapped init code simply stops running after the first swap). Same rule for static JS files loaded from swapped content (`creator_tabs.js`, `gdrive_recovery.js`).
2. **Inline `on*` handlers need explicit exports.** Functions referenced by `onclick=` etc. must be assigned to `window.*` from inside the IIFE.
3. **Listeners on persistent targets go through `VectoPage.on(target, type, fn)`.** Anything attached to `document`, `window`, or another persistent node by a swapped script would stack once per visit. The registry removes them on the next `htmx:beforeSwap`. Listeners on elements *inside* the region need nothing — they die with their elements.
4. **Timers must self-terminate.** A `setInterval`/polling loop keeps running after its page is swapped away. Guard each tick with `document.body.contains(el)` (see the UTC clocks, log_viewer's poll loops).
5. **Async callbacks must tolerate a vanished page.** A fetch started on page A can resolve after the user navigated to page B. Writes to closure-captured elements become harmless no-ops; fresh `document.getElementById(...)` lookups need null guards.
6. **Programmatic submits use `form.requestSubmit()`, never `form.submit()`** — the latter skips the `submit` event htmx listens for and hard-navigates (killing audio).
7. **JS-built form fields must be rebuilt in `htmx:configRequest`.** htmx serializes boosted forms in its own `submit` listener, and its order relative to a page's payload-building listener is not guaranteed. Any form whose fields are assembled by JS (JSON payloads, hidden inputs) needs a hook that rebuilds them at request-build time and writes them into `e.detail.parameters`:

   ```js
   window.VectoPage.on(document.body, 'htmx:configRequest', (e) => {
       if (e.detail.elt !== form) return;
       buildPayload();
       e.detail.parameters.set('payload', payloadInput.value);
   });
   ```

   Live examples: the episode edit-suggestion form, the publish form, the home search form, and the inbox approve forms (where losing this race silently converted full approvals into rejections).

## Lazy-loaded regions inside the boosted region

Heavy pages fetch their parts on demand (Creator Settings tabs via `creator_tab_partial`, a show's form via `creator_show_form`, an audit edit's diff via `creator_audit_edit`). A lazy loader nests htmx inside htmx, and the interaction has sharp edges.

**A lazy region that hosts boosted forms/links must be split in two** — see `creator_tabs/_lazy_pane.html`, the canonical shape:

- The **outer** element carries the `#boosted-region` target/select/swap. It fires no request; it exists purely so the loaded content inherits a clean region swap.
- The **inner** loader does the one-shot `hx-get`, carrying its self-targeting attrs (`hx-target` = the outer, `hx-swap="innerHTML"`, `hx-select="unset"`) plus `hx-disinherit="*"` so they never cascade into the loaded body.

Why it can't be one element: `hx-target="this"` **resolves to the nearest ancestor that sets the attribute, not to the inheriting child** (htmx's `getTarget`/`findThisElement`), and an inherited `hx-select="unset"` reads as "no select". So a boosted form in the loaded body would swap an entire page response into its own loader. Hiding the attrs with `hx-disinherit="hx-target ..."` does not fix it — disinherit *blocks* inheritance, so the form falls back to htmx's boosted default of `<body>`, whose innerHTML swap destroys the player. Both failure modes were live bugs. `LazyPaneBoostTargetTests` guards the shape.

Other rules for lazy content:

- **`hx-select="unset"` on any loader whose response is a bare fragment.** It overrides the inherited `hx-select="#boosted-region"`, which would otherwise search a fragment for a region it doesn't contain and swap in nothing.
- **`hx-push-url="false"` on in-pane navigation** — the corollary is that the address bar goes stale inside a tab, so a reload returns to the tab's default state.
- **A self-targeting `hx-swap="outerHTML"` element must have no styling wrapper.** The swap replaces that element alone; any wrapper survives and the swapped-in content renders *inside* it, inheriting its styles. A `.text-center` wrapper around the shows "load more" button centre-aligned every appended row.
- **Call `htmx.process(el)` after injecting HTML yourself.** htmx only scans content it swapped in. `applyLiveFilter` sets `innerHTML` from a raw `fetch`, so without it every row in a filtered list is inert — lazy loaders never fire.
- **Styles shared across sibling panes belong in the shell**, never in one tab body: tabs are separate lazy panes, so a style defined in the inbox body is simply absent when the audit tab is open. `.pts-badge`, `.tag-pill` and `.chapter-row` live in `creator_settings.html` for exactly this reason.
- **Never nest a boosted `<a>`/`<form>` inside a Bootstrap collapse/accordion toggle button** — the click both toggles and navigates.
- **Scripts that decorate lazily-loaded content must re-run per swap.** Bind to `htmx:afterSwap` on the container (htmx events bubble): a listener on the container dies with it, whereas one on `document`/`window` stacks once per tab reload.

## Boost opt-outs

`hx-boost="false"` on anything that must be a real navigation or request:

- **Multipart forms** (file uploads) — native progress behavior beats a silent XHR.
- **Download links** (`download` attribute) and generated files (CSV exports).
- **Links leaving the app shell**: `/admin/` (Django admin doesn't extend `base.html`, so `hx-select` would find nothing), logout.

External links (`target="_blank"`) are ignored by boost automatically.

---

## Vendored front-end assets

All JS/CSS libraries are self-hosted under `pod_manager/static/pod_manager/` — **no CDN references, ever**, including indirect ones (Plyr's `blankVideo` default was a runtime cdn.plyr.io request until pinned to the local `blank.wav`). `VendoredAssetTests` in `tests.py` is the regression net: it greps every template for CDN `<script>`/`<link>` hosts and asserts each expected asset exists on disk with a version marker. **When vendoring or upgrading a library, update `EXPECTED_ASSETS` in the same change.**

Current inventory: Bootstrap 5.3.8 (+icons), Plyr, Quill, jsdiff, htmx 2.0.8, plus fonts, the Plyr icon sprite, and `audio/blank.wav`.

Two loading rules born from htmx's script handling:

- **Shared libraries load once from `<head>`** (bootstrap, quill, diff). htmx does not make inline scripts in swapped content wait for `<script src>` tags ahead of them, so "load the lib right before the inline script that uses it" is a race that occasionally loses (`Diff library failed to load`, missing Quill editors).
- **Cache-bust static JS that swapped pages load** (currently `creator_tabs.js?v=6` in `creator_settings.html`) whenever its execution contract changes. This is not optional hygiene: `STORAGES["staticfiles"]` is whitenoise's `CompressedStaticFilesStorage`, **not** the `Manifest` variant, so filenames are never hashed and this query param is the only cache-bust in the project. Skip the bump and a returning browser runs the old file against the new templates — the fixes silently revert for that user while working perfectly for you, and a stale copy executing inside a swap produces errors the current source can't explain.
