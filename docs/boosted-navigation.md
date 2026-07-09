# Boosted Navigation & the Persistent Audio Player

Vecto uses [htmx](https://htmx.org/) (v2.0.8, vendored) to turn every in-app navigation into an AJAX partial swap instead of a full page load. The point of the whole system is **uninterrupted audio**: the global now-playing bar and its hidden `<audio>` engine live *outside* the swapped region, so starting an episode and then navigating anywhere — breadcrumbs, dashboard filters, Creator Settings tabs, another episode's page — never stops or restarts playback.

This is not cosmetic plumbing. It changes the runtime contract for **every inline script in every template**: scripts re-execute on each navigation, listeners can stack, and async work can outlive its page. The rules below exist because each one was a real bug first.

---

## Architecture

`base.html` is split into two zones:

1. **Persistent zone** (lives for the browser tab's lifetime): everything in `<head>` (Bootstrap, htmx, quill.js, diff.min.js), the floating player bar `#floatingPlayer` + `#vGlobalAudio` + the `VectoPlayer`/`VectoPage` script near the top of `<body>`, and the impersonation banner. **Order matters**: the player block sits *before* the boosted region so `window.VectoPlayer`/`window.VectoPage` exist when page scripts execute at parse time.
2. **Boosted zone** — one wrapper around the header + content container:

   ```html
   <div id="boosted-region" hx-boost="true" hx-target="#boosted-region"
        hx-select="#boosted-region" hx-swap="outerHTML show:window:top"
        hx-push-url="true" hx-history-elt>
   ```

   Every same-origin link and form inside it fetches the full server-rendered page and swaps in only the response's `#boosted-region`. Views render normally — the server knows nothing about htmx.

Supporting config and safety nets (all in `base.html`):

- `<meta name="htmx-config" content='{"historyCacheSize": 0}'>` — back/forward always refetches from the server through the normal swap pipeline (scripts re-run, content fresh). `hx-history-elt` confines history restores to the region so a restore can never destroy the player.
- A fallback handler on `htmx:sendError` / `htmx:responseError` / `htmx:swapError` degrades to a real navigation, so a network error, 500, or cross-origin login redirect never leaves a dead click.
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
- **Cache-bust static JS that swapped pages load** (`creator_tabs.js?v=2`) when its execution contract changes — a stale cached copy executing inside a swap produces errors the current source can't explain.
