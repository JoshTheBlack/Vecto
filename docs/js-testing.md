# JavaScript testing

Two layers, both dependency-free for the project (no npm packages, nothing added to
requirements).

## 1. Unit tests (Node's built-in runner)

`tests_js/*.test.js`. The site's scripts are IIFEs that hang an API off `window`;
`tests_js/load.js` loads one into a `vm` sandbox with a fake `window`, so pure logic
(`chapter_editor.js`: time parsing, HTML escaping, row serialisation, payload building)
is tested without a browser. Rows are stubbed with `fakeRow()`.

    node --test "tests_js/*.test.js"

`manage.py test` runs them too (`JavaScriptUnitTests`), and skips if `node` is absent.

Add a test by creating `tests_js/<name>.test.js`. Logic buried in a template's inline
`<script>` can't be loaded this way: extract it into `static/pod_manager/js/` first (the
episode page's speaker-list derivation is the obvious candidate).

## 2. Browser smoke test (Playwright)

`tests_js/smoke/boosted_nav_smoke.py` checks that a boosted click and history back/forward
keep the same nav and floating player elements and never full-reload. Read-only. It needs
a running dev server and a logged-in session, so it is run by hand before a release, not by
`manage.py test`. Setup (Playwright lives in the system Python, not the venv):

1. `DEBUG=IDE LOG_LEVEL=INFO DEBUG_PAGES=0 ./.venv/Scripts/python.exe manage.py runserver 127.0.0.1:8765 --noreload`
2. In `manage.py shell`: `Client().force_login(User.objects.get(username='josh'))`, read `c.cookies['sessionid'].value`.
3. `python tests_js/smoke/boosted_nav_smoke.py --session <sessionid>`
4. Delete that Session row afterwards.
