"""Browser smoke test for the boosted-navigation shell. Read-only.

Guards the regressions that only show up in a real browser: an htmx boosted click or a
history back must NOT reload the page, wipe the nav, or drop the floating player.

Not part of `manage.py test`: it needs a running dev server, a logged-in session and
Playwright (installed in the system Python, not the project venv; see docs/js-testing.md).

    python tests_js/smoke/boosted_nav_smoke.py --session <sessionid>
        [--base http://baldmove.local:8765] [--map-host baldmove.local=127.0.0.1]

Exit status is non-zero on any failed check.
"""
import argparse
import sys

from playwright.sync_api import sync_playwright

failures = []


def check(ok, label):
    print(('  ok   ' if ok else '  FAIL ') + label)
    if not ok:
        failures.append(label)


def shell_state(page):
    """Counts of the shell's persistent pieces, plus the markers planted before navigating."""
    return page.evaluate("""() => ({
        reloaded: window.__smoke !== 1,
        navMarked: !!document.querySelector('nav.navbar[data-smoke]'),
        playerMarked: !!document.querySelector('#floatingPlayer[data-smoke]'),
        navs: document.querySelectorAll('nav.navbar').length,
        players: document.querySelectorAll('#floatingPlayer').length,
        audios: document.querySelectorAll('#vGlobalAudio').length,
        regions: document.querySelectorAll('#boosted-region').length,
        path: location.pathname + location.search,
    })""")


def assert_shell_intact(page, when):
    s = shell_state(page)
    check(not s['reloaded'], f'{when}: page was not fully reloaded')
    check(s['navMarked'], f'{when}: the same nav element survived')
    check(s['playerMarked'], f'{when}: the same floating player element survived')
    check((s['navs'], s['players'], s['audios'], s['regions']) == (1, 1, 1, 1),
          f'{when}: exactly one nav / player / audio / region (got {s["navs"]}/{s["players"]}/{s["audios"]}/{s["regions"]})')
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--base', default='http://baldmove.local:8765')
    ap.add_argument('--session', required=True, help='sessionid cookie of a logged-in user')
    ap.add_argument('--map-host', default='baldmove.local=127.0.0.1')
    args = ap.parse_args()
    host, ip = args.map_host.split('=')
    domain = args.base.split('//')[1].split(':')[0]

    with sync_playwright() as pw:
        browser = pw.chromium.launch(args=[f'--host-resolver-rules=MAP {host} {ip}'])
        ctx = browser.new_context(viewport={'width': 1280, 'height': 900})
        ctx.add_cookies([{'name': 'sessionid', 'value': args.session, 'domain': domain, 'path': '/'}])
        page = ctx.new_page()

        print('boosted navigation keeps the shell')
        page.goto(args.base + '/')
        page.wait_for_selector('nav.navbar')
        page.evaluate("""() => {
            window.__smoke = 1;
            document.querySelector('nav.navbar').setAttribute('data-smoke', '1');
            document.querySelector('#floatingPlayer').setAttribute('data-smoke', '1');
        }""")
        start = shell_state(page)['path']

        # a nav link that stays inside the boosted shell (not logout / external / opt-out)
        link = page.locator(
            'nav.navbar a[href^="/"]:not([hx-boost="false"]):not([target]):not([href^="/logout"])'
            f':not([href^="/admin"]):not([href="{start}"]):not([href^="#"])').first
        if link.count() == 0:
            check(False, 'found a boosted nav link to click')
        else:
            link.click()
            page.wait_for_function('(p) => location.pathname + location.search !== p', arg=start)
            page.wait_for_selector('#boosted-region')
            assert_shell_intact(page, 'after a boosted click')

            page.go_back()
            page.wait_for_function('(p) => location.pathname + location.search === p', arg=start)
            page.wait_for_selector('#boosted-region')
            assert_shell_intact(page, 'after history back')

            page.go_forward()
            page.wait_for_selector('#boosted-region')
            assert_shell_intact(page, 'after history forward')

        browser.close()

    print('FAILED' if failures else 'all checks passed')
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())
