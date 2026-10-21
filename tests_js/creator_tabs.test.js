'use strict';
// creator_tabs.js auto-import: after Add New Show (?auto_import=<id>) the page
// must expand that show's row and start its import, but the row arrives with the
// lazily-loaded Shows tab and the Import button with the lazily-loaded show form.
// These tests drive htmx:load by hand to prove it waits for both, then fires once.
const test = require('node:test');
const assert = require('node:assert');
const { loadScript } = require('./load');

function fakeElement(id) {
    return {
        id,
        disabled: false,
        innerHTML: '',
        textContent: '',
        classList: { add() {}, remove() {}, replace() {}, toggle() {} },
    };
}

// A page with nothing in it yet; add(id) simulates htmx swapping an element in,
// and swapIn() fires the htmx:load listeners registered through VectoPage.on.
function fakePage(search) {
    const elements = {};
    const loadListeners = [];
    const fetches = [];
    const expanded = [];
    const document = {
        cookie: '',
        body: {},
        getElementById: (id) => elements[id] || null,
        querySelector: () => null,
        querySelectorAll: () => [],
    };
    const window = {
        location: { search, pathname: '/creator/', hash: '' },
        history: { replaceState() {} },
        VectoPage: {
            on(target, type, fn) { if (type === 'htmx:load') loadListeners.push(fn); },
        },
    };
    const bootstrap = {
        Tab: class { show() {} },
        Collapse: { getOrCreateInstance: (el) => ({ show: () => expanded.push(el.id) }) },
    };
    loadScript('creator_tabs.js', {
        window, document, bootstrap, URLSearchParams,
        history: window.history,
        fetch: (url) => { fetches.push(url); return new Promise(() => {}); },
    });
    return {
        fetches,
        expanded,
        add(...ids) { ids.forEach((id) => { elements[id] = fakeElement(id); }); },
        swapIn() { loadListeners.forEach((fn) => fn({ target: {} })); },
    };
}

const IMPORT_PANEL = ['btn-import-5', 'terminal-container-5', 'terminal-5'];

test('waits for the row, expands it, then waits for the Import button', () => {
    const page = fakePage('?network=bm&auto_import=5');
    assert.deepStrictEqual(page.expanded, []);
    assert.deepStrictEqual(page.fetches, []);

    page.add('collapse-5');          // the Shows tab lands
    page.swapIn();
    assert.deepStrictEqual(page.expanded, ['collapse-5']);
    assert.deepStrictEqual(page.fetches, []);

    page.add(...IMPORT_PANEL);       // the show's form lands
    page.swapIn();
    assert.deepStrictEqual(page.fetches, ['/import/start/5/']);
});

test('starts the import only once however many swaps follow', () => {
    const page = fakePage('?network=bm&auto_import=5');
    page.add('collapse-5', ...IMPORT_PANEL);
    page.swapIn();
    page.swapIn();
    page.swapIn();
    assert.deepStrictEqual(page.fetches, ['/import/start/5/']);
    assert.deepStrictEqual(page.expanded, ['collapse-5']);
});

test('ignores swaps that bring in other shows', () => {
    const page = fakePage('?network=bm&auto_import=5');
    page.add('collapse-6', 'btn-import-6');
    page.swapIn();
    assert.deepStrictEqual(page.expanded, []);
    assert.deepStrictEqual(page.fetches, []);
});

test('does nothing without ?auto_import', () => {
    const page = fakePage('?network=bm');
    page.add('collapse-5', ...IMPORT_PANEL);
    page.swapIn();
    assert.deepStrictEqual(page.expanded, []);
    assert.deepStrictEqual(page.fetches, []);
});
