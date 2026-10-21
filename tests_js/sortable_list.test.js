'use strict';
// sortable_list.js: binds [data-sortable-list] containers once (on load and on
// every htmx:load), POSTs the full new order on drop, and puts the list back
// when the server rejects it. SortableJS itself is faked: create() records the
// options so a test can call onStart/onEnd the way a real drag would.
const test = require('node:test');
const assert = require('node:assert');
const { loadScript } = require('./load');

function fakeList(ids) {
    const classes = new Set();
    return {
        dataset: { reorderUrl: '/creator/?network=bm', reorderAction: 'reorder_things' },
        order: ids.slice(),
        classList: {
            add: (c) => classes.add(c),
            remove: (c) => classes.delete(c),
            contains: (c) => classes.has(c),
        },
        matches: (sel) => sel === '[data-sortable-list]',
        querySelector: () => null,
        querySelectorAll: () => [],
    };
}

function fakePage(lists, respond) {
    const loadListeners = [];
    const posts = [];
    const created = [];
    const window = {
        Sortable: {
            create(list, options) {
                const instance = {
                    options,
                    toArray: () => list.order.slice(),
                    sort: (order) => { list.order = order.slice(); },
                };
                created.push(list);
                return instance;
            },
        },
    };
    const document = {
        cookie: 'csrftoken=tok',
        querySelector: () => null,
        querySelectorAll: () => lists,
        addEventListener: (type, fn) => { if (type === 'htmx:load') loadListeners.push(fn); },
    };
    loadScript('sortable_list.js', {
        window, document, URLSearchParams,
        fetch: (url, init) => {
            posts.push({ url, body: init.body.toString(), csrf: init.headers['X-CSRFToken'] });
            return Promise.resolve({ ok: respond(), status: respond() ? 204 : 400 });
        },
    });
    return {
        posts,
        created,
        swapIn: (target) => loadListeners.forEach((fn) => fn({ target })),
        // Simulate a drag: the user moves items so the list reads `newOrder`.
        drag: async (list, newOrder) => {
            const { options } = list._sortable;
            options.onStart();
            list.order = newOrder;
            options.onEnd({ oldIndex: 0, newIndex: 1 });
            await new Promise((r) => setImmediate(r));
        },
    };
}

test('posts the full new order with the action and CSRF token', async () => {
    const list = fakeList(['1', '2', '3']);
    const page = fakePage([list], () => true);
    await page.drag(list, ['3', '1', '2']);
    assert.deepStrictEqual(page.posts, [{
        url: '/creator/?network=bm',
        body: 'action=reorder_things&order=3%2C1%2C2',
        csrf: 'tok',
    }]);
    assert.deepStrictEqual(list.order, ['3', '1', '2']);
    assert.strictEqual(list.classList.contains('sortable-failed'), false);
});

test('a rejected reorder restores the previous order and flags the list', async () => {
    const list = fakeList(['1', '2', '3']);
    const page = fakePage([list], () => false);
    await page.drag(list, ['3', '1', '2']);
    assert.deepStrictEqual(list.order, ['1', '2', '3']);
    assert.strictEqual(list.classList.contains('sortable-failed'), true);
});

test('a drop in the same place posts nothing', () => {
    const list = fakeList(['1', '2']);
    const page = fakePage([list], () => true);
    list._sortable.options.onStart();
    list._sortable.options.onEnd({ oldIndex: 1, newIndex: 1 });
    assert.deepStrictEqual(page.posts, []);
});

test('binds lists that arrive later, and each list only once', () => {
    const early = fakeList(['1']);
    const page = fakePage([early], () => true);
    const late = fakeList(['2']);
    page.swapIn(late);
    page.swapIn(late);
    page.swapIn({ querySelectorAll: () => [early] });
    assert.deepStrictEqual(page.created, [early, late]);
});
