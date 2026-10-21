/*
 * Drag-to-reorder for server-rendered lists, on top of vendored SortableJS.
 *
 * Usage: mark the list container and give each item a data-id:
 *
 *   <div data-sortable-list
 *        data-reorder-url="/creator/?network=slug"
 *        data-reorder-action="reorder_support_links">
 *     <div data-id="3"> <i data-sortable-handle class="bi bi-grip-vertical"></i> ...</div>
 *   </div>
 *
 * On drop it POSTs action=<data-reorder-action> and order=<comma-separated ids>
 * (the full list, in the new order) to data-reorder-url. The server answers
 * 204; anything else puts the list back the way it was and flags it with
 * .sortable-failed. A [data-sortable-handle] inside an item, when present, is
 * the only grab point, so text inside the item stays selectable.
 *
 * Loaded ONCE per full page load from base.html, outside the boosted region,
 * like confirm_slide.js: one persistent htmx:load listener with a bind guard
 * (NOT VectoPage.on, whose region-swap cleanup would remove a listener this
 * script never re-registers). htmx:load fires for every swapped-in fragment,
 * lazy creator tabs included, so lists bind whenever they arrive.
 */
(function () {
    'use strict';
    if (window.__sortableListBound) return;
    window.__sortableListBound = true;

    function csrfToken() {
        var input = document.querySelector('[name=csrfmiddlewaretoken]');
        if (input) return input.value;
        var cookie = document.cookie.split(';').find(function (c) {
            return c.trim().startsWith('csrftoken=');
        });
        return cookie ? cookie.split('=')[1] : '';
    }

    function bind(list) {
        if (list._sortable || !window.Sortable) return;
        var handle = list.querySelector('[data-sortable-handle]') ? '[data-sortable-handle]' : null;
        list._sortable = window.Sortable.create(list, {
            handle: handle,
            animation: 150,
            ghostClass: 'sortable-ghost',
            onStart: function () {
                list._orderBefore = list._sortable.toArray();
            },
            onEnd: function (evt) {
                if (evt.oldIndex === evt.newIndex) return;
                var body = new URLSearchParams({
                    action: list.dataset.reorderAction,
                    order: list._sortable.toArray().join(','),
                });
                fetch(list.dataset.reorderUrl, {
                    method: 'POST',
                    headers: { 'X-CSRFToken': csrfToken() },
                    body: body,
                }).then(function (resp) {
                    if (!resp.ok) throw new Error('reorder rejected: ' + resp.status);
                    list.classList.remove('sortable-failed');
                }).catch(function () {
                    list._sortable.sort(list._orderBefore, true);
                    list.classList.add('sortable-failed');
                });
            },
        });
    }

    function bindWithin(root) {
        if (root.matches && root.matches('[data-sortable-list]')) bind(root);
        if (root.querySelectorAll) root.querySelectorAll('[data-sortable-list]').forEach(bind);
    }

    bindWithin(document);
    document.addEventListener('htmx:load', function (evt) { bindWithin(evt.target); });
})();
