/*
 * Shared tag pill-input widget — the Enter/comma-to-add, backspace-to-pop,
 * paste-a-csv tag editor used on the publish and episode-detail forms, now
 * extracted so new consumers (the merge editor) reuse it instead of growing
 * a fourth inline copy. Styling rides the existing .tag-input-wrapper /
 * .tag-pill rules (each consumer page carries them).
 *
 * Usage:
 *   var ed = window.TagEditor.mount(wrapperEl, inputEl, initialTagsArray);
 *   ed.get()  -> current tags (commits any half-typed input first)
 *   ed.set(a) -> replace the tag list
 *
 * Pure element-scoped listeners — nothing on document/window, so it needs no
 * VectoPage cleanup and re-executes safely on boosted swaps.
 * NOTE: publish_episode.html / episode_detail.html / tab_inbox.html still run
 * their own older inline copies; converting them here is a follow-up.
 */
(function () {
    'use strict';

    function mount(wrapper, input, initial) {
        var tags = Array.isArray(initial) ? initial.slice() : [];

        function render() {
            wrapper.querySelectorAll('.tag-pill').forEach(function (p) { p.remove(); });
            tags.forEach(function (tag, idx) {
                var pill = document.createElement('span');
                pill.className = 'tag-pill';
                pill.innerHTML = '<span class="tag-text"></span><button type="button" class="tag-remove" title="Remove tag">&times;</button>';
                pill.querySelector('.tag-text').textContent = tag;
                pill.querySelector('.tag-remove').addEventListener('click', function () {
                    tags.splice(idx, 1);
                    render();
                });
                wrapper.insertBefore(pill, input);
            });
        }

        function commit() {
            var raw = input.value.trim().replace(/,$/, '').trim();
            if (!raw) return;
            raw.split(',').map(function (t) { return t.trim(); }).filter(Boolean)
                .forEach(function (t) { if (tags.indexOf(t) === -1) tags.push(t); });
            input.value = '';
            render();
        }

        input.addEventListener('keydown', function (e) {
            if (e.key === 'Enter' || e.key === ',') { e.preventDefault(); commit(); }
            else if (e.key === 'Backspace' && !input.value && tags.length) { tags.pop(); render(); }
        });
        input.addEventListener('blur', commit);
        wrapper.addEventListener('click', function (e) { if (e.target === wrapper) input.focus(); });
        render();

        return {
            get: function () { commit(); return tags.slice(); },
            set: function (next) { tags = Array.isArray(next) ? next.slice() : []; render(); },
        };
    }

    window.TagEditor = { mount: mount };
})();
