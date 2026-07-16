// Runs on every load of the Creator Settings page — including htmx boosted
// swaps, which re-execute this file. Everything lives in one IIFE so re-runs
// can't collide with earlier ones; functions used by inline on* handlers are
// exported on window at the bottom.
(function () {

// ==========================================
// MERGE DESK LOGIC
// ==========================================
function checkMergeState() {
    const pubSelected = document.querySelector('input[name="public_episode_id"]:checked');
    const privSelected = document.querySelector('input[name="private_episode_id"]:checked');
    const mergeBtn = document.getElementById('btn-execute-merge');
    if (mergeBtn) mergeBtn.disabled = !(pubSelected && privSelected);
}

// ==========================================
// THEME CONFIG VALIDATION
// ==========================================
const networkForm = document.getElementById('networkSettingsForm');
if (networkForm) {
    networkForm.addEventListener('submit', function(e) {
        const jsonTextarea = document.querySelector('textarea[name="theme_config"]');
        if (jsonTextarea && jsonTextarea.value.trim() !== '') {
            try {
                JSON.parse(jsonTextarea.value);
            } catch (error) {
                e.preventDefault();
                alert("Invalid JSON in Theme Config:\n\n" + error.message + "\n\nPlease fix any formatting errors before saving.");
                jsonTextarea.focus();
            }
        }
    });
}

// ==========================================
// LIVE IMPORT STREAMING (polling, not SSE — more reliable behind gunicorn/Traefik)
// ==========================================
function importCsrfToken() {
    const el = document.querySelector('[name=csrfmiddlewaretoken]');
    if (el) return el.value;
    const cookie = document.cookie.split(';').find(c => c.trim().startsWith('csrftoken='));
    return cookie ? cookie.split('=')[1] : '';
}

function startLiveImport(showId) {
    const btn = document.getElementById(`btn-import-${showId}`);
    const terminalContainer = document.getElementById(`terminal-container-${showId}`);
    const terminal = document.getElementById(`terminal-${showId}`);

    // UI Updates
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Running...';
    terminalContainer.classList.remove('d-none');
    terminal.textContent = ''; // Clear previous logs

    function complete() {
        btn.innerHTML = '<i class="bi bi-check-circle-fill"></i> Import Complete';
        btn.classList.replace('btn-success', 'btn-outline-success');
        setTimeout(() => {
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-cloud-arrow-down-fill"></i> Run Live Import';
            btn.classList.replace('btn-outline-success', 'btn-success');
        }, 3000);
    }
    function fail(msg) {
        terminal.textContent += '\n' + msg + '\n';
        terminal.scrollTop = terminal.scrollHeight;
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-cloud-arrow-down-fill"></i> Retry Import';
    }

    // The buffer is SSE-framed (data: <line>\n\n); parse complete frames client-side,
    // keeping a remainder across polls so a split frame never corrupts a line.
    let remainder = '';
    let sawDone = false;
    function consume(raw) {
        remainder += raw;
        const parts = remainder.split('\n\n');
        remainder = parts.pop();
        parts.forEach(part => {
            if (part.indexOf('data: ') === 0) {
                const line = part.slice(6);
                if (line === '[DONE]') { sawDone = true; return; }
                terminal.textContent += line + '\n';
            } else if (part.trim()) {
                terminal.textContent += part + '\n';
            }
        });
        terminal.scrollTop = terminal.scrollHeight;
    }

    let offset = 0;
    function poll() {
        fetch(`/import/poll/${showId}/?offset=${offset}`, { headers: { 'X-Requested-With': 'XMLHttpRequest' } })
            .then(r => r.json())
            .then(data => {
                if (data.chunk) { offset = data.offset; consume(data.chunk); }
                if (sawDone || data.done) { complete(); return; }
                setTimeout(poll, 1500);
            })
            .catch(() => fail('[CONNECTION ERROR] Lost connection to server.'));
    }

    fetch(`/import/start/${showId}/`, {
        method: 'POST',
        headers: { 'X-CSRFToken': importCsrfToken(), 'X-Requested-With': 'XMLHttpRequest' },
    })
        .then(r => r.json().then(d => ({ ok: r.ok, d })))
        .then(({ ok, d }) => {
            if (!ok) { fail('[ERROR] ' + (d.error || 'Could not start import.')); return; }
            poll();
        })
        .catch(() => fail('[ERROR] Could not start import.'));
}

// ==========================================
// TAB PERSISTENCE & AUTO-ACTIONS
// ==========================================
const TAB_PARAM_MAP = {
    'network':      '#list-networks',
    'shows':        '#list-shows',
    'mixes':        '#list-mixes',
    'merge':        '#list-merge',
    'move':         '#list-move',
    'inbox':        '#list-inbox',
    'audit':        '#list-audit',
    'sync':         '#list-sync',
    'gdrive':       '#list-gdrive-recovery',
    'transcripts':  '#list-transcripts',
};
const TAB_ID_MAP = Object.fromEntries(Object.entries(TAB_PARAM_MAP).map(([k, v]) => [v, k]));

(function () {
    const urlParams = new URLSearchParams(window.location.search);
    let activeTabId = null;

    // 1. Force specific tabs based on URL context clues
    if (urlParams.has('merge_view')) {
        activeTabId = '#list-merge';
    } else if (urlParams.has('auto_import') || urlParams.has('show_q') || urlParams.has('show_sort') || urlParams.has('show_mix')) {
        activeTabId = '#list-shows';
    } else if (urlParams.get('tab')) {
        activeTabId = TAB_PARAM_MAP[urlParams.get('tab')] || null;
    } else if (window.location.hash) {
        activeTabId = window.location.hash;
    }
    // No sessionStorage — Network tab is the default (active in HTML)

    // 2. Activate the correct tab instantly
    if (activeTabId) {
        const triggerEl = document.querySelector(`a[href="${activeTabId}"][data-bs-toggle="list"]`);
        if (triggerEl) {
            const tab = new bootstrap.Tab(triggerEl);
            tab.show();
        }
    }

    // 3. When the user clicks a tab, update ?tab=X in the URL so any form
    //    submission on that tab will redirect back to the same tab.
    const tabElements = document.querySelectorAll('a[data-bs-toggle="list"]');
    tabElements.forEach(el => {
        el.addEventListener('shown.bs.tab', function (event) {
            const targetHref = event.target.getAttribute('href');
            const tabParam = TAB_ID_MAP[targetHref] || targetHref.replace('#list-', '');
            const params = new URLSearchParams(window.location.search);
            params.set('tab', tabParam);
            history.replaceState(null, null, window.location.pathname + '?' + params.toString() + targetHref);
        });
    });

    // 4. Trigger auto-import if flagged in URL
    const autoImportId = urlParams.get('auto_import');
    if (autoImportId) {
        setTimeout(() => {
            const collapseElement = document.getElementById(`collapse-${autoImportId}`);
            if (collapseElement) {
                const bsCollapse = new bootstrap.Collapse(collapseElement, {toggle: false});
                bsCollapse.show();
                startLiveImport(autoImportId);
                window.history.replaceState({}, document.title, window.location.pathname + "?network=" + urlParams.get('network'));
            }
        }, 500);
    }
})();

// ==========================================
// FEED VISIBILITY & CROSS-PUBLISH (hidden-but-not-cross-published warning)
// ==========================================
function refreshHiddenWarning(form) {
    if (!form) return;
    const toggle = form.querySelector('.cp-hidden-toggle');
    const warning = form.querySelector('.cp-hidden-warning');
    if (!toggle || !warning) return;
    const hasTargets = form.querySelectorAll('input[name="auto_crosspublish_target_ids"]:checked').length > 0;
    warning.classList.toggle('d-none', !toggle.checked || hasTargets);
}

function refreshAllHiddenWarnings() {
    const accordion = document.getElementById('showsAccordion');
    if (!accordion) return;
    accordion.querySelectorAll('form').forEach(refreshHiddenWarning);
}

// Bind the Manage Podcasts accordion. Idempotent (guarded by a dataset flag)
// and re-callable: the tab is lazy-loaded, so #showsAccordion may not exist at
// page load and only arrives via htmx:load.
function initShowsAccordion() {
    const accordion = document.getElementById('showsAccordion');
    if (!accordion || accordion.dataset.accordionInit) return;
    accordion.dataset.accordionInit = '1';

    accordion.addEventListener('change', (e) => {
        if (e.target.matches('.cp-hidden-toggle') || e.target.matches('input[name="auto_crosspublish_target_ids"]')) {
            refreshHiddenWarning(e.target.closest('form'));
        }
    });

    // Bring the expanded show's header row to the top of the viewport (just
    // below the sticky nav). window.scrollTo clamps to the max scroll, so near
    // the page bottom it simply lands as close to the top as it can.
    accordion.addEventListener('shown.bs.collapse', (e) => {
        const item = e.target.closest('.accordion-item');
        if (!item) return;
        const nav = document.querySelector('.sticky-top');
        const offset = (nav ? nav.offsetHeight : 0) + 8;
        const y = item.getBoundingClientRect().top + window.pageYOffset - offset;
        window.scrollTo({ top: Math.max(0, y), behavior: 'smooth' });
    });

    refreshAllHiddenWarnings();
}

initShowsAccordion();

// The Manage Podcasts tab and each show form arrive later via htmx: (re)bind the
// accordion, and refresh any freshly-swapped show form's hidden-feed warning.
// VectoPage.on tears the listener down before the next boosted swap.
if (window.VectoPage) {
    window.VectoPage.on(document.body, 'htmx:load', (e) => {
        initShowsAccordion();
        if (e.target && e.target.querySelectorAll) {
            e.target.querySelectorAll('form').forEach(refreshHiddenWarning);
        }
    });
}

// ==========================================
// LIVE FILTERING (AJAX)
// ==========================================
let filterTimeout = null;

function applyLiveFilter() {
    clearTimeout(filterTimeout);

    const accordion = document.getElementById('showsAccordion');
    accordion.style.opacity = '0.5'; // Dim slightly to indicate loading

    // 300ms debounce so we don't spam requests on every single keystroke
    filterTimeout = setTimeout(() => {
        const form = document.getElementById('manage-podcasts-form');
        const params = new URLSearchParams(new FormData(form));

        // The Manage Podcasts tab is lazy — its accordion lives in the shows
        // partial now, not the full page — so fetch that fragment directly.
        fetch('/creator/tab/shows/?' + params.toString(), { headers: { 'HX-Request': 'true' } })
            .then(response => response.text())
            .then(html => {
                const parser = new DOMParser();
                const doc = parser.parseFromString(html, 'text/html');

                const newAccordion = doc.getElementById('showsAccordion');
                if (newAccordion) {
                    accordion.innerHTML = newAccordion.innerHTML;
                    // Assigning innerHTML does NOT register hx-* attrs — htmx only
                    // scans content it swapped in itself. Without this every row in
                    // a filtered list is inert: expanding a show never fetches its
                    // form (spinner forever) and "Load more shows" does nothing.
                    if (window.htmx) window.htmx.process(accordion);
                }

                refreshAllHiddenWarnings();
                accordion.style.opacity = '1';
                // Mirror the filter into the address bar so a reload restores it.
                window.history.replaceState({}, '', window.location.pathname + '?' + params.toString() + '&tab=shows#list-shows');
            })
            .catch(error => {
                console.error("Live filter failed:", error);
                accordion.style.opacity = '1';
            });
    }, 300);
}

window.checkMergeState = checkMergeState;
window.startLiveImport = startLiveImport;
window.applyLiveFilter = applyLiveFilter;

})();
