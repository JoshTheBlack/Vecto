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
document.addEventListener('DOMContentLoaded', function() {
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
});

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

function startAllLiveImports() {
    const importButtons = document.querySelectorAll('[id^="btn-import-"]');
    importButtons.forEach(btn => {
        if(!btn.disabled) {
            const showId = btn.id.replace('btn-import-', '');
            const collapseElement = document.getElementById(`collapse-${showId}`);
            if (collapseElement && !collapseElement.classList.contains('show')) {
                const bsCollapse = new bootstrap.Collapse(collapseElement, {toggle: false});
                bsCollapse.show();
            }
            startLiveImport(showId);
        }
    });
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

document.addEventListener('DOMContentLoaded', function() {
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
});

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
        const url = new URL(window.location.pathname, window.location.origin);
        
        // Grab all inputs from the form automatically
        const params = new URLSearchParams(new FormData(form));
        
        fetch(url.pathname + '?' + params.toString())
            .then(response => response.text())
            .then(html => {
                const parser = new DOMParser();
                const doc = parser.parseFromString(html, 'text/html');
                
                // Extract just the new accordion HTML and replace the old one
                const newAccordion = doc.getElementById('showsAccordion');
                if (newAccordion) {
                    accordion.innerHTML = newAccordion.innerHTML;
                }
                
                accordion.style.opacity = '1';
                // Silently update the URL bar so browser back-buttons still work
                window.history.replaceState({}, '', url.pathname + '?' + params.toString() + '#list-shows');
            })
            .catch(error => {
                console.error("Live filter failed:", error);
                accordion.style.opacity = '1';
            });
    }, 300); 
}
