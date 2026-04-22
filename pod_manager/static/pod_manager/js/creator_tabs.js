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
// LIVE IMPORT STREAMING
// ==========================================
function startLiveImport(showId) {
    const btn = document.getElementById(`btn-import-${showId}`);
    const terminalContainer = document.getElementById(`terminal-container-${showId}`);
    const terminal = document.getElementById(`terminal-${showId}`);
    
    // UI Updates
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Running...';
    terminalContainer.classList.remove('d-none');
    terminal.textContent = ''; // Clear previous logs
    
    // Open Server-Sent Events Connection
    const eventSource = new EventSource(`/import/stream/${showId}/`);
    
    let isDone = false;
    
    eventSource.onmessage = function(event) {
        if (event.data === '[DONE]') {
            isDone = true; 
            eventSource.close();
            btn.innerHTML = '<i class="bi bi-check-circle-fill"></i> Import Complete';
            btn.classList.replace('btn-success', 'btn-outline-success');
            setTimeout(() => { 
                btn.disabled = false; 
                btn.innerHTML = '<i class="bi bi-cloud-arrow-down-fill"></i> Run Live Import';
                btn.classList.replace('btn-outline-success', 'btn-success');
            }, 3000);
        } else {
            terminal.textContent += event.data + '\n';
            terminal.scrollTop = terminal.scrollHeight;
        }
    };

    eventSource.onerror = function() {
        if (isDone) return; 
        
        terminal.textContent += '\n[CONNECTION ERROR] Lost connection to server.\n';
        eventSource.close();
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-cloud-arrow-down-fill"></i> Retry Import';
    };
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
document.addEventListener('DOMContentLoaded', function() {
    const urlParams = new URLSearchParams(window.location.search);
    let activeTabId = sessionStorage.getItem('activeCreatorTab');

    // 1. Force specific tabs based on URL context clues
    if (urlParams.has('merge_view')) {
        activeTabId = '#list-merge';
    } else if (urlParams.has('auto_import') || urlParams.has('show_q') || urlParams.has('show_sort') || urlParams.has('show_mix')) {
        activeTabId = '#list-shows';
    } else if (window.location.hash) {
        activeTabId = window.location.hash;
    }

    // 2. Activate the correct tab instantly
    if (activeTabId) {
        const triggerEl = document.querySelector(`a[href="${activeTabId}"][data-bs-toggle="list"]`);
        if (triggerEl) {
            const tab = new bootstrap.Tab(triggerEl);
            tab.show();
        }
    }

    // 3. Remember the tab if the user clicks one manually
    const tabElements = document.querySelectorAll('a[data-bs-toggle="list"]');
    tabElements.forEach(el => {
        el.addEventListener('shown.bs.tab', function (event) {
            const targetHref = event.target.getAttribute('href');
            sessionStorage.setItem('activeCreatorTab', targetHref);
            history.replaceState(null, null, window.location.pathname + window.location.search + targetHref);
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
