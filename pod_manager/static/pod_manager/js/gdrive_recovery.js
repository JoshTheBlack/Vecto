// ============================================================
// GDrive Audio Recovery Tab
// ============================================================

(function () {
    'use strict';

    // ── State ──────────────────────────────────────────────────
    let activeStreams = {};  // run_id → poll controller ({ stop })
    let runLogs = {};        // run_id → captured log text

    // ── Bootstrap ──────────────────────────────────────────────
    document.addEventListener('DOMContentLoaded', () => {
        const tab = document.getElementById('list-gdrive-recovery-list');
        if (!tab) return;

        // Load on subsequent tab activations
        tab.addEventListener('shown.bs.tab', () => {
            if (!document.getElementById('recovery-file-list').dataset.loaded) {
                recoveryRefresh();
            }
        });

        // creator_tabs.js fires shown.bs.tab during its own DOMContentLoaded handler
        // (before ours runs), so we check whether the pane is already active here.
        const pane = document.getElementById('list-gdrive-recovery');
        if (pane && pane.classList.contains('active') &&
                !document.getElementById('recovery-file-list').dataset.loaded) {
            recoveryRefresh();
        }
    });

    // ── Public: triggered by toolbar Refresh button ─────────────
    window.recoveryRefresh = function () {
        const container = document.getElementById('recovery-file-list');
        container.innerHTML = `
            <div class="text-center text-muted py-4">
                <div class="spinner-border spinner-border-sm me-2" role="status"></div>
                Loading recovery files…
            </div>`;

        return fetch('/creator/gdrive-recovery/files/')
            .then(r => r.json())
            .then(data => {
                renderFileList(data.files || []);
                const ts = new Date().toLocaleTimeString();
                const el = document.getElementById('recovery-last-refreshed');
                if (el) el.textContent = `Last refreshed: ${ts}`;
                container.dataset.loaded = '1';
            })
            .catch(() => {
                container.innerHTML = '<p class="text-danger">Failed to load recovery files.</p>';
            });
    };

    // ── Render the full file list ───────────────────────────────
    function renderFileList(files) {
        const container = document.getElementById('recovery-file-list');
        if (!files.length) {
            container.innerHTML = `
                <div class="text-muted small border border-secondary rounded p-3">
                    No CSV files found in <code>MEDIA_ROOT/Recovery/</code>.
                    Upload input CSVs there to get started.
                </div>`;
            return;
        }

        container.innerHTML = files.map(f => buildCsvCard(f)).join('');

        // Collect saved logs and apply initial "All Podcasts" toggle state
        files.forEach(f => {
            (f.runs || []).forEach(r => { if (r.log) runLogs[r.run_id] = r.log; });
            const cardId = cssId(f.filename);
            const allCheck = document.getElementById(`all-pods-${cardId}`);
            if (allCheck) recoveryToggleAll(cardId, allCheck.checked);
        });
    }

    // ── Build one CSV card ──────────────────────────────────────
    function buildCsvCard(file) {
        const cardId = cssId(file.filename);
        const podcasts = (window.RECOVERY_PODCASTS || []);
        const podCheckboxes = podcasts.map(p => {
            const s3Badge = p.s3_count > 0
                ? `<span class="badge bg-warning text-dark ms-1" title="${p.s3_count} episodes still on S3">${p.s3_count}</span>`
                : (p.s3_count === 0 ? `<span class="badge bg-success ms-1" title="No episodes on S3">✓</span>` : '');
            return `
            <div class="form-check">
                <input class="form-check-input pod-check-${cardId}" type="checkbox"
                       value="${escHtml(p.title)}" id="pod-${cardId}-${p.id}">
                <label class="form-check-label small d-flex align-items-center justify-content-between pe-1" for="pod-${cardId}-${p.id}">
                    <span>${escHtml(p.title)}</span>${s3Badge}
                </label>
            </div>`;
        }).join('');

        const runsHtml = buildRunsTable(file.runs || [], file.filename);

        // Header summary: entry count + run info
        const runs = file.runs || [];
        const runCount = runs.length;
        const lastRun = runCount > 0 ? runs[0] : null;
        const totalEntries = file.total_entries || 0;

        const entriesChip = totalEntries > 0
            ? `<span class="badge bg-secondary me-2">${totalEntries} entries</span>`
            : '';

        let lastRunHtml = '';
        if (lastRun) {
            const when = lastRun.started_at
                ? new Date(lastRun.started_at + 'Z').toLocaleDateString()
                : '';
            const statusClass = lastRun.status === 'completed' ? 'run-status-completed'
                              : lastRun.status === 'failed'    ? 'run-status-failed'
                              : 'text-muted';
            lastRunHtml = `<span class="text-muted small me-2">${runCount} run${runCount !== 1 ? 's' : ''}</span>
                           <span class="small ${statusClass} me-1">${lastRun.mode} · ${lastRun.status} · ${when}</span>`;
        } else {
            lastRunHtml = `<span class="text-muted small me-2">no runs yet</span>`;
        }
        const summaryHtml = entriesChip + lastRunHtml;

        return `
<div class="recovery-csv-card" id="card-${cardId}">
    <div class="card-header d-flex align-items-center gap-2 recovery-card-toggle"
         role="button"
         data-bs-toggle="collapse"
         data-bs-target="#body-${cardId}"
         aria-expanded="false"
         aria-controls="body-${cardId}">
        <i class="bi bi-file-earmark-spreadsheet text-warning"></i>
        <strong class="me-auto">${escHtml(file.filename)}</strong>
        ${summaryHtml}
        <i class="bi bi-chevron-down recovery-chevron"></i>
    </div>
    <div class="collapse" id="body-${cardId}">
    <div class="p-3">
        <div class="row g-3">

            <!-- Left: podcast selector -->
            <div class="col-md-5">
                <div class="d-flex align-items-center gap-2 mb-1">
                    <span class="small fw-semibold">Target Podcasts</span>
                    <div class="form-check mb-0 ms-auto">
                        <input class="form-check-input all-pods-check" type="checkbox"
                               id="all-pods-${cardId}" checked
                               onchange="recoveryToggleAll('${cardId}', this.checked)">
                        <label class="form-check-label small" for="all-pods-${cardId}">All Podcasts</label>
                    </div>
                </div>
                <div class="recovery-podcast-list border border-secondary rounded p-2 ${podcasts.length === 0 ? 'd-none' : ''}"
                     id="pod-list-${cardId}">
                    ${podCheckboxes}
                </div>
                ${podcasts.length === 0 ? '<p class="text-muted small">No podcasts found.</p>' : ''}
            </div>

            <!-- Right: mode + run controls -->
            <div class="col-md-7">
                <div class="mb-2">
                    <span class="small fw-semibold d-block mb-1">Mode</span>
                    <div class="btn-group btn-group-sm" role="group">
                        <input type="radio" class="btn-check" name="mode-${cardId}"
                               id="mode-dry-${cardId}" value="dry" checked>
                        <label class="btn btn-outline-info" for="mode-dry-${cardId}">
                            <i class="bi bi-eye me-1"></i>Dry Run
                        </label>
                        <input type="radio" class="btn-check" name="mode-${cardId}"
                               id="mode-live-${cardId}" value="live">
                        <label class="btn btn-outline-warning" for="mode-live-${cardId}">
                            <i class="bi bi-lightning-fill me-1"></i>Live Run
                        </label>
                    </div>
                </div>

                <div class="mb-2">
                    <span class="small fw-semibold d-block mb-1">Min Confidence</span>
                    <div class="btn-group btn-group-sm" role="group">
                        <input type="radio" class="btn-check" name="conf-${cardId}"
                               id="conf-high-${cardId}" value="HIGH" checked>
                        <label class="btn btn-outline-secondary" for="conf-high-${cardId}">High</label>
                        <input type="radio" class="btn-check" name="conf-${cardId}"
                               id="conf-med-${cardId}" value="MEDIUM">
                        <label class="btn btn-outline-secondary" for="conf-med-${cardId}">Medium</label>
                        <input type="radio" class="btn-check" name="conf-${cardId}"
                               id="conf-low-${cardId}" value="LOW">
                        <label class="btn btn-outline-secondary" for="conf-low-${cardId}">Low</label>
                    </div>
                </div>

                <button class="btn btn-sm btn-primary mt-1"
                        onclick="recoveryStartRun('${cardId}', '${escHtml(file.filename)}')">
                    <i class="bi bi-play-fill me-1"></i>Run Recovery
                </button>
            </div>
        </div>

        <!-- Active stream panels injected here -->
        <div class="recovery-streams mt-3" id="streams-${cardId}"></div>

        <!-- Past runs table -->
        ${runsHtml}
    </div>
    </div>
</div>`;
    }

    // ── Podcast "All" toggle ────────────────────────────────────
    window.recoveryToggleAll = function (cardId, allChecked) {
        const podList = document.getElementById(`pod-list-${cardId}`);
        if (!podList) return;
        podList.style.opacity = allChecked ? '0.4' : '1';
        podList.style.pointerEvents = allChecked ? 'none' : '';
        if (allChecked) {
            podList.querySelectorAll(`input.pod-check-${cardId}`).forEach(cb => {
                cb.checked = false;
            });
        }
    };

    // ── Start a recovery run ────────────────────────────────────
    window.recoveryStartRun = function (cardId, csvFilename) {
        const allCheck = document.getElementById(`all-pods-${cardId}`);
        const isDryRun = document.querySelector(`input[name="mode-${cardId}"]:checked`).value === 'dry';
        const minConfidence = document.querySelector(`input[name="conf-${cardId}"]:checked`).value;

        let podcastTitles = [];
        if (!allCheck.checked) {
            document.querySelectorAll(`.pod-check-${cardId}:checked`).forEach(cb => {
                podcastTitles.push(cb.value);
            });
            if (!podcastTitles.length) {
                alert('Select at least one podcast, or check "All Podcasts".');
                return;
            }
        }

        fetch('/creator/gdrive-recovery/run/', {
            method: 'POST',
            headers: {'Content-Type': 'application/json', 'X-CSRFToken': getCsrf()},
            body: JSON.stringify({
                csv_filename: csvFilename,
                podcast_titles: podcastTitles,
                dry_run: isDryRun,
                min_confidence: minConfidence,
            }),
        })
        .then(r => r.json())
        .then(data => {
            if (data.error) { alert(data.error); return; }
            data.runs.forEach(r => openRunStream(cardId, r.run_id, r.podcast_title, isDryRun));
        })
        .catch(() => alert('Failed to start recovery run.'));
    };

    // ── Open an SSE stream and render a terminal panel ──────────
    function openRunStream(cardId, runId, podcastTitle, isDryRun) {
        const streamsDiv = document.getElementById(`streams-${cardId}`);
        if (!streamsDiv) return;

        // Clone panel template
        const tpl = document.getElementById('recovery-run-panel-tpl');
        const panel = tpl.content.cloneNode(true).firstElementChild;
        panel.dataset.runId = runId;

        const modeLabel = isDryRun ? 'Dry Run' : 'Live Run';
        panel.querySelector('.run-panel-label').textContent =
            `${modeLabel} → ${podcastTitle === 'all' ? 'All Podcasts' : podcastTitle}`;
        const badge = panel.querySelector('.run-panel-badge');
        badge.textContent = 'Running';
        badge.className = 'badge bg-warning text-dark';

        const terminal = panel.querySelector('.recovery-terminal');
        const dismissBtn = panel.querySelector('.run-panel-dismiss');
        streamsDiv.prepend(panel);

        // Dismiss: stop any active poll, remove panel, refresh list, then re-expand this card
        dismissBtn.addEventListener('click', () => {
            if (activeStreams[runId]) { activeStreams[runId].stop = true; delete activeStreams[runId]; }
            panel.remove();
            recoveryRefresh().then(() => {
                const newBody = document.getElementById(`body-${cardId}`);
                if (newBody) {
                    bootstrap.Collapse.getOrCreateInstance(newBody, {toggle: false}).show();
                    document.getElementById(`card-${cardId}`)
                        ?.scrollIntoView({behavior: 'smooth', block: 'nearest'});
                }
            });
        });

        // Poll the run's log buffer (polling, not SSE — more reliable behind
        // gunicorn/Traefik). The buffer is SSE-framed (data: <line>\n\n), so we parse
        // complete frames client-side and keep a remainder across polls.
        const ctl = { stop: false };
        activeStreams[runId] = ctl;
        let offset = 0;
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

        function pollRun() {
            if (ctl.stop) return;
            fetch(`/creator/gdrive-recovery/poll/${runId}/?offset=${offset}`, {
                headers: { 'X-Requested-With': 'XMLHttpRequest' },
            })
                .then(r => r.json())
                .then(data => {
                    if (ctl.stop) return;
                    if (data.chunk) { offset = data.offset; consume(data.chunk); }
                    if (sawDone || data.done) {
                        delete activeStreams[runId];
                        badge.textContent = 'Complete — dismiss to refresh table';
                        badge.className = 'badge bg-success';
                        return;
                    }
                    setTimeout(pollRun, 1500);
                })
                .catch(() => {
                    delete activeStreams[runId];
                    terminal.textContent += '\n[CONNECTION ERROR] Lost connection to server.\n';
                    badge.textContent = 'Error';
                    badge.className = 'badge bg-danger';
                });
        }
        pollRun();
    }

    // ── Build past-runs table for one CSV ───────────────────────
    function buildRunsTable(runs, csvFilename) {
        if (!runs.length) return '';

        const rows = runs.map(r => {
            const modeClass = r.mode === 'dry-run' ? 'run-status-dry-run'
                            : r.mode === 'rewind'   ? 'text-muted'
                            : r.status === 'failed' ? 'run-status-failed'
                            : 'run-status-completed';
            const modeIcon = r.mode === 'dry-run' ? '👁' : r.mode === 'rewind' ? '↺' : '⚡';
            const when = r.started_at ? new Date(r.started_at + 'Z').toLocaleString() : '—';
            const target = r.podcast_title === 'all' ? '<em>All Podcasts</em>' : escHtml(r.podcast_title);

            // S3 before/after cell
            let s3Cell = '<span class="text-muted">—</span>';
            if (r.mode === 'live' && r.s3_before != null) {
                if (r.s3_after != null) {
                    const recovered = r.s3_before - r.s3_after;
                    const diffHtml = recovered > 0
                        ? ` <span class="run-status-completed">(−${recovered})</span>`
                        : '';
                    s3Cell = `${r.s3_before} → ${r.s3_after}${diffHtml}`;
                } else {
                    s3Cell = `${r.s3_before}`;
                }
            } else if (r.mode === 'dry-run' && r.s3_before != null) {
                const recoverHtml = r.would_recover != null
                    ? ` <span class="run-status-completed">· ${r.would_recover} recoverable</span>`
                    : '';
                s3Cell = `${r.s3_before}${recoverHtml}`;
            }

            const csvLink = r.recovery_csv_url
                ? `<a href="${escHtml(r.recovery_csv_url)}" class="btn btn-xs btn-outline-secondary py-0 px-1" title="Recovery CSV" target="_blank"><i class="bi bi-filetype-csv"></i></a>`
                : '';
            const discLink = r.discord_txt_url
                ? `<a href="${escHtml(r.discord_txt_url)}" class="btn btn-xs btn-outline-secondary py-0 px-1" title="Discord Report" target="_blank"><i class="bi bi-discord"></i></a>`
                : '';
            const logBtn = r.log
                ? `<button class="btn btn-xs btn-outline-info py-0 px-1" title="View log"
                           onclick="recoveryShowLog('${cssId(csvFilename)}', '${escHtml(r.run_id)}')">
                       <i class="bi bi-terminal"></i>
                   </button>`
                : '';

            const rewindBtn = (r.mode === 'live' && r.status === 'completed' && r.recovery_csv_url)
                ? `<button class="btn btn-xs btn-outline-danger py-0 px-1"
                           title="Rewind this run"
                           onclick="recoveryRewind('${escHtml(r.recovery_csv_url)}', '${cssId(csvFilename)}')">
                       <i class="bi bi-arrow-counterclockwise"></i>
                   </button>`
                : '';

            const confLabel = r.min_confidence && r.min_confidence !== 'HIGH'
                ? ` <span class="text-muted">· ${r.min_confidence.toLowerCase()}</span>` : '';

            return `<tr>
                <td class="text-muted" style="white-space:nowrap">${when}</td>
                <td>${target}</td>
                <td class="${modeClass}">${modeIcon} ${r.mode}${confLabel}</td>
                <td><span class="${modeClass}">${r.status}</span></td>
                <td style="white-space:nowrap">${s3Cell}</td>
                <td class="d-flex gap-1">${csvLink}${discLink}${logBtn}</td>
                <td>${rewindBtn}</td>
            </tr>`;
        }).join('');

        return `
<div class="mt-3">
    <p class="small fw-semibold text-muted mb-1">Past Runs</p>
    <div class="table-responsive">
        <table class="table table-sm table-dark table-hover recovery-runs-table mb-0">
            <thead><tr>
                <th>When</th><th>Podcast</th><th>Mode</th><th>Status</th><th>S3</th><th>Reports</th><th>Rewind</th>
            </tr></thead>
            <tbody>${rows}</tbody>
        </table>
    </div>
</div>`;
    }

    // ── Rewind a past live run ──────────────────────────────────
    window.recoveryRewind = function (recoveryCsvUrl, cardId) {
        if (!confirm('Rewind this recovery run? This will restore the original S3 URLs for these episodes.')) return;

        fetch('/creator/gdrive-recovery/rewind/', {
            method: 'POST',
            headers: {'Content-Type': 'application/json', 'X-CSRFToken': getCsrf()},
            body: JSON.stringify({recovery_csv_url: recoveryCsvUrl}),
        })
        .then(r => r.json())
        .then(data => {
            if (data.error) { alert(data.error); return; }
            openRunStream(cardId, data.run_id, 'all', false);
        })
        .catch(() => alert('Failed to start rewind.'));
    };

    // ── Replay a saved run log ──────────────────────────────────
    window.recoveryShowLog = function (cardId, runId) {
        const log = runLogs[runId];
        if (!log) return;

        const streamsDiv = document.getElementById(`streams-${cardId}`);
        if (!streamsDiv) return;

        // Don't duplicate an open panel for the same run
        if (streamsDiv.querySelector(`[data-run-id="${runId}"]`)) return;

        const tpl = document.getElementById('recovery-run-panel-tpl');
        const panel = tpl.content.cloneNode(true).firstElementChild;
        panel.dataset.runId = runId;
        panel.querySelector('.run-panel-label').textContent = 'Saved Log';
        const badge = panel.querySelector('.run-panel-badge');
        badge.textContent = 'Replay';
        badge.className = 'badge bg-secondary';

        const terminal = panel.querySelector('.recovery-terminal');
        terminal.textContent = log;

        panel.querySelector('.run-panel-dismiss').addEventListener('click', () => panel.remove());

        streamsDiv.prepend(panel);
        terminal.scrollTop = terminal.scrollHeight;
    };

    // ── Helpers ─────────────────────────────────────────────────
    function cssId(str) {
        return str.replace(/[^a-zA-Z0-9]/g, '_');
    }

    function escHtml(str) {
        return String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function getCsrf() {
        const el = document.querySelector('[name=csrfmiddlewaretoken]');
        if (el) return el.value;
        const cookie = document.cookie.split(';').find(c => c.trim().startsWith('csrftoken='));
        return cookie ? cookie.trim().split('=')[1] : '';
    }

})();
