/* Admin Command Console frontend (design §10).
 *
 * Data-driven: there is no per-command template. The detail pane, generated form,
 * live command-line builder, primary action, and run history are all rendered from
 * the JSON the §9 endpoints return. Log delivery is POLLING (run_poll), not SSE (§8).
 */
(function () {
    "use strict";

    const CFG = window.ADMIN_CONSOLE || {};
    const BASE = CFG.base || "/admin-console/";
    const CSRF = CFG.csrfToken;

    // ── URL builders ──────────────────────────────────────────────
    const url = {
        detail: (name) => `${BASE}command/${encodeURIComponent(name)}/`,
        build: (name) => `${BASE}command/${encodeURIComponent(name)}/build/`,
        run: (name) => `${BASE}command/${encodeURIComponent(name)}/run/`,
        poll: (id) => `${BASE}run/${id}/poll/`,
        cancel: (id) => `${BASE}run/${id}/cancel/`,
        runDetail: (id) => `${BASE}run/${id}/`,
        history: () => `${BASE}runs/`,
        episodes: () => `${BASE}lookup/episodes/`,
    };

    // ── tiny helpers ──────────────────────────────────────────────
    const $ = (sel, root = document) => root.querySelector(sel);
    const el = (tag, cls, txt) => {
        const n = document.createElement(tag);
        if (cls) n.className = cls;
        if (txt != null) n.textContent = txt;
        return n;
    };
    const esc = (s) => String(s == null ? "" : s);

    function getJSON(u) {
        return fetch(u, { headers: { "X-Requested-With": "XMLHttpRequest" } })
            .then((r) => r.json().then((d) => ({ ok: r.ok, status: r.status, data: d })));
    }
    function postJSON(u, body) {
        return fetch(u, {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-CSRFToken": CSRF },
            body: JSON.stringify(body),
        }).then((r) => r.json().then((d) => ({ ok: r.ok, status: r.status, data: d })));
    }
    function debounce(fn, ms) {
        let t = null;
        return function (...a) {
            clearTimeout(t);
            t = setTimeout(() => fn.apply(this, a), ms);
        };
    }

    function fmtTime(iso) {
        if (!iso) return "—";
        try {
            return new Date(iso).toLocaleString([], {
                month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
            });
        } catch (e) { return iso; }
    }
    function fmtDuration(secs) {
        if (secs == null) return "";
        if (secs < 60) return secs.toFixed(1) + "s";
        const m = Math.floor(secs / 60), s = Math.round(secs % 60);
        return `${m}m${s}s`;
    }
    function statusBadge(status) {
        const b = el("span", "ac-status-badge ac-status-" + (status || "queued"), status || "—");
        return b;
    }

    // ── State ─────────────────────────────────────────────────────
    let currentName = null;     // selected command
    let currentSchema = null;   // its detail schema
    let formState = {};         // dest -> value (string | bool | array)
    let poll = { id: null, offset: 0, timer: null, remainder: "", lastGrowth: 0, done: false };

    // ── Sidebar ───────────────────────────────────────────────────
    const sidebar = $("#ac-sidebar");
    const detailEmpty = $("#ac-detail-empty");
    const detailBody = $("#ac-detail-body");

    sidebar.addEventListener("click", (e) => {
        const item = e.target.closest(".ac-cmd-item");
        if (!item) return;
        selectCommand(item.dataset.command, item);
    });

    $("#ac-filter").addEventListener("input", function () {
        const q = this.value.trim().toLowerCase();
        sidebar.querySelectorAll(".ac-cmd-item").forEach((it) => {
            it.style.display = it.dataset.command.includes(q) ? "" : "none";
        });
        sidebar.querySelectorAll(".ac-cat-group").forEach((g) => {
            const anyVisible = Array.from(g.querySelectorAll(".ac-cmd-item"))
                .some((it) => it.style.display !== "none");
            g.style.display = anyVisible ? "" : "none";
        });
    });

    function markActive(name) {
        sidebar.querySelectorAll(".ac-cmd-item").forEach((it) => {
            it.classList.toggle("ac-active", it.dataset.command === name);
        });
    }

    function selectCommand(name, itemEl) {
        currentName = name;
        markActive(name);
        if (!itemEl) {
            const found = sidebar.querySelector(`.ac-cmd-item[data-command="${name}"]`);
            if (found) found.classList.add("ac-active");
        }
        detailEmpty.hidden = true;
        detailBody.hidden = false;
        detailBody.innerHTML = '<div class="ac-empty"><i class="bi bi-hourglass-split"></i><p>Loading…</p></div>';
        getJSON(url.detail(name)).then(({ ok, data }) => {
            if (!ok) {
                detailBody.innerHTML = "";
                detailBody.appendChild(el("div", "ac-import-error", data.error || "Failed to load command."));
                return;
            }
            currentSchema = data;
            renderDetail(data, {});
        });
    }

    // ── Detail pane ───────────────────────────────────────────────
    function renderDetail(schema, prefill) {
        formState = {};
        detailBody.innerHTML = "";

        // Title + badges
        const title = el("div", "ac-cmd-title");
        title.appendChild(el("h5", null, schema.name));
        title.appendChild(el("span", "ac-badge ac-badge-cat", schema.category));
        if (schema.danger) title.appendChild(el("span", "ac-badge ac-badge-danger", "destructive"));
        if (!schema.runnable) title.appendChild(el("span", "ac-badge ac-badge-docs", schema.deep_link ? "deep-link" : "docs only"));
        detailBody.appendChild(title);

        if (schema.import_error) {
            detailBody.appendChild(el("div", "ac-import-error",
                "This command can't be introspected in this environment: " + schema.import_error));
            return;
        }

        // Docs (§6)
        if (schema.summary) detailBody.appendChild(el("p", "ac-summary", schema.summary));
        if (schema.long_doc) {
            detailBody.appendChild(el("div", "ac-section-label", "Documentation"));
            detailBody.appendChild(el("div", "ac-longdoc", schema.long_doc));
        }
        if (schema.examples && schema.examples.length) {
            detailBody.appendChild(el("div", "ac-section-label", "Examples"));
            const exWrap = el("div", "ac-longdoc", schema.examples.join("\n"));
            detailBody.appendChild(exWrap);
        }

        // Form
        if (schema.fields.length) {
            detailBody.appendChild(el("div", "ac-section-label", "Arguments"));
            const form = el("div", "ac-form");
            schema.fields.forEach((f) => form.appendChild(renderField(f, prefill)));
            detailBody.appendChild(form);
        } else {
            detailBody.appendChild(el("p", "ac-noaction", "This command takes no arguments."));
        }

        // Command-line builder (§5b)
        const builder = el("div", "ac-builder");
        builder.appendChild(el("div", "ac-section-label", "Command line"));
        const cl = el("div", "ac-cmdline-wrap");
        const cmdline = el("div", "ac-cmdline");
        cmdline.id = "ac-cmdline";
        const copyBtn = el("button", "btn btn-sm btn-outline-secondary");
        copyBtn.innerHTML = '<i class="bi bi-clipboard"></i>';
        copyBtn.title = "Copy";
        copyBtn.addEventListener("click", () => {
            const txt = cmdline.dataset.cmd || "";
            if (txt) navigator.clipboard && navigator.clipboard.writeText(txt);
        });
        cl.appendChild(cmdline);
        cl.appendChild(copyBtn);
        builder.appendChild(cl);
        detailBody.appendChild(builder);

        // Actions + danger gate
        detailBody.appendChild(renderActions(schema));

        // Recent runs (§8a)
        const runsWrap = el("div");
        runsWrap.appendChild(el("div", "ac-section-label", "Recent runs"));
        const runsList = el("div", "ac-runs");
        runsList.id = "ac-cmd-runs";
        renderRunList(runsList, schema.recent_runs || []);
        runsWrap.appendChild(runsList);
        detailBody.appendChild(runsWrap);

        refreshBuilder();
    }

    // ── Field rendering (semantic widgets, §5a) ───────────────────
    function renderField(f, prefill) {
        const wrap = el("div", "ac-field");
        const isDangerField = (currentSchema.danger_fields || []).includes(f.dest);
        if (isDangerField) wrap.classList.add("ac-field-danger");

        const widget = f.widget;
        const prefVal = prefill && Object.prototype.hasOwnProperty.call(prefill, f.dest) ? prefill[f.dest] : undefined;

        // Checkbox (flags) get an inline layout
        if (widget === "flag") {
            const row = el("label", "ac-check");
            const cb = el("input");
            cb.type = "checkbox";
            cb.className = "form-check-input";
            cb.checked = prefVal !== undefined ? !!prefVal : !!f.default;
            formState[f.dest] = cb.checked;
            cb.addEventListener("change", () => { formState[f.dest] = cb.checked; onFormChange(); });
            row.appendChild(cb);
            const lbl = el("span", "ac-field-label", f.label);
            if (isDangerField) lbl.appendChild(makeFlagTag(f));
            row.appendChild(lbl);
            wrap.appendChild(row);
            if (f.help) wrap.appendChild(el("div", "ac-field-help", f.help));
            return wrap;
        }

        // label row
        const labelRow = el("div", "ac-field-row");
        const label = el("span", "ac-field-label", f.label);
        if (f.required) label.appendChild(el("span", "ac-req", "*"));
        labelRow.appendChild(label);
        labelRow.appendChild(makeFlagTag(f));
        wrap.appendChild(labelRow);

        let control;
        const selectWidgets = ["network", "podcast", "choice", "csv_path"];
        const multiSelectWidgets = ["network_multi", "podcast_multi"];

        if (widget === "episode") {
            control = renderEpisodeTypeahead(f, prefVal);
        } else if (multiSelectWidgets.includes(widget) || (selectWidgets.includes(widget) && f.multi) || widget.indexOf("enum_multi") === 0) {
            control = renderMultiSelect(f, prefVal);
        } else if (selectWidgets.includes(widget) || widget.indexOf("enum:") === 0) {
            // Single-valued enum picker (e.g. enum:whisper_models) → dropdown.
            control = renderSelect(f, prefVal);
        } else if (widget === "number") {
            control = renderInput(f, "number", prefVal);
        } else {
            control = renderInput(f, "text", prefVal);
        }
        wrap.appendChild(control);
        if (f.help) wrap.appendChild(el("div", "ac-field-help", f.help));
        if (f.default != null && f.default !== "" && widget !== "flag") {
            wrap.appendChild(el("div", "ac-field-help", "Default: " + f.default));
        }
        return wrap;
    }

    function makeFlagTag(f) {
        const t = f.positional ? `${f.dest}` : (f.flags[0] || f.dest);
        return el("span", "ac-field-flag", t);
    }

    function renderInput(f, type, prefVal) {
        const inp = el("input", "form-control form-control-sm ac-control");
        inp.type = type;
        if (f.default != null && f.default !== "" && !f.positional) inp.placeholder = String(f.default);
        if (prefVal !== undefined && prefVal !== null) inp.value = prefVal;
        formState[f.dest] = inp.value;
        inp.addEventListener("input", () => { formState[f.dest] = inp.value; onFormChange(); });
        return inp;
    }

    function renderSelect(f, prefVal) {
        const sel = el("select", "form-select form-select-sm ac-control");
        const blank = el("option", null, f.required ? "— choose —" : "— (default) —");
        blank.value = "";
        sel.appendChild(blank);
        (f.options || []).forEach((o) => {
            const opt = el("option", null, o.label);
            opt.value = o.value;
            sel.appendChild(opt);
        });
        if (prefVal !== undefined && prefVal !== null) sel.value = prefVal;
        formState[f.dest] = sel.value;
        sel.addEventListener("change", () => { formState[f.dest] = sel.value; onFormChange(); });
        return sel;
    }

    function renderMultiSelect(f, prefVal) {
        // A checkbox list, not a native <select multiple>: the native control
        // replaces the whole selection on a plain click and needs ctrl/cmd-click to
        // deselect (undiscoverable). Checkboxes toggle independently and clear to
        // empty — which, for these scoping fields, means "all" (e.g. all podcasts).
        const pre = Array.isArray(prefVal) ? prefVal.map(String) : (prefVal != null ? [String(prefVal)] : []);
        const selected = new Set(pre);
        const options = f.options || [];
        const wrap = el("div", "ac-multiselect ac-control");

        const sync = () => { formState[f.dest] = Array.from(selected); onFormChange(); };

        options.forEach((o) => {
            const value = String(o.value);
            const row = el("label", "ac-multiselect-opt");
            const cb = el("input");
            cb.type = "checkbox";
            cb.value = value;
            cb.checked = selected.has(value);
            cb.addEventListener("change", () => {
                if (cb.checked) selected.add(value); else selected.delete(value);
                sync();
            });
            row.appendChild(cb);
            row.appendChild(el("span", null, o.label));
            wrap.appendChild(row);
        });

        if (!options.length) {
            wrap.appendChild(el("div", "ac-field-help", "No options available."));
        }

        formState[f.dest] = Array.from(selected);
        return wrap;
    }

    function renderEpisodeTypeahead(f, prefVal) {
        const wrap = el("div", "ac-typeahead");
        const inp = el("input", "form-control form-control-sm ac-control");
        inp.type = "text";
        inp.placeholder = "Search episodes by title, or paste an id…";
        const chip = el("div", "ac-field-help");
        const results = el("div", "ac-typeahead-results");
        results.hidden = true;

        function setValue(v, label) {
            formState[f.dest] = v;
            chip.textContent = label ? `Selected: ${label} (id ${v})` : (v ? `id ${v}` : "");
            onFormChange();
        }
        if (prefVal !== undefined && prefVal !== null && prefVal !== "") setValue(String(prefVal), null);

        const search = debounce(() => {
            const q = inp.value.trim();
            if (/^\d+$/.test(q)) { setValue(q, null); results.hidden = true; return; }
            if (!q) { results.hidden = true; return; }
            getJSON(url.episodes() + "?q=" + encodeURIComponent(q)).then(({ data }) => {
                results.innerHTML = "";
                (data.results || []).forEach((r) => {
                    const b = el("button", null);
                    b.type = "button";
                    b.innerHTML = `${esc(r.title)} <span class="ac-ta-podcast">${esc(r.podcast || "")}</span>`;
                    b.addEventListener("click", () => {
                        inp.value = r.title;
                        setValue(String(r.id), r.title);
                        results.hidden = true;
                    });
                    results.appendChild(b);
                });
                results.hidden = !results.children.length;
            });
        }, 300);
        inp.addEventListener("input", search);
        document.addEventListener("click", (e) => { if (!wrap.contains(e.target)) results.hidden = true; });

        wrap.appendChild(inp);
        wrap.appendChild(results);
        wrap.appendChild(chip);
        return wrap;
    }

    // ── Actions + danger gate (§5b / §11) ─────────────────────────
    function renderActions(schema) {
        const wrap = el("div", "ac-actions");
        wrap.id = "ac-actions";

        if (!schema.runnable) {
            if (schema.deep_link && schema.deep_link.url) {
                const a = el("a", "ac-btn-deeplink");
                a.href = schema.deep_link.url;
                a.innerHTML = `<i class="bi bi-box-arrow-up-right me-1"></i>${esc(schema.deep_link.label)}`;
                wrap.appendChild(a);
            } else {
                wrap.appendChild(el("div", "ac-noaction",
                    "This command is not runnable from the console — copy the command line above to run it in a terminal."));
            }
            return wrap;
        }

        // Danger gate (created up-front, shown/hidden dynamically)
        const gate = el("div", "ac-confirm-gate");
        gate.id = "ac-confirm-gate";
        gate.hidden = true;
        const gateLabel = el("label", null);
        gateLabel.innerHTML = `<i class="bi bi-shield-fill-exclamation me-1"></i>Destructive — type <code>${esc(schema.name)}</code> to confirm`;
        const gateInput = el("input", "form-control form-control-sm ac-control");
        gateInput.id = "ac-confirm-input";
        gateInput.placeholder = schema.name;
        gateInput.addEventListener("input", updateExecuteEnabled);
        gate.appendChild(gateLabel);
        gate.appendChild(gateInput);
        wrap.appendChild(gate);

        const btn = el("button", "btn ac-btn-execute");
        btn.id = "ac-execute";
        btn.innerHTML = '<i class="bi bi-play-fill me-1"></i>Execute';
        btn.addEventListener("click", execute);
        wrap.appendChild(btn);

        return wrap;
    }

    function dangerActiveNow() {
        if (!currentSchema) return false;
        const destructive = currentSchema.danger ||
            (currentSchema.danger_fields || []).some((d) => !!formState[d]);
        if (!destructive) return false;
        // A preview/dry-run mutates nothing — only require confirmation once the run
        // actually executes (--apply). If the command has no apply flag at all, fall
        // back to always confirming a destructive command.
        const hasApply = (currentSchema.fields || []).some((f) => f.dest === "apply");
        return hasApply ? !!formState.apply : true;
    }

    function updateExecuteEnabled() {
        const btn = $("#ac-execute");
        const gate = $("#ac-confirm-gate");
        if (!btn || !currentSchema || !currentSchema.runnable) return;
        const danger = dangerActiveNow();
        if (gate) gate.hidden = !danger;
        btn.classList.toggle("ac-execute-danger", danger);
        if (danger) {
            const input = $("#ac-confirm-input");
            btn.disabled = !input || input.value.trim() !== currentSchema.name;
        } else {
            btn.disabled = false;
        }
    }

    // ── Builder: refresh the copy box from form state (§5b) ───────
    const refreshBuilder = debounce(function () {
        if (!currentName) return;
        const cmdline = $("#ac-cmdline");
        if (!cmdline) return;
        postJSON(url.build(currentName), { fields: formState }).then(({ data }) => {
            if (data.valid && data.command_line) {
                cmdline.classList.remove("ac-invalid");
                cmdline.textContent = data.command_line;
                cmdline.dataset.cmd = data.command_line;
            } else {
                cmdline.classList.add("ac-invalid");
                cmdline.textContent = data.error || "Fill in the required fields to build the command line.";
                cmdline.dataset.cmd = "";
            }
        });
    }, 250);

    function onFormChange() {
        updateExecuteEnabled();
        refreshBuilder();
    }

    // ── Execute → run → poll (§7 / §8) ────────────────────────────
    function execute() {
        const btn = $("#ac-execute");
        const confirmInput = $("#ac-confirm-input");
        const body = { fields: formState };
        if (confirmInput) body.confirm = confirmInput.value.trim();
        btn.disabled = true;
        const orig = btn.innerHTML;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Dispatching…';
        postJSON(url.run(currentName), body).then(({ ok, data }) => {
            btn.innerHTML = orig;
            updateExecuteEnabled();
            if (!ok) {
                showLogPane(currentName);
                appendRawLine("[ERROR] " + (data.error || "Run rejected."), "error");
                setLogStatus("failed");
                return;
            }
            startPolling(data.run_id, currentName);
        });
    }

    // ── Log pane ──────────────────────────────────────────────────
    const logPane = $("#ac-log");
    const logBody = $("#ac-log-body");
    const logCmd = $("#ac-log-cmd");
    const logStatusEl = $("#ac-log-status");
    const logFollow = $("#ac-log-follow");
    const logCancel = $("#ac-log-cancel");
    $("#ac-log-copy").addEventListener("click", () => {
        if (navigator.clipboard) navigator.clipboard.writeText(logBody.textContent || "");
    });
    logCancel.addEventListener("click", () => {
        if (poll.id) cancelRun(poll.id, logCancel);
    });
    $("#ac-log-dismiss").addEventListener("click", () => {
        stopPolling();
        logPane.hidden = true;
        logBody.textContent = "";
    });

    function showLogPane(name) {
        logPane.hidden = false;
        logCmd.textContent = name;
        logBody.textContent = "";
        logPane.scrollIntoView({ behavior: "smooth", block: "nearest" });
    }

    function setLogStatus(status) {
        logStatusEl.className = "ac-status-badge ac-status-" + status;
        logStatusEl.textContent = status;
        if (logCancel) logCancel.hidden = !(status === "queued" || status === "running" || status === "stalled");
    }

    function appendRawLine(line, kind) {
        const span = el("span");
        if (kind === "error" || line.indexOf("[ERROR]") === 0) span.className = "ac-log-line-error";
        else if (line.indexOf("[SYSTEM]") === 0) span.className = "ac-log-line-system";
        span.textContent = line + "\n";
        logBody.appendChild(span);
        if (logFollow.checked) logBody.scrollTop = logBody.scrollHeight;
    }

    function startPolling(runId, name) {
        stopPolling();
        showLogPane(name);
        setLogStatus("queued");
        poll = { id: runId, offset: 0, timer: null, remainder: "", lastGrowth: Date.now(), done: false };
        doPoll();
    }

    function stopPolling() {
        if (poll.timer) { clearTimeout(poll.timer); poll.timer = null; }
    }

    function parseFrames(raw) {
        // The buffer is SSE-framed: "data: <line>\n\n" per line (log_stream.py).
        poll.remainder += raw;
        const parts = poll.remainder.split("\n\n");
        poll.remainder = parts.pop();
        parts.forEach((part) => {
            if (part.indexOf("data: ") === 0) {
                const line = part.slice(6);
                if (line === "[DONE]") { poll.done = true; return; }
                appendRawLine(line);
            } else if (part.trim()) {
                appendRawLine(part);
            }
        });
    }

    function doPoll() {
        getJSON(url.poll(poll.id) + "?offset=" + poll.offset).then(({ ok, data }) => {
            if (!ok) { poll.timer = setTimeout(doPoll, 3000); return; }
            if (data.chunk) {
                poll.offset = data.offset;
                poll.lastGrowth = Date.now();
                parseFrames(data.chunk);
            }
            const status = data.status || "queued";
            const terminal = status === "completed" || status === "failed";
            if (terminal || poll.done) {
                // If we saw [DONE] before the row flipped, treat it as completed.
                setLogStatus(terminal ? status : "completed");
                stopPolling();
                if (currentSchema && currentSchema.name === logCmd.textContent) refreshRecentRuns();
                return;
            }
            // Stall indicator (§8): running but quiet for > 2 min.
            if (status === "running" && Date.now() - poll.lastGrowth > 120000) {
                setLogStatus("stalled");
            } else {
                setLogStatus(status);
            }
            poll.timer = setTimeout(doPoll, 1500);
        });
    }

    // ── Recent runs (per-command, §8a) ────────────────────────────
    function refreshRecentRuns() {
        if (!currentName) return;
        getJSON(url.detail(currentName)).then(({ ok, data }) => {
            if (!ok) return;
            currentSchema.recent_runs = data.recent_runs;
            const list = $("#ac-cmd-runs");
            if (list) renderRunList(list, data.recent_runs || []);
        });
    }

    // Cancel a queued/running run: revoke the live task and clear the row (also the
    // manual escape hatch for a zombie row whose worker already died).
    function cancelRun(runId, btn) {
        if (btn) { btn.disabled = true; btn.textContent = "Cancelling…"; }
        postJSON(url.cancel(runId), {}).then(({ ok, data }) => {
            if (!ok) {
                if (btn) { btn.disabled = false; btn.textContent = "Cancel"; }
                window.alert((data && data.error) || "Cancel failed.");
                return;
            }
            if (poll.id === runId) { stopPolling(); setLogStatus("failed"); }
            refreshRecentRuns();
            if (histModalEl && histModalEl.classList.contains("show")) loadHistory();
        });
    }

    function renderRunList(container, runs, showCommand) {
        container.innerHTML = "";
        if (!runs.length) {
            container.appendChild(el("div", "ac-runs-empty", "No runs yet."));
            return;
        }
        runs.forEach((run) => container.appendChild(renderRun(run, showCommand)));
    }

    // Compact "k: v · k: v" string from a result_summary dict, for an at-a-glance chip.
    function summaryChipText(summary) {
        return Object.keys(summary).map((k) => `${k}: ${summary[k]}`).join(" · ");
    }

    // Structured result_summary block (key/value grid) for the run-detail expand panel.
    function renderSummary(summary) {
        const wrap = el("div", "ac-summary-block");
        wrap.appendChild(el("div", "ac-section-label", "Summary"));
        const grid = el("div", "ac-summary-grid");
        Object.keys(summary).forEach((k) => {
            const item = el("div", "ac-summary-item");
            item.appendChild(el("span", "ac-summary-k", k));
            item.appendChild(el("span", "ac-summary-v", String(summary[k])));
            grid.appendChild(item);
        });
        wrap.appendChild(grid);
        return wrap;
    }

    function renderRun(run, showCommand) {
        const box = el("div", "ac-run");
        const summary = el("div", "ac-run-summary");
        summary.appendChild(statusBadge(run.status));
        if (showCommand) summary.appendChild(el("span", "ac-run-cmd", run.command));
        const meta = el("span", "ac-run-meta");
        const dur = run.duration_seconds != null ? " · " + fmtDuration(run.duration_seconds) : "";
        meta.textContent = `${run.user || "—"} · ${fmtTime(run.created_at)}${dur}`;
        summary.appendChild(meta);
        if (run.result_summary) {
            summary.appendChild(el("span", "ac-run-sumchip", summaryChipText(run.result_summary)));
        }

        const rerun = el("button", "btn btn-sm ac-link-btn ac-run-rerun", "Re-run");
        rerun.addEventListener("click", (e) => { e.stopPropagation(); rerunFrom(run); });
        summary.appendChild(rerun);

        if (run.status === "queued" || run.status === "running") {
            const cancel = el("button", "btn btn-sm ac-link-btn ac-run-cancel", "Cancel");
            cancel.addEventListener("click", (e) => {
                e.stopPropagation();
                cancelRun(run.run_id, cancel);
            });
            summary.appendChild(cancel);
        }

        const detail = el("div", "ac-run-detail");
        detail.hidden = true;
        let loaded = false;
        summary.addEventListener("click", () => {
            detail.hidden = !detail.hidden;
            if (!detail.hidden && !loaded) {
                loaded = true;
                detail.innerHTML = '<div class="ac-runs-empty">Loading…</div>';
                getJSON(url.runDetail(run.run_id)).then(({ ok, data }) => {
                    detail.innerHTML = "";
                    if (!ok) { detail.appendChild(el("div", "ac-runs-empty", "Failed to load run.")); return; }
                    detail.appendChild(el("div", "ac-field-help", data.command_line || ""));
                    if (data.error) detail.appendChild(el("div", "ac-import-error", data.error));
                    if (data.result_summary) detail.appendChild(renderSummary(data.result_summary));
                    const pre = el("pre", null, data.log || "(no output captured)");
                    detail.appendChild(pre);
                });
            }
        });

        box.appendChild(summary);
        box.appendChild(detail);
        return box;
    }

    // ── Re-run: prefill the form from a stored run (§8a) ──────────
    function rerunFrom(run) {
        if (run.command === currentName && currentSchema) {
            renderDetail(currentSchema, buildPrefill(run, currentSchema));
            window.scrollTo({ top: 0, behavior: "smooth" });
            if (historyModal) historyModal.hide();
            return;
        }
        // Different command: load its schema first, then prefill from it.
        currentName = run.command;
        markActive(run.command);
        detailEmpty.hidden = true;
        detailBody.hidden = false;
        getJSON(url.detail(run.command)).then(({ ok, data }) => {
            if (!ok) return;
            currentSchema = data;
            renderDetail(data, buildPrefill(run, data));
            window.scrollTo({ top: 0, behavior: "smooth" });
            if (historyModal) historyModal.hide();
        });
    }

    function buildPrefill(run, schema) {
        // run.options is {dest: value}; run.args is positionals in declared order.
        const prefill = {};
        const opts = run.options || {};
        Object.keys(opts).forEach((k) => { prefill[k] = opts[k]; });
        const args = run.args || [];
        const positionals = (schema.fields || []).filter((f) => f.positional);
        let i = 0;
        positionals.forEach((f) => {
            if (f.multi) { prefill[f.dest] = args.slice(i); i = args.length; }
            else if (i < args.length) { prefill[f.dest] = args[i++]; }
        });
        return prefill;
    }

    // ── Global history modal (§8a) ────────────────────────────────
    // bootstrap.bundle.js loads *after* this script (it's at the end of base.html),
    // so resolve the Modal lazily on first use rather than at parse time.
    let historyModal = null;
    const histModalEl = $("#ac-history-modal");
    function ensureHistoryModal() {
        if (!historyModal && window.bootstrap && histModalEl) {
            historyModal = new bootstrap.Modal(histModalEl);
        }
        return historyModal;
    }

    $("#ac-history-btn").addEventListener("click", () => {
        const m = ensureHistoryModal();
        if (m) m.show();
        loadHistory();
    });
    $("#ac-hist-refresh").addEventListener("click", loadHistory);
    ["ac-hist-command", "ac-hist-user", "ac-hist-status"].forEach((id) => {
        const node = document.getElementById(id);
        node.addEventListener("change", loadHistory);
    });

    function loadHistory() {
        const list = $("#ac-history-list");
        list.innerHTML = '<div class="ac-runs-empty">Loading…</div>';
        const params = new URLSearchParams();
        const cmd = $("#ac-hist-command").value.trim();
        const user = $("#ac-hist-user").value.trim();
        const status = $("#ac-hist-status").value;
        if (cmd) params.set("command", cmd);
        if (user) params.set("user", user);
        if (status) params.set("status", status);
        getJSON(url.history() + "?" + params.toString()).then(({ ok, data }) => {
            if (!ok) { list.innerHTML = ""; list.appendChild(el("div", "ac-runs-empty", "Failed to load.")); return; }
            renderRunList(list, data.runs || [], true);
        });
    }
})();
