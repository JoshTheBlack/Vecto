# Management Command Conventions

A short reference for writing new Django management commands in this project so they
are safe, self-documenting, and automatically usable from the **Admin Command
Console**. For the console's full design see [admin-command-console.md](admin-command-console.md).

---

## 1. Safety idiom (uniform across all commands)

Any command that **changes state** follows one convention:

1. **Preview by default.** With no flags the command does a *dry run*: it reports
   what it would do and changes nothing.
2. **`--apply` executes.** One flag, same name everywhere, performs the changes.
3. **`--apply --yes` for destruction.** Commands that *irreversibly delete*
   objects/files/rows require `--yes` **in addition to** `--apply`; with `--apply`
   but no `--yes` they abort with a clear message.
4. **Exempt: read-only and single-target actions.** Reports, lookups, smoke tests,
   and "do this one thing" operational commands (ingest one feed, transcribe one
   episode) take neither flag — running them *is* the intent.

Rule of thumb: **reversible mutation → `--apply`; irreversible deletion →
`--apply --yes`; read-only / single action → neither.**

Do **not** introduce `--dry-run` (preview is already the default) or use `--yes`
alone to mean "go" (`--apply` means go; `--yes` only confirms destruction). A
destructive sub-mode (e.g. `--prune`) requires `--yes` when combined with `--apply`.

```python
def add_arguments(self, parser):
    parser.add_argument('--apply', action='store_true',
                        help='Perform the changes (default is a dry run that only reports).')
    # destructive commands only:
    parser.add_argument('--yes', action='store_true',
                        help='Confirm irreversible deletion (required with --apply).')

def handle(self, *args, **options):
    apply = options['apply']
    if apply and self.IS_DESTRUCTIVE and not options['yes']:
        raise CommandError('This deletes data irreversibly. Re-run with --apply --yes.')
    ...
    verb = 'Deleted' if apply else 'Would delete'
    self.stdout.write(self.style.SUCCESS(f'{verb} {n} item(s).'))
    if not apply:
        self.stdout.write('Re-run with --apply to perform the changes.')
```

---

## 2. Output: write to `self.stdout` / `self.stderr`

The console captures live output by piping `call_command(stdout=…, stderr=…)`. Write
all user-facing progress with `self.stdout.write(...)` / `self.stderr.write(...)`
(use `self.style.SUCCESS/WARNING/ERROR` for color). **Do not** rely solely on the
`logging` framework for progress meant for the operator — `logger.info(...)` does
**not** reach the captured stream.

---

## 3. Documentation (three in-code sources)

The console builds each command's docs entirely from in-code sources — no separate
doc file. Fill in all three:

1. **`Command.help`** — one-line summary / what it does.
2. **Per-argument `help=`** — every `add_argument` gets a clear `help=` string. No
   bare arguments.
3. **Module docstring** — the rich body: what it's for, when to use it, and a few
   copy-pasteable examples. This is what the console renders in the detail pane.

```python
"""One-line summary of the command.

A paragraph of context: what problem it solves, when to run it, any gotchas.

    python manage.py my_command --network=baldmove            # dry run
    python manage.py my_command --network=baldmove --apply    # execute
"""
```

Examples should show the new defaults — i.e. include `--apply` for the live form.

---

## 4. Arguments → console widgets

The console auto-generates a form from `add_arguments`. Most fields render correctly
with **zero extra work** if you follow the naming/shape conventions:

| You write | Console renders |
|---|---|
| `--flag` (`action='store_true'`) | checkbox |
| `choices=[...]` | dropdown (auto) |
| `type=int` / `type=float` | number input |
| dest `network` (single) | Network dropdown (slugs) |
| dest `network(s)` + `action='append'` | Network multi-select |
| dest `podcast` / `podcast_id` | Podcast dropdown |
| dest `podcasts` + `action='append'` | Podcast multi-select |
| dest `episode` / `episode_id` | Episode typeahead |
| positional / text | text input |

**Prefer these conventional dest names** (`--network`, `--podcast`, `--episode`) for
scoping flags so the pickers light up automatically. If an argument can't describe
itself through argparse (e.g. a fixed enum that isn't `choices=`, or a file picker),
add a `field_widgets` override in the registry entry (§5) — see the
[admin-command-console.md §5a](admin-command-console.md) widget list.

If an argument carries a secret (token, cookie, password), mark it sensitive in the
registry so it is redacted in run history and the command-line preview.

---

## 5. Register it in the Admin Command Console

Commands are auto-discovered but only **shown** when registered. Add an entry to
`pod_manager/admin_console/registry.py`:

```python
"my_command": CommandSpec(
    name="my_command",
    category="R2 / Storage",       # sidebar grouping
    danger=True,                   # destructive → red styling + typed confirm
    runnable=True,                 # False = docs-only (daemons, vestigial)
    # deep_link=("creator_settings", "Open …"),   # execute elsewhere; still build CLI
    # field_widgets={"origins": "enum_multi:audio_origins"},  # only if argparse can't infer
),
```

- `danger=True` for anything that deletes/purges (gets the typed-confirm gate and
  binds `--apply`/`--yes` to it).
- `runnable=False` for daemons or commands that shouldn't be launched from the web
  (they still show docs + a copy-pasteable command line).
- A newly added command stays **invisible** until it has a registry entry. The
  console shows a "discovered but unregistered" notice (and a CI test can enforce it)
  so nothing is silently forgotten.

---

## 6. New-command checklist

- [ ] Mutating? Default to a dry run; add `--apply` (and `--yes` if it deletes).
- [ ] Exempt (read-only / single action)? No safety flags.
- [ ] All progress goes to `self.stdout` / `self.stderr`, not just `logging`.
- [ ] `Command.help` set; every argument has `help=`; module docstring with examples.
- [ ] Scoping flags use conventional dest names (`--network` / `--podcast` /
      `--episode`) so pickers auto-render.
- [ ] Any secret argument flagged sensitive for redaction.
- [ ] Registry entry added (`category`, `danger`, `runnable`, overrides as needed).
- [ ] Tests assert the contract: no-flag = no writes, `--apply` = writes, destructive
      without `--yes` = aborts.
- [ ] Any programmatic `call_command(...)` call sites pass `apply=`/`yes=` as needed.
