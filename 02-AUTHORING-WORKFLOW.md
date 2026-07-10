# Authoring Workflow — Claude Code as the Course Engine

How the 36 modules actually get built: session rhythm, slash commands, the injection pipeline, and the build order that de-risks the lab engines.

---

## 1. Build Order (de-risk the engines first)

Do **not** author modules front-to-back. The lab engines are the technical risk; content is not.

| Phase | What | Why first |
|---|---|---|
| 0 | Repo scaffold + `inject.py`/`validate.py`/`build.py` + empty player shell with MODS renderer | Everything else depends on the pipeline |
| 1 | **Engine spikes:** `sqlrunner.js` (T1), `sparksim.js` core (T2: read/select/filter/groupBy + DAG viz), one T3 trace player | Proves the browser-lab premise before writing 36 modules against it |
| 2 | **Vertical slice: module D3** (DataFrames & Lazy Evaluation) end-to-end — content, T2 lab, checks, injected, verified | One fully-real module calibrates effort, schema, and design for all others |
| 3 | Track D remainder (D1–D5) — the mental-model core, heaviest engine usage | Hardest content while engine knowledge is fresh |
| 4 | Tracks A, B, C (mostly T1/T3, lighter) | Fast wins, fills the on-ramp |
| 5 | Tracks E, F, G | Practice tracks, reuse D-track engine configs |
| 6 | Track H capstone + full-course QA pass | Needs everything before it |

## 2. Session Rhythm (one module per session)

```
/new-module D4        → scaffolds fragment from template, pulls blueprint row
  ... author concept sections, analogy, java bridge ...
/lab D4 T2            → scaffolds lab config against sparksim schema
  ... write both lab paths, checks, field notes ...
/validate D4          → schema + lint
/inject D4            → fragment → MODS array in player
/build                → node --check + full report
  ... update docs/PROGRESS.md ...
```

## 3. Slash Commands (`.claude/commands/`)

### `/new-module <ID>` — `new-module.md`
```markdown
Create content/modules/$ARGUMENTS.js for module $ARGUMENTS.
1. Read docs/00-COURSE-BLUEPRINT.md and find the module row for $ARGUMENTS
   (title, core idea, lab tier). Read CLAUDE.md schema.
2. Read one completed module from the same track (or D3 if none) as the
   style reference.
3. Scaffold the full MODS entry with: coldOpen (NimbusMart incident),
   concept sections including exactly one `analogy` (Freight Line) and one
   `javaBridge` section, lab stub with correct tier, 4 check questions,
   fieldNotes. Mark unfinished prose with TODO markers, never lorem ipsum.
4. Run: python scripts/validate.py content/modules/$ARGUMENTS.js
5. Report what needs human/author attention as a short list.
```

### `/lab <ID> <TIER>` — `lab.md`
```markdown
Build the lab for module $1 at tier $2.
1. Read docs/03-LAB-ENGINE-SPEC.md section for tier $2. Read the engine
   file to confirm supported ops/config keys — never invent engine API.
2. Understand-It path: write the engine config (T1: dataset + starter
   query; T2: sparksim scenario; T3: trace JSON in engine/traces/).
3. Build-It-with-AI path: write a complete, copy-paste Claude Code prompt
   that scaffolds the equivalent real local PySpark project (venv, pip
   install pyspark, dataset generation script, the exercise, a pytest
   assertion). The prompt must be self-contained — assume the student's
   Claude Code has never seen this course.
4. Verify: T1/T2 configs reference only ops the engine implements; T3
   trace validates against traces/schema.json.
```

### `/validate <ID>` — runs `python scripts/validate.py` and summarizes failures with fixes.

### `/inject <ID>` — runs `python scripts/inject.py`, then the `node --check` extraction, reports pass/fail. On fail: diagnose, fix the *fragment* (never patch the player directly), re-inject.

### `/build` — full rebuild of all fragments, `node --check`, PROGRESS.md refresh, and a coverage report: modules missing analogy/javaBridge/labs/checks.

### `/qa-track <LETTER>` — cross-module pass over one track: terminology consistency, threshold-constant usage, no duplicated cold opens, check-question difficulty curve.

## 4. Injection Pipeline (`scripts/`)

**`inject.py <ID>`**
1. Parse `content/modules/<ID>.js` (fragment exports one object literal).
2. Locate `/* MODS:BEGIN */ ... /* MODS:END */` markers in `player/index.html`.
3. Replace or insert the entry for `<ID>` keeping track order (A1…H4).
4. Re-serialize; write player; print a diff summary (bytes, entry count).

**`validate.py <ID>`** — asserts: required keys present, exactly one `analogy` and one `javaBridge` section, lab tier ∈ {T1,T2,T3}, both lab paths non-empty, 3–5 checks, `FRAUD_REVIEW_THRESHOLD` never appears as a bare `0.80`/`0.8` literal, no TODO markers remain (warning in draft mode, error in `--final`).

**`build.py`** — inject all fragments → extract `<script>` blocks to temp `.js` files → `node --check` each → emit `docs/PROGRESS.md` table + coverage report. Windows-friendly (pathlib, no shell-isms); PowerShell wrapper `build.ps1` provided for parity with prior course repos.

## 5. Definition of Done (per module)

- [ ] All schema fields complete, validator passes in `--final` mode
- [ ] Freight Line analogy + ☕ Java bridge present and *specific* (not generic restatement)
- [ ] Understand-It lab runs against the engine (config keys verified)
- [ ] Build-It-with-AI prompt is self-contained and copy-paste ready
- [ ] Check questions verified (outputs traced, one distractor per MCQ is a *plausible* misconception)
- [ ] Injected, `node --check` green, PROGRESS.md updated
