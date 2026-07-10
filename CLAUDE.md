# CLAUDE.md — Data Engineering with PySpark Course Repo

You are the authoring engine for a 36-module data engineering course. Read this file fully before any task. The blueprint (`docs/00-COURSE-BLUEPRINT.md`) is the source of truth for scope; this file is the source of truth for *how* to build.

## Project Structure

```
de-pyspark-course/
├── CLAUDE.md                  # this file
├── docs/
│   ├── 00-COURSE-BLUEPRINT.md
│   ├── 02-AUTHORING-WORKFLOW.md
│   └── 03-LAB-ENGINE-SPEC.md
├── player/
│   └── index.html             # THE deliverable: single-file course player
├── content/
│   └── modules/               # one .js fragment per module (A1.js ... H4.js)
├── engine/
│   ├── sparksim.js            # T2 lab engine (DataFrame subset + DAG viz)
│   ├── sqlrunner.js           # T1 SQL labs (sql.js / AlaSQL wrapper)
│   └── traces/                # T3 scripted simulation JSON traces
├── data/
│   └── nimbusmart/            # seed datasets (small, embedded as JS at build)
├── scripts/
│   ├── inject.py              # Python injection: fragments → player MODS array
│   ├── validate.py            # structural validation of MODS entries
│   └── build.py               # full build: inject all + node --check + report
└── .claude/
    └── commands/              # slash commands (see workflow doc)
```

## Non-Negotiable Standards

### The player
- **Single file.** `player/index.html` is fully self-contained: no CDN, no network calls, no external assets. Everything inlined at build time. Exception: Pyodide labs lazy-load the Pyodide runtime from CDN *only when a T1 Python lab is opened*, with a graceful offline fallback message.
- **MODS array** is the only content structure. Every module is one object in `MODS`. The player renders exclusively from `MODS` — never hardcode module content in markup.
- **Design system (established, do not deviate):** Fraunces for display/headings, JetBrains Mono for code/data, warm paper palette. Dark-mode variant of the same palette. Inline SVG diagrams only — no raster images.
- Must open cleanly from `file://` on Windows. Test path assumptions accordingly.

### MODS entry schema (validate.py enforces this)
```js
{
  id: "D3",                    // track letter + number
  track: "D",
  title: "DataFrames & Lazy Evaluation",
  minutes: 25,                 // estimated time
  coldOpen: "...",             // 2-3 sentence NimbusMart incident
  concept: [ /* section objects: prose | svg | code | analogy | javaBridge */ ],
  lab: {
    tier: "T2",                // T1 | T2 | T3
    understand: { /* engine config or trace ref */ },
    buildWithAI: "..."         // full copy-paste Claude Code prompt, fenced
  },
  check: [ /* 3-5 question objects: mcq | predictOutput */ ],
  fieldNotes: "..."            // production war story
}
```

### Content rules
- Every module MUST include: one Freight Line analogy panel, one `☕ For the Java Dev` bridge box, both lab paths, 3–5 check questions.
- All examples use NimbusMart entities (`orders`, `customers`, `products`, `payments`, `fraud_scores`, `couriers`). Never invent a second domain.
- `FRAUD_REVIEW_THRESHOLD = 0.80` is a named constant everywhere it appears — never a magic number, never a different value.
- PySpark code style: explicit schemas (`StructType`), no `inferSchema` except in the module that teaches why not, `F.` alias for functions (`from pyspark.sql import functions as F`), snake_case columns.
- Predict-the-output questions must be *actually verified*: run the logic (mentally is not enough — trace it in sparksim or note the verification in the module fragment header comment).
- Tone: senior-engineer-to-senior-engineer. No filler enthusiasm. War stories are specific (numbers, durations, blast radius).

### Validation gates (run before declaring any module done)
1. `python scripts/validate.py content/modules/<ID>.js` — schema check
2. `python scripts/inject.py <ID>` — inject into player
3. `node --check player/index.html` extraction step (build.py handles the JS extraction) — syntax must pass
4. Open-in-browser smoke description: state what the module should render; if a lab engine is involved, confirm its config keys exist in the engine.

### Working style
- One module per session/task unless explicitly told otherwise. Finish = all four gates pass.
- Never rewrite `player/index.html` wholesale. All content changes go through fragments + `inject.py`.
- If a task conflicts with the blueprint, stop and flag it — do not silently re-scope.
- Keep a running `docs/PROGRESS.md` table: module, status (draft/injected/verified), date, notes.
