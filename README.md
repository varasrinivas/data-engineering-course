# From Backend to Big Data — Data Engineering with PySpark

A browser-based, install-free course that takes **Java / backend developers** into data
engineering. One domain — **NimbusMart**, a mid-size online marketplace — is carried
through **36 modules across 8 tracks**, from "why can't the orders database answer this?"
to a full Bronze/Silver/Gold lakehouse with an orchestrated DAG.

The entire course is a **single self-contained file**: [`player/index.html`](player/index.html).
No build step to *read* it, no server, no dependencies. Just open it.

```
# Windows
start player\index.html
# macOS
open player/index.html
# Linux
xdg-open player/index.html
```

Progress (completed modules, light/dark theme) is saved in your browser's local storage.

---

## What's inside

**8 tracks · 36 modules**, each with a NimbusMart cold open, a recurring *Freight Line*
warehouse analogy, a `☕ For the Java Dev` bridge (Spark ↔ Streams, Catalyst ↔ the JIT,
Delta MERGE ↔ JPA upsert…), two lab paths, verified check questions, and a production
war story.

| Track | Theme |
|-------|-------|
| **A** | Foundations: OLTP vs OLAP, file formats, partitioning, batch vs streaming |
| **B** | Data modeling & the lakehouse: star schemas, SCD2, Medallion, table formats |
| **C** | The working toolkit: Python, analytics SQL, pandas, data quality |
| **D** | Spark core mental model: driver/executors, lazy eval, the DAG, Catalyst |
| **E** | PySpark in practice: reads, transforms, joins, windows, UDFs, Delta |
| **F** | Pipelines & orchestration: idempotency, Airflow, backfills, CI/CD |
| **G** | Quality, governance & ops: contracts, validation gates, lineage, tuning |
| **H** | Capstone: the NimbusMart platform, six sources → dashboard |

**The spine:** `FRAUD_REVIEW_THRESHOLD = 0.80`. Orders scored below it auto-fulfill; at or
above it they land in a human review queue. That one number — and the **43-row review
queue** it defines on the seed data — recurs from module A1 through the capstone, in
deeper contexts each time.

### Dual-path labs

Every module has two ways to practice:

- **Path 1 — Understand it (browser):** interactive, zero setup, runs in the page.
- **Path 2 — Build it with AI (local):** a copy-paste [Claude Code](https://claude.com/claude-code)
  prompt that scaffolds the *real* equivalent locally — a venv, `pip install pyspark`, a
  deterministic NimbusMart data generator, the exercise, and a pytest that proves the result.

### Three lab tiers — real where it's cheap, honest simulation where it isn't

| Tier | Engine | What it does |
|------|--------|--------------|
| **T1** | `sqlrunner.js` / Pyodide | **Real execution.** A hand-rolled SQL engine (CTEs, joins, window functions) and Pyodide/pandas run actual queries over the seed data, with an expected-result diff. |
| **T2** | `sparksim.js` | **SparkSim.** A teaching subset of the DataFrame API whose real product is the visualization: a lazy-evaluation ribbon, a job→stage→task **DAG** with shuffle boundaries, and logical→optimized→physical **plans** showing predicate pushdown and projection pruning live. Results are real; parallelism and timings are badged `simulated`. |
| **T3** | `traceplayer.js` | **Scripted traces.** For cluster-scale behavior no browser can honestly run — skew meltdowns, small-files explosions, backfills, the UDF serialization tax, the whole-platform finale. Authored, deterministic, play/pause/scrub, badged `simulation`. |

The one permitted network call is Pyodide's runtime, lazy-loaded only when a Python lab is
opened, with a graceful offline fallback. Nothing else touches the network.

---

## Repository layout

```
player/index.html        THE deliverable — single-file course player (zero runtime deps)
content/modules/         one .js fragment per module (A1.js … H4.js)
engine/                  sqlrunner.js, sparksim.js, traceplayer.js, pyrunner.js
engine/traces/           14 authored T3 trace timelines (+ schema.json)
data/nimbusmart/         deterministic seed generator (generate.py → seed.js)
scripts/                 inject.py, validate.py, build.py (+ build.ps1), qa.js, smoke_dom.js
docs/                    blueprint, authoring workflow, lab-engine spec, engine contract, PROGRESS.md
.claude/commands/        authoring slash commands
CLAUDE.md                authoring engine instructions
```

The player is assembled from parts: [`scripts/build.py`](scripts/build.py) splices the seed
data, the four engines, the traces, and all 36 module fragments into `player/index.html`
between comment-fenced regions. **Content is never hand-edited in the player** — you edit a
fragment and re-inject.

---

## Working with the source

Requires **Python 3.10+** and **Node 18+**. Lab-engine QA additionally uses `jsdom` and
`playwright` (dev-only; the shipped player needs neither).

```bash
python data/nimbusmart/generate.py     # regenerate the deterministic seed (seed 42)
python scripts/validate.py --all --final   # schema + threshold-discipline check on all modules
python scripts/build.py                # assemble player, node --check gate, refresh PROGRESS.md
node scripts/qa.js                     # mount every lab in a headless DOM; check trace refs, dup cold-opens
```

Authoring one module follows a fixed rhythm (see [`docs/02-AUTHORING-WORKFLOW.md`](docs/02-AUTHORING-WORKFLOW.md)):
scaffold the fragment → write the lab against the engine contract
([`docs/04-ENGINE-CONTRACT.md`](docs/04-ENGINE-CONTRACT.md)) → `validate` → `inject` → `build`.
`validate.py` enforces the schema: exactly one Freight Line analogy and one Java bridge per
module, both lab paths present, 3–5 checks, valid lab tier, and that `FRAUD_REVIEW_THRESHOLD`
is always a named constant (never a bare `0.80`).

### Verified facts the labs depend on

The seed generator (`random.seed(42)`) produces a stable world so expected outputs never drift:

- **240 orders**, fraud scores for **225** of them (15 unscored)
- exactly **43 orders** at or above `FRAUD_REVIEW_THRESHOLD`, **4** of them scored *exactly* 0.80
  (the inclusive-boundary lesson)
- seller **S-777** owns **80/240** orders — the skew hot key
- customer **C-0042** moved Paris → Munich → Hamburg mid-quarter — the SCD2 poster child

---

## Design system

Fraunces for display, JetBrains Mono for code and data, a warm-paper palette with a full
dark-mode variant. All diagrams are inline SVG using CSS custom properties, so they recolor
correctly in both themes.

---

*Authored with [Claude Code](https://claude.com/claude-code).*
