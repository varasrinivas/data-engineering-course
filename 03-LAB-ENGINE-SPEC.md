# Lab Engine Spec — Browser-Simulated Labs (No Install)

The one genuinely new component vs. prior course kits. Three tiers, matched per-module in the blueprint. Design principle: **real execution where it's cheap, honest simulation where it isn't** — and always label which is which so students trust the tool.

---

## Tier 1 — Real In-Browser Execution

For Python/pandas/SQL modules (Track C, plus quality labs). Nothing simulated.

**T1-SQL (`engine/sqlrunner.js`)**
- Wraps **AlaSQL** (pure JS, inlineable — preferred for the single-file constraint) over the embedded NimbusMart seed datasets.
- UI: query editor (JetBrains Mono), Run button, result grid, expected-result diff ("your queue has 47 orders; expected 43 — you forgot `score >= FRAUD_REVIEW_THRESHOLD` is inclusive").
- Datasets embedded as JS constants at build time (each ≤ 500 rows, generated deterministically by `data/nimbusmart/generate.py` with a fixed seed so expected outputs are stable).

**T1-Python (Pyodide)**
- Lazy-loads Pyodide **only when the student opens a Python lab** (the single permitted network dependency; offline fallback shows the completed notebook as a readable walkthrough).
- Supports pandas — enough for C1/C3/C4. *Not* PySpark (no JVM in a browser); the player never pretends otherwise.

## Tier 2 — SparkSim (`engine/sparksim.js`)

The signature component: a small JS engine implementing a **teaching subset of the DataFrame API** over the seed datasets, whose real product is not the result table — it's the **plan and DAG visualization**.

**Supported ops (v1, keep it ruthless):**
`read`, `select`, `filter`/`where`, `withColumn`, `groupBy().agg()`, `orderBy`, `join` (inner/left, broadcast or sort-merge), `limit`, window functions (`row_number`, `rank`, `sum over`), `count`/`show`/`write` as actions.

**What it renders per scenario:**
1. **Code panel** — PySpark syntax (students read/edit real `F.col(...)` code; a tolerant parser maps it to the op tree; unsupported ops produce a friendly "SparkSim v1 doesn't implement X — here's the concept anyway" card, never a crash)
2. **Lazy-eval ribbon** — transformations queue up visibly; nothing computes until an action fires (the D3 "aha" moment)
3. **DAG view** — jobs → stages → tasks; **shuffle boundaries drawn as red cross-dock lines** (Freight Line iconography)
4. **Plan view** — simplified logical → optimized → physical plan, with optimizations highlighted (predicate pushdown shown as the filter node *moving* down the tree, animated)
5. **Result grid** — actual computed output on the seed data

**Honesty rule:** SparkSim computes single-threaded on tiny data. Partition counts, task parallelism, and timings are *modeled*, and the UI badges them `simulated`. Correctness of results is real; performance is illustrative.

## Tier 3 — Scripted Traces (`engine/traces/*.json`)

For cluster-scale behavior no browser can honestly execute: skew meltdowns, small-files explosions, backfill timelines, executor loss/retry, cost curves.

- Each trace is a JSON timeline: `steps[] = { t, narration, visualState, metrics }`.
- A generic **trace player** renders play/pause/scrub over a visual state machine (executor grid, task bars, partition heatmap).
- Traces are *authored* (deterministic, reviewed) — they are storytelling instruments, badged `simulation` in the UI.
- Flagship traces: `e3-skew-salting.json` (one hot seller key melts a stage; salting fixes it — before/after scrubber), `a4-small-files.json`, `f3-backfill.json`, `e5-udf-tax.json` (built-in vs Python UDF vs pandas UDF timing bars).

## Dual-Path Labs (every module)

**Path 1 — Understand It (browser):** the tiered experience above. Zero setup, phone-friendly for T3.

**Path 2 — Build It with AI (Claude Code, local, optional):** a complete copy-paste prompt that scaffolds the *real* equivalent: venv + `pip install pyspark`, `generate_data.py` (same seed logic as the course datasets), the exercise file with TODOs, and a pytest that asserts the outcome (e.g., "the review queue contains exactly the orders with `fraud_score >= FRAUD_REVIEW_THRESHOLD`"). This is the bridge from simulation to the real JVM — and it doubles as Claude Code fluency training, consistent with the AI-DLC course philosophy.

## Engine Build Notes

- All engines are dependency-light and inlined into the single-file player at build (AlaSQL inlined; Pyodide the sole lazy CDN load).
- `engine/traces/schema.json` + a validator hook in `validate.py` keeps traces well-formed.
- SparkSim scenarios are declarative configs inside MODS lab entries: `{ dataset, starterCode, expected, dagAnnotations }` — the engine, not the module, owns rendering.
- Target budget: SparkSim ≤ ~60KB minified, trace player ≤ ~15KB, so the full player stays comfortably openable from `file://`.
