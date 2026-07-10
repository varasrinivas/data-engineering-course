# Engine Contract — the binding API between player, engines, and module fragments

This file is the **single source of truth** for every config key a module fragment
may use and every API an engine must expose. `validate.py` enforces parts of it.
If you need a key that isn't here, add it here first — never invent silently.

---

## 1. Global data (already inlined into the player)

`window.NIMBUS` — object of arrays of plain row objects. Tables and columns:

| table | columns |
|---|---|
| `customers` (60) | customer_id, name, email, city (nullable), country, segment, created_at |
| `customer_updates` (8) | customer_id, city, country, updated_at — SCD2 feed |
| `products` (40) | product_id, name, category **{dept, aisle}** (nested), price, tags **[array]**, attrs **{brand, weight_kg}** (nested) |
| `orders` (240) | order_id, customer_id, seller_id, order_ts, status, total_amount, item_count, country, channel |
| `fraud_scores` (225) | order_id, fraud_score, model_version, scored_at — **15 orders unscored** |
| `payments` (228) | payment_id, order_id, method, amount, status |
| `order_events` (361) | event_id, order_id, event_type, event_ts, device (**sometimes missing**), app_version (**sometimes present** — schema drift) |
| `couriers` (12) | courier_id, name, home_zone |
| `courier_pings` (278) | ping_id, courier_id, order_id, status, zone, event_ts, ingested_at (**lags event_ts; sometimes > 1h = late data**) |

`window.FRAUD_REVIEW_THRESHOLD` — `0.80`. Engineered facts:
exactly **43** orders have `fraud_score >= 0.80`; **4** of them exactly `0.80`;
seller `S-777` owns **80/240** orders (the skew hot key).

Timestamps are ISO strings `"2026-05-14T09:31:00"` — lexicographic order == time order.

## 2. Engine mount API (every engine implements this)

```js
window.Engines = window.Engines || {};
window.Engines.<name> = {
  mount(el, config, ctx) { /* render lab into el; return optional {destroy()} */ }
};
```
- `<name>` ∈ `sql` (sqlrunner.js) | `sparksim` (sparksim.js) | `trace` (traceplayer.js) | `pyodide` (pyrunner.js)
- `el` — an empty container div inside the module page.
- `config` — the module's `lab.understand` object (schemas below).
- `ctx` — `{ NIMBUS, TRACES, dark }` (engines may also read the globals directly).
- Engines own ALL of their internal rendering. They must use the player's CSS
  variables (`--paper, --ink, --accent, --rust, --mono, ...`) so dark mode works.
- Engines must never throw on bad user input: show a friendly card instead.
- Honesty badges: sparksim renders a `simulated` badge on modeled metrics;
  traceplayer renders a `simulation` badge always; sqlrunner/pyodide render `real execution`.

## 3. `lab.understand` config schemas

### engine: "sql"  (T1)
```js
{
  engine: "sql",
  datasets: ["orders", "fraud_scores"],   // tables shown in the schema sidebar
  task: "<html: what to write and why>",
  starterQuery: "SELECT ...",              // pre-filled, usually subtly wrong/incomplete
  solutionQuery: "SELECT ...",             // engine runs this to compute the expected grid + diff
  hint: "<optional html>"
}
```
SQL dialect supported (subset, enforced by the engine — do not use anything else):
`WITH ... AS (...)` CTEs, `SELECT [DISTINCT] expr [AS alias]`, `FROM t [alias]`,
`[INNER|LEFT] JOIN t [alias] ON a.c = b.c`, `WHERE` (`= != <> < <= > >=`, `AND OR NOT`,
`IN (...)`, `BETWEEN`, `LIKE`, `IS [NOT] NULL`), `GROUP BY`, `HAVING`, `ORDER BY ... [ASC|DESC]`,
`LIMIT`, aggregates `COUNT(*) COUNT(x) COUNT(DISTINCT x) SUM AVG MIN MAX`,
scalar fns `ROUND(x,n) COALESCE UPPER LOWER TRIM LENGTH SUBSTR`, `CASE WHEN ... THEN ... [ELSE] END`,
window functions `ROW_NUMBER() | RANK() | DENSE_RANK() | SUM(x) | COUNT(*) OVER (PARTITION BY ... ORDER BY ... [DESC])`.
No subqueries in FROM/WHERE (use CTEs). Identifiers are case-insensitive; strings single-quoted.

### engine: "sparksim"  (T2)
```js
{
  engine: "sparksim",
  datasets: ["orders", "fraud_scores"],
  task: "<html>",
  starterCode: "review = (spark.read.table(\"orders\")\n  ...)\nreview.show()",
  solutionCode: "...",                     // optional; shown via 'reveal solution'
  expect: { rows: 7, cols: ["country","orders_in_review"] },  // optional sanity check on the action result
  dagNotes: "<optional html shown under the DAG>"
}
```
PySpark subset the parser accepts (anything else → friendly "not in SparkSim v1" card):
- Sources: `spark.read.table("name")` (NIMBUS tables only); assignment to vars; parenthesized chains.
- Transformations: `.select("a", F.col("b"), expr.alias("x"))`, `.filter(cond)` / `.where(cond)`,
  `.withColumn("name", expr)`, `.withColumnRenamed("a","b")`, `.drop("a")`, `.distinct()`,
  `.dropDuplicates(["a","b"])`, `.groupBy("a", ...)` + `.agg(F.count("*").alias("n"), F.sum("x"), F.avg|F.min|F.max|F.countDistinct)`,
  `.orderBy("a", F.col("b").desc())`, `.limit(n)`, `.union(df)`,
  `.join(df2, "col")` / `.join(df2, on=["c"], how="left")` / `.join(F.broadcast(df2), "col")`,
  window: `Window.partitionBy("a").orderBy(F.col("b").desc())` with
  `F.row_number().over(w)`, `F.rank().over(w)`, `F.sum("x").over(w)` inside `.withColumn`.
- Expressions: `F.col("x")`, `F.lit(v)`, comparisons `> >= < <= == !=`, arithmetic `+ - * /`,
  `&` / `|` of parenthesized conditions, `F.col("x").isNull()`, `.isNotNull()`, `FRAUD_REVIEW_THRESHOLD` as a bare name.
- Actions (exactly one per run triggers execution): `.show()`, `.count()`, `.collect()`,
  `.write.format("delta").mode(...).saveAsTable("...")` (write is displayed, not persisted).
- `.cache()`, `.explain()`, `.printSchema()` accepted as no-op/inspection.

### engine: "trace"  (T3)
```js
{
  engine: "trace",
  trace: "e3-skew-salting",     // engine/traces/<name>.json must exist
  task: "<html: what to watch for while scrubbing>"
}
```

### engine: "pyodide"  (T1 Python)
```js
{
  engine: "pyodide",
  datasets: ["orders"],          // injected as pandas DataFrames with these names
  task: "<html>",
  starterCode: "import pandas as pd\n...",
  solutionCode: "...",           // used for the offline walkthrough fallback too
  assertCode: "assert int(queue_size) == 43"   // optional; run after user code; PASS/FAIL banner
}
```

## 4. Trace JSON schema (`engine/traces/*.json`)

```js
{
  "id": "e3-skew-salting",
  "title": "One hot key melts a stage",
  "badge": "simulation",
  "compare": false,              // true = A/B scrubber (before/after arrays in each step's state)
  "steps": [
    {
      "t": 0,                    // seconds on the scrubber timeline
      "narration": "<html — one or two sentences>",
      "state": {                 // ALL KEYS OPTIONAL — renderer draws what's present
        "executors": [ { "id": "exec-1", "tasks": [ { "pct": 80, "hot": true, "label": "p3" } ] } ],
        "partitions": [ { "label": "S-777", "size": 96, "hot": true } ],
        "bars":       [ { "label": "stage 2", "value": 41, "max": 60, "unit": "s", "hot": false } ],
        "files":      { "count": 12000, "avgKb": 14 },
        "note": "<short html annotation>"
      },
      "metrics": { "wall_clock_s": 41, "shuffle_gb": 2.3 }   // shown as stat chips
    }
  ]
}
```
`engine/traces/schema.json` mirrors this for the validator. Steps play in order;
scrubbing interpolates nothing — each step is a discrete keyframe.

## 5. Module fragment shape (recap; validate.py enforces)

```js
export default {
  id: "D3", track: "D", title: "...", minutes: 25,
  coldOpen: "...",
  concept: [
    { type: "prose",  html: "<p>...</p>" },
    { type: "svg",    svg: "<svg viewBox=...>...</svg>", caption: "..." },
    { type: "code",   lang: "python"|"sql", code: "...", caption: "..." },
    { type: "analogy",    title: "...", html: "..." },   // exactly one — Freight Line
    { type: "javaBridge", html: "..." }                  // exactly one — ☕ For the Java Dev
  ],
  lab: { tier: "T1"|"T2"|"T3", understand: { ...above }, buildWithAI: "..." },
  check: [
    { type: "mcq",     q: "...", options: ["..",".."], answer: 0, explain: "..." },
    { type: "predict", q: "...", code: "...", options: ["..",".."], answer: 2, explain: "..." }
  ],
  fieldNotes: "..."
}
```
SVGs: inline only, `viewBox` required, use CSS variables for colors
(`var(--ink)`, `var(--accent)`, `var(--rust)`, `var(--paper2)`) so dark mode works.
Code strings: real PySpark style — explicit `StructType` schemas where relevant,
`F.` alias, snake_case, `FRAUD_REVIEW_THRESHOLD` named constant always.
