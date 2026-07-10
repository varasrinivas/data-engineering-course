// D3 — DataFrames & Lazy Evaluation (vertical slice / style reference)
// Verified facts used by lab + checks (from data/nimbusmart/generate.py, seed 42):
//   orders = 240 rows; fraud_scores = 225 rows (15 orders unscored)
//   inner join orders×fraud_scores = 225 rows
//   fraud_score >= FRAUD_REVIEW_THRESHOLD (0.80) = 43 rows
export default {
  id: "D3",
  track: "D",
  title: "DataFrames & Lazy Evaluation",
  minutes: 25,
  coldOpen: "A new platform engineer pushes the fraud-review pipeline to the cluster and Slacks the channel: “ran the whole notebook in 0.4 seconds, Spark is unbelievable.” Twenty minutes later the same notebook is still sitting on the final cell, fans screaming. Nothing in the first eleven cells ever ran. Everything ran in the twelfth.",
  concept: [
    { type: "prose", html: `
<p>A <strong>DataFrame</strong> is not a container of rows sitting in memory. It's a <em>description</em> of a dataset: a schema plus a recipe for how to produce the rows from some source. The rows may live in Parquet files on object storage, sliced into partitions across a hundred executors — or nowhere at all yet.</p>
<p>Every operation on a DataFrame falls into one of two castes, and the entire Spark mental model hangs on the distinction:</p>
<ul>
<li><strong>Transformations</strong> — <code>select</code>, <code>filter</code>, <code>withColumn</code>, <code>join</code>, <code>groupBy().agg()</code> — return a <em>new</em> DataFrame. They execute nothing. They append a step to the recipe.</li>
<li><strong>Actions</strong> — <code>show()</code>, <code>count()</code>, <code>collect()</code>, <code>write</code> — demand actual rows. Only an action turns the recipe into a running <em>job</em>.</li>
</ul>
<p>That's why the notebook “ran” in 0.4 seconds: eleven cells of transformations built an ever-longer recipe, and the twelfth cell — a <code>write</code> — paid for all of it at once.</p>` },
    { type: "code", lang: "python", code: `from pyspark.sql import functions as F

FRAUD_REVIEW_THRESHOLD = 0.80

orders = spark.read.table("orders")            # nothing read
fraud  = spark.read.table("fraud_scores")      # still nothing

review = (orders
    .join(fraud, "order_id")                   # no shuffle happened
    .filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)   # no rows scanned
    .select("order_id", "customer_id", "fraud_score"))        # zero work so far

review.show()   # <-- HERE: read, join, filter, project — one optimized job`, caption: "Five statements of intent, one statement of work." },
    { type: "prose", html: `
<p>Laziness isn't an implementation quirk — it's the point. Because Spark sees the <em>whole</em> recipe before running any of it, the optimizer gets to rewrite it: push the fraud-score filter below the join so fewer rows shuffle, prune the columns you never selected so less data leaves the disk, pick a join strategy based on both sides. An eager engine executing line-by-line can never do this; it has already done the work by the time it learns the work was unnecessary.</p>
<p>The tax you pay: <strong>every action re-runs the recipe from the source</strong>. Call <code>review.count()</code> and then <code>review.show()</code> and you've read, joined, and filtered twice. If a DataFrame feeds multiple actions, that's what <code>cache()</code> is for — but that's a deliberate decision, not a default.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 240" font-family="var(--mono)" font-size="12">
<defs><marker id="d3arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0 L10 5 L0 10 z" fill="var(--ink2)"/></marker></defs>
<text x="20" y="28" fill="var(--ink2)" font-size="11">TRANSFORMATIONS — the recipe grows, nothing runs</text>
<g>
<rect x="20" y="42" width="96" height="40" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="68" y="66" text-anchor="middle" fill="var(--ink)">read</text>
<rect x="146" y="42" width="96" height="40" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="194" y="66" text-anchor="middle" fill="var(--ink)">join</text>
<rect x="272" y="42" width="96" height="40" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="320" y="66" text-anchor="middle" fill="var(--ink)">filter</text>
<rect x="398" y="42" width="96" height="40" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="446" y="66" text-anchor="middle" fill="var(--ink)">select</text>
<line x1="116" y1="62" x2="144" y2="62" stroke="var(--ink2)" marker-end="url(#d3arr)"/>
<line x1="242" y1="62" x2="270" y2="62" stroke="var(--ink2)" marker-end="url(#d3arr)"/>
<line x1="368" y1="62" x2="396" y2="62" stroke="var(--ink2)" marker-end="url(#d3arr)"/>
</g>
<rect x="560" y="34" width="130" height="56" rx="10" fill="none" stroke="var(--accent)" stroke-width="2"/>
<text x="625" y="58" text-anchor="middle" fill="var(--accent)" font-weight="bold">show()</text>
<text x="625" y="76" text-anchor="middle" fill="var(--accent)" font-size="10">ACTION</text>
<line x1="494" y1="62" x2="558" y2="62" stroke="var(--accent)" stroke-width="2" marker-end="url(#d3arr)"/>
<line x1="625" y1="90" x2="625" y2="128" stroke="var(--rust)" stroke-width="2" stroke-dasharray="5 4" marker-end="url(#d3arr)"/>
<text x="20" y="150" fill="var(--ink2)" font-size="11">ONE JOB — the whole recipe, optimized end-to-end, then executed</text>
<rect x="20" y="162" width="670" height="52" rx="10" fill="var(--paper2)" stroke="var(--rust)"/>
<text x="355" y="186" text-anchor="middle" fill="var(--ink)">scan (pruned cols) → filter pushed below join → join → project → results</text>
<text x="355" y="204" text-anchor="middle" fill="var(--ink2)" font-size="10">the optimizer rewrote your order of operations — because it saw all of them first</text>
</svg>`, caption: "Transformations describe; the action executes — after the optimizer has seen the whole plan." },
    { type: "analogy", title: "The pick list, not the picking", html: `
<p>In the NimbusMart warehouse, nobody grabs a trolley the moment an order line arrives. Order lines accumulate on a <strong>pick list</strong>. Only when the dispatcher yells “go” does a picker take the list and walk the floor — and because they can read the <em>whole</em> list first, they plan one route: skip aisle 14 entirely (that item was cancelled — a filter, pushed down), grab both items from aisle 3 in one pass (column pruning), leave the bulky item for last.</p>
<p>Transformations are lines on the pick list. The action is the dispatcher's “go”. A picker who sprinted to the shelves after every line — that's an eager engine, and that's why Spark isn't one.</p>` },
    { type: "javaBridge", html: `
<p>You already know this model: <code>orders.stream().map(...).filter(...)</code> does nothing until a terminal operation like <code>collect(toList())</code> — intermediate ops are lazy, the terminal op pulls. Spark transformations are intermediate ops; actions are terminal ops. Two upgrades to your intuition:</p>
<ul>
<li>A Java stream pipeline runs <em>as written</em> — <code>map</code> then <code>filter</code> stays map-then-filter. Spark hands the whole pipeline to the <strong>Catalyst optimizer</strong>, which happily reorders it (filter before join, prune columns at the scan). You write intent, not an execution order — closer to how the JIT rewrites your bytecode than to how streams execute.</li>
<li>A stream is consumed once and gone. A DataFrame recipe can be re-run by every action — so the surprise isn't “why can't I reuse it?”, it's “why did it run twice?” The answer to that is <code>cache()</code>, used sparingly.</li>
</ul>` },
  ],
  lab: {
    tier: "T2",
    understand: {
      engine: "sparksim",
      datasets: ["orders", "fraud_scores"],
      task: `<p><strong>Build the fraud-review queue and watch <em>when</em> work happens.</strong> The starter code joins <code>orders</code> to <code>fraud_scores</code> but ships every order to the review team — the filter is missing. Add a <code>.filter()</code> keeping only rows with <code>fraud_score >= FRAUD_REVIEW_THRESHOLD</code>, then Run.</p><p>Watch the <em>lazy ribbon</em>: every transformation queues as a gray chip; nothing computes until the <code>show()</code> chip fires. Then check the plan view — did your filter stay where you wrote it?</p>`,
      starterCode: `orders = spark.read.table("orders")
fraud = spark.read.table("fraud_scores")

review = (orders
    .join(fraud, "order_id")
    .select("order_id", "customer_id", "fraud_score", "total_amount"))

review.show()`,
      solutionCode: `orders = spark.read.table("orders")
fraud = spark.read.table("fraud_scores")

review = (orders
    .join(fraud, "order_id")
    .filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)
    .select("order_id", "customer_id", "fraud_score", "total_amount"))

review.show()`,
      expect: { rows: 43, cols: ["order_id", "customer_id", "fraud_score", "total_amount"] },
      dagNotes: `<p>Note the shuffle line before the join — both tables cross the dock so matching <code>order_id</code>s land in the same zone. In the optimized plan, your filter moved <em>below</em> the join: Spark would rather filter 225 fraud rows before shuffling than 225 joined rows after. You wrote the filter last; the optimizer disagreed, politely.</p>`
    },
    buildWithAI: `I'm learning PySpark (transformations vs actions / lazy evaluation). Set up a real local project that demonstrates it. I'm on my own machine; assume nothing is installed beyond Python 3.10+.

1. Create a project folder \`nimbusmart-lazy\` with a venv, and install pyspark (pin any recent 3.5.x) and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator writing two CSVs into \`data/\`:
   - \`orders.csv\`: 240 rows — order_id (O-10001..), customer_id (C-0001..C-0060), total_amount (8..950, 2dp), status from [placed, shipped, delivered, cancelled, returned], country from [DE, US, IN, BR, JP, FR, AU]
   - \`fraud_scores.csv\`: scores for exactly 225 of those orders (15 unscored, chosen with the same seed) — order_id, fraud_score (0.01..0.99, 2dp)

3. Create \`lazy_lab.py\` that:
   - defines FRAUD_REVIEW_THRESHOLD = 0.80 as a named constant
   - builds a SparkSession (local[*])
   - reads both CSVs with EXPLICIT StructType schemas (no inferSchema)
   - builds: review = orders.join(fraud, "order_id").filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD).select("order_id", "total_amount", "fraud_score")
   - prints a wall-clock timestamp BEFORE and AFTER the transformation block, and again around review.show() — so I can see the transformations cost ~0ms and the action pays for everything
   - calls review.explain(True) and prints it, so I can find the PushedFilters in the physical plan

4. Create \`test_lazy_lab.py\` (pytest) asserting:
   - review.count() equals the number of generated fraud rows with score >= FRAUD_REVIEW_THRESHOLD (compute the expected number by re-reading the CSV with plain csv module — do NOT hardcode it)
   - orders.join(fraud, "order_id").count() == 225 (inner join drops unscored orders)
   - the physical plan string for review contains a pushed filter (search explain output for "fraud_score" inside a Filter/PushedFilters section below the join)

5. Run the generator, the lab, and pytest. Show me the explain() output and point at the line proving the filter was pushed below the join. Windows-friendly paths please.`
  },
  check: [
    {
      type: "mcq",
      q: "Eleven notebook cells of <code>select</code>/<code>join</code>/<code>filter</code> “run” in 0.4 seconds; the twelfth cell takes 20 minutes. What is the twelfth cell?",
      options: [
        "An action — the first line that actually demands rows, so it executes the entire accumulated plan",
        "A transformation so complex that Catalyst couldn't optimize it",
        "A cell that triggers garbage collection of the eleven cached DataFrames",
        "A schema inference pass over the source files"
      ],
      answer: 0,
      explain: "Transformations only build the plan; the first action (show/count/collect/write) executes all of it. The 20 minutes was never in cell twelve — it was the bill for cells one through eleven."
    },
    {
      type: "predict",
      q: "On the NimbusMart seed data (240 orders, 15 of them with no fraud score), how many rows does this print?",
      code: `joined = orders.join(fraud_scores, "order_id")   # inner join
print(joined.count())`,
      options: ["240", "225", "255", "43"],
      answer: 1,
      explain: "An inner join keeps only orders with a matching fraud_scores row: 240 − 15 unscored = 225. The 15 unmatched orders silently vanish — which is exactly why the fraud pipeline in E3 uses a left join and treats null scores as 'needs review'."
    },
    {
      type: "mcq",
      q: "A colleague adds <code>review.count()</code> for a log line right before <code>review.write.parquet(...)</code>. What actually happens?",
      options: [
        "Nothing extra — Spark caches the result of count() and reuses it for the write",
        "The whole recipe (read, join, filter) executes twice: once for count, once again for write",
        "count() fails because write hasn't materialized the DataFrame yet",
        "Spark merges both actions into a single job automatically"
      ],
      answer: 1,
      explain: "Every action re-runs the lineage from the sources unless the DataFrame is explicitly cached. One 'innocent' logging count doubles the pipeline's work — see the field notes below for how that compounds."
    },
    {
      type: "mcq",
      q: "Why does Spark's laziness make your pipeline <em>faster</em>, rather than just deferred?",
      options: [
        "Deferring work lets Spark run it on cheaper spot instances",
        "Seeing the whole plan lets the optimizer push filters below joins, prune unread columns, and choose join strategies before any byte is read",
        "Lazy DataFrames use less driver memory, so more executors fit on the cluster",
        "It doesn't — laziness is purely an API convenience inherited from Java Streams"
      ],
      answer: 1,
      explain: "The optimizer needs the full recipe to rewrite it. An eager engine has already shuffled the un-filtered rows by the time it encounters your filter. (And no — Java Streams don't reorder your pipeline; that's the upgrade, not the inheritance.)"
    }
  ],
  fieldNotes: `A marketplace team (name withheld, story real) had a nightly PySpark job that grew from 12 minutes to a little over 3 hours across two quarters, with no meaningful data growth. The postmortem found fourteen <code>df.count()</code> calls added one at a time for "progress logging" — each one an action, each one re-executing the full lineage from S3: the job was reading its source tables roughly fifteen times per run. The fix was deleting thirteen log lines and adding one <code>cache()</code> before the two legitimately-needed actions. Runtime: 11 minutes. The expensive part wasn't the mistake — it was that for two quarters, every engineer who opened the file assumed a count is free, because on the OLTP databases they came from, <code>SELECT COUNT(*)</code> with an index basically is. On Spark, an action is never free; it's the moment the meter starts.`
};
