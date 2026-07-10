// E2 — Transformations Deep-Dive (Track E)
// Verified facts (data/nimbusmart/generate.py, seed 42):
//   orders = 240 rows; total_amount >= 500 in 114 rows (126 below).
//   distinct status values = 5 (placed, shipped, delivered, cancelled, returned).
//   groupBy("high_value") over the >=500 flag yields 2 groups (114 true / 126 false).
export default {
  id: "E2",
  track: "E",
  title: "Transformations Deep-Dive",
  minutes: 24,
  coldOpen: "A NimbusMart data engineer rewrites the daily revenue rollup and the numbers look right — but the job that used to finish in 4 minutes now takes 26. The diff is three lines: two harmless-looking withColumn calls and a groupBy that was always there. Nothing about the data changed. The cost changed because one of those transformations quietly redrew a stage boundary, and the other two didn't.",
  concept: [
    { type: "prose", html: `
<p>Almost every PySpark job is built from five verbs. Knowing which caste each belongs to — <strong>narrow</strong> or <strong>wide</strong> — tells you where the money goes before you run a thing.</p>
<ul>
<li><code>select(...)</code> — choose or compute columns. <em>Narrow.</em></li>
<li><code>filter(...)</code> / <code>where(...)</code> — choose rows. <em>Narrow.</em></li>
<li><code>withColumn(name, expr)</code> — add or replace one column. <em>Narrow.</em></li>
<li><code>groupBy(...).agg(...)</code> — collapse many rows into one per key. <em>Wide — it shuffles.</em></li>
<li><code>orderBy(...)</code> — total ordering across the whole set. <em>Wide.</em></li>
</ul>
<p>A <strong>narrow</strong> transformation touches each row using only that row's data, so a task can run start-to-finish inside one partition — no data moves between machines. A <strong>wide</strong> transformation needs rows from other partitions to sit together (every <code>country</code>'s rows must meet to be counted), which forces a <em>shuffle</em>: a stage boundary, data written to disk and pulled across the network. That's the line the revenue-rollup engineer crossed.</p>` },
    { type: "code", lang: "python", code: `from pyspark.sql import functions as F

HIGH_VALUE_EUR = 500

rollup = (spark.read.table("orders")
    .filter(F.col("status") != "cancelled")                       # narrow: drop rows
    .withColumn("high_value",                                     # narrow: add a column
                F.col("total_amount") >= F.lit(HIGH_VALUE_EUR))
    .withColumn("net_amount",                                     # narrow: another column
                F.col("total_amount") - F.lit(2.50))
    .groupBy("country", "high_value")                            # WIDE: shuffle boundary
    .agg(F.count("*").alias("orders"),
         F.sum("total_amount").alias("gross"),
         F.avg("total_amount").alias("avg_ticket")))

rollup.show()`, caption: "Three narrow steps ride inside one stage; the groupBy is where the stage — and the bill — breaks." },
    { type: "prose", html: `
<p>Two things about <code>withColumn</code> that trip up newcomers from an imperative background:</p>
<p><strong>It's declarative, not a loop.</strong> <code>F.col("total_amount") >= F.lit(500)</code> doesn't run a comparison — it builds an <em>expression tree</em> that Catalyst compiles into generated code over the columnar buffers. You're describing a column, the way you'd write a SQL <code>CASE</code>, not iterating rows. That's why there's no per-row Python cost here (contrast E5's UDFs, where there is).</p>
<p><strong>Chaining many <code>withColumn</code>s is free-ish, but not a free-for-all.</strong> Ten chained <code>withColumn</code> calls don't mean ten passes — Catalyst folds them into one projection. But <code>withColumn</code> returns a <em>new</em> DataFrame each time, and if a name already exists it's <em>replaced</em>, silently. Add <code>high_value</code> twice and only the second definition survives — a classic source of "why is my flag always false".</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 240" font-family="var(--mono)" font-size="12">
<defs><marker id="e2arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0 L10 5 L0 10 z" fill="var(--ink2)"/></marker></defs>
<text x="20" y="24" fill="var(--accent)" font-size="11">STAGE 1 — narrow: each partition runs alone, no data moves</text>
<rect x="20" y="34" width="130" height="150" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="85" y="52" text-anchor="middle" fill="var(--ink2)" font-size="10">partition A</text>
<rect x="34" y="60" width="102" height="22" rx="5" fill="none" stroke="var(--line)"/><text x="85" y="75" text-anchor="middle" fill="var(--ink)" font-size="10">filter</text>
<rect x="34" y="88" width="102" height="22" rx="5" fill="none" stroke="var(--line)"/><text x="85" y="103" text-anchor="middle" fill="var(--ink)" font-size="10">withColumn</text>
<rect x="34" y="116" width="102" height="22" rx="5" fill="none" stroke="var(--line)"/><text x="85" y="131" text-anchor="middle" fill="var(--ink)" font-size="10">withColumn</text>
<rect x="170" y="34" width="130" height="150" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="235" y="52" text-anchor="middle" fill="var(--ink2)" font-size="10">partition B</text>
<rect x="184" y="60" width="102" height="22" rx="5" fill="none" stroke="var(--line)"/><text x="235" y="75" text-anchor="middle" fill="var(--ink)" font-size="10">filter</text>
<rect x="184" y="88" width="102" height="22" rx="5" fill="none" stroke="var(--line)"/><text x="235" y="103" text-anchor="middle" fill="var(--ink)" font-size="10">withColumn</text>
<rect x="184" y="116" width="102" height="22" rx="5" fill="none" stroke="var(--line)"/><text x="235" y="131" text-anchor="middle" fill="var(--ink)" font-size="10">withColumn</text>
<line x1="300" y1="110" x2="360" y2="110" stroke="var(--rust)" stroke-width="2" stroke-dasharray="5 4" marker-end="url(#e2arr)"/>
<text x="330" y="100" text-anchor="middle" fill="var(--rust)" font-size="9">SHUFFLE</text>
<text x="380" y="24" fill="var(--rust)" font-size="11">STAGE 2 — wide: rows re-routed by key so groups meet</text>
<rect x="380" y="34" width="320" height="150" rx="8" fill="var(--paper2)" stroke="var(--rust)"/>
<text x="540" y="54" text-anchor="middle" fill="var(--ink2)" font-size="10">groupBy(country) → agg</text>
<rect x="400" y="66" width="130" height="26" rx="5" fill="none" stroke="var(--line)"/><text x="465" y="83" text-anchor="middle" fill="var(--ink)" font-size="10">DE · sum · count</text>
<rect x="400" y="100" width="130" height="26" rx="5" fill="none" stroke="var(--line)"/><text x="465" y="117" text-anchor="middle" fill="var(--ink)" font-size="10">US · sum · count</text>
<rect x="550" y="66" width="130" height="26" rx="5" fill="none" stroke="var(--line)"/><text x="615" y="83" text-anchor="middle" fill="var(--ink)" font-size="10">JP · sum · count</text>
<rect x="550" y="100" width="130" height="26" rx="5" fill="none" stroke="var(--line)"/><text x="615" y="117" text-anchor="middle" fill="var(--ink)" font-size="10">FR · sum · count</text>
<text x="540" y="150" text-anchor="middle" fill="var(--ink2)" font-size="9">240 rows in → one row per country out</text>
</svg>`, caption: "Narrow verbs stack inside a stage for free; the groupBy is the stage boundary where rows cross the network." },
    { type: "analogy", title: "Your own bench vs. the cross-dock", html: `
<p>A picker at the NimbusMart warehouse does plenty of work without ever leaving their station: check a parcel's label (<code>filter</code>), slap a "HIGH VALUE — insure" sticker on anything over 500 euros (<code>withColumn</code>), scratch out the old weight and write the reweighed one (<code>withColumn</code> replacing a column). All of it happens at one bench, on parcels already in front of them — that's a <strong>narrow</strong> transformation, and a whole crew does it in parallel with zero coordination.</p>
<p>Now the shift lead asks for <em>the total value of parcels per destination country</em>. No single bench can answer that — parcels for Germany are scattered across forty benches. Everything bound for Germany must be forklifted to one consolidation zone, everything for Japan to another: the <strong>cross-dock transfer</strong>. That trip is the shuffle, and it's the single most expensive move in the building. <code>groupBy</code> orders it every time.</p>` },
    { type: "javaBridge", html: `
<p>You've written this pipeline before, single-machine:</p>
<pre style="font-size:11px;overflow-x:auto"><code>orders.stream()
  .filter(o -&gt; !o.status.equals("cancelled"))   // filter
  .map(o -&gt; o.withHighValue(o.total &gt;= 500))     // withColumn
  .collect(groupingBy(Order::country,             // groupBy
           summingDouble(Order::total)));         // agg</code></pre>
<p>The mapping is one-to-one — <code>filter</code>→<code>filter</code>, <code>map</code>→<code>withColumn</code>, <code>Collectors.groupingBy</code>→<code>groupBy().agg()</code>. Two upgrades to your intuition:</p>
<ul>
<li><strong>The grouping is where "distributed" stops being free.</strong> <code>groupingBy</code> builds one <code>HashMap</code> in one heap — every element is already reachable. Spark's <code>groupBy</code> can't assume that: same-key rows live on different machines, so it must <em>move them</em> (the shuffle) before it can fold them. That network step has no analogue in your stream, and it's usually the most expensive thing in the job.</li>
<li><strong><code>withColumn</code> isn't a lambda, it's an AST.</strong> Your <code>map</code> runs a closure per element; <code>F.col(...) >= F.lit(500)</code> builds an expression node that Catalyst compiles once into vectorized code. Closer to constructing a <code>CriteriaQuery</code> than to running a lambda.</li>
</ul>` },
  ],
  lab: {
    tier: "T2",
    understand: {
      engine: "sparksim",
      datasets: ["orders"],
      task: `<p><strong>Split NimbusMart's order book into high- and low-value tiers, then measure each.</strong> The starter flags any order at or above 500 euros with a <code>high_value</code> column and groups by that flag — but it only counts orders. Finance also wants the <em>gross revenue</em> in each tier. Add a <code>F.sum("total_amount").alias("gross")</code> to the <code>.agg(...)</code>, then Run.</p><p>You'll get exactly two rows — <code>true</code> and <code>false</code>. Note that the <code>withColumn</code> and <code>filter</code> ran inside one stage, but the <code>groupBy</code> drew a shuffle boundary: watch it appear in the DAG.</p>`,
      starterCode: `orders = spark.read.table("orders")

tagged = orders.withColumn(
    "high_value", F.col("total_amount") >= F.lit(500))

summary = (tagged
    .groupBy("high_value")
    .agg(F.count("*").alias("orders")))

summary.show()`,
      solutionCode: `orders = spark.read.table("orders")

tagged = orders.withColumn(
    "high_value", F.col("total_amount") >= F.lit(500))

summary = (tagged
    .groupBy("high_value")
    .agg(F.count("*").alias("orders"),
         F.sum("total_amount").alias("gross")))

summary.show()`,
      expect: { rows: 2, cols: ["high_value", "orders", "gross"] },
      dagNotes: `<p>Two rows out of 240 in — the groupBy collapsed the set to one row per distinct <code>high_value</code> value (114 orders land in the <code>true</code> tier, 126 in <code>false</code>). The <code>withColumn</code> added a column without moving a single row; the <code>groupBy</code> is the one operation here that shuffled. In the plan, everything above the exchange node is Stage 1, everything below is Stage 2 — that exchange is the network hop the cross-dock analogy is warning you about.</p>`
    },
    buildWithAI: `I'm learning PySpark transformations (select / filter / withColumn / groupBy / agg, and the narrow-vs-wide distinction). Build me a real local project on my own machine. Assume nothing beyond Python 3.10+.

1. Create a folder \`nimbusmart-transforms\` with a venv; install pyspark (recent 3.5.x) and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) writer emitting \`data/orders.csv\`:
   - 240 rows: order_id (O-10001..), customer_id (C-0001..C-0060), seller_id from [S-101,S-204,S-355,S-410,S-777,S-812,S-903] with S-777 ~35%, order_ts ISO string, status from [placed,shipped,delivered,cancelled,returned], total_amount (8..950, 2dp), item_count (1..6), country from [DE,US,IN,BR,JP,FR,AU], channel from [web,app].

3. Create \`transforms_lab.py\` that builds a SparkSession (local[*]), reads the CSV with an EXPLICIT StructType (no inferSchema), and:
   - defines HIGH_VALUE_EUR = 500 as a named constant
   - adds a boolean column high_value = total_amount >= HIGH_VALUE_EUR via withColumn
   - builds: summary = tagged.groupBy("high_value").agg(F.count("*").alias("orders"), F.sum("total_amount").alias("gross"), F.avg("total_amount").alias("avg_ticket"))
   - also builds a by_country rollup: groupBy("country").agg(count, sum) ordered by gross desc
   - calls summary.explain(True) and prints it — point me at the Exchange (shuffle) node the groupBy introduces, and confirm the withColumn adds NO shuffle

4. Create \`test_transforms_lab.py\` (pytest) asserting — computing every expected value by re-reading the CSV with the plain csv module, never hardcoding:
   - the count of orders with total_amount >= 500 matches the summary's high_value=true 'orders' cell
   - the two tiers' order counts sum to 240
   - the by_country rollup has one row per distinct country present in the data
   - the physical plan string for summary contains exactly one Exchange node (one shuffle)

5. Run the generator, the lab, and pytest. Show me the summary table, the by_country table, and the explain() output with the Exchange node highlighted. Windows-friendly paths please.`
  },
  check: [
    {
      type: "predict",
      q: "On the NimbusMart seed data, how many orders does this count?",
      code: `hv = (spark.read.table("orders")
    .filter(F.col("total_amount") >= F.lit(500)))
print(hv.count())`,
      options: ["114", "126", "240", "43"],
      answer: 0,
      explain: "114 of the 240 orders are at or above 500 euros (the other 126 fall below). This is a pure narrow filter — no shuffle — so the count is simply how many rows survive the predicate."
    },
    {
      type: "mcq",
      q: "Of <code>filter</code>, <code>withColumn</code>, and <code>groupBy().agg()</code>, which one draws a stage boundary — and why?",
      options: [
        "<code>groupBy().agg()</code> — same-key rows live on different partitions, so Spark must shuffle them together before it can aggregate",
        "<code>filter</code> — dropping rows renumbers the partitions, forcing a re-shuffle",
        "<code>withColumn</code> — adding a column rewrites every partition and triggers an exchange",
        "None of them — all three are narrow and run inside a single stage"
      ],
      answer: 0,
      explain: "filter and withColumn are narrow: each task works within one partition, no data moves. groupBy is wide — rows for the same key must be co-located, which means a shuffle: the exchange node, the stage boundary, and usually the dominant cost."
    },
    {
      type: "predict",
      q: "How many rows does this produce on the seed data?",
      code: `by_status = (spark.read.table("orders")
    .groupBy("status")
    .agg(F.count("*").alias("n")))
print(by_status.count())`,
      options: ["5", "240", "7", "3"],
      answer: 0,
      explain: "groupBy collapses the 240 orders to one row per distinct status. The seed data uses five status values — placed, shipped, delivered, cancelled, returned — so the result has 5 rows regardless of how many orders fall in each."
    },
    {
      type: "mcq",
      q: "A colleague writes <code>df.withColumn(\"high_value\", ...500...)</code> and, twenty lines later, <code>df2.withColumn(\"high_value\", ...50...)</code> on the derived frame. The flag is wrong everywhere downstream. Why?",
      options: [
        "withColumn replaces a column when the name already exists, so the second definition silently overwrote the first",
        "withColumn can only be called once per DataFrame; the second call is ignored",
        "Two withColumn calls with the same name throw an AnalysisException at runtime",
        "The first flag is cached, so the second call never actually recomputes it"
      ],
      answer: 0,
      explain: "withColumn(name, expr) adds the column, or replaces it in place if name already exists — no error, no warning. Reusing high_value redefined it against a different threshold, and only the last definition survives in the schema."
    }
  ],
  fieldNotes: `A retail analytics team shipped a "small" refactor that reordered a pipeline: they moved a groupBy earlier to \"aggregate sooner\" and pushed a filter after it for readability. Runtime went from 6 minutes to 22. The filter had been removing 70% of rows before the shuffle; now it ran after, so the groupBy shuffled the full dataset — three times the bytes across the network, three times the spill to disk. The data was identical; only the order of two transformations changed. The fix was a one-line move putting the filter back above the groupBy, and the takeaway went into their review checklist: narrow before wide, always — every row you can drop before a shuffle is a row you never pay to move. Catalyst usually pushes filters down for you, but the moment a UDF or a non-deterministic expression sits between them, it stops, and you're back to paying for the rows you meant to discard.`
};
