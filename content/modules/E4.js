// E4 — Window Functions in Spark (Track E) — T2 sparksim
// Verified facts (data/nimbusmart/generate.py, seed 42):
//   orders = 240; a window adds a column and keeps all 240 rows (no collapse).
//   order_events = 361 rows across 180 distinct order_ids — dedup (row_number==1) => 180.
//   courier_pings = 278 across all 12 couriers; K-08 leads with 39 pings.
export default {
  id: "E4",
  track: "E",
  title: "Window Functions in Spark",
  minutes: 26,
  coldOpen: "The NimbusMart courier-performance dashboard needs a running total of revenue delivered per seller and a leaderboard of the busiest couriers. A new engineer reaches for groupBy, gets one collapsed row per seller, and can't figure out how to also keep every individual order visible. They spend an afternoon writing a self-join to bolt the totals back on. The whole thing is one window spec — three lines — and it doesn't shuffle the way the self-join did.",
  concept: [
    { type: "prose", html: `
<p>A window function answers questions of the shape "for each row, compute something over the rows <em>related</em> to it" — a running total, a rank, the latest version — <strong>without collapsing the rows</strong>. That's the whole difference from <code>groupBy</code>: an aggregate <em>replaces</em> its group with one row; a window <em>annotates</em> each row and leaves all of them standing.</p>
<p>Every window has up to three ingredients:</p>
<ul>
<li><code>partitionBy(...)</code> — which rows count as "related" (all of one seller's orders, all of one order's events). Like a groupBy key, but nothing collapses.</li>
<li><code>orderBy(...)</code> — the sequence within a partition (by <code>order_ts</code>, by <code>total_amount desc</code>). This is what makes "running" and "rank" mean anything.</li>
<li>the <strong>frame</strong> — which slice of the ordered partition to aggregate. Defaults matter, and they bite (below).</li>
</ul>` },
    { type: "code", lang: "python", code: `from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Leaderboard: rank couriers by ping volume (busiest first)
by_volume = Window.partitionBy("home_zone").orderBy(F.col("pings").desc())
leaderboard = (courier_pings
    .groupBy("courier_id").agg(F.count("*").alias("pings"))
    .join(couriers, "courier_id")
    .withColumn("zone_rank", F.rank().over(by_volume)))

# 2. Running revenue per seller, in chronological order
to_date = Window.partitionBy("seller_id").orderBy("order_ts")
running = orders.withColumn(
    "revenue_to_date", F.sum("total_amount").over(to_date))

# 3. Deduplicate events: keep the latest row per order_id
latest = Window.partitionBy("order_id").orderBy(F.col("event_ts").desc())
deduped = (order_events
    .withColumn("rn", F.row_number().over(latest))
    .filter(F.col("rn") == 1)
    .drop("rn"))`, caption: "One spec, three shapes: rank, running aggregate, and the dedup idiom every pipeline needs." },
    { type: "prose", html: `
<p>Two things decide whether a window does what you meant.</p>
<p><strong>Ordered vs unordered changes the frame — and the answer.</strong> <code>F.sum("x").over(partitionBy(k))</code> with <em>no</em> orderBy sums the whole partition: every row gets the same group total. Add an <code>orderBy</code> and the default frame becomes "unbounded preceding through the current row" — now the same <code>F.sum</code> is a <em>running</em> total that grows down the partition. Same function, one clause apart, completely different column. Ordering by <code>total_amount</code> instead of <code>order_ts</code> gives a running total that climbs by order size, not by time — a real and common bug.</p>
<p><strong>The three rankers differ only on ties.</strong> <code>row_number()</code> assigns 1,2,3,4 — arbitrary but unique, which is exactly what dedup needs (one winner per key). <code>rank()</code> gives 1,2,2,4 (ties share, then it skips). <code>dense_rank()</code> gives 1,2,2,3 (ties share, no gap). For "keep the latest row per order", you want <code>row_number() == 1</code> — <code>rank</code> would keep <em>both</em> rows on a timestamp tie and re-duplicate the thing you're de-duplicating.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 240" font-family="var(--mono)" font-size="12">
<text x="20" y="22" fill="var(--ink2)" font-size="11">partition: seller S-777, ordered by order_ts — running sum grows, rows stay</text>
<g>
<rect x="20" y="36" width="150" height="26" rx="4" fill="var(--paper2)" stroke="var(--line)"/><text x="30" y="53" fill="var(--ink)" font-size="10">order 1 · 120</text><text x="150" y="53" text-anchor="end" fill="var(--accent)" font-size="10">120</text>
<rect x="20" y="66" width="150" height="26" rx="4" fill="var(--paper2)" stroke="var(--line)"/><text x="30" y="83" fill="var(--ink)" font-size="10">order 2 · 80</text><text x="150" y="83" text-anchor="end" fill="var(--accent)" font-size="10">200</text>
<rect x="20" y="96" width="150" height="26" rx="4" fill="var(--paper2)" stroke="var(--line)"/><text x="30" y="113" fill="var(--ink)" font-size="10">order 3 · 300</text><text x="150" y="113" text-anchor="end" fill="var(--accent)" font-size="10">500</text>
<rect x="20" y="126" width="150" height="26" rx="4" fill="var(--paper2)" stroke="var(--line)"/><text x="30" y="143" fill="var(--ink)" font-size="10">order 4 · 50</text><text x="150" y="143" text-anchor="end" fill="var(--accent)" font-size="10">550</text>
</g>
<path d="M188 49 L188 139" stroke="var(--rust)" stroke-width="2" marker-end="url(#e4a)"/>
<defs><marker id="e4a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0 L10 5 L0 10 z" fill="var(--rust)"/></marker></defs>
<text x="200" y="66" fill="var(--rust)" font-size="10">frame: unbounded</text>
<text x="200" y="80" fill="var(--rust)" font-size="10">preceding →</text>
<text x="200" y="94" fill="var(--rust)" font-size="10">current row</text>
<text x="200" y="128" fill="var(--ink2)" font-size="10">(revenue_to_date)</text>
<line x1="360" y1="30" x2="360" y2="215" stroke="var(--line)" stroke-dasharray="3 3"/>
<text x="390" y="22" fill="var(--ink2)" font-size="11">same rows, three rankers on a tie at rows 2 &amp; 3</text>
<text x="390" y="50" fill="var(--ink)" font-size="11">row_number():  1 · 2 · 3 · 4  </text><text x="640" y="50" fill="var(--accent)" font-size="10">unique → dedup</text>
<text x="390" y="80" fill="var(--ink)" font-size="11">rank():        1 · 2 · 2 · 4  </text><text x="640" y="80" fill="var(--ink2)" font-size="10">gap after tie</text>
<text x="390" y="110" fill="var(--ink)" font-size="11">dense_rank():  1 · 2 · 2 · 3  </text><text x="640" y="110" fill="var(--ink2)" font-size="10">no gap</text>
<text x="390" y="160" fill="var(--ink2)" font-size="10">keep-latest-per-key uses row_number()==1:</text>
<text x="390" y="176" fill="var(--ink2)" font-size="10">it breaks ties, so exactly one row per order</text>
<text x="390" y="192" fill="var(--ink2)" font-size="10">survives — rank() would keep both and re-dupe.</text>
</svg>`, caption: "An ordered window makes sum() cumulative; the three rankers diverge only when two rows tie." },
    { type: "analogy", title: "The chalk tally, not the sealed carton", html: `
<p>At the end of each seller's packing lane, a clerk keeps a running <strong>chalk tally</strong> on a board: as every parcel rolls past, they add its value and update the number — but the parcel keeps moving down the line, untouched. Anyone can look at any parcel and read the running total <em>as of that parcel</em>. That's a window function: the aggregate rides alongside each row, and every row is still there.</p>
<p>Contrast the <code>groupBy</code> from the last module: that clerk takes the whole lane's parcels, seals them into one carton labelled "seller total: 12,400", and the individual parcels are gone. Both answers are useful — but when the question is "show me each order <em>and</em> the running total it contributed to", you want the chalk board, not the sealed carton. The busiest-courier leaderboard is the same board with numbered tickets instead of a sum: rank each courier in their zone without removing anyone from the queue.</p>` },
    { type: "javaBridge", html: `
<p>You already know windows from SQL — <code>ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY order_ts)</code> is the exact same construct; PySpark's <code>Window.partitionBy(...).orderBy(...)</code> is that <code>OVER</code> clause as a builder object. The Java-stream instinct is where it gets interesting:</p>
<ul>
<li><strong>A running total is a stateful fold that emits per element.</strong> In Java you'd hand-roll it: sort the list, carry a running accumulator, emit <code>(row, accumulator)</code> at each step — a stateful loop, because <code>Stream.reduce</code> gives you only the <em>final</em> value, not the intermediate ones. A window function is that loop expressed declaratively, and Spark distributes it: partitions are shuffled by the <code>partitionBy</code> key, sorted by the <code>orderBy</code> key, then scanned once.</li>
<li><strong>It shuffles like a groupBy, so the same skew rules apply.</strong> <code>partitionBy("seller_id")</code> puts all of S-777's 80 orders on one task, exactly as E3's join did. A window over a hot key is a straggler waiting to happen — the ordering within the partition is the cheap part; getting the partition co-located is the shuffle you pay for.</li>
</ul>` },
  ],
  lab: {
    tier: "T2",
    understand: {
      engine: "sparksim",
      datasets: ["orders"],
      task: `<p><strong>Build each seller's running revenue — in the order it actually accrued.</strong> The starter computes a cumulative <code>revenue_to_date</code> with a window, but it orders that window by <code>total_amount</code> descending, so the "running" total climbs from the biggest order down instead of forward through time. It's cumulative, just not chronological. Change the window's <code>.orderBy(...)</code> to <code>"order_ts"</code>, then Run.</p><p>The row count won't budge — a window annotates, it never collapses — but every <code>revenue_to_date</code> value changes, because the frame ("everything up to this row") now walks the calendar instead of the price tag.</p>`,
      starterCode: `orders = spark.read.table("orders")

w = Window.partitionBy("seller_id").orderBy(F.col("total_amount").desc())

running = orders.withColumn(
    "revenue_to_date", F.sum("total_amount").over(w))

running.select(
    "seller_id", "order_ts", "total_amount", "revenue_to_date"
).show()`,
      solutionCode: `orders = spark.read.table("orders")

w = Window.partitionBy("seller_id").orderBy("order_ts")

running = orders.withColumn(
    "revenue_to_date", F.sum("total_amount").over(w))

running.select(
    "seller_id", "order_ts", "total_amount", "revenue_to_date"
).show()`,
      expect: { rows: 240, cols: ["seller_id", "order_ts", "total_amount", "revenue_to_date"] },
      dagNotes: `<p>240 rows in, 240 rows out — proof that a window is not an aggregate. The plan shuffles once, by <code>seller_id</code> (the partitionBy key), then sorts each partition by the orderBy key and scans it. Because S-777 owns 80 of the 240 orders, that seller's partition is 3.5x the others — the same skew shape you saw in E3's join, now inside a window. The last row of each seller's partition holds that seller's grand total; every row above it is the running total as of that moment.</p>`
    },
    buildWithAI: `I'm learning PySpark window functions (ranking, running aggregates, and the keep-latest-per-key dedup idiom). Build me a real local project on my own machine. Assume nothing beyond Python 3.10+.

1. Create a folder \`nimbusmart-windows\` with a venv; install pyspark (recent 3.5.x) and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) writer emitting Parquet into \`data/\`:
   - orders: 240 rows — order_id, seller_id from [S-101,S-204,S-355,S-410,S-777,S-812,S-903] with S-777 ~35%, order_ts as an ISO string, total_amount (8..950, 2dp).
   - order_events: ~360 rows across ~180 distinct order_ids (so many orders have MULTIPLE events) — event_id, order_id, event_type from [cart_add,checkout_start,payment_submitted,fraud_check,fulfillment_hold,shipped_scan], event_ts as an ISO string.

3. Create \`windows_lab.py\` that builds a SparkSession (local[*]), reads both with explicit StructType, and:
   - running revenue: w = Window.partitionBy("seller_id").orderBy("order_ts"); orders.withColumn("revenue_to_date", F.sum("total_amount").over(w)) — show a few sellers so I see the total climb
   - dedup: latest = Window.partitionBy("order_id").orderBy(F.col("event_ts").desc()); order_events.withColumn("rn", F.row_number().over(latest)).filter("rn = 1").drop("rn")
   - show the SAME dedup done wrong with rank() instead of row_number(), and print a case where a timestamp tie makes rank() keep two rows

4. Create \`test_windows_lab.py\` (pytest) asserting — deriving expected values from the raw files, never hardcoding:
   - the running-revenue frame preserves the row count (input rows == output rows)
   - each seller's final revenue_to_date equals that seller's plain SUM(total_amount)
   - the deduped event set has exactly one row per distinct order_id, and the count equals the number of distinct order_ids in the source

5. Run the generator, the lab, and pytest. Show me a seller's running total climbing row by row, and the row_number-vs-rank dedup difference on a tie. Windows-friendly paths please.`
  },
  check: [
    {
      type: "predict",
      q: "On the seed data (240 orders), how many rows does this print?",
      code: `w = Window.partitionBy("seller_id").orderBy("order_ts")
running = orders.withColumn(
    "revenue_to_date", F.sum("total_amount").over(w))
print(running.count())`,
      options: ["240", "7", "80", "1"],
      answer: 0,
      explain: "A window function annotates each row; it never collapses the set the way groupBy does. All 240 orders survive, each gaining a revenue_to_date column. If this were groupBy('seller_id').agg(...), you'd get 7 rows — one per seller — instead."
    },
    {
      type: "mcq",
      q: "What is the core difference between <code>groupBy(\"seller_id\").agg(F.sum(...))</code> and <code>F.sum(...).over(Window.partitionBy(\"seller_id\"))</code>?",
      options: [
        "The window keeps every input row and attaches the aggregate to each; the groupBy collapses each seller to a single output row",
        "The window is faster because it never shuffles, while groupBy always shuffles",
        "They are identical — one is just SQL syntax and the other is the DataFrame API",
        "The window can only compute counts, while groupBy can compute any aggregate"
      ],
      answer: 0,
      explain: "Both aggregate per seller and both shuffle by the key. The difference is output cardinality: groupBy returns one row per group; the window returns every original row with the aggregate alongside. Choose the window when you need the detail rows and the aggregate together."
    },
    {
      type: "predict",
      q: "order_events has 361 rows across 180 distinct order_ids. How many rows survive this dedup?",
      code: `w = Window.partitionBy("order_id").orderBy(
    F.col("event_ts").desc())
deduped = (order_events
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1))
print(deduped.count())`,
      options: ["180", "361", "240", "1"],
      answer: 0,
      explain: "row_number()==1 keeps exactly the top row of each partition, and there is one partition per distinct order_id. With 180 distinct order_ids, 180 rows survive — the latest event for each order. This is the canonical keep-latest-per-key pattern that sits under a Delta MERGE (E6)."
    },
    {
      type: "mcq",
      q: "For keep-latest-per-key dedup, why <code>row_number()</code> rather than <code>rank()</code>?",
      options: [
        "row_number() is always unique within a partition, so exactly one row wins even when two timestamps tie; rank() gives tied rows the same 1 and keeps both",
        "rank() cannot be used inside a filter, so it would raise an error",
        "row_number() is faster because it doesn't need the orderBy clause",
        "There is no difference — both keep exactly one row per key"
      ],
      answer: 0,
      explain: "On a timestamp tie, rank() assigns 1 to both rows, so filtering rank==1 keeps two rows and re-introduces the duplicate you were removing. row_number() breaks ties arbitrarily but uniquely, guaranteeing a single winner per key."
    }
  ],
  fieldNotes: `A payments team built a "latest status per transaction" view with a window deduped on rank()==1, ordered by updated_at. It was correct for eighteen months — until a batch reprocessing job rewrote a day of history and stamped thousands of rows with the exact same updated_at second. rank() gave every tied row a 1, the dedup kept all of them, and the downstream reconciliation double-counted 4.3 million euros of settled payments before anyone noticed the ledger didn't foot. The one-character fix — rank to row_number — made the winner unique again, but the deeper lesson was that their ordering key wasn't total: they added a tiebreaker (updated_at, then a monotonic event sequence) so "latest" was never ambiguous. Any dedup whose sort key can tie is a duplicate waiting for the day two rows share a timestamp.`
};
