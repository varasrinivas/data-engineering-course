// G4 — Performance & Cost Tuning Basics (Track G, Quality/Governance/Ops)
// Verified facts used by lab + checks (from data/nimbusmart/seed.js, seed 42):
//   orders = 240; fraud_scores = 225 (inner join = 225); review >= FRAUD_REVIEW_THRESHOLD = 43.
//   Lab: 'enriched' (the join) feeds multiple downstream actions; without cache each
//     action recomputes the join+scan; .cache() materializes it once. review = 43 rows.
export default {
  id: "G4",
  track: "G",
  title: "Performance & Cost Tuning Basics",
  minutes: 26,
  coldOpen: "The nightly fraud-metrics job costs $60 in compute. A new hire adds three summary tables off the same enriched DataFrame — reasonable, each is one small aggregate — and the bill jumps to $190 overnight with no more data. The enriched join wasn't cached, so each of the four actions re-read both source tables and re-shuffled the join from scratch. Four honest one-liners, four full recomputes. The fix was one word: cache().",
  concept: [
    { type: "prose", html: `
<p>Performance tuning in Spark is not folklore or a bag of magic configs. It's three levers you pull deliberately, each backed by a number you can read off the <strong>Spark UI</strong>: how many <strong>partitions</strong> the work is split into, whether a reused DataFrame is <strong>cached</strong>, and how much data each stage <strong>reads and shuffles</strong>. Get those three right and most jobs are fast; the rest is details.</p>
<ul>
<li><strong>Caching</strong> — every action re-runs a DataFrame's full recipe from the source (the lazy-evaluation tax from D3). If one DataFrame feeds <em>multiple</em> actions, <code>cache()</code> materializes it once and the later actions read the cached copy. Cache exactly the DataFrames that are (a) reused and (b) expensive to rebuild — not everything, because cache costs memory.</li>
<li><strong>Partition count</strong> — too few and you can't use all your cores (idle executors); too many and you drown in per-task scheduling overhead (the small-files problem, in memory). The default post-shuffle count is 200 (<code>spark.sql.shuffle.partitions</code>); for a 43-row review queue that's 200 tasks doing nothing — a real, if tiny, waste.</li>
<li><strong>Read/shuffle volume</strong> — the cheapest byte is the one you never move. <strong>Projection</strong> (select only the columns you need) shrinks every scan and every shuffle; <strong>filtering early</strong> shrinks what crosses the network. A shuffle is the expensive cross-dock forklift trip; less data on the forklift is the whole game.</li>
</ul>
<p>The discipline: don't tune by vibes. Open the Spark UI, find the stage that dominates wall-clock, and read <em>why</em> — is it re-scanning because nothing's cached? Shuffling columns you never use? Split into 200 tasks for 43 rows? Every one of those is visible, and every fix is a specific line of code.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 250" font-family="var(--mono)" font-size="12">
<text x="20" y="20" fill="var(--ink2)" font-size="11">WITHOUT cache — 'enriched' (a join) feeds 3 actions · each re-runs the whole recipe</text>
<rect x="20" y="34" width="110" height="34" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="75" y="55" text-anchor="middle" fill="var(--ink)" font-size="10">scan+join</text>
<rect x="20" y="74" width="110" height="34" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="75" y="95" text-anchor="middle" fill="var(--ink)" font-size="10">scan+join</text>
<rect x="20" y="114" width="110" height="34" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="75" y="135" text-anchor="middle" fill="var(--ink)" font-size="10">scan+join</text>
<rect x="180" y="34" width="90" height="34" rx="8" fill="none" stroke="var(--rust)"/><text x="225" y="55" text-anchor="middle" fill="var(--rust)" font-size="10">count()</text>
<rect x="180" y="74" width="90" height="34" rx="8" fill="none" stroke="var(--rust)"/><text x="225" y="95" text-anchor="middle" fill="var(--rust)" font-size="10">show()</text>
<rect x="180" y="114" width="90" height="34" rx="8" fill="none" stroke="var(--rust)"/><text x="225" y="135" text-anchor="middle" fill="var(--rust)" font-size="10">write()</text>
<line x1="130" y1="51" x2="178" y2="51" stroke="var(--rust)" stroke-width="1.5"/>
<line x1="130" y1="91" x2="178" y2="91" stroke="var(--rust)" stroke-width="1.5"/>
<line x1="130" y1="131" x2="178" y2="131" stroke="var(--rust)" stroke-width="1.5"/>
<text x="150" y="176" fill="var(--rust)" font-size="10">3× the scan + 3× the shuffle</text>
<line x1="330" y1="30" x2="330" y2="185" stroke="var(--line)" stroke-dasharray="4 4"/>
<text x="380" y="20" fill="var(--ink2)" font-size="11">WITH .cache() — the join runs ONCE, materialized, then reused</text>
<rect x="380" y="74" width="120" height="34" rx="8" fill="var(--paper2)" stroke="var(--accent)" stroke-width="2"/><text x="440" y="95" text-anchor="middle" fill="var(--accent)" font-size="10">scan+join ✓cached</text>
<rect x="560" y="34" width="90" height="30" rx="8" fill="none" stroke="var(--accent)"/><text x="605" y="54" text-anchor="middle" fill="var(--accent)" font-size="10">count()</text>
<rect x="560" y="78" width="90" height="30" rx="8" fill="none" stroke="var(--accent)"/><text x="605" y="98" text-anchor="middle" fill="var(--accent)" font-size="10">show()</text>
<rect x="560" y="122" width="90" height="30" rx="8" fill="none" stroke="var(--accent)"/><text x="605" y="142" text-anchor="middle" fill="var(--accent)" font-size="10">write()</text>
<line x1="500" y1="91" x2="558" y2="49" stroke="var(--accent)" stroke-width="1.5"/>
<line x1="500" y1="91" x2="558" y2="93" stroke="var(--accent)" stroke-width="1.5"/>
<line x1="500" y1="91" x2="558" y2="137" stroke="var(--accent)" stroke-width="1.5"/>
<text x="500" y="176" fill="var(--accent)" font-size="10">1× scan + 1× shuffle, read 3×</text>
<text x="20" y="215" fill="var(--ink2)" font-size="11">Spark UI tell: same Stage DAG appears 3× (uncached) vs once with a green "cached" dot on the scan.</text>
<text x="20" y="234" fill="var(--ink2)" font-size="10">Projection (select fewer columns) shrinks every one of those scans and shuffles.</text>
</svg>`, caption: "Uncached: the join recomputes per action. Cached: it materializes once and every action reads the copy — the exact shape the Spark UI shows you." },
    { type: "code", lang: "python", code: `from pyspark.sql import functions as F

FRAUD_REVIEW_THRESHOLD = 0.80

orders = spark.read.table("orders")
fraud  = spark.read.table("fraud_scores")

# 'enriched' is expensive (a shuffle join) AND reused by three actions below.
# Projection first: carry only the 4 columns downstream work needs, so the
# join shuffles less data across the network.
enriched = (orders
    .join(fraud, "order_id")
    .select("order_id", "seller_id", "fraud_score", "total_amount")
    .cache())                      # materialize ONCE — the three actions reuse it

review = enriched.filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)

review.count()                     # action 1  -> builds + caches enriched
review.show()                      # action 2  -> reads cache, no re-join
(enriched.groupBy("seller_id")
    .agg(F.avg("fraud_score").alias("avg_score"))
    .write.format("delta").mode("overwrite").saveAsTable("gold.seller_risk"))  # action 3 -> reads cache

# Without .cache(): 3 actions = 3 full scans of orders+fraud_scores and 3 shuffle joins.
# With it: 1 scan, 1 shuffle join, materialized; the other two actions are cheap reads.`, caption: "Cache the DataFrame that is both expensive and reused; project columns before the join so the one shuffle carries less." },
    { type: "analogy", title: "Pre-staging the pick cart", html: `
<p>A NimbusMart picker filling several orders that all draw from the same three aisles has two options. The naive one: walk the full route from the dock to aisle 14 and back for <em>every</em> order — same long walk, three times. The senior one: walk it <strong>once</strong>, pull everything those aisles will need onto a <strong>staging cart</strong> parked at the packing bench, then fill all three orders from the cart without walking the floor again. The expensive part was the walking (the shuffle, the scan); staging the cart pays for one walk and reuses it.</p>
<p>That staging cart is <code>cache()</code>. You use it precisely when the same goods feed multiple orders — not for a one-off pick, where staging is just an extra step. And carrying only the items you'll actually pack (not the whole aisle) is <strong>projection</strong>: a lighter cart is a faster trip, every trip.</p>` },
    { type: "javaBridge", html: `
<p>You already do all of this on hot JVM paths — Spark just moves the same instincts across a cluster:</p>
<ul>
<li><strong>Caching ↔ memoization.</strong> When a pure, expensive computation feeds several callers, you don't recompute it per call — you memoize it (a <code>ConcurrentHashMap</code>, <code>@Cacheable</code>, a <code>Supplier</code> you compute once). <code>df.cache()</code> is memoizing a DataFrame's materialized result across the actions that reuse it — and, exactly like memoization, it's a win <em>only</em> when there's reuse, and it costs memory you have to budget.</li>
<li><strong>Reading the Spark UI ↔ reading a profiler.</strong> You wouldn't guess which method is your bottleneck — you'd attach async-profiler or JFR and read the flame graph for the hot frame. The Spark UI's stage timeline and DAG are that flame graph for a distributed job: find the stage that dominates wall-clock, see whether it's re-scanning, over-shuffling, or skewed, then fix that one thing.</li>
</ul>
<p>The upgrade: on one JVM a redundant recompute wastes microseconds. On a cluster, an un-memoized DataFrame re-reads terabytes across the network — same mistake, a bill with four more zeros. Profile, don't guess, is the same rule; the cost of ignoring it is what changed.</p>` },
  ],
  lab: {
    tier: "T2",
    understand: {
      engine: "sparksim",
      datasets: ["orders", "fraud_scores"],
      task: `<p><strong>Cache the DataFrame that's reused.</strong> In the starter, <code>enriched</code> is a shuffle join, and in the real pipeline it feeds three downstream actions (a count, a show, and a write). As written it carries no <code>.cache()</code>, so every one of those actions re-reads both tables and re-runs the join from scratch — three full recomputes of the expensive step.</p>
<p>Add <code>.cache()</code> to <code>enriched</code> so the join materializes once and the later actions reuse it. Run it: the review queue still resolves to its 43 rows, but the DAG now shows the join computed a single time. (In SparkSim, <code>cache()</code> is accepted as a planning hint — the win is what it does to the recompute, described under the DAG.)</p>`,
      starterCode: `orders = spark.read.table("orders")
fraud = spark.read.table("fraud_scores")

# 'enriched' feeds THREE actions downstream — but it is recomputed each time.
enriched = (orders
    .join(fraud, "order_id")
    .select("order_id", "seller_id", "fraud_score", "total_amount"))

review = enriched.filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)
review.show()`,
      solutionCode: `orders = spark.read.table("orders")
fraud = spark.read.table("fraud_scores")

# Cache the reused, expensive join so it materializes once, not per action.
enriched = (orders
    .join(fraud, "order_id")
    .select("order_id", "seller_id", "fraud_score", "total_amount")
    .cache())

review = enriched.filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)
review.show()`,
      expect: { rows: 43, cols: ["order_id", "seller_id", "fraud_score", "total_amount"] },
      dagNotes: `<p>The shuffle line before the join is the expensive stage — both tables cross the dock so matching <code>order_id</code>s co-locate. Without <code>cache()</code>, that stage runs once <em>per action</em>: three actions, three scans of orders+fraud_scores, three shuffle joins. With <code>cache()</code> on <code>enriched</code>, the join runs once, the result is held, and <code>count</code>/<code>show</code>/<code>write</code> read the cached copy — the Spark UI shows the repeated stage collapse to a single computed job with a green "cached" marker. Note the <code>select</code> before the join keeps only 4 columns, so the one shuffle that <em>does</em> run carries less data. Cache the reused-and-expensive DataFrame; don't cache a one-shot one — that just burns memory for nothing.</p>`
    },
    buildWithAI: `I'm learning Spark performance basics (caching, partitions, projection, reading the Spark UI). Build me a real local project that MEASURES the difference. I'm on my own machine; assume nothing beyond Python 3.10+.

1. Create a project folder \`nimbusmart-tuning\` with a venv, and install pyspark (pin a recent 3.5.x) and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator writing two CSVs into \`data/\` matching NimbusMart:
   - \`orders.csv\`: 240 rows (order_id O-10001.., customer_id, seller_id with S-777 taking ~35%, total_amount, status, country)
   - \`fraud_scores.csv\`: scores for exactly 225 orders (15 unscored), with EXACTLY 43 at fraud_score >= 0.80 (the review queue)

3. Create \`tuning_lab.py\` that:
   - defines FRAUD_REVIEW_THRESHOLD = 0.80, builds a SparkSession (local[*]), reads both CSVs with EXPLICIT StructType schemas
   - builds enriched = orders.join(fraud, "order_id").select("order_id","seller_id","fraud_score","total_amount")
   - runs THREE actions off enriched (a .count(), a .show(), and a groupBy(seller_id).agg(avg).collect()) and times the whole block with time.perf_counter()
   - then does the SAME three actions on enriched.cache() (call an action once first to warm the cache) and times it
   - prints both wall-clock times and the speedup, plus enriched.explain() so I can see the plan
   - prints spark.sparkContext.uiWebUrl so I can open the Spark UI and compare the Jobs/Stages between the cached and uncached runs

4. Create \`test_tuning.py\` (pytest) asserting: enriched.count() == 225; review (fraud_score >= FRAUD_REVIEW_THRESHOLD).count() == 43; and that the cached run's wall-clock is <= the uncached run's (allow a small tolerance). Compute expected numbers from the CSVs, don't hardcode inside tuning_lab.py.

5. Run the generator, tuning_lab, and pytest. Point me at the exact place in the Spark UI where I can see the join stage run 3× uncached vs 1× cached, and explain how projecting fewer columns changed the shuffle size. Windows-friendly paths please.`
  },
  check: [
    {
      type: "predict",
      q: "A DataFrame feeds several actions without being cached. On the seed data, what does this print — and what work happens?",
      code: `enriched = orders.join(fraud, "order_id")   # not cached
review = enriched.filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)
print(review.count())`,
      options: [
        "43, and the join runs once because Spark auto-caches joins",
        "43, and each action on 'enriched' re-runs the scan + shuffle join from source",
        "225, because count() ignores the filter",
        "39, because count() uses a strict boundary"
      ],
      answer: 1,
      explain: "The count is 43 (inner join 225, then the inclusive >= filter). But without cache(), every action that touches 'enriched' recomputes the full recipe — read both tables, shuffle, join — from scratch. That recompute-per-action is exactly what cache() eliminates when the DataFrame is reused."
    },
    {
      type: "mcq",
      q: "When is <code>cache()</code> the right call, and when is it just wasted memory?",
      options: [
        "Always cache every DataFrame — more cache is always faster",
        "Cache a DataFrame that is both reused by multiple actions and expensive to rebuild; don't cache a one-shot DataFrame used by a single action",
        "Only cache DataFrames smaller than one partition",
        "Never cache — Spark's optimizer already reuses everything automatically"
      ],
      answer: 1,
      explain: "Cache is memoization: it pays off only when there's reuse, and it costs memory you have to budget. A DataFrame consumed by exactly one action gains nothing from caching — you'd hold memory to avoid a recompute that never happens."
    },
    {
      type: "mcq",
      q: "Your review queue is 43 rows, but the write stage spawns 200 tasks and the job spends most of its time in scheduling. What's the likely cause and fix?",
      options: [
        "The data is too big; add more executors",
        "The default post-shuffle partition count (spark.sql.shuffle.partitions = 200) is far too many for 43 rows — reduce it (or coalesce) so you don't pay 200× per-task overhead for tiny work",
        "The fraud_score filter is inefficient; remove it",
        "Caching the result will reduce the task count to 1"
      ],
      answer: 1,
      explain: "200 tasks for 43 rows is the small-files problem in memory: near-zero real work per task, wrapped in scheduling and commit overhead. The lever is the partition count — lower spark.sql.shuffle.partitions or coalesce before the write so task count matches the actual data volume."
    },
    {
      type: "mcq",
      q: "Why does adding a <code>.select()</code> of only the needed columns <em>before</em> a join help performance?",
      options: [
        "It changes the join result, producing fewer rows",
        "Projection reduces the bytes each side carries into the shuffle, so the expensive cross-network step moves less data",
        "It forces Spark to cache the DataFrame automatically",
        "It has no effect; Spark always reads every column regardless"
      ],
      answer: 1,
      explain: "A shuffle moves data across the network — the most expensive step. Projecting to the 4 columns you actually need means every row on the 'forklift' is smaller, shrinking the shuffle. (Catalyst often pushes projection down for you, but doing it explicitly guarantees it and documents intent.)"
    }
  ],
  fieldNotes: `A team I advised had a Spark job whose cost had crept from about $40 to $210 a night over a quarter, and the on-call theory was "we need a bigger cluster." We opened the Spark UI instead and looked at the stage timeline for one run. The same three-stage pattern — read two large source tables, shuffle, join — appeared verbatim SEVEN times in a single job. Someone had built the pipeline as seven independent output tables, each derived from the same un-cached enriched join, and each output was an action that re-ran the join from object storage. The cluster wasn't too small; it was doing the identical terabyte-scale shuffle seven times because nothing told it the result was reusable. The fix was two lines: cache the enriched DataFrame after building it once, and unpersist it at the end of the job. Nightly cost went from $210 back to roughly $45, and the wall-clock dropped from 50 minutes to 9. Nobody had profiled because "it worked" — the job succeeded every night. The lesson the lead pinned in the channel: the Spark UI is free, and it will tell you in thirty seconds what a week of guessing won't — you are almost never CPU-bound, you are recompute-bound and shuffle-bound, and both are visible if you just look.`
};
