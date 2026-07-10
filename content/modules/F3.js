// F3 — Backfills, Catch-up & SLAs (Track F, T3 scripted trace: f3-backfill REUSED)
// Concept: reprocessing history safely; execution_date mental model;
//   idempotent overwrite-partition vs naive append that double-counts.
// Trace facts (engine/traces/f3-backfill.json): 14 stale daily partitions 06-25..07-08,
//   naive append doubles 1.42M -> 2.84M; overwrite is idempotent; rebuilt in ~40 min.
export default {
  id: "F3",
  track: "F",
  title: "Backfills, Catch-up & SLAs",
  minutes: 24,
  coldOpen: "A release on 2026-06-25 quietly joined daily_revenue on the ingestion-day FX rate instead of the order-day rate. Fourteen daily partitions were wrong before anyone noticed, and finance's month-end close was three days out. The instinct — 'just re-run the job for those days' — is right. The instinct that follows it, 're-run it with an INSERT INTO', is how you turn a 14-day error into a 14-day error that also double-counts revenue.",
  concept: [
    { type: "prose", html: `
<p>A <strong>backfill</strong> is not “fixing the data.” It is <em>re-running the transformation for a range of past logical dates</em>, exactly as if each of those days were running on schedule right now. You already write your pipeline as a function of a date; a backfill just calls that function again for dates in the past. Everything hard about backfills comes from one question: <strong>what does the second run do to output the first run already produced?</strong></p>
<p>There are two ways history gets rewritten, and only one of them is safe:</p>
<ul>
<li><strong>Naive append</strong> — <code>INSERT INTO revenue SELECT … WHERE ds = '2026-06-25'</code>. The partition already holds the (wrong) rows; now it holds wrong <em>plus</em> corrected. Row count and every SUM double. An append-based backfill is a corruption multiplier: each rerun stacks another full copy.</li>
<li><strong>Idempotent overwrite at partition granularity</strong> — <code>INSERT OVERWRITE … PARTITION (ds = '2026-06-25')</code>, or in Spark <code>.mode("overwrite")</code> with <code>partitionOverwriteMode=dynamic</code>. Delete-and-replace the day atomically. Run it once or five times: the partition holds exactly one correct copy. This is F1's idempotency, applied at the grain of a partition instead of a row.</li>
</ul>
<p>The second concern is the <strong>SLA</strong>: a 14-day backfill and tonight's production run compete for the same cluster. Cap the backfill's parallelism below production's share so replaying history never starves the present. Because each day-partition is independent, you can run them in bounded waves and restart from any day after a failure — the overwrites make every retry free.</p>` },
    { type: "code", lang: "python", code: `from pyspark.sql import functions as F

# The pipeline is a PURE FUNCTION of its logical date — never now().
def build_daily_revenue(spark, execution_date):
    orders = spark.read.table("orders").where(F.col("order_ts").startswith(execution_date))
    # ... join on the ORDER-DAY fx rate (the bug was using ingestion-day) ...
    revenue = orders.groupBy("country").agg(F.sum("total_amount").alias("revenue"))

    # IDEMPOTENT: overwrite only THIS date's partition. Rerun-safe.
    (revenue
        .withColumn("ds", F.lit(execution_date))
        .write.format("delta")
        .mode("overwrite").option("partitionOverwriteMode", "dynamic")
        .partitionBy("ds").saveAsTable("daily_revenue"))

# Backfill = call the same function for a range of past dates, in bounded waves.
for ds in date_range("2026-06-25", "2026-07-08"):   # 14 logical dates
    build_daily_revenue(spark, execution_date=ds)    # each overwrites its own ds`, caption: "execution_date is a parameter; the write overwrites one partition — so replaying 14 days is safe and boring." },
    { type: "svg", svg: `<svg viewBox="0 0 720 240" font-family="var(--mono)" font-size="11.5">
<text x="20" y="22" fill="var(--ink2)" font-size="11">NAIVE append — rerun 06-25 stacks a second copy onto the partition</text>
<rect x="20" y="34" width="150" height="30" rx="6" fill="var(--paper2)" stroke="var(--line)"/><text x="95" y="54" text-anchor="middle" fill="var(--ink)">1.42M rows (bad)</text>
<rect x="180" y="34" width="150" height="30" rx="6" fill="var(--paper2)" stroke="var(--rust)"/><text x="255" y="54" text-anchor="middle" fill="var(--ink)">+1.42M (good)</text>
<rect x="20" y="70" width="310" height="30" rx="6" fill="var(--paper2)" stroke="var(--rust)" stroke-width="2"/><text x="175" y="90" text-anchor="middle" fill="var(--rust)" font-weight="bold">2.84M rows — SUM(revenue) doubles</text>
<text x="390" y="22" fill="var(--ink2)" font-size="11">IDEMPOTENT overwrite — rerun replaces the partition</text>
<rect x="390" y="34" width="150" height="30" rx="6" fill="var(--paper2)" stroke="var(--line)"/><text x="465" y="54" text-anchor="middle" fill="var(--ink)">1.42M rows (bad)</text>
<text x="548" y="54" fill="var(--ink2)">→ replaced by →</text>
<rect x="390" y="70" width="310" height="30" rx="6" fill="var(--paper2)" stroke="var(--accent)" stroke-width="2"/><text x="545" y="90" text-anchor="middle" fill="var(--accent)" font-weight="bold">1.42M rows — correct, once or five reruns</text>
<text x="20" y="132" fill="var(--ink2)" font-size="11">14 independent day-partitions · run in bounded waves so tonight's SLA is never starved</text>
<g>
<rect x="20" y="144" width="44" height="26" rx="4" fill="var(--paper2)" stroke="var(--accent)"/><text x="42" y="162" text-anchor="middle" fill="var(--ink)" font-size="10">6-25</text>
<rect x="70" y="144" width="44" height="26" rx="4" fill="var(--paper2)" stroke="var(--accent)"/><text x="92" y="162" text-anchor="middle" fill="var(--ink)" font-size="10">6-26</text>
<rect x="120" y="144" width="44" height="26" rx="4" fill="var(--paper2)" stroke="var(--accent)"/><text x="142" y="162" text-anchor="middle" fill="var(--ink)" font-size="10">6-27</text>
<rect x="170" y="144" width="44" height="26" rx="4" fill="var(--paper2)" stroke="var(--accent)"/><text x="192" y="162" text-anchor="middle" fill="var(--ink)" font-size="10">6-28</text>
<text x="228" y="162" fill="var(--ink2)" font-size="10">… wave by wave …</text>
<rect x="560" y="144" width="60" height="26" rx="4" fill="var(--paper2)" stroke="var(--line)" stroke-dasharray="4 3"/><text x="590" y="162" text-anchor="middle" fill="var(--ink2)" font-size="10">today</text>
</g>
<text x="20" y="200" fill="var(--ink2)" font-size="11">the backfill range ends at yesterday · today's hourly loads keep landing, untouched</text>
<rect x="20" y="210" width="680" height="24" rx="6" fill="var(--paper2)" stroke="var(--line)"/><text x="360" y="226" text-anchor="middle" fill="var(--ink)" font-size="10.5">run = f(code, inputs, execution_date) — a pure function of its logical date, never now()</text>
</svg>`, caption: "Append stacks copies; partition-overwrite replaces. Independent day-partitions replay in bounded waves without touching today." },
    { type: "analogy", title: "The inventory recount", html: `
<p>NimbusMart discovers its shelf counts for the last two weeks were off — a miscalibrated scanner logged every scan as two units. You need to recount weeks 24 and 25. You do <strong>not</strong> walk the floor <em>adding</em> your fresh count to the wrong number already in the system — that just compounds the error. You <strong>overwrite</strong>: for aisle 14, the count <em>is</em> what you just recounted, replacing whatever was there. Recount aisle 14 twice by mistake and it's fine — the second recount replaces the first with the same number.</p>
<p>And you recount without stopping today's shipments. The recount crew works the historical aisles; the day shift keeps receiving and picking the current ones. They never collide, because a recount of last week's aisle and a fresh pick of today's aisle touch different bins. That is a backfill: overwrite the past a partition at a time, bounded so it doesn't starve the present, while today keeps arriving on schedule.</p>` },
    { type: "javaBridge", html: `
<p>The one idea that makes backfills click is <strong><code>execution_date</code> as a parameter, not <code>now()</code></strong> — and you already enforce this discipline in your own code, you just call it different names:</p>
<ul>
<li>A <strong>parameterized job run</strong> vs. one that reads the system clock. You'd never write a batch reconciliation that hard-codes <code>LocalDate.now()</code> deep in its logic — because then you could never re-run yesterday's reconciliation and get yesterday's answer. Airflow formalizes that: every run receives its logical date; the code is a pure function of it.</li>
<li>A <strong>pure function</strong> vs. a side-effecting one. <code>run = f(code, inputs, execution_date)</code>. Same inputs, same date, byte-for-byte same output — the property you rely on for a deterministic unit test is the exact property that makes a backfill trustworthy.</li>
<li>Any <code>now()</code> / <code>CURRENT_DATE</code> buried in a transform is the equivalent of a hidden <code>System.currentTimeMillis()</code> in a method you're trying to make idempotent: it silently makes the rerun produce a different answer than the original run.</li>
</ul>
<p>Design the pipeline as a pure function of its logical date, and a 14-day backfill is one loop over dates. Let <code>now()</code> leak in, and every rerun is a new bug.</p>` },
  ],
  lab: {
    tier: "T3",
    understand: {
      engine: "trace",
      trace: "f3-backfill",
      task: `<p>Scrub the <code>daily_revenue</code> backfill: a bad release baked the wrong FX rate into 14 daily partitions (06-25 → 07-08), and you're rebuilding them while today's hourly loads keep landing. Watch for four moves:</p>
<ul>
<li><strong>The naive attempt, shown so you never ship it.</strong> Rerunning 06-25 with an <code>append</code> takes the partition from 1.42M rows to 2.84M — SUM(revenue) doubles. An append-based backfill is a corruption multiplier.</li>
<li><strong>The idempotent form.</strong> Switch to <code>INSERT OVERWRITE PARTITION</code> (Spark: <code>.mode("overwrite")</code>, dynamic partition overwrite). Run it once → 1.42M. Run it five times → still 1.42M. Reruns are free.</li>
<li><strong>Bounded waves.</strong> The range rebuilds in waves of 4 under a resource-pool cap, so the backfill never starves tonight's production SLA. Watch the "today" hourly bar keep advancing, untouched, the whole time.</li>
<li><strong>The mental model.</strong> Each run is <code>f(code, inputs, execution_date)</code> — the 06-25 run executed today must reproduce exactly what it would have produced on 06-25. Any <code>now()</code> in the logic breaks that contract.</li>
</ul>
<p>Badge: <em>simulation</em> — the row counts and 40-minute wall clock are illustrative, but the append-doubles / overwrite-replaces behavior is exactly how partitioned tables behave.</p>`
    },
    buildWithAI: `I'm learning to run safe backfills over historical partitions. Build me a real local project that proves append-corrupts vs overwrite-is-idempotent, driven by an execution_date parameter. Assume Python 3.10+ and nothing else installed.

1. Create a folder \`nimbusmart-backfill\` with a venv. Install pyspark (recent 3.5.x) and pytest. Enable a local Delta-style setup, or just write partitioned Parquet under ./warehouse — whichever is simplest without extra services.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator writing \`data/orders.csv\` — 240 orders across 14 order dates 2026-06-25..2026-07-08 (columns order_id, country, total_amount, order_ts as an ISO date within that range). Print how many orders fall on 2026-06-25 so I can assert against it.

3. Create \`revenue.py\` with FRAUD_REVIEW_THRESHOLD = 0.80 defined as a named constant (unused here but keep the house rule), and build_daily_revenue(spark, execution_date, mode) that: reads orders for that execution_date, groups by country summing total_amount into a 'revenue' column, adds a ds=execution_date column, and writes partitioned by ds to ./warehouse/daily_revenue. Support mode="append" and mode="overwrite" (use partitionOverwriteMode=dynamic for overwrite). Use an explicit StructType on the read.

4. Create \`test_backfill.py\` (pytest) that:
   - runs build_daily_revenue for ds=2026-06-25 TWICE with mode="append", reads the ds=2026-06-25 partition back, and asserts the row count is exactly DOUBLE the single-run count (append corrupts). Derive the single-run count from the CSV with the plain csv module — no hardcoding.
   - runs it TWICE with mode="overwrite" and asserts the partition row count equals the single-run count (idempotent).
   - runs the full 14-date backfill loop with mode="overwrite" and asserts exactly 14 ds partitions exist and today's date is NOT among them.

5. Run the generator, then pytest -v, and show me both the doubling (append) and the stability (overwrite) in the output. Windows-friendly paths please.`
  },
  check: [
    {
      type: "predict",
      q: "The 2026-06-25 partition holds 1.42M wrong rows. You rebuild it with the naive statement below. What is the partition's row count afterward?",
      code: `-- backfill attempt for one day
INSERT INTO daily_revenue
SELECT * FROM rebuilt_revenue WHERE ds = '2026-06-25';
-- the partition already contained 1.42M (wrong) rows`,
      options: [
        "1.42M — the insert replaces the day's rows",
        "2.84M — the insert appends a second full copy alongside the wrong rows",
        "0 — INSERT INTO clears the partition first",
        "710K — half the rows are deduplicated automatically"
      ],
      answer: 1,
      explain: "INSERT INTO is append semantics: it adds rows, it does not replace them. The partition now holds the original 1.42M wrong rows plus 1.42M corrected ones = 2.84M, and every SUM doubles. This is why an append-based backfill is a corruption multiplier — you must overwrite the partition, not add to it."
    },
    {
      type: "mcq",
      q: "Which write makes a 14-day backfill safe to re-run — including safe to restart from any day after the runner dies midway?",
      options: [
        "<code>INSERT INTO … WHERE ds = :execution_date</code>, wrapped in a transaction",
        "<code>INSERT OVERWRITE … PARTITION (ds = :execution_date)</code> — delete-and-replace the day atomically, so each rerun lands on one correct copy",
        "<code>INSERT INTO</code> plus a nightly dedupe job to clean up any doubles",
        "Truncate the whole <code>daily_revenue</code> table first, then re-insert all 14 days"
      ],
      answer: 1,
      explain: "Overwrite at partition granularity is idempotent: running day 06-25 once or five times leaves exactly one correct copy. Because each day-partition is independent, a runner that dies after day 8 just reruns days 9–14, and the overwrites make those retries free. Truncating the whole table would also wipe partitions outside the backfill range — including today's."
    },
    {
      type: "mcq",
      q: "During the backfill, why does capping its parallelism matter, and why can the rebuild of past days run at all while today's hourly job keeps writing?",
      options: [
        "It doesn't matter; Spark isolates jobs automatically so they never compete",
        "The cap keeps the backfill from starving tonight's production SLA on the shared cluster; and past-day and today's partitions are different <code>ds</code> values, so the two writes never touch the same partition",
        "The backfill must pause the hourly job, because two writers to the same table always corrupt it",
        "Today's job is safe only because the backfill runs on a completely separate cluster"
      ],
      answer: 1,
      explain: "A backfill and production share the cluster, so an uncapped backfill can blow tonight's SLA — bound its resource pool below production's share. Isolation between history and present is free here because the backfill range ends at yesterday: it overwrites past ds partitions while the hourly job appends to today's, and different partitions never collide."
    },
    {
      type: "predict",
      q: "The transform contains the line below, buried in its filter. You backfill 2026-06-25 by running the job today (2026-07-09). What breaks?",
      code: `# inside build_daily_revenue(execution_date='2026-06-25')
orders = orders.where(F.col("order_ts") >= F.date_sub(F.current_date(), 1))`,
      options: [
        "Nothing — current_date() and execution_date are the same thing",
        "The rerun filters on today minus 1 (2026-07-08), not on the execution_date, so the 06-25 backfill produces the wrong day's rows — reruns are no longer reproducible",
        "The job throws, because current_date() is not allowed inside a filter",
        "It silently produces zero rows for every backfill date"
      ],
      answer: 1,
      explain: "current_date() reads the wall clock, so the 06-25 run executed on 07-09 filters on 07-08's data instead of 06-25's — the run is no longer a pure function of its logical date. This is the cardinal backfill sin: any now()/current_date() in transformation logic makes reruns produce a different answer than the original, silently. Parameterize on execution_date instead."
    }
  ],
  fieldNotes: `A fintech reporting team shipped a timezone fix on a Thursday: convert event timestamps to UTC before bucketing into daily partitions. Correct change, correct code. The rollout plan was to backfill 30 days so history matched the new logic — and the backfill script used INSERT INTO. Nobody caught it in review because on an empty dev table, append and overwrite are indistinguishable. In prod, the 30 partitions already had rows, so the backfill doubled every one: 30 days of transaction counts and dollar sums exactly 2× reality. The dashboards looked like the business had a record month, and a VP forwarded the 'growth' to the board before an analyst noticed the daily active users had also, impossibly, doubled overnight. Recovery took two days: overwrite-rebuild all 30 days from the raw events, then a reconciliation query against the ledger to prove each partition was back to a single copy. The one-line root cause — INSERT INTO where INSERT OVERWRITE PARTITION belonged — is now the first thing that team's backfill checklist asks, in bold.`
};
