// H2 — Build Bronze → Silver
// Verified against data/nimbusmart/seed.js (seed 42) via the sparksim engine:
//   orders LEFT JOIN fraud_scores = 240 rows (starter, all orders incl. unscored)
//   + dropDuplicates(["order_id"]) + filter(fraud_score IS NOT NULL) = 225 rows (Silver)
//   quarantine (fraud_score IS NULL) = 15 rows; customers with null city = 3
//   FRAUD_REVIEW_THRESHOLD = 0.80
export default {
  id: "H2",
  track: "H",
  title: "Build Bronze → Silver",
  minutes: 28,
  coldOpen: "Week two of the capstone. A teammate wires the fraud pipeline straight off the raw orders export with an inner join to fraud_scores — clean, fast, ships. Three days later a fraud analyst asks why order O-10188 never appeared in review, or anywhere. It had no fraud score, so the inner join ate it, and nothing logged the deletion. Fifteen orders had quietly evaporated between the dock and the dashboard. Bronze and Silver exist so that never happens: raw lands untouched, and every row Silver drops is dropped on the record.",
  concept: [
    { type: "prose", html: `
<p>Bronze and Silver are two different jobs with two different contracts, and the most common capstone mistake is collapsing them into one.</p>
<p><strong>Bronze — the receiving dock.</strong> Its only promise is <em>land everything, change nothing, reject nothing</em>. You append the raw export exactly as it arrived — same columns, same types (usually all strings), drift and dupes and nulls intact — and you record when it landed. Bronze is your replay buffer: if Silver logic is wrong, you fix the code and re-derive from Bronze without re-fetching from the source. You never clean data in Bronze, because the moment you do, you've destroyed the evidence.</p>
<p><strong>Silver — the QC station.</strong> Here you enforce the contract: cast to real types, deduplicate on the business key, and run <em>quality gates</em> — expectations every row must satisfy. A row that fails a gate is not deleted; it is <strong>routed to a quarantine table</strong> with the reason attached. Silver is the first place the data is trustworthy, and quarantine is what makes "trustworthy" auditable rather than aspirational.</p>` },
    { type: "code", lang: "python", code: `from pyspark.sql import functions as F

FRAUD_REVIEW_THRESHOLD = 0.80

# --- Bronze: append raw, reject nothing (all 240 orders, all columns) ---
bronze_orders = spark.read.table("orders")          # verbatim landing
bronze_fraud  = spark.read.table("fraud_scores")    # 225 rows; 15 orders absent

# --- Silver: type, dedupe on business key, gate, quarantine failures ---
joined = bronze_orders.join(bronze_fraud, "order_id", "left")   # keep all 240

silver_orders = (joined
    .dropDuplicates(["order_id"])                   # idempotency: one row per order
    .filter(F.col("fraud_score").isNotNull()))      # gate: must be scored → 225

quarantine = joined.filter(F.col("fraud_score").isNull())       # the 15 unscored`, caption: "Bronze keeps all 240; Silver keeps 225 and parks 15 in quarantine — nothing vanishes." },
    { type: "prose", html: `
<p>Two lines carry the whole lesson, and both are load-bearing:</p>
<p><strong><code>dropDuplicates(["order_id"])</code> is not decoration — it is your idempotency guard.</strong> Bronze appends. If tonight's ingest re-runs after a partial failure, or an upstream export ships the same batch twice, Bronze now holds two copies of some orders. Silver's dedup on the business key collapses them back to one, so a re-run produces the same Silver table as a clean run. Without it, a retry silently doubles rows and every downstream sum is wrong. (On tonight's clean seed data every order_id is already unique, so this is a no-op — which is exactly when you must still write it, because the day it isn't a no-op is the day it saves you.)</p>
<p><strong>The <code>filter</code> defines the gate, and the complementary filter defines quarantine.</strong> An order with no fraud score cannot enter the review pipeline — that's a real contract violation, not a Spark inconvenience. The <em>left</em> join is deliberate: an inner join would have dropped the 15 unscored orders invisibly. The left join keeps them so the <code>isNull()</code> branch can route them to quarantine, where an analyst sees "15 orders excluded: no fraud score" instead of a silent hole in the numbers.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 250" font-family="var(--mono)" font-size="11">
<text x="16" y="18" fill="var(--rust)" font-size="10">BRONZE — append, reject nothing</text>
<rect x="16" y="26" width="150" height="44" rx="8" fill="var(--paper2)" stroke="var(--rust)"/>
<text x="91" y="46" text-anchor="middle" fill="var(--ink)">orders + fraud</text>
<text x="91" y="62" text-anchor="middle" fill="var(--ink2)" font-size="9">left join · 240 rows</text>
<text x="230" y="18" fill="var(--accent)" font-size="10">SILVER GATES</text>
<rect x="230" y="26" width="150" height="44" rx="8" fill="var(--paper2)" stroke="var(--accent)"/>
<text x="305" y="46" text-anchor="middle" fill="var(--ink)">dropDuplicates</text>
<text x="305" y="62" text-anchor="middle" fill="var(--ink2)" font-size="9">one row per order_id</text>
<rect x="230" y="82" width="150" height="44" rx="8" fill="var(--paper2)" stroke="var(--accent)"/>
<text x="305" y="102" text-anchor="middle" fill="var(--ink)">gate: is scored?</text>
<text x="305" y="118" text-anchor="middle" fill="var(--ink2)" font-size="9">fraud_score IS NOT NULL</text>
<line x1="166" y1="48" x2="228" y2="48" stroke="var(--ink2)"/>
<line x1="305" y1="70" x2="305" y2="80" stroke="var(--ink2)"/>
<rect x="470" y="26" width="150" height="52" rx="8" fill="var(--paper2)" stroke="var(--green)"/>
<text x="545" y="48" text-anchor="middle" fill="var(--green)" font-weight="bold">SILVER</text>
<text x="545" y="66" text-anchor="middle" fill="var(--ink)" font-size="10">225 trusted rows</text>
<rect x="470" y="96" width="150" height="52" rx="8" fill="var(--paper2)" stroke="var(--gold)" stroke-dasharray="4 3"/>
<text x="545" y="118" text-anchor="middle" fill="var(--gold)" font-weight="bold">QUARANTINE</text>
<text x="545" y="136" text-anchor="middle" fill="var(--ink)" font-size="10">15 rows · reason: unscored</text>
<line x1="380" y1="104" x2="468" y2="60" stroke="var(--green)" stroke-width="1.5"/>
<line x1="380" y1="104" x2="468" y2="120" stroke="var(--gold)" stroke-width="1.5" stroke-dasharray="4 3"/>
<text x="16" y="180" fill="var(--ink2)" font-size="10">225 + 15 = 240 — the row count is conserved; nothing is dropped, only routed</text>
<rect x="16" y="190" width="604" height="40" rx="8" fill="var(--paper2)" stroke="var(--line)"/>
<text x="318" y="215" text-anchor="middle" fill="var(--ink)" font-size="10">Silver trust = (typed) ∧ (deduped on business key) ∧ (every failing row accounted for in quarantine)</text>
</svg>`, caption: "The invariant that makes Silver auditable: kept + quarantined = ingested. 225 + 15 = 240." },
    { type: "analogy", title: "The QC station rejects to a bay, not the bin", html: `
<p>At the NimbusMart receiving dock (Bronze), every truck is waved in and its pallets logged — no inspection, because the dock's job is to <em>accept</em>. Cleaning at the dock would mean throwing away a damaged pallet before anyone recorded it arrived, and then the supplier's invoice never reconciles.</p>
<p>Inspection happens one station downstream, at QC (Silver). A pallet with a missing spec sheet doesn't get quietly binned — it rolls to a clearly marked <strong>reject bay</strong> with a tag: "no spec sheet, hold for supplier." Anyone can walk the bay and see exactly what was held and why. That reject bay is your quarantine table. An inner join that silently deletes the 15 unscored orders is a warehouse worker binning damaged pallets with no paperwork — the count is wrong and nobody can say where the goods went.</p>` },
    { type: "javaBridge", html: `
<p>You already draw this boundary in service code. Bronze is the <strong>raw request log</strong> you write before validation — the untouched payload you keep so you can replay a bug. Silver is the <strong>validation layer</strong>: DTO binding (typing), an idempotency key on the incoming message (your <code>dropDuplicates</code> on the business key — the same guard that stops a retried Kafka message from being processed twice), and a validator that rejects bad input.</p>
<ul>
<li>Quarantine is your <strong>dead-letter queue</strong>. A message that fails validation doesn't get dropped on the floor and it doesn't crash the consumer — it goes to the DLQ with the failure reason, for inspection and replay. Silver's quarantine table <em>is</em> a DLQ for rows.</li>
<li>The idempotency guard matters for the same reason it does on your consumer: at-least-once delivery means you <em>will</em> see the same record twice. Dedupe on the business key or double-count — there's no third option at scale.</li>
</ul>` },
  ],
  lab: {
    tier: "T2",
    understand: {
      engine: "sparksim",
      datasets: ["orders", "fraud_scores"],
      task: `<p><strong>Promote the Bronze landing to a trustworthy Silver table.</strong> The starter left-joins raw <code>orders</code> to <code>fraud_scores</code> and stops — so it returns all 240 rows, including the 15 unscored orders carrying a null <code>fraud_score</code>. That's Bronze, not Silver.</p>
<p>Add the two gates that define Silver: a <code>.dropDuplicates(["order_id"])</code> as your idempotency guard (one row per business key), then a <code>.filter(...)</code> keeping only rows where <code>fraud_score</code> <code>.isNotNull()</code>. Run it — Silver should land at exactly <strong>225 rows</strong>, the 15 unscored parked in quarantine.</p>
<p>Watch the plan: your <code>isNotNull</code> filter pushes down toward the scan, and the dedup becomes a hash-aggregate exchange. Silver is where "raw" becomes "typed, deduped, and gated" — the boundary the rest of the capstone builds on.</p>`,
      starterCode: `orders = spark.read.table("orders")
fraud = spark.read.table("fraud_scores")

silver = (orders
    .join(fraud, on=["order_id"], how="left")
    .select("order_id", "customer_id", "seller_id", "total_amount", "fraud_score"))

silver.show()`,
      solutionCode: `orders = spark.read.table("orders")
fraud = spark.read.table("fraud_scores")

silver = (orders
    .join(fraud, on=["order_id"], how="left")
    .dropDuplicates(["order_id"])
    .filter(F.col("fraud_score").isNotNull())
    .select("order_id", "customer_id", "seller_id", "total_amount", "fraud_score"))

silver.show()`,
      expect: { rows: 225, cols: ["order_id", "customer_id", "seller_id", "total_amount", "fraud_score"] },
      dagNotes: `<p>The optimizer pushes <code>fraud_score IS NOT NULL</code> down the plan, and the <code>dropDuplicates</code> shows up as a HashAggregate with a shuffle (Exchange) on <code>order_id</code> — dedup needs all rows for a key in one place, so it costs a shuffle, same as a groupBy. On this seed every order_id is already unique, so the dedup removes nothing tonight; you keep it because Bronze appends, and the night an upstream retry double-lands a batch, this line is the only thing standing between you and doubled revenue.</p>`
    },
    buildWithAI: `I'm building the Bronze→Silver layer of a data-engineering capstone (NimbusMart). Set up a real local project that demonstrates raw ingestion plus quality gates with a quarantine table. Assume Python 3.10+ and nothing installed.

1. Create a project folder \`nimbusmart-silver\` with a venv, install pyspark (recent 3.5.x) and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator writing ALL SIX NimbusMart sources into \`data/\` (CSV/JSON):
   - orders.csv (240 rows: order_id, customer_id, seller_id incl. S-777 as ~35%, status, total_amount, country, channel)
   - order_events.json (~361 rows with schema drift: some omit device, some add app_version)
   - customers.csv (60 rows, 3 with null city, 2 casing-duplicate emails) + customer_updates.csv (8 rows, C-0042 moves Munich then Hamburg)
   - products.json (40 rows, nested category{dept,aisle}, tags[], attrs{brand,weight_kg})
   - fraud_scores.csv (scores for 225 of the 240 orders; 15 unscored; exactly 43 scores >= FRAUD_REVIEW_THRESHOLD which is 0.80, exactly 4 == 0.80)
   - payments.csv (~228 rows) and couriers.csv/courier_pings.csv (~278 pings, ingested_at lagging event_ts)

3. Create \`bronze.py\`: read orders and fraud_scores raw (all columns, all rows) and write them to \`bronze/orders\` and \`bronze/fraud_scores\` as Parquet, mode="append" — no cleaning, no typing beyond what the reader gives.

4. Create \`silver.py\` defining FRAUD_REVIEW_THRESHOLD = 0.80 as a named constant. Read the two Bronze tables with EXPLICIT StructType schemas (no inferSchema). Build:
   - joined = bronze_orders.join(bronze_fraud, "order_id", "left")
   - silver_orders = joined.dropDuplicates(["order_id"]).filter(F.col("fraud_score").isNotNull())
   - quarantine = joined.filter(F.col("fraud_score").isNull()).withColumn("quarantine_reason", F.lit("missing_fraud_score"))
   Write silver_orders to \`silver/orders\` and quarantine to \`silver/_quarantine\`.

5. Create \`test_silver.py\` (pytest) asserting, re-deriving expected counts from the CSVs with the plain csv module (do NOT hardcode):
   - silver_orders.count() == number of orders that HAVE a fraud score (expect 225)
   - quarantine.count() == number of unscored orders (expect 15)
   - silver_orders.count() + quarantine.count() == bronze_orders.count() (conservation: nothing vanished)
   - re-running silver.py twice yields the SAME silver_orders.count() (idempotency: prove dropDuplicates holds even if you append the Bronze batch twice — append it twice in the test and assert the Silver count is still 225)

6. Run generator → bronze → silver → pytest. Print the Silver and quarantine counts and confirm 225 + 15 = 240. Windows-friendly paths.`
  },
  check: [
    {
      type: "predict",
      q: "The starter left-joins orders to fraud_scores and stops. On the seed data, how many rows does it return — and why is that not yet Silver?",
      code: `silver = (orders
    .join(fraud, on=["order_id"], how="left")
    .select("order_id", "customer_id", "seller_id", "total_amount", "fraud_score"))
print(silver.count())`,
      options: ["225 — the unscored orders are already gone", "240 — every order survives the left join, 15 carrying a null fraud_score", "43 — only the review queue", "255 — the join duplicates rows"],
      answer: 1,
      explain: "A LEFT join keeps every left row whether or not it matched, so all 240 orders survive; the 15 unscored ones get a null fraud_score. That's raw Bronze shape — untyped and ungated. Silver only exists once you dedupe on the business key and route those 15 nulls to quarantine, leaving 225 trusted rows."
    },
    {
      type: "predict",
      q: "After adding the Silver gates, how many rows land in Silver and how many in quarantine?",
      code: `silver = (orders.join(fraud, "order_id", "left")
    .dropDuplicates(["order_id"])
    .filter(F.col("fraud_score").isNotNull()))
quarantine = (orders.join(fraud, "order_id", "left")
    .filter(F.col("fraud_score").isNull()))
print(silver.count(), quarantine.count())`,
      options: ["240 and 0", "225 and 15", "43 and 197", "210 and 30"],
      answer: 1,
      explain: "The isNotNull gate keeps the 225 scored orders as Silver; the complementary isNull filter routes the 15 unscored to quarantine. 225 + 15 = 240 — the conservation invariant that proves nothing was silently dropped, which an inner join would have violated by deleting those 15 with no record."
    },
    {
      type: "mcq",
      q: "On tonight's clean seed data, <code>dropDuplicates([\"order_id\"])</code> removes zero rows. Why keep it in the Silver code?",
      options: [
        "It makes the query faster by hinting at the primary key",
        "It's an idempotency guard: Bronze appends, so a retried or double-shipped ingest can put two copies of an order in Bronze — the dedup makes Silver reproducible regardless",
        "Spark requires a dropDuplicates before every filter",
        "It converts the DataFrame to a typed schema"
      ],
      answer: 1,
      explain: "A no-op on clean data is not a useless line — it's insurance. Because Bronze's contract is append-everything, at-least-once ingestion means you will eventually land a batch twice. Dedup on the business key guarantees Silver is the same whether the source was delivered once or five times. The night it stops being a no-op is the night it prevents doubled revenue."
    },
    {
      type: "mcq",
      q: "A colleague argues quarantine is over-engineering: \"just use an inner join, the unscored orders aren't ready anyway.\" What's the strongest counter?",
      options: [
        "Inner joins are slower than left joins in Spark",
        "The result is identical, so it doesn't matter",
        "An inner join deletes the 15 unscored orders with no record, so you can't distinguish 'excluded on purpose' from 'lost to a bug' — quarantine makes the exclusion auditable and reprocessable",
        "Quarantine tables are required by GDPR"
      ],
      answer: 2,
      explain: "Both produce 225 Silver rows — the difference is entirely about what happens to the other 15. Inner join = silent deletion, invisible in every downstream count. Quarantine = an explicit, queryable record of what was held and why, that an analyst can inspect and a backfill can reprocess once scores arrive. Observability is the whole reason Silver isn't just 'the inner join'."
    }
  ],
  fieldNotes: `A payments team ran an inner join between their orders export and a risk-scores feed for eleven months before anyone noticed. The risk model occasionally failed to score an order (a timeout, a malformed feature) — a few dozen a day, silently dropped by the join. Nobody saw it because the dashboard only ever showed the orders that survived; there was no denominator. It surfaced when finance reconciled quarterly revenue against the ledger and found a persistent 0.3% shortfall that tracked exactly to unscored orders. The postmortem's fix was two lines: change the inner join to a left join, and route null-score rows to a quarantine table with a reason column. The very first night, quarantine caught 51 orders — and an on-call engineer noticed the count spiking to 400 a week later, which caught a genuine outage in the scoring service six hours before it would have hit an SLA. The lesson the team wrote down: a row your pipeline drops without recording is a bug you've pre-agreed never to find. Bronze keeps the evidence; Silver's quarantine keeps the receipts.`
};
