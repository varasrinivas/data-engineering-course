// G2 — Validation Gates (Track G, Quality/Governance/Ops)
// Verified facts used by lab + checks (from data/nimbusmart/seed.js, seed 42):
//   orders = 240; fraud_scores = 225 (15 unscored); inner join = 225.
//   fraud_score >= FRAUD_REVIEW_THRESHOLD (0.80) => 43 rows; EXACTLY 4 equal the threshold.
//   strict '>' instead of '>=' at FRAUD_REVIEW_THRESHOLD => 39 rows (drops the 4 boundary rows).
//   15 orders have no fraud_scores row -> null score after a left join = quarantine.
export default {
  id: "G2",
  track: "G",
  title: "Validation Gates",
  minutes: 26,
  coldOpen: "A batch of orders lands in Bronze with a fraud_score of 1.7 — a scoring service bug that briefly emitted un-normalized logits instead of probabilities. Nothing validates the range on the way into Silver, so the Gold review queue happily accepts them, and a analyst spends Monday morning explaining to compliance why four orders have a '170% fraud probability'. The fix wasn't in the model. It was the missing gate between Bronze and Silver.",
  concept: [
    { type: "prose", html: `
<p>Bronze accepts everything — that's its job: land the raw truck, reject nothing, keep the receipt. But the moment data crosses into <strong>Silver</strong>, it becomes something the rest of the company will <em>trust</em>. A <strong>validation gate</strong> is the checkpoint on that boundary: a set of <strong>expectations</strong> every row must satisfy to pass, and a defined path — a <strong>quarantine table</strong> — for the rows that don't.</p>
<p>The critical design choice is what happens to a bad row. There are three options, and only one is production-grade:</p>
<ul>
<li><strong>Crash the job</strong> — one malformed row halts the pipeline for everyone. Brittle; a single bad record from an upstream you don't control takes down tonight's run.</li>
<li><strong>Silently drop it</strong> — the bad row vanishes with no record. Now your row counts don't reconcile and nobody can explain the gap. This is the quiet corruption from G1, wearing a different hat.</li>
<li><strong>Quarantine it</strong> — the good rows flow to Silver; the bad rows are written to a <em>separate, inspectable</em> table with the reason they failed. The pipeline stays up, nothing is lost, and someone can triage the quarantine on Monday.</li>
</ul>
<p>Expectations come in a few flavors you'll use constantly: <strong>not-null</strong> on required keys, <strong>range</strong> checks (<code>0.0 &lt;= fraud_score &lt;= 1.0</code> would have caught the cold open), <strong>set membership</strong> (<code>status IN (...)</code>), and <strong>referential</strong> (every <code>order_id</code> in fraud_scores exists in orders). Each expectation is a boolean per row; the gate is just "keep the rows where all expectations hold, divert the rest."</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 250" font-family="var(--mono)" font-size="12">
<text x="20" y="22" fill="var(--ink2)" font-size="11">THE BRONZE → SILVER GATE — expectations decide pass vs quarantine</text>
<rect x="20" y="40" width="120" height="52" rx="10" fill="var(--paper2)" stroke="var(--line)"/>
<text x="80" y="64" text-anchor="middle" fill="var(--ink)">Bronze</text>
<text x="80" y="81" text-anchor="middle" fill="var(--ink2)" font-size="10">240 orders · raw</text>
<rect x="250" y="34" width="150" height="64" rx="10" fill="none" stroke="var(--accent)" stroke-width="2"/>
<text x="325" y="56" text-anchor="middle" fill="var(--accent)" font-weight="bold">VALIDATION GATE</text>
<text x="325" y="73" text-anchor="middle" fill="var(--ink2)" font-size="9.5">not-null · range 0..1</text>
<text x="325" y="87" text-anchor="middle" fill="var(--ink2)" font-size="9.5">score present?</text>
<line x1="140" y1="66" x2="248" y2="66" stroke="var(--ink2)" stroke-width="1.5"/>
<rect x="540" y="24" width="160" height="46" rx="10" fill="var(--paper2)" stroke="var(--accent)"/>
<text x="620" y="44" text-anchor="middle" fill="var(--accent)" font-weight="bold">Silver ✓ PASS</text>
<text x="620" y="60" text-anchor="middle" fill="var(--ink2)" font-size="10">225 scored, in range</text>
<rect x="540" y="92" width="160" height="46" rx="10" fill="var(--paper2)" stroke="var(--rust)"/>
<text x="620" y="112" text-anchor="middle" fill="var(--rust)" font-weight="bold">QUARANTINE ✗</text>
<text x="620" y="128" text-anchor="middle" fill="var(--ink2)" font-size="10">15 unscored + reason</text>
<line x1="400" y1="58" x2="538" y2="46" stroke="var(--accent)" stroke-width="1.5"/>
<line x1="400" y1="74" x2="538" y2="114" stroke="var(--rust)" stroke-width="1.5" stroke-dasharray="5 4"/>
<text x="20" y="176" fill="var(--ink2)" font-size="11">DOWNSTREAM — the review-queue rule runs only on rows that PASSED the gate</text>
<rect x="20" y="188" width="680" height="46" rx="10" fill="var(--paper2)" stroke="var(--line)"/>
<text x="360" y="209" text-anchor="middle" fill="var(--ink)">filter(fraud_score &gt;= FRAUD_REVIEW_THRESHOLD) → 43 rows to human review</text>
<text x="360" y="226" text-anchor="middle" fill="var(--ink2)" font-size="10">the 43 is a TESTED number — 4 orders sit exactly on the FRAUD_REVIEW_THRESHOLD boundary</text>
</svg>`, caption: "Good rows flow to Silver; bad rows divert to quarantine with a reason. The review rule runs only on trusted, gated data." },
    { type: "code", lang: "python", code: `from pyspark.sql import functions as F

FRAUD_REVIEW_THRESHOLD = 0.80   # the review rule is a TESTED expectation, not a magic number

orders = spark.read.table("orders")            # 240 rows, Bronze
fraud  = spark.read.table("fraud_scores")      # 225 rows, Bronze

# --- The gate: expectations as boolean columns ---------------------------
scored = orders.join(fraud, "order_id", "left")     # left join: keep ALL orders

passes = (
    F.col("fraud_score").isNotNull()                # required: must be scored
    & (F.col("fraud_score") >= F.lit(0.0))          # range floor
    & (F.col("fraud_score") <= F.lit(1.0))          # range ceiling (would catch the 1.7 bug)
)

silver     = scored.filter(passes)                  # 225 rows -> trusted Silver
quarantine = scored.filter(~passes) \\
                   .withColumn("reason", F.lit("missing_or_out_of_range_score"))  # 15 rows

# --- Downstream: the review-queue rule, expressed as a checked expectation --
review = silver.filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)   # EXACTLY 43

# A gate without a test is a hope. Assert the contract in CI:
assert review.count() == 43, "review-queue expectation broke — check the >= boundary"`, caption: "The gate splits pass/quarantine on explicit expectations; the review rule's row count is asserted, so a regression fails the build." },
    { type: "analogy", title: "The QC station, not the receiving dock", html: `
<p>At the NimbusMart warehouse the receiving dock (Bronze) takes every truck without argument — that's deliberate, you never want to reject a supplier at the gate and lose the goods. The real inspection happens one station in, at <strong>QC</strong> (Silver). A worker pulls each item against a checklist: barcode scans? weight in range? not crushed? Items that pass move to the showroom-bound shelves. Items that fail don't get thrown in the skip and forgotten — they go to a labelled <strong>quarantine cage</strong> with a tag saying <em>why</em>: "seal broken", "wrong SKU", "weight 0". Someone works that cage down every shift.</p>
<p>A validation gate is that QC station. The expectations are the checklist. The quarantine table is the cage-with-a-tag. And the rule that a scored order at or above <code>FRAUD_REVIEW_THRESHOLD</code> goes to human review is just another checklist line — one you <em>test</em>, because 43 orders getting the right treatment depends on it being exactly right at the boundary.</p>` },
    { type: "javaBridge", html: `
<p>You've built this gate before — it's your <strong>Bean Validation layer at the service boundary</strong>. A request DTO comes in annotated <code>@NotNull</code>, <code>@Min(0)</code>, <code>@Max(1)</code>, <code>@Pattern(...)</code>, and a <code>@Valid</code> controller argument runs every constraint before your business logic ever sees the object. Invalid requests get a structured 422 with the field and the violated rule — not a 500, and definitely not silent acceptance.</p>
<ul>
<li>Your <code>@NotNull</code> / <code>@Min</code> / <code>@Max</code> constraints ↔ the not-null and range <strong>expectations</strong> on the gate.</li>
<li>The 422-with-violations response ↔ the <strong>quarantine table with a reason</strong>: rejected, but explained and recoverable, never silently dropped.</li>
<li>Your <code>@Valid</code> at the controller edge ↔ running the gate at the <strong>Bronze→Silver boundary</strong> — validate once, at the trust boundary, not scattered through downstream code.</li>
</ul>
<p>The upgrade: Bean Validation rejects <em>one request</em> at a time and the caller retries. A data gate processes a whole batch, so "reject" can't mean "throw" — it means <em>route to quarantine</em>, because the other 224 rows still need to get through tonight.</p>` },
  ],
  lab: {
    tier: "T2",
    understand: {
      engine: "sparksim",
      datasets: ["orders", "fraud_scores"],
      task: `<p><strong>Fix the review-queue expectation at the boundary.</strong> The starter builds the review queue from gated (scored, in-range) orders — but a previous engineer wrote the threshold check as a strict <code>&gt;</code>. That silently drops every order sitting <em>exactly</em> on <code>FRAUD_REVIEW_THRESHOLD</code>. On NimbusMart's data that's 4 orders — the difference between a queue of 39 and the correct 43.</p>
<p>This is why the review rule is a <em>tested</em> expectation: the boundary is business-critical. Change the comparison so an order whose score equals the threshold is included, then Run. The gate's contract says this queue must be exactly 43.</p>`,
      starterCode: `orders = spark.read.table("orders")
fraud = spark.read.table("fraud_scores")

# gate already passed these rows (scored + in range); now apply the review rule
review = (orders
    .join(fraud, "order_id")
    .filter(F.col("fraud_score") > FRAUD_REVIEW_THRESHOLD)
    .select("order_id", "customer_id", "fraud_score", "seller_id"))

review.show()`,
      solutionCode: `orders = spark.read.table("orders")
fraud = spark.read.table("fraud_scores")

# gate already passed these rows (scored + in range); now apply the review rule
review = (orders
    .join(fraud, "order_id")
    .filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)
    .select("order_id", "customer_id", "fraud_score", "seller_id"))

review.show()`,
      expect: { rows: 43, cols: ["order_id", "customer_id", "fraud_score", "seller_id"] },
      dagNotes: `<p>The inner join is the implicit gate here: it keeps only the 225 orders that have a score, quietly diverting the 15 unscored ones (in a real pipeline those go to a <em>quarantine</em> table, not into the void — a left join plus <code>fraud_score IS NULL</code> is how you'd capture them). Then the review filter: with strict <code>&gt;</code> you get 39; with <code>&gt;=</code> you get 43, because 4 orders score exactly <code>FRAUD_REVIEW_THRESHOLD</code>. The 4-row gap is invisible unless you <em>assert the count</em> — which is the entire argument for treating the threshold as a tested rule.</p>`
    },
    buildWithAI: `I'm learning validation gates and quarantine tables in PySpark (expectations at the Bronze->Silver boundary). Set up a real local project. I'm on my own machine; assume nothing beyond Python 3.10+.

1. Create a project folder \`nimbusmart-gates\` with a venv, and install pyspark (pin a recent 3.5.x) and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator writing two CSVs into \`data/\`, matching NimbusMart:
   - \`orders.csv\`: 240 rows — order_id (O-10001..), customer_id (C-0001..C-0060), seller_id (one of S-101,S-204,S-355,S-410,S-777,S-812,S-903 with S-777 taking ~35%), total_amount (8..950, 2dp), status, country
   - \`fraud_scores.csv\`: scores for exactly 225 of those orders (15 unscored, same seed) — order_id, fraud_score, model_version. Engineer it so EXACTLY 43 orders have fraud_score >= FRAUD_REVIEW_THRESHOLD (0.80) and EXACTLY 4 of those equal the threshold exactly (the inclusive-boundary rows). Also inject 3 deliberately CORRUPT rows with fraud_score = 1.7 (out of range) so the range expectation has something to catch.

3. Create \`gate.py\` that:
   - defines FRAUD_REVIEW_THRESHOLD = 0.80 as a named constant
   - builds a SparkSession (local[*]), reads both CSVs with EXPLICIT StructType schemas (no inferSchema)
   - left-joins orders to fraud_scores, then computes a boolean 'passes' expectation = fraud_score IS NOT NULL AND 0.0 <= fraud_score <= 1.0
   - writes silver = rows that pass, and quarantine = rows that fail WITH a 'reason' column
   - builds review = silver.filter(fraud_score >= FRAUD_REVIEW_THRESHOLD)
   - prints counts for silver, quarantine, and review

4. Create \`test_gate.py\` (pytest) asserting: review.count() == 43; the count using strict '>' instead of '>=' == 39 (proving the boundary matters); quarantine contains the 3 out-of-range rows and the 15 unscored orders; and silver + quarantine row counts reconcile to the input. Compute expected numbers by re-reading the CSVs with the plain csv module — do NOT hardcode them inside gate.py.

5. Run the generator, gate.py, and pytest. Show me the quarantine table contents and explain why '>' would have shipped 4 fraudulent-enough orders straight past human review. Windows-friendly paths please.`
  },
  check: [
    {
      type: "predict",
      q: "The review-queue expectation was written with a strict comparison. On the seed data (4 orders score exactly at the threshold), how many rows does this produce?",
      code: `review = (orders
    .join(fraud, "order_id")
    .filter(F.col("fraud_score") > FRAUD_REVIEW_THRESHOLD))
print(review.count())`,
      options: ["43", "39", "45", "225"],
      answer: 1,
      explain: "Strict > excludes the 4 orders sitting exactly on FRAUD_REVIEW_THRESHOLD, so 43 − 4 = 39. The correct rule is >= (inclusive), which yields 43. This 4-row silent gap is the textbook case for asserting the count as a tested expectation."
    },
    {
      type: "mcq",
      q: "A row arrives in Bronze with <code>fraud_score = 1.7</code>. What should a well-designed validation gate do with it?",
      options: [
        "Crash the Silver job so the whole batch stops until someone fixes the score",
        "Silently drop it so it never reaches the review queue",
        "Divert it to a quarantine table with a reason (out-of-range), letting the other rows pass to Silver",
        "Clamp it to 1.0 automatically and pass it through as normal"
      ],
      answer: 2,
      explain: "Quarantine keeps the pipeline up AND keeps a record. Crashing punishes 239 good rows for one bad one; silent-drop breaks reconciliation; auto-clamping hides a real upstream bug behind a plausible-looking value. Quarantine-with-reason is the only option that's both resilient and auditable."
    },
    {
      type: "mcq",
      q: "Why express the review-queue rule (<code>fraud_score &gt;= FRAUD_REVIEW_THRESHOLD → 43</code>) as an asserted expectation rather than just a filter?",
      options: [
        "Assertions make Spark run the filter faster",
        "So a later refactor that changes the boundary (>= to >), the join type, or the source table fails the build instead of silently shipping the wrong queue",
        "Because filters can't be used on joined DataFrames",
        "It's only needed for streaming pipelines, not batch"
      ],
      answer: 1,
      explain: "The row count is the contract. Asserting review.count() == 43 turns any regression — an off-by-boundary operator, an inner join that drops rows, a renamed column — into a red CI build. Without the assertion, the queue can drift from 43 and nobody notices until compliance does."
    },
    {
      type: "mcq",
      q: "Bronze accepts every row unconditionally, but Silver runs a strict gate. What justifies the different treatment at each layer?",
      options: [
        "Bronze is smaller, so validation there would be wasted effort",
        "Bronze's job is lossless capture of raw truth; Silver's job is to be trustworthy — validation belongs at the trust boundary, not before it",
        "Silver data is never read by anyone, so its quality doesn't matter",
        "Validation can only run after data is compressed, which happens in Silver"
      ],
      answer: 1,
      explain: "If you validate at ingestion you can lose the raw record you might need to replay. Bronze captures everything (even the 1.7 bug — you'll want it for the postmortem); the gate runs at Bronze→Silver, the exact point where data stops being 'what arrived' and becomes 'what we trust'."
    }
  ],
  fieldNotes: `A logistics client ran their Bronze→Silver load with no range check on a sensor feed, on the theory that "the upstream device firmware guarantees 0–100". For fourteen months it did. Then a firmware update changed a temperature reading from Celsius to a raw 12-bit ADC count on one hardware revision, and a subset of readings started arriving as values like 2,048. No job failed — the numbers were valid doubles. They flowed into Silver, into a Gold "cold-chain compliance" mart, and into an automated report that told a pharmaceutical customer their vaccine shipments had held temperature perfectly. They hadn't; the gate that would have flagged "reading &gt; 100 is impossible for this sensor" didn't exist. The remediation cost more than the entire data platform had that year, and the fix was eleven lines: a range expectation, a quarantine table, and an assert on the quarantine row count that pages if it's ever non-zero for more than an hour. The lesson the tech lead wrote in the retro: "an unvalidated assumption isn't a contract, it's a bet — and we'd been letting it ride for fourteen months without knowing we'd placed it."`
};
