// H1 — Capstone Brief: The NimbusMart Platform
// Verified against data/nimbusmart/seed.js (seed 42) via the sqlrunner engine:
//   orders = 240; fraud_scores = 225 (15 orders unscored)
//   orders LEFT JOIN fraud_scores WHERE fraud_score >= FRAUD_REVIEW_THRESHOLD (0.80) = 43 rows
//   exactly-0.80 rows = 4 (O-10096, O-10132, O-10137, O-10140); inner join = 225
//   review-queue by country = 7 groups (FR highest at 11)
export default {
  id: "H1",
  track: "H",
  title: "Capstone Brief: The NimbusMart Platform",
  minutes: 20,
  coldOpen: "The VP of Ops drops one line in the channel: \"I want a single dashboard — revenue by country, and how many orders are sitting in fraud review right now.\" Six teams own the six source feeds; nobody owns the number. The last person who answered it by hand pulled a CSV, fat-fingered a join, and reported 61 orders in review. The real answer was 43. This is the capstone: build the pipeline that makes 43 the only answer anyone can get.",
  concept: [
    { type: "prose", html: `
<p>Everything in this course has been a part; the capstone is the <em>whole machine</em>. NimbusMart hands you <strong>six messy source feeds</strong> and asks for one trustworthy executive dashboard. Your job is the pipeline in between — and the discipline that makes its numbers reproducible.</p>
<p>The six sources, exactly as they arrive at the dock:</p>
<ul>
<li><strong>orders</strong> (240 rows) — the OLTP export: order_id, customer_id, seller_id, status, total_amount, country, channel.</li>
<li><strong>order_events</strong> (361 rows) — clickstream, with real schema drift: <code>device</code> missing on 17 rows, a new <code>app_version</code> present on 25.</li>
<li><strong>customers</strong> (60) + <strong>customer_updates</strong> (8) — dirty master data: 3 null cities, casing-duplicated emails, and C-0042 who moved Munich → Hamburg mid-quarter (your SCD2 case).</li>
<li><strong>products</strong> (40) — nested catalog: <code>category {dept, aisle}</code>, <code>tags []</code>, <code>attrs {brand, weight_kg}</code>.</li>
<li><strong>fraud_scores</strong> (225) + <strong>payments</strong> (228) — the risk feed: <strong>15 orders are unscored</strong>, and the whole platform pivots on one number, <code>FRAUD_REVIEW_THRESHOLD = 0.80</code>.</li>
<li><strong>couriers</strong> (12) + <strong>courier_pings</strong> (278) — delivery telemetry with late data: <code>ingested_at</code> lags <code>event_ts</code>, sometimes by more than an hour.</li>
</ul>
<p>The deliverable is not "some Spark code." It's a dashboard whose every tile is <strong>reproducible, tested, and traceable</strong> back to a source. The spine that holds the whole thing together is the fraud-review threshold: the review queue must contain <strong>exactly 43 orders</strong>, end to end, every night.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 300" font-family="var(--mono)" font-size="11">
<text x="16" y="20" fill="var(--ink2)" font-size="10">SIX SOURCES</text>
<text x="300" y="20" fill="var(--ink2)" font-size="10">MEDALLION</text>
<text x="600" y="20" fill="var(--ink2)" font-size="10">SERVE</text>
<g fill="var(--ink)">
<rect x="16" y="30" width="150" height="26" rx="6" fill="var(--paper2)" stroke="var(--line)"/><text x="26" y="47">orders · order_events</text>
<rect x="16" y="62" width="150" height="26" rx="6" fill="var(--paper2)" stroke="var(--line)"/><text x="26" y="79">customers · updates</text>
<rect x="16" y="94" width="150" height="26" rx="6" fill="var(--paper2)" stroke="var(--line)"/><text x="26" y="111">products</text>
<rect x="16" y="126" width="150" height="26" rx="6" fill="var(--paper2)" stroke="var(--line)"/><text x="26" y="143">fraud_scores · payments</text>
<rect x="16" y="158" width="150" height="26" rx="6" fill="var(--paper2)" stroke="var(--line)"/><text x="26" y="175">couriers · pings</text>
</g>
<rect x="230" y="40" width="120" height="60" rx="8" fill="var(--paper2)" stroke="var(--rust)"/><text x="290" y="66" text-anchor="middle" fill="var(--rust)" font-weight="bold">BRONZE</text><text x="290" y="84" text-anchor="middle" fill="var(--ink2)" font-size="9">append · reject nothing</text>
<rect x="230" y="120" width="120" height="60" rx="8" fill="var(--paper2)" stroke="var(--accent)"/><text x="290" y="146" text-anchor="middle" fill="var(--accent)" font-weight="bold">SILVER</text><text x="290" y="164" text-anchor="middle" fill="var(--ink2)" font-size="9">typed · deduped · gated</text>
<rect x="230" y="200" width="120" height="60" rx="8" fill="var(--paper2)" stroke="var(--gold)"/><text x="290" y="226" text-anchor="middle" fill="var(--gold)" font-weight="bold">GOLD</text><text x="290" y="244" text-anchor="middle" fill="var(--ink2)" font-size="9">star · SCD2 · marts</text>
<line x1="166" y1="105" x2="228" y2="70" stroke="var(--ink2)"/><line x1="290" y1="100" x2="290" y2="118" stroke="var(--ink2)"/><line x1="290" y1="180" x2="290" y2="198" stroke="var(--ink2)"/>
<rect x="430" y="120" width="130" height="60" rx="8" fill="none" stroke="var(--ink)" stroke-dasharray="4 3"/><text x="495" y="146" text-anchor="middle" fill="var(--ink)">fraud_review</text><text x="495" y="162" text-anchor="middle" fill="var(--ink)">_queue</text><text x="495" y="176" text-anchor="middle" fill="var(--rust)" font-size="9">acceptance = 43</text>
<line x1="350" y1="150" x2="428" y2="150" stroke="var(--ink2)"/>
<rect x="600" y="90" width="104" height="120" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="652" y="112" text-anchor="middle" fill="var(--ink2)" font-size="9">DASHBOARD</text>
<rect x="612" y="122" width="80" height="24" rx="4" fill="none" stroke="var(--gold)"/><text x="652" y="138" text-anchor="middle" fill="var(--ink)" font-size="9">revenue/country</text>
<rect x="612" y="152" width="80" height="24" rx="4" fill="none" stroke="var(--rust)"/><text x="652" y="168" text-anchor="middle" fill="var(--ink)" font-size="9">review = 43</text>
<rect x="612" y="182" width="80" height="20" rx="4" fill="none" stroke="var(--accent)"/><text x="652" y="196" text-anchor="middle" fill="var(--ink)" font-size="9">freshness</text>
<line x1="560" y1="150" x2="598" y2="150" stroke="var(--ink2)"/>
</svg>`, caption: "Six sources → Bronze/Silver/Gold → the fraud-review-queue mart → the dashboard. The 43 is the contract every stage must preserve." },
    { type: "prose", html: `
<p>The brief is only real if it has <strong>acceptance criteria</strong> — testable statements that pass or fail, not vibes. Here are the capstone's, and every one is a query or an assertion you will actually run:</p>
<ol>
<li><strong>Completeness.</strong> All 240 orders are ingested to Bronze; none are dropped silently. The 15 unscored orders are <em>quarantined</em>, not lost.</li>
<li><strong>The review queue is exactly 43.</strong> <code>orders LEFT JOIN fraud_scores</code>, kept where <code>fraud_score >= FRAUD_REVIEW_THRESHOLD</code> (0.80), returns 43 rows — including the 4 that sit exactly on the FRAUD_REVIEW_THRESHOLD (an inclusive boundary, tested).</li>
<li><strong>Every customer resolves.</strong> The order fact joins to a customer dimension with zero orphans; C-0042's May orders map to Munich and June orders to Hamburg (SCD2).</li>
<li><strong>Revenue reconciles.</strong> Revenue-by-country sums to the same total the finance ledger reports, within rounding.</li>
<li><strong>It's reproducible.</strong> Re-running the whole pipeline for the same logical date produces byte-identical marts — no <code>now()</code>, no append-into-non-empty, no order-dependent output.</li>
</ol>
<p>Criterion 2 is the one you'll build first, in this module's lab, because it is the smallest testable definition of "correct" — and the rest of the capstone (H2, H3, H4) exists to satisfy it at scale.</p>` },
    { type: "code", lang: "sql", code: `-- The acceptance test, as a single query (this module's lab).
-- FRAUD_REVIEW_THRESHOLD = 0.80 is the named business cutoff.
SELECT o.order_id, o.customer_id, f.fraud_score
FROM orders o
LEFT JOIN fraud_scores f ON o.order_id = f.order_id
WHERE f.fraud_score >= 0.80        -- the FRAUD_REVIEW_THRESHOLD, inclusive
-- returns exactly 43 rows. If the capstone ever returns a different number,
-- something upstream broke. This query is the contract.`, caption: "The whole capstone reduces to one truth: this query returns 43." },
    { type: "analogy", title: "The build spec before the build", html: `
<p>No foreman breaks ground on a NimbusMart fulfilment centre without a <strong>build spec</strong>: the loading dock takes trucks of any size (Bronze), the QC station rejects damaged pallets to a marked bay rather than the shop floor (Silver + quarantine), the showroom is laid out for shoppers not forklifts (Gold). And crucially, the spec ends with an <em>acceptance walk-through</em>: a checklist the client signs — "43 pallets flagged for inspection, revenue board reconciles, every pallet traceable to a supplier."</p>
<p>This module is the spec and the acceptance walk-through. H2–H4 pour the concrete. When the inspector counts 43 flagged pallets and the number matches the paperwork, the building is accepted. Count 61, and you don't argue — you find which station miscounted.</p>` },
    { type: "javaBridge", html: `
<p>You've shipped a service against an <strong>acceptance test suite</strong> before: the ticket isn't "done" because the code compiles — it's done when the integration tests are green. Same discipline here, two adjustments:</p>
<ul>
<li>Your acceptance criteria are <strong>queries over data</strong>, not assertions over objects. "The review queue is 43" is this course's <code>assertEquals(43, reviewQueue.size())</code> — and like any good test, it fails loudly the moment an upstream change (a bad join, a dropped filter, a null slipping through) moves the number.</li>
<li>The pipeline is the system under test, and the <strong>seed dataset is the fixture</strong>. Just as you pin a test fixture so the suite is deterministic, NimbusMart's data is generated from a fixed seed — which is exactly why "43" is a hard number and not a moving target.</li>
</ul>` },
  ],
  lab: {
    tier: "T1",
    understand: {
      engine: "sql",
      datasets: ["orders", "fraud_scores"],
      task: `<p><strong>Write the acceptance test that defines the entire capstone.</strong> The review queue is every order whose fraud score is at or above <code>FRAUD_REVIEW_THRESHOLD</code> (0.80). The starter runs an <em>inner</em> join with no threshold filter, so it returns all 225 scored orders — far too many.</p>
<p>Fix it into the canonical definition: <code>orders LEFT JOIN fraud_scores</code>, keep only rows where <code>fraud_score >= FRAUD_REVIEW_THRESHOLD</code> (0.80), returning <code>order_id</code>, <code>customer_id</code>, <code>fraud_score</code>. Get it to <strong>exactly 43 rows</strong> — that is the contract H2, H3 and H4 must all reproduce.</p>
<p>Why <code>LEFT</code> and not <code>INNER</code>, when the threshold filter drops the nulls either way? Because the left join is how the pipeline <em>sees</em> the 15 unscored orders at all — they're the ones you quarantine in H2. Same 43 in the queue; very different visibility into what was excluded.</p>`,
      starterQuery: `SELECT o.order_id, o.customer_id, f.fraud_score
FROM orders o
JOIN fraud_scores f ON o.order_id = f.order_id`,
      solutionQuery: `SELECT o.order_id, o.customer_id, f.fraud_score
FROM orders o
LEFT JOIN fraud_scores f ON o.order_id = f.order_id
WHERE f.fraud_score >= 0.80        -- the FRAUD_REVIEW_THRESHOLD`,
      hint: `The threshold is inclusive — use >=, not >, or you'll drop the 4 orders sitting exactly on the FRAUD_REVIEW_THRESHOLD (0.80) and land on 39.`
    },
    buildWithAI: `I'm building the acceptance test for a data-engineering capstone ("the NimbusMart platform"). Set up a real local project that pins the one number the whole pipeline must reproduce. Assume Python 3.10+ and nothing else installed.

1. Create a project folder \`nimbusmart-capstone\` with a venv, and install pyspark (pin a recent 3.5.x) and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator that writes ALL SIX NimbusMart sources as CSV/JSON into \`data/\`:
   - \`orders.csv\`: 240 rows — order_id (O-10001..O-10240), customer_id (C-0001..C-0060), seller_id from [S-101,S-204,S-355,S-410,S-777,S-812,S-903] with S-777 deliberately ~35% (the skew hot key), status from [placed,shipped,delivered,delivered,delivered,cancelled,returned], total_amount (8..950, 2dp), country from [DE,US,IN,BR,JP,FR,AU], channel from [web,app,app]
   - \`order_events.json\`: ~361 clickstream rows with schema drift (some rows omit \`device\`, some add \`app_version\`)
   - \`customers.csv\`: 60 rows with 3 null cities and 2 casing-duplicated emails; plus \`customer_updates.csv\` (8 rows) where C-0042 moves Munich then Hamburg
   - \`products.json\`: 40 rows with nested category{dept,aisle}, tags[], attrs{brand,weight_kg}
   - \`fraud_scores.csv\`: scores for exactly 225 of the 240 orders (15 unscored), with EXACTLY 43 scores >= FRAUD_REVIEW_THRESHOLD (0.80) and EXACTLY 4 equal to 0.80
   - \`payments.csv\` (~228 rows) and \`couriers.csv\`/\`courier_pings.csv\` (~278 pings, ingested_at lagging event_ts)
   Add assertions in the generator so it refuses to write unless the review queue (score >= FRAUD_REVIEW_THRESHOLD, 0.80) has 43 rows, the exactly-0.80 count is 4, and unscored is 15.

3. Create \`acceptance.py\` defining FRAUD_REVIEW_THRESHOLD = 0.80 as a named constant, building a local[*] SparkSession, reading orders and fraud_scores with EXPLICIT StructType schemas (no inferSchema), and computing:
   review_queue = orders.join(fraud_scores, "order_id", "left").filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)

4. Create \`test_acceptance.py\` (pytest) asserting, WITHOUT hardcoding by re-deriving from the CSVs with the plain csv module:
   - review_queue.count() == the number of generated scores >= FRAUD_REVIEW_THRESHOLD (expect 43)
   - the count of scores exactly == FRAUD_REVIEW_THRESHOLD is 4 (proves the boundary is inclusive)
   - orders.count() == 240 and the LEFT join keeps all 240 before the filter
   - swapping >= for > drops the count to 39 (a guard test that documents why the boundary matters)

5. Run the generator, then pytest. Print the review-queue size and confirm it is 43. Windows-friendly paths please.`
  },
  check: [
    {
      type: "predict",
      q: "On the NimbusMart seed data, how many rows does the capstone's acceptance query return?",
      code: `SELECT o.order_id, o.customer_id, f.fraud_score
FROM orders o
LEFT JOIN fraud_scores f ON o.order_id = f.order_id
WHERE f.fraud_score >= 0.80   -- FRAUD_REVIEW_THRESHOLD`,
      options: ["225", "43", "240", "39"],
      answer: 1,
      explain: "Of the 225 scored orders, exactly 43 meet or exceed FRAUD_REVIEW_THRESHOLD (0.80). The LEFT join first keeps all 240 orders; the WHERE then drops the 15 rows with a NULL fraud_score (NULL >= the threshold is never true) and the 182 below-threshold rows, leaving 43. 240 is the pre-filter join; 225 is the inner join; 39 is the trap you hit by writing > instead of >=."
    },
    {
      type: "mcq",
      q: "Why does the capstone brief insist the 15 unscored orders be <em>quarantined</em> rather than dropped by the join?",
      options: [
        "Quarantining is faster than filtering in Spark",
        "So the exclusion is visible and reprocessable — a dropped row is an invisible data-loss bug; a quarantined row is an auditable decision",
        "Because a LEFT join cannot remove rows, only an INNER join can",
        "Because unscored orders always turn out to be fraudulent"
      ],
      answer: 1,
      explain: "An INNER join silently vanishes the 15 unscored orders — nobody can tell later whether they were excluded on purpose or lost to a bug. Routing them to a quarantine table makes the exclusion an explicit, auditable, re-runnable decision. That's the whole point of acceptance criterion 1."
    },
    {
      type: "predict",
      q: "A teammate 'tightens' the review rule to strictly greater-than. What does the queue size become, and why is that a bug?",
      code: `SELECT COUNT(*) AS in_review
FROM orders o
LEFT JOIN fraud_scores f ON o.order_id = f.order_id
WHERE f.fraud_score > 0.80   -- note: > not >=, vs FRAUD_REVIEW_THRESHOLD`,
      options: ["43 — unchanged", "39 — it drops the 4 orders sitting exactly on the threshold", "0 — nothing scores above the FRAUD_REVIEW_THRESHOLD", "225 — the filter is ignored"],
      answer: 1,
      explain: "Exactly 4 orders score precisely at FRAUD_REVIEW_THRESHOLD (0.80). The business rule is 'at or above', so > silently excludes those 4 real review cases, giving 39. This is why the acceptance test pins both the 43 and the inclusive boundary — the difference between > and >= is 4 orders a fraud analyst never sees."
    },
    {
      type: "mcq",
      q: "Which statement best captures why H1 is an acceptance test and not just 'the first query'?",
      options: [
        "It's the fastest query in the capstone, so it runs first",
        "It's the smallest testable definition of 'correct' — every later stage (Bronze, Silver, Gold, the DAG) is validated by whether it still reproduces this 43",
        "It uses a LEFT join, which all downstream queries must also use",
        "It's the only query that touches the fraud_scores table"
      ],
      answer: 1,
      explain: "The value of an acceptance test is that it pins a single, unambiguous pass/fail fact before you build anything. '43' becomes the invariant: ingest, quality gates, joins, and orchestration are all judged by whether the end-to-end run still lands on it. Break the number anywhere upstream and this test goes red."
    }
  ],
  fieldNotes: `The 61-vs-43 discrepancy in the cold open is a composite of two real incidents, and both root-caused to the same thing: no one had written down what "in review" meant as a runnable query. In the first, an analyst used an INNER join and a > filter in a spreadsheet, so the 15 unscored orders vanished and the 4 boundary orders dropped — coincidentally landing near the right ballpark, which is the worst outcome, because it looked plausible for three weeks. In the second, a different team counted distinct customer_ids instead of orders. The fix in both cases was not better spreadsheets; it was promoting one query to a tested artifact that the dashboard, the alert, and the finance reconciliation all called. Once "the review queue" had exactly one definition that returned 43 on the fixture, the arguments stopped — you can't debate a number three systems compute identically. That promotion, from tribal knowledge to acceptance test, is the entire job of this capstone brief.`
};
