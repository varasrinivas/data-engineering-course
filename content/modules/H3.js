// H3 — Build Gold
// Verified against data/nimbusmart/seed.js (seed 42) via the sparksim engine:
//   Silver review queue (orders join fraud_scores, fraud_score >= FRAUD_REVIEW_THRESHOLD 0.80) = 43 rows
//   queue joined to customers on customer_id = 43 rows (every order resolves to a customer)
//   revenue-by-country groupBy = 7 rows (FR highest: 58 orders, ~€22,655)
//   in the 43-row queue, seller S-777 owns 19; segments = 33 consumer / 10 business
export default {
  id: "H3",
  track: "H",
  title: "Build Gold",
  minutes: 28,
  coldOpen: "The fraud lead opens the review queue you built in H2 and immediately bounces it back: \"This is 43 order IDs and a score. Who's the customer? Which seller? Is this the mega-seller again? I can't action a spreadsheet of foreign keys.\" She's right — Silver is correct but unreadable. Gold is where correct data becomes usable: the same 43 orders, now carrying the customer's name and segment, the seller, and the amount at risk — arranged for the human who has to decide, not the machine that stored it.",
  concept: [
    { type: "prose", html: `
<p>Silver is trustworthy but shaped for the warehouse: normalized, keyed, joinable. <strong>Gold is shaped for the reader.</strong> It's the showroom — denormalized marts and dimensions arranged around the questions people actually ask, with the joins already done so a fraud analyst or a finance dashboard never writes a join at all.</p>
<p>Gold in the capstone has three deliverables, and each maps to a modeling idea you've already met:</p>
<ul>
<li><strong>A customer dimension with history (SCD2).</strong> C-0042 moved Munich → Hamburg mid-quarter. A May order must still show Munich; a June order, Hamburg. That means <em>two versions</em> of the customer row, each with a validity window — not an overwrite. (This is B2's slowly-changing dimension, now for real.)</li>
<li><strong>An order fact + rollup marts.</strong> The order fact is the grain (one row per order) joined to its dimensions; from it you roll up <code>revenue_by_country</code> with a single <code>groupBy().agg()</code> — 240 fact rows collapse to 7 country summaries. (This is B1's star schema and E4's aggregation.)</li>
<li><strong>The fraud-review-queue mart.</strong> The capstone spine: the 43 orders at or above <code>FRAUD_REVIEW_THRESHOLD</code> (0.80), enriched with customer and seller context so the queue is <em>actionable</em>, not just correct.</li>
</ul>` },
    { type: "code", lang: "python", code: `from pyspark.sql import functions as F

FRAUD_REVIEW_THRESHOLD = 0.80

# From Silver: the trusted, scored orders (H2 output)
scored = silver_orders            # 225 rows, one per order, all scored

# --- Gold mart 1: revenue by country (order fact → rollup) ---
revenue_by_country = (scored
    .groupBy("country")
    .agg(F.count("*").alias("orders"),
         F.sum("total_amount").alias("revenue"))
    .orderBy(F.col("revenue").desc()))          # 7 rows, FR on top

# --- Gold mart 2: the fraud-review queue, enriched to be actionable ---
review_queue = (scored
    .filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)   # 43 rows
    .join(customers, "customer_id")                           # add name, segment
    .select("order_id", "name", "segment", "seller_id",
            "fraud_score", "total_amount")
    .orderBy(F.col("fraud_score").desc()))`, caption: "Two Gold marts from one Silver source: a rollup for finance, an enriched queue for fraud." },
    { type: "prose", html: `
<p>The enrichment join is where Gold earns its keep, and it hides a trap the capstone is built to teach. To attach customer context you join the review queue to the customer dimension on <code>customer_id</code> — but <strong>both tables also carry a <code>country</code> column</strong>. Join them naively and you get two <code>country</code> columns and an ambiguous-reference error the first time anyone selects it.</p>
<p>The fix is to decide the grain before you join: the review queue selects only the columns it owns (<code>order_id</code>, <code>customer_id</code>, <code>seller_id</code>, <code>fraud_score</code>, <code>total_amount</code>) before joining, so the only shared column is the join key. This is the discipline of a star schema — the fact holds foreign keys and measures; the dimension holds descriptive attributes; you resolve overlaps <em>at the join</em>, not after. Get it right and the 43 rows come back enriched and unambiguous; get it wrong and every downstream reader inherits your column collision.</p>
<p>One more thing the enriched mart reveals that raw scores hide: of the 43 orders in review, <strong>19 belong to seller S-777</strong> — the mega-seller from E3's skew lab. The same hot key that melts a shuffle stage also concentrates fraud risk. You only see that once the seller_id rides along in Gold; in Silver it was just another foreign key.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 260" font-family="var(--mono)" font-size="11">
<text x="16" y="18" fill="var(--accent)" font-size="10">SILVER (grain: one row per order)</text>
<rect x="16" y="26" width="150" height="60" rx="8" fill="var(--paper2)" stroke="var(--accent)"/>
<text x="91" y="50" text-anchor="middle" fill="var(--ink)">silver_orders</text>
<text x="91" y="68" text-anchor="middle" fill="var(--ink2)" font-size="9">225 scored orders</text>
<text x="16" y="120" fill="var(--gold)" font-size="10">GOLD (arranged for the reader)</text>
<rect x="230" y="26" width="170" height="52" rx="8" fill="var(--paper2)" stroke="var(--gold)"/>
<text x="315" y="46" text-anchor="middle" fill="var(--ink)">revenue_by_country</text>
<text x="315" y="64" text-anchor="middle" fill="var(--ink2)" font-size="9">groupBy → 7 rows</text>
<rect x="230" y="96" width="170" height="52" rx="8" fill="var(--paper2)" stroke="var(--gold)"/>
<text x="315" y="116" text-anchor="middle" fill="var(--ink)">fraud_review_queue</text>
<text x="315" y="134" text-anchor="middle" fill="var(--rust)" font-size="9">filter >= threshold → 43</text>
<rect x="470" y="96" width="150" height="52" rx="8" fill="var(--paper2)" stroke="var(--line)"/>
<text x="545" y="116" text-anchor="middle" fill="var(--ink)" font-size="10">+ customer dim</text>
<text x="545" y="134" text-anchor="middle" fill="var(--ink2)" font-size="9">join customer_id</text>
<line x1="166" y1="52" x2="228" y2="52" stroke="var(--ink2)"/>
<line x1="166" y1="60" x2="228" y2="120" stroke="var(--ink2)"/>
<line x1="400" y1="122" x2="468" y2="122" stroke="var(--ink2)"/>
<rect x="470" y="26" width="150" height="52" rx="8" fill="none" stroke="var(--rust)" stroke-dasharray="4 3"/>
<text x="545" y="46" text-anchor="middle" fill="var(--rust)" font-size="10">watch: both carry</text>
<text x="545" y="63" text-anchor="middle" fill="var(--rust)" font-size="10">country → collision</text>
<line x1="400" y1="52" x2="468" y2="52" stroke="var(--rust)" stroke-dasharray="4 3"/>
<text x="16" y="185" fill="var(--ink2)" font-size="10">Gold rule: pick the grain and the columns BEFORE the join — the fact owns keys + measures,</text>
<text x="16" y="202" fill="var(--ink2)" font-size="10">the dimension owns descriptions; the only shared column at the join should be the key.</text>
<rect x="16" y="214" width="604" height="34" rx="8" fill="var(--paper2)" stroke="var(--line)"/>
<text x="318" y="235" text-anchor="middle" fill="var(--ink)" font-size="10">43 review orders · enriched with name, segment, seller · S-777 owns 19 of them</text>
</svg>`, caption: "Gold turns 225 trusted rows into a 7-row finance rollup and a 43-row actionable queue — joins pre-resolved." },
    { type: "analogy", title: "The showroom, not the stockroom", html: `
<p>The NimbusMart stockroom (Silver) is organized for the warehouse: everything on numbered shelves, each item tagged with SKUs and bin references. Correct, dense, and useless to a shopper — nobody browses by bin number.</p>
<p>The showroom (Gold) is the same inventory, re-arranged for the person deciding: products grouped by what you'd buy together, prices and specs already on the label, the popular items at eye level. Nobody in the showroom cross-references a SKU against a bin map — that work was done when the display was built. Your <code>revenue_by_country</code> mart is the sales board at the entrance; your enriched review queue is the "needs a manager's decision" shelf, each item tagged with who, how much, and which supplier — so the manager acts in seconds instead of walking to the stockroom to look up 43 SKUs.</p>` },
    { type: "javaBridge", html: `
<p>Gold is the <strong>read model</strong> in CQRS, and Silver is the write model. You've built this split: the normalized entities you write against (Silver — 3NF, foreign keys, one source of truth) versus the denormalized projection you read from (Gold — the DTO / view model, joins pre-computed, shaped for one screen).</p>
<ul>
<li>The SCD2 customer dimension is a <strong>temporal read model</strong>: instead of <code>customer.getCity()</code> returning today's value, the dimension answers "what was this customer's city <em>as of</em> this order's date?" — the same as an audit/history table you'd query by effective date, but materialized so the join is cheap.</li>
<li>The enrichment join collision (two <code>country</code> columns) is the exact ambiguity you hit when two JPA entities in a join both map a field of the same name — you disambiguate with an explicit projection (a constructor expression / DTO select) rather than <code>SELECT *</code>. Same fix here: name your columns, don't <code>select("*")</code> across a join.</li>
</ul>` },
  ],
  lab: {
    tier: "T2",
    understand: {
      engine: "sparksim",
      datasets: ["orders", "fraud_scores", "customers"],
      task: `<p><strong>Build the Gold fraud-review-queue mart — the capstone's headline deliverable.</strong> The starter has the correct 43-row queue (orders joined to fraud_scores, filtered at the threshold) but it's just foreign keys and a score: unusable for the fraud team.</p>
<p>Enrich it. Join the queue to the <code>customers</code> dimension on <code>customer_id</code>, then <code>.select</code> the columns a reviewer actually needs: <code>order_id</code>, <code>name</code>, <code>segment</code>, <code>seller_id</code>, <code>fraud_score</code>, <code>total_amount</code>. Order by <code>fraud_score</code> descending so the riskiest orders sit on top. The mart must still be exactly <strong>43 rows</strong> — enrichment adds columns, never rows.</p>
<p>Note the starter already selects a tight column set before the customer join. That's deliberate: if you'd carried <code>orders.country</code> into the join, it would collide with <code>customers.country</code>. Choosing the grain before the join is what keeps Gold unambiguous.</p>`,
      starterCode: `orders = spark.read.table("orders")
fraud = spark.read.table("fraud_scores")
customers = spark.read.table("customers")

queue = (orders
    .join(fraud, "order_id")
    .filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)
    .select("order_id", "customer_id", "seller_id", "fraud_score", "total_amount"))

queue.show()`,
      solutionCode: `orders = spark.read.table("orders")
fraud = spark.read.table("fraud_scores")
customers = spark.read.table("customers")

queue = (orders
    .join(fraud, "order_id")
    .filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)
    .select("order_id", "customer_id", "seller_id", "fraud_score", "total_amount"))

review_mart = (queue
    .join(customers, "customer_id")
    .select("order_id", "name", "segment", "seller_id", "fraud_score", "total_amount")
    .orderBy(F.col("fraud_score").desc()))

review_mart.show()`,
      expect: { rows: 43, cols: ["order_id", "name", "segment", "seller_id", "fraud_score", "total_amount"] },
      dagNotes: `<p>The mart is two joins and a filter: <code>orders ⋈ fraud_scores</code> (inner, drops the 15 unscored), a threshold filter down to 43, then <code>⋈ customers</code> to enrich. Row count holds at 43 through the second join because every order references a real customer — an inner join here is safe precisely because Silver already guaranteed referential integrity. If the enrichment join had changed the count, that would be a red flag: either a missing customer (fewer rows) or a duplicate customer row (more) — both bugs the acceptance test would catch.</p>`
    },
    buildWithAI: `I'm building the Gold layer of a data-engineering capstone (NimbusMart): an SCD2 customer dimension, an order fact with a revenue rollup, and the enriched fraud-review-queue mart. Set up a real local project. Assume Python 3.10+ and nothing installed.

1. Create \`nimbusmart-gold\` with a venv; install pyspark (recent 3.5.x) and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator for ALL SIX NimbusMart sources into \`data/\`:
   - orders.csv (240 rows: order_id, customer_id, seller_id with S-777 ~35%, status, total_amount, country, channel)
   - order_events.json (~361 rows, schema drift), products.json (40 nested rows)
   - customers.csv (60 rows; 3 null cities; 2 casing-dup emails) + customer_updates.csv (8 rows; C-0042 moves Munich on one date then Hamburg later)
   - fraud_scores.csv (225 of 240 scored; 15 unscored; exactly 43 >= FRAUD_REVIEW_THRESHOLD which is 0.80; exactly 4 == 0.80)
   - payments.csv (~228), couriers.csv + courier_pings.csv (~278, late ingested_at)

3. Create \`gold.py\` defining FRAUD_REVIEW_THRESHOLD = 0.80 as a named constant, reading Silver-shaped inputs with EXPLICIT StructType schemas (no inferSchema), and building:
   - dim_customer_scd2: start from customers, apply customer_updates as new versions with valid_from / valid_to / is_current columns (SCD2). C-0042 must produce 2+ rows with non-overlapping validity windows.
   - fact_orders: one row per order joined to seller and to the SCD2 customer version effective at order_ts.
   - gold_revenue_by_country: fact_orders.groupBy("country").agg(count, sum(total_amount)).orderBy(revenue desc) — expect 7 rows.
   - gold_review_queue: orders.join(fraud_scores,"order_id").filter(fraud_score >= FRAUD_REVIEW_THRESHOLD).join(dim_customer current version,"customer_id").select(order_id, name, segment, seller_id, fraud_score, total_amount). IMPORTANT: select a tight column set BEFORE the customer join so orders.country and customers.country don't collide.

4. Create \`test_gold.py\` (pytest), re-deriving expectations from the CSVs with the plain csv module (no hardcoding):
   - gold_review_queue.count() == 43, and the enrichment join does NOT change the count (assert it equals the pre-enrichment queue count)
   - dim_customer_scd2 has exactly one is_current=True row per customer_id, and C-0042 has >= 2 total versions with contiguous, non-overlapping validity windows
   - gold_revenue_by_country row count == number of distinct countries (expect 7) and its revenue sum == fact_orders total revenue (reconciliation)
   - count of seller S-777 rows inside gold_review_queue (print it — it should be 19, showing the skew hot key concentrates fraud risk)

5. Run generator → gold → pytest. Print the review-queue size (43) and the S-777 share of it. Windows-friendly paths.`
  },
  check: [
    {
      type: "predict",
      q: "You enrich the 43-row review queue by joining it to the customers dimension on customer_id. How many rows does the enriched mart have?",
      code: `review_mart = (queue                      # 43 rows
    .join(customers, "customer_id")
    .select("order_id", "name", "segment",
            "seller_id", "fraud_score", "total_amount"))
print(review_mart.count())`,
      options: ["225 — the join re-expands to all scored orders", "43 — every order resolves to exactly one customer, so enrichment adds columns, not rows", "60 — one row per customer", "86 — the join doubles the rows"],
      answer: 1,
      explain: "Each order has exactly one customer_id pointing at exactly one customer row, so the inner join matches 1:1 and the count stays 43. Enrichment is meant to widen rows (add name, segment), never multiply them. If this returned anything but 43, it would signal a missing customer (fewer) or a duplicate dimension row (more) — which is exactly why the acceptance test pins the count across the join."
    },
    {
      type: "predict",
      q: "You roll the 225 scored orders up to revenue by country. How many rows does the result have?",
      code: `revenue_by_country = (scored              # 225 rows
    .groupBy("country")
    .agg(F.sum("total_amount").alias("revenue"))
    .orderBy(F.col("revenue").desc()))
print(revenue_by_country.count())`,
      options: ["225 — one per order", "7 — one row per distinct country (DE, US, IN, BR, JP, FR, AU)", "43 — the review queue", "1 — a single grand total"],
      answer: 1,
      explain: "groupBy collapses the 225 order rows to one row per distinct country. NimbusMart orders span 7 countries, so the rollup returns 7 rows — the shape a finance dashboard wants. This is the fact-to-mart move: the grain goes from 'one row per order' up to 'one row per country', and FR leads with 58 orders."
    },
    {
      type: "mcq",
      q: "Why does the mart select a tight column set from the orders side <em>before</em> joining to the customers dimension?",
      options: [
        "To make the join run faster by reading fewer bytes",
        "Because both orders and customers carry a country column — projecting first leaves the join key as the only shared column, avoiding an ambiguous-reference collision",
        "Because Spark can't join tables with more than 5 columns",
        "To convert the columns to the correct types before joining"
      ],
      answer: 1,
      explain: "orders.country and customers.country would both survive a naive join, producing a duplicated, ambiguous column that errors the moment anyone selects it. Choosing the fact's columns first (keys + measures) so the only overlap is customer_id is the star-schema discipline: the fact owns keys and measures, the dimension owns descriptions, and you resolve overlaps at the join — not with SELECT * afterward. (Column pruning is a nice side effect, not the reason.)"
    },
    {
      type: "mcq",
      q: "The enriched queue reveals seller S-777 owns 19 of the 43 review orders. What does this illustrate about Gold?",
      options: [
        "Gold introduced a bug that duplicated S-777's orders",
        "Gold surfaces business signal that Silver's foreign keys hid — the same skew hot key from E3 also concentrates fraud risk, and you only see it once seller_id rides along in the mart",
        "S-777 should be filtered out of the review queue",
        "The threshold is set too low for large sellers"
      ],
      answer: 1,
      explain: "S-777 is the mega-seller (80 of 240 orders, the E3 skew hot key). In Silver it was just a foreign key on each order; carrying it into the Gold mart makes the concentration visible — nearly half the review queue is one seller. That's the point of Gold: correct data reshaped so a human sees the pattern and can act on it, not just store it."
    }
  ],
  fieldNotes: `A retail analytics team shipped a "review queue" that was technically flawless and operationally dead: it was the raw join output — order_id, customer_id, score — and the fraud analysts refused to use it, falling back to manually pulling each order in the admin console. Adoption was near zero for a quarter, and the pipeline was nearly cancelled as "built but unused." The turnaround was one afternoon of Gold work: join the customer name and segment, carry the seller_id, add the amount at risk, sort by score. Same 43 rows, now actionable — and within a week an analyst spotted that one seller accounted for a wildly disproportionate slice of the queue, which turned out to be a compromised seller account pushing fraudulent orders. The correct-but-unreadable version had contained that exact signal for three months; nobody could see it through a wall of foreign keys. The lesson: Silver's job is to be right, and Gold's job is to be usable, and a pipeline that stops at "right" quietly fails at the only thing anyone measures it by — whether a human acted on it.`
};
