// G3 — Lineage, Cataloging & Observability (Track G, Quality/Governance/Ops)
// Verified facts used by lab + checks (from data/nimbusmart/seed.js, seed 42):
//   Lineage of the review queue: orders (240) + fraud_scores (225) --inner join-->
//     orders_scored (225) --filter fraud_score >= FRAUD_REVIEW_THRESHOLD--> 43 rows.
//   4 of the 43 sit exactly on the boundary (inclusive >=). Strict '>' would give 39.
//   Freshness: courier_pings ingested_at lags event_ts; worst tonight = 130 min,
//     late batch = 95 min, both breach the 60-min freshness SLO on gold.delivery_sla.
export default {
  id: "G3",
  track: "G",
  title: "Lineage, Cataloging & Observability",
  minutes: 24,
  coldOpen: "Two minutes before a compliance review, a risk analyst messages: “the dashboard says 43 orders are in the fraud-review queue — where does that number actually come from, and is it even current?” Nobody can answer from memory. The pipeline is green, every job succeeded, and yet neither question has an answer anyone would stake their job on. That gap — between 'it ran' and 'I can prove where this came from and how old it is' — is what lineage and observability exist to close.",
  concept: [
    { type: "prose", html: `
<p>A number on a dashboard is the last node of a graph. <strong>Lineage</strong> is that graph made explicit: for any table, column, or metric, the recorded chain of <em>what produced it</em> — the upstream tables, the transforms, the exact predicates — all the way back to raw sources. When someone asks "where did the 43 come from?", lineage lets you <em>traverse</em> an answer instead of reconstructing one by reading code under pressure.</p>
<p>Our review queue traces cleanly: <code>gold.fraud_review_queue (43)</code> ← <code>filter(fraud_score &gt;= FRAUD_REVIEW_THRESHOLD)</code> ← <code>silver.orders_scored (225)</code> ← <code>orders (240)</code> ⋈ <code>fraud_scores (225)</code>. Column-level lineage adds the detail that wins arguments: 4 of the 43 ride the inclusive boundary, kept only because the predicate is <code>&gt;=</code> and not <code>&gt;</code>.</p>
<p>Lineage answers <em>where</em>. <strong>Observability</strong> answers a second question the analyst also asked: <em>is it still true?</em> The three signals that matter most for a data pipeline:</p>
<ul>
<li><strong>Freshness</strong> — how old is the newest data feeding this table? Governed by a <strong>freshness SLO</strong>: a written promise like "no more than 60 minutes behind event time." A correct number computed over stale inputs is still wrong.</li>
<li><strong>Volume</strong> — did roughly the expected number of rows land? A partition that's 10% of yesterday's size is a silent upstream failure.</li>
<li><strong>Schema</strong> — did the columns or types drift (the G1 problem), and did anything downstream inherit it?</li>
</ul>
<p>The payoff of pairing the two: when a freshness monitor fires, lineage tells it the <strong>blast radius</strong> — every downstream node that just went stale — and, just as usefully, everything that <em>didn't</em>. That's the difference between "something's wrong somewhere" and "these two tiles are stale because <code>courier_pings</code> is 95 minutes late; the fraud queue is unaffected."</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 270" font-family="var(--mono)" font-size="12">
<text x="20" y="20" fill="var(--ink2)" font-size="11">LINEAGE GRAPH — traverse the 43 backward to its raw sources</text>
<rect x="20" y="40" width="150" height="44" rx="10" fill="var(--paper2)" stroke="var(--line)"/>
<text x="95" y="60" text-anchor="middle" fill="var(--ink)">bronze.orders</text>
<text x="95" y="76" text-anchor="middle" fill="var(--ink2)" font-size="10">240 rows</text>
<rect x="20" y="100" width="150" height="44" rx="10" fill="var(--paper2)" stroke="var(--line)"/>
<text x="95" y="120" text-anchor="middle" fill="var(--ink)">bronze.fraud_scores</text>
<text x="95" y="136" text-anchor="middle" fill="var(--ink2)" font-size="10">225 rows</text>
<rect x="290" y="70" width="150" height="44" rx="10" fill="var(--paper2)" stroke="var(--accent)"/>
<text x="365" y="90" text-anchor="middle" fill="var(--accent)" font-weight="bold">silver.orders_scored</text>
<text x="365" y="106" text-anchor="middle" fill="var(--ink2)" font-size="10">225 rows · inner join</text>
<rect x="540" y="70" width="160" height="44" rx="10" fill="var(--paper2)" stroke="var(--rust)"/>
<text x="620" y="90" text-anchor="middle" fill="var(--rust)" font-weight="bold">gold.fraud_review_queue</text>
<text x="620" y="106" text-anchor="middle" fill="var(--ink2)" font-size="10">43 rows</text>
<line x1="170" y1="62" x2="288" y2="86" stroke="var(--ink2)" stroke-width="1.5"/>
<line x1="170" y1="122" x2="288" y2="98" stroke="var(--ink2)" stroke-width="1.5"/>
<line x1="440" y1="92" x2="538" y2="92" stroke="var(--rust)" stroke-width="1.5"/>
<text x="489" y="84" text-anchor="middle" fill="var(--ink2)" font-size="9">&gt;= THRESHOLD</text>
<text x="20" y="180" fill="var(--ink2)" font-size="11">FRESHNESS SLO — a separate branch, and a separate question: is it current?</text>
<rect x="20" y="196" width="160" height="44" rx="10" fill="var(--paper2)" stroke="var(--line)"/>
<text x="100" y="216" text-anchor="middle" fill="var(--ink)">bronze.courier_pings</text>
<text x="100" y="232" text-anchor="middle" fill="var(--rust)" font-size="10">lag 95 min · LATE</text>
<rect x="300" y="196" width="150" height="44" rx="10" fill="var(--paper2)" stroke="var(--rust)" stroke-dasharray="5 4"/>
<text x="375" y="216" text-anchor="middle" fill="var(--rust)" font-weight="bold">gold.delivery_sla</text>
<text x="375" y="232" text-anchor="middle" fill="var(--ink2)" font-size="10">STALE · SLO 60 min</text>
<line x1="180" y1="218" x2="298" y2="218" stroke="var(--rust)" stroke-width="1.5" stroke-dasharray="5 4"/>
<rect x="520" y="196" width="180" height="44" rx="10" fill="none" stroke="var(--rust)"/>
<text x="610" y="216" text-anchor="middle" fill="var(--rust)" font-size="10">ALERT: breach 95 &gt; 60 min</text>
<text x="610" y="232" text-anchor="middle" fill="var(--ink2)" font-size="9">blast radius = 2 tiles + digest</text>
<line x1="450" y1="218" x2="518" y2="218" stroke="var(--rust)" stroke-width="1.5"/>
</svg>`, caption: "Lineage: traverse the 43 back to raw tables. Observability: a freshness SLO on a different branch fires with a known blast radius." },
    { type: "code", lang: "python", code: `from pyspark.sql import functions as F

FRAUD_REVIEW_THRESHOLD = 0.80

# --- Lineage, reproduced: this IS the recorded recipe for the 43 ----------
orders = spark.read.table("bronze.orders")            # 240 rows (leaf)
fraud  = spark.read.table("bronze.fraud_scores")      # 225 rows (leaf)

orders_scored = orders.join(fraud, "order_id")        # silver: 225 rows
review_queue  = orders_scored.filter(                 # gold:   43 rows
    F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)

# --- Observability: a freshness SLO monitor on a different table ----------
FRESHNESS_SLO_MIN = 60
pings = spark.read.table("bronze.courier_pings")

freshness = (pings
    .agg(F.max("event_ts").alias("newest_event"))     # newest event we've seen
    .withColumn("lag_min",
        (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("newest_event")) / 60))

# The alert predicate the monitor evaluates every few minutes:
#   lag_min > FRESHNESS_SLO_MIN  ->  page + mark every downstream node STALE
# Tonight: lag_min = 95  ->  95 > 60  ->  breach on gold.delivery_sla`, caption: "The lineage is the recipe you can re-run; the freshness monitor is a clock on the source, compared against a written SLO." },
    { type: "analogy", title: "The lot-and-batch traceability record", html: `
<p>When a NimbusMart supplier's product gets recalled, nobody guesses which pallets shipped it. Every item carries a <strong>lot number</strong>, and the warehouse keeps a traceability record: this lot arrived on that truck, was inspected at QC on this date, split across those three showroom bays, and went out on these customer orders. Pull one thread — a bad lot — and you can walk it <em>forward</em> to every affected shipment, or <em>backward</em> from a customer complaint to the exact receiving batch. No archaeology, no reading the whole warehouse.</p>
<p>Data lineage is that traceability record for numbers. The 43 has a lot number: it came from <em>this</em> Silver table, built by <em>that</em> join, from <em>those</em> two Bronze sources. And freshness is the expiry date stamped on the lot — because a perfectly traceable pallet of yoghurt that's three weeks old is still not something you put in the showroom.</p>` },
    { type: "javaBridge", html: `
<p>You already read one of these every week: a <strong>stack trace</strong>. When a service throws, you don't guess where the <code>NullPointerException</code> came from — you read the frames from the throw site back down through every caller to <code>main()</code>. Data lineage is a stack trace for a <em>value</em> instead of an exception: the frames are tables and transforms, and you read from the dashboard cell back to the raw source that produced it.</p>
<ul>
<li>Stack frame (method + line) ↔ <strong>lineage node</strong> (table + transform).</li>
<li>"Caused by:" chain ↔ <strong>upstream edges</strong> — the 43 was "caused by" a filter, "caused by" a join, "caused by" two raw tables.</li>
<li>APM/observability (latency, error rate, saturation) ↔ <strong>data observability</strong> (freshness, volume, schema) — same discipline of "is this healthy right now?", pointed at data instead of at request traffic.</li>
</ul>
<p>The upgrade: a stack trace is generated for free by the runtime the instant something breaks. Data lineage mostly isn't — you get it by <em>emitting</em> it (a catalog, OpenLineage events, dbt/Delta metadata). The teams that can answer "where did this number come from?" in two minutes are the ones who paid to record the frames <em>before</em> they were paged for them.</p>` },
  ],
  lab: {
    tier: "T3",
    understand: {
      engine: "trace",
      trace: "g3-lineage",
      task: `<p>Scrub the simulation and watch two questions get answered mechanically. <strong>First (steps 1–5): lineage.</strong> Follow the 43 backward hop by hop — Gold → Silver → the two Bronze leaves. Note how the row count is <em>set at the join</em> (240 + 225 → 225) and only <em>trimmed at the filter</em> (225 → 43), and watch for the 4 orders that survive only because the boundary is inclusive.</p>
<p><strong>Then (steps 6–11): observability.</strong> A different branch — <code>gold.delivery_sla</code>, fed by <code>courier_pings</code> — breaches its 60-minute freshness SLO when late data arrives 95–130 minutes behind event time. Watch the alert fire <em>with a blast radius</em>: it names the stale downstream nodes and, crucially, confirms the fraud queue is unaffected because it shares no ancestor with <code>courier_pings</code>. Ask yourself at each step: could I answer "where did this come from" and "is it fresh" without this graph?</p>`
    },
    buildWithAI: `I'm learning data lineage and freshness observability in PySpark. Build me a real local project that makes both concrete. I'm on my own machine; assume nothing beyond Python 3.10+.

1. Create a project folder \`nimbusmart-lineage\` with a venv, and install pyspark (pin a recent 3.5.x) and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator writing CSVs into \`data/\` matching NimbusMart:
   - \`orders.csv\`: 240 rows (order_id O-10001.., customer_id, seller_id, total_amount, status, country)
   - \`fraud_scores.csv\`: scores for exactly 225 orders (15 unscored) with EXACTLY 43 at fraud_score >= FRAUD_REVIEW_THRESHOLD (0.80) and 4 of them equal to the threshold exactly
   - \`courier_pings.csv\`: ~278 pings with event_ts AND ingested_at, where ingested_at lags event_ts by a few minutes normally but a deliberate batch lags 95–130 minutes (late data)

3. Create \`pipeline.py\` that defines FRAUD_REVIEW_THRESHOLD = 0.80 and builds the medallion lineage:
   - bronze = raw reads; silver_orders_scored = orders.join(fraud, "order_id"); gold_review_queue = silver.filter(fraud_score >= FRAUD_REVIEW_THRESHOLD)
   - print the row count at EACH layer (240/225 -> 225 -> 43) so the lineage is visible as shrinking counts
   - emit a tiny lineage.json: for each output table, its upstream inputs and the transform description (a hand-rolled OpenLineage-style record)

4. Create \`freshness.py\` that defines FRESHNESS_SLO_MIN = 60, computes lag_min = (now - max(event_ts)) for courier_pings, and prints BREACH if lag_min > FRESHNESS_SLO_MIN, listing the downstream tables that would be marked stale (read them from lineage.json).

5. Create \`test_lineage.py\` (pytest) asserting: gold_review_queue.count() == 43; the strict '>' variant == 39 (the 4 boundary rows); every gold/silver table in lineage.json resolves to raw bronze leaves; and freshness.py reports BREACH for the injected late batch. Compute expected numbers from the CSVs, don't hardcode inside pipeline.py.

6. Run generator, pipeline, freshness, and pytest. Show me lineage.json and the freshness BREACH output, and explain how you'd find every dashboard affected by the late courier data. Windows-friendly paths.`
  },
  check: [
    {
      type: "mcq",
      q: "An analyst asks \"where did the 43 in the fraud-review queue come from?\" What does lineage let you answer that reading the pipeline code under time pressure does not?",
      options: [
        "It makes the query run faster so you can re-derive the number live",
        "It gives a recorded, traversable graph: gold_review_queue ← filter(>= FRAUD_REVIEW_THRESHOLD) ← orders_scored (join) ← orders + fraud_scores — including that 4 rows ride the inclusive boundary",
        "It proves the number is correct without needing to know the transforms",
        "Nothing — lineage and reading the code give identical speed and confidence"
      ],
      answer: 1,
      explain: "Lineage is the value's 'stack trace': the recorded chain of upstream tables and transforms. You traverse it instead of reconstructing it, and column-level lineage surfaces the boundary detail (4 of 43 kept by >=) that a hurried code read would miss."
    },
    {
      type: "predict",
      q: "Reproducing the lineage of the review queue on the seed data, what does the final count print?",
      code: `orders = spark.read.table("orders")           # 240
fraud  = spark.read.table("fraud_scores")     # 225
review = (orders.join(fraud, "order_id")
    .filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD))
print(review.count())`,
      options: ["240", "225", "43", "39"],
      answer: 2,
      explain: "The inner join drops the 15 unscored orders (240 → 225), then the inclusive filter keeps those at or above the threshold: 43. (225 is the Silver count before filtering; 39 is what a buggy strict '>' would give by dropping the 4 boundary rows.)"
    },
    {
      type: "mcq",
      q: "The <code>gold.delivery_sla</code> job succeeded, its table has rows, and every dashboard tile is green — yet the freshness monitor pages. How is that possible?",
      options: [
        "The monitor is misconfigured; a successful job with rows is by definition fresh",
        "Freshness measures the AGE of the newest data, not job success — late courier_pings (95 min behind) mean the table is built on stale inputs even though the run completed",
        "The table must be empty; freshness only fires on zero rows",
        "The job actually failed and the green status is a rendering bug"
      ],
      answer: 1,
      explain: "Job success and data freshness are orthogonal signals. The pipeline can complete perfectly over inputs that are two hours old. Freshness = now − max(event_ts); when late data pushes that past the 60-minute SLO, the number is correct-but-stale, which is still wrong to act on."
    },
    {
      type: "mcq",
      q: "When the freshness alert fires on <code>gold.delivery_sla</code>, why does having lineage make the triage dramatically faster?",
      options: [
        "Lineage automatically fixes the late data by re-ingesting it",
        "It gives the alert a blast radius: it names every downstream node that inherited the staleness AND confirms fraud_review_queue is safe because it shares no ancestor with courier_pings",
        "It silences all other alerts so you can focus",
        "It converts the stale rows into fresh ones retroactively"
      ],
      answer: 1,
      explain: "Lineage turns 'something is stale somewhere' into 'these 2 tiles and the ops digest are stale, caused by courier_pings at 95 min; the fraud queue is unaffected.' You triage the actual blast radius instead of auditing the whole platform at 17:30."
    }
  ],
  fieldNotes: `The most expensive lineage gap I've watched play out cost nothing to fix and a quarter to discover. A finance team trusted a "daily active sellers" metric on an exec dashboard for months. One day it dropped 30% overnight and three VPs wanted an answer in an hour. There was no lineage catalog, so two engineers spent that hour grep-ing notebooks and Airflow DAGs trying to find which of nine possible upstream tables fed the tile — and they found the wrong one first. The actual cause: an upstream team had changed a seller-status enum (a schema drift straight out of G1), and the metric's filter silently excluded a whole category. What made it a five-alarm fire wasn't the bug; it was that nobody could say where the number came from, so nobody could say what else was affected. They shipped a lineage catalog and freshness SLOs the following month. The next time a similar drift happened, the alert arrived pre-scoped — "this changed, here are the 6 downstream metrics that inherit it" — and the fix was a 20-minute conversation instead of a war room. Observability doesn't prevent incidents; it collapses the time from "something's off" to "here's exactly what and where."`
};
