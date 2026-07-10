// A2 — The Freight Line: Anatomy of a Data Platform (Track A, story-sim T3)
// Verified facts used by checks (from data/nimbusmart/generate.py, seed 42):
//   orders = 240; fraud_scores = 225 (15 unscored / null score)
//   fraud_score >= FRAUD_REVIEW_THRESHOLD (0.80) = 43; LEFT-join review rule
//   (score >= threshold OR score IS NULL) = 43 + 15 = 58
export default {
  id: "A2",
  track: "A",
  title: "The Freight Line: Anatomy of a Data Platform",
  minutes: 20,
  coldOpen: "A NimbusMart engineer, trying to save storage, wires the clickstream pipeline to parse-clean-and-overwrite in one step: raw events in, tidy events out, nothing kept in between. It runs beautifully for six weeks. Then a deploy ships a bug that maps every event's timestamp to 1970, and by the time anyone notices, there is no raw copy left to reprocess from. Six weeks of history is unrecoverable — not because the data was lost, but because the platform had no place to keep it. This module is the shape of the platform that mistake was missing.",
  concept: [
    { type: "prose", html: `
<p>Strip every data platform — Databricks, Snowflake, a hand-rolled Spark-on-S3 stack, doesn't matter — down to its skeleton and you get the same three verbs in the same order: <strong>ingest → transform → serve</strong>. Raw data arrives from source systems; it gets cleaned and reshaped; the result is served to whoever consumes it — a dashboard, an ML model, a reviewer working the fraud queue. Everything else is detail hung on those three hooks.</p>
<p>The industry name for how you <em>stage</em> that flow is the <strong>Medallion architecture</strong>: three layers, each an immutable-ish table you can point a query at, each with a different contract about what it promises.</p>
<ul>
<li><strong>Bronze</strong> — ingest. Land the raw data exactly as it arrived, plus metadata about when and from where. Reject nothing. Fix nothing.</li>
<li><strong>Silver</strong> — transform. Parse, type, deduplicate, conform to a schema, and route the damaged rows to a quarantine table (with a reason). One clean row per real event.</li>
<li><strong>Gold</strong> — serve. Join and aggregate into business-shaped tables: the fraud-review queue, the hourly funnel, the exec dashboard. Shaped for a consumer, not for storage.</li>
</ul>
<p>The reason you separate them — the reason the cold open's one-step pipeline was doomed — is that each layer optimizes for a different, conflicting virtue. Collapse them and you lose the one you didn't optimize for. Usually it's the one you needed at 3 a.m.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 250" font-family="var(--mono)" font-size="11">
<defs><marker id="a2arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto"><path d="M0 0 L10 5 L0 10 z" fill="var(--ink2)"/></marker></defs>
<text x="16" y="20" fill="var(--ink2)" font-size="11">INGEST → TRANSFORM → SERVE — volume falls, value climbs</text>
<!-- Bronze -->
<rect x="16" y="34" width="200" height="150" rx="10" fill="var(--paper2)" stroke="var(--rust)"/>
<text x="116" y="56" text-anchor="middle" fill="var(--rust)" font-weight="bold">BRONZE — receiving dock</text>
<text x="116" y="76" text-anchor="middle" fill="var(--ink2)" font-size="10">accept every truck · reject nothing</text>
<text x="116" y="108" text-anchor="middle" fill="var(--ink)" font-size="18">1,000,000</text>
<text x="116" y="124" text-anchor="middle" fill="var(--ink2)" font-size="10">raw events, as-arrived</text>
<text x="116" y="150" text-anchor="middle" fill="var(--ink2)" font-size="10">buys: FIDELITY</text>
<text x="116" y="168" text-anchor="middle" fill="var(--ink2)" font-size="10">(immutable · replayable)</text>
<!-- Silver -->
<rect x="260" y="34" width="200" height="150" rx="10" fill="var(--paper2)" stroke="var(--accent)"/>
<text x="360" y="56" text-anchor="middle" fill="var(--accent)" font-weight="bold">SILVER — QC station</text>
<text x="360" y="76" text-anchor="middle" fill="var(--ink2)" font-size="10">type · dedup · conform · quarantine</text>
<text x="360" y="108" text-anchor="middle" fill="var(--ink)" font-size="18">958,000</text>
<text x="360" y="124" text-anchor="middle" fill="var(--ink2)" font-size="10">clean rows · 42,000 quarantined</text>
<text x="360" y="150" text-anchor="middle" fill="var(--ink2)" font-size="10">buys: TRUST</text>
<text x="360" y="168" text-anchor="middle" fill="var(--ink2)" font-size="10">(typed · deduped)</text>
<!-- Gold -->
<rect x="504" y="34" width="200" height="150" rx="10" fill="var(--paper2)" stroke="var(--green)"/>
<text x="604" y="56" text-anchor="middle" fill="var(--green)" font-weight="bold">GOLD — showroom</text>
<text x="604" y="76" text-anchor="middle" fill="var(--ink2)" font-size="10">join · aggregate · label</text>
<text x="604" y="108" text-anchor="middle" fill="var(--ink)" font-size="18">5,740</text>
<text x="604" y="124" text-anchor="middle" fill="var(--ink2)" font-size="10">business rows (queue + funnel)</text>
<text x="604" y="150" text-anchor="middle" fill="var(--ink2)" font-size="10">buys: USEFULNESS</text>
<text x="604" y="168" text-anchor="middle" fill="var(--ink2)" font-size="10">(served to consumers)</text>
<line x1="216" y1="109" x2="258" y2="109" stroke="var(--ink2)" stroke-width="2" marker-end="url(#a2arr)"/>
<line x1="460" y1="109" x2="502" y2="109" stroke="var(--ink2)" stroke-width="2" marker-end="url(#a2arr)"/>
<text x="16" y="212" fill="var(--ink2)" font-size="10">serve FROM Gold · debug THROUGH Silver · replay FROM Bronze — one flow, three contracts, one lineage</text>
<text x="16" y="234" fill="var(--rust)" font-size="10">the cold-open pipeline had no Bronze — so when Silver's logic broke, there was nothing to replay from</text>
</svg>`, caption: "The freight line: the dock accepts everything, the QC station conforms and rejects, the showroom arranges for the customer. Each layer trades volume for a different kind of value." },
    { type: "prose", html: `
<p>The layer boundaries are where the interesting decisions live — especially what each one <em>refuses</em> to do:</p>
<ul>
<li><strong>Bronze refuses to fix.</strong> NimbusMart's <code>order_events</code> stream drifts: some rows are missing <code>device</code>, some carry a surprise <code>app_version</code>. Bronze keeps the drift, untouched. That's not laziness — it's what makes the data <em>replayable</em>. When Silver's cleaning logic has a bug (and it will), you re-run Silver against Bronze and recover. No Bronze, no recovery — exactly the cold open.</li>
<li><strong>Silver refuses to delete.</strong> A malformed row — null <code>order_id</code>, unparseable timestamp — doesn't get silently dropped. It's routed to a <strong>quarantine table with a reason column</strong>, so you can query "what's failing and why" and go fix the producer. Deleting a bad row destroys the evidence; quarantining it files the bug.</li>
<li><strong>Gold refuses to leave decisions implicit.</strong> This is where the business rule gets applied and named: orders with <code>fraud_score &gt;= FRAUD_REVIEW_THRESHOLD</code> (the 0.80 rule) land in the review-queue mart. And Gold has to decide what an <em>unscored</em> order means — the 15 orders with no fraud score. Silently dropping them (an inner join) would hide risk; the honest Gold rule is "score at/above threshold <em>or</em> no score yet → review."</li>
</ul>
<p><strong>Batch vs streaming, at 10,000 feet:</strong> the same three layers run under either clock. <em>Batch</em> processes a bounded chunk on a schedule (last night's orders, at 02:00) — cheap, simple, high-latency. <em>Streaming</em> processes events continuously — fresh to seconds, pricier, operationally heavier. Most real platforms land on <em>micro-batch</em>: the streaming API triggered every couple of minutes. You'll take that trade apart in A5; for now, just know the freight line's shape doesn't change with the clock — only how often the trucks arrive.</p>` },
    { type: "code", lang: "python", code: `from pyspark.sql import functions as F

FRAUD_REVIEW_THRESHOLD = 0.80

# BRONZE — ingest: land raw, add lineage metadata, change nothing else
bronze = (spark.read.json("s3://nimbus/raw/order_events/ds=2026-05-14/")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name()))
bronze.write.format("delta").mode("append").saveAsTable("bronze.order_events")

# SILVER — transform: type, conform, dedup; damaged rows go to quarantine, not /dev/null
typed = (spark.read.table("bronze.order_events")
    .withColumn("event_ts", F.to_timestamp("event_ts"))
    .withColumn("device", F.coalesce(F.col("device"), F.lit("unknown"))))
clean = typed.filter(F.col("order_id").isNotNull() & F.col("event_ts").isNotNull())
quarantine = typed.filter(F.col("order_id").isNull() | F.col("event_ts").isNull())
clean.dropDuplicates(["event_id"]).write.saveAsTable("silver.order_events")
quarantine.write.saveAsTable("silver.order_events_quarantine")   # keep the evidence

# GOLD — serve: join to scores, apply the business rule, shape for the reviewer
gold = (spark.read.table("silver.order_events")
    .join(spark.read.table("fraud_scores"), "order_id", how="left")
    .filter((F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD) | F.col("fraud_score").isNull()))
gold.write.saveAsTable("gold.fraud_review_queue")`, caption: "The same three verbs, in code: land raw (Bronze), conform + quarantine (Silver), join + apply the rule (Gold). Note the left join at Gold — an unscored order is a decision, not a drop." },
    { type: "analogy", title: "The dock, the QC bench, and the showroom floor", html: `
<p>NimbusMart's fulfillment warehouse <em>is</em> the platform, physically. Trucks back up to the <strong>receiving dock</strong> all day. The dock's only job: sign for every pallet and log it — the right SKUs, the wrong ones, the crushed box, the mystery crate with no manifest. It accepts all of them and edits none of them. That's Bronze. The signed manifest is your replay log; if anything downstream goes wrong, you start again from what the dock recorded.</p>
<p>Behind the dock is the <strong>QC bench</strong>. Workers unpack each pallet, check it against the spec, standardize the labels, and pull anything damaged — but a rejected item doesn't hit the bin; it goes on the <strong>quarantine shelf with a tag saying why</strong>, so a buyer can chase the supplier. That's Silver: conform, deduplicate, and reject <em>with a record</em>.</p>
<p>Out front is the <strong>showroom</strong>, arranged entirely for the customer — by category, priced, only the sellable stock, nothing raw in sight. That's Gold. A shopper browses the showroom; they never walk the dock. And when a price looks wrong, a manager traces it back through QC to the dock manifest — because every layer kept its record. Serve from the showroom, audit to the dock.</p>` },
    { type: "javaBridge", html: `
<p>You've built this exact shape before; it just had different names. A request comes into your <strong>controller</strong> as a raw JSON payload, gets validated and mapped into a typed <strong>DTO/domain object</strong> in the <strong>service</strong> layer, and is finally assembled into a <strong>view model / response</strong> shaped for the client. Raw → validated → presentation. That's Bronze → Silver → Gold, one request at a time instead of a billion rows at a time.</p>
<ul>
<li><strong>Quarantine ≈ your dead-letter queue.</strong> When a message fails validation, a decent backend doesn't <code>catch (Exception e) {}</code> and drop it — it routes it to a DLQ with the failure reason so someone can inspect and replay. Silver's quarantine table is the same instinct at table scale: reject loudly and keep the body.</li>
<li><strong>Bronze ≈ the append-only event log.</strong> If you've done event sourcing, Bronze is the immutable event store and Silver/Gold are materialized read models you can always rebuild by replaying the log. The cold-open team deleted their event log to save disk — the one thing event sourcing tells you never to touch.</li>
<li><strong>The layers are a build pipeline, not a monolith.</strong> Think Jenkins stages: compile → test → package. You don't fuse them into one script precisely so that when "test" fails you still have the "compile" output to inspect. Same reason Bronze and Silver are separate tables.</li>
</ul>` },
  ],
  lab: {
    tier: "T3",
    understand: {
      engine: "trace",
      trace: "a2-platform-flow",
      task: `<p><strong>Follow one NimbusMart order event — checkout_start for O-10188 — down the whole freight line and watch the pile shrink.</strong> Scrub the timeline and track three things:</p>
<ul>
<li><em>What Bronze keeps</em> (t=3, t=6): the event lands with a <em>missing</em> <code>device</code> field and Bronze preserves the drift untouched. Note the count in ≈ count kept — nothing rejected.</li>
<li><em>What Silver rejects, and where it goes</em> (t=9, t=12): 42,000 rows quarantined — null <code>order_id</code>, bad timestamps, duplicates — routed to a reason-tagged table, not deleted. Our event's <code>device</code> becomes <code>'unknown'</code> and it passes.</li>
<li><em>What Gold produces</em> (t=15, t=18): a million raw events collapse to a few thousand business rows, with the <code>FRAUD_REVIEW_THRESHOLD</code> rule applied. Watch volume fall while value climbs at every stage.</li>
</ul>
<p>The through-line to hold: <strong>serve from Gold, debug through Silver, replay from Bronze</strong> — three contracts, one lineage.</p>`
    },
    buildWithAI: `I'm learning the Medallion architecture (Bronze/Silver/Gold) for a data engineering course. Build me a real, runnable local PySpark project that stages a NimbusMart order-events feed through all three layers. Assume nothing installed beyond Python 3.10+; I'm on Windows (use pathlib, no bash-isms).

1. Create a folder \`nimbusmart-medallion\` with a venv, and install pyspark (pin a recent 3.5.x) and pytest.

2. Create \`generate_events.py\` — deterministic (random.seed(42)) — that writes \`data/raw/order_events.json\` (newline-delimited JSON) modeling NimbusMart's DRIFTY, MESSY clickstream:
   - ~1,000 events across order_ids O-10001..O-10240. Fields: event_id (unique), order_id, event_type (cart_add, checkout_start, payment_submitted, fraud_check, shipped_scan), event_ts (ISO string), device (ios/android/web).
   - Inject realistic damage on purpose: ~6% of rows OMIT the device field; ~8% add a surprise app_version field (schema drift); exactly 50 rows have a NULL/empty order_id; exactly 40 rows have an unparseable event_ts like "NOT_A_DATE"; duplicate exactly 30 event_ids (at-least-once delivery). Print the counts of each defect so tests can rely on them.

3. Create \`pipeline.py\` that runs three stages and writes each to its own Parquet folder under \`lake/\`:
   - BRONZE: read the raw JSON as-is, add _ingested_at (current_timestamp) and _source_file (input_file_name), write to lake/bronze WITHOUT changing any source field. Keep the drift.
   - SILVER: from Bronze, cast event_ts to timestamp, coalesce missing device to 'unknown', dropDuplicates on event_id. Route rows with null order_id OR null(cast-failed) event_ts to lake/silver_quarantine WITH a reason column; write the survivors to lake/silver.
   - GOLD: define FRAUD_REVIEW_THRESHOLD = 0.80 as a named constant; left-join Silver to a small fraud_scores.csv you also generate (score some order_ids >= the threshold, leave some unscored). Build a fraud_review_queue where fraud_score >= FRAUD_REVIEW_THRESHOLD OR fraud_score IS NULL. Write to lake/gold.

4. Create \`test_pipeline.py\` (pytest) asserting:
   - Bronze row count == raw row count (Bronze rejects nothing).
   - Silver + quarantine counts reconcile to Bronze minus the duplicate event_ids (dedup removed exactly the injected dupes).
   - Every quarantined row has a non-null reason; no quarantined row leaked into Silver.
   - The Gold queue includes unscored orders (proving the LEFT join + IS NULL rule), and recompute the expected queue size from the CSVs with the plain csv module rather than hardcoding it.

5. Run generator, pipeline, then pytest. Then show me: pick one order_id that appears in Bronze, trace it into Silver (or quarantine), and into Gold — printing the row at each layer so I can see fidelity → trust → usefulness with my own eyes.`
  },
  check: [
    {
      type: "mcq",
      q: "NimbusMart's <code>order_events</code> stream has schema drift — some rows lack <code>device</code>, some add <code>app_version</code>. Which layer stores the drift <em>untouched</em>, and why?",
      options: [
        "Bronze — landing the raw event unchanged is what makes the data replayable when a downstream transform later turns out to be buggy",
        "Silver — it's the layer that stores data, so it keeps whatever arrives",
        "Gold — consumers need the raw fields, so drift is preserved all the way to serving",
        "None — drift is an error and should be rejected at ingest so it never enters the platform"
      ],
      answer: 0,
      explain: "Bronze's contract is raw + immutable precisely so you can replay history when Silver's cleaning logic has a bug. Silver conforms the drift (coalesce missing device, enforce schema); it doesn't preserve it. Rejecting drift at ingest (the misconception) throws away exactly the raw copy you'll need to recover."
    },
    {
      type: "predict",
      q: "Gold applies the business rule with a <em>left</em> join so unscored orders aren't hidden. On the seed data (240 orders; 43 with score ≥ threshold; 15 with no score at all), how many rows land in the review queue?",
      code: `SELECT COUNT(*)
FROM   orders o
LEFT JOIN fraud_scores f ON f.order_id = o.order_id
WHERE  f.fraud_score >= FRAUD_REVIEW_THRESHOLD   -- 0.80
   OR  f.fraud_score IS NULL;                     -- unscored → needs review`,
      options: ["43", "58", "225", "15"],
      answer: 1,
      explain: "43 orders score at/above the threshold, and the left join keeps the 15 unscored orders whose score is NULL — the rule sends those to review too. 43 + 15 = 58. An inner join would silently drop the 15 unscored orders (hiding risk) and return only 43."
    },
    {
      type: "mcq",
      q: "A row arrives at Silver with a null <code>order_id</code>. What does a well-built Silver layer do with it?",
      options: [
        "Silently drop it, since a row with no order_id is useless downstream",
        "Route it to a quarantine table with a reason column, so the failure is queryable and the upstream producer can be fixed",
        "Pass it through to Gold and let the consumer decide what to do",
        "Overwrite the null order_id with a generated placeholder id so the row can still join"
      ],
      answer: 1,
      explain: "Silver rejects with a record: quarantine the row plus why it failed. Silently dropping it (the misconception) destroys the evidence you need to fix the producer, and fabricating an id would corrupt every downstream join. Quarantine files the bug instead of hiding it."
    },
    {
      type: "mcq",
      q: "The cold-open pipeline cleaned-and-overwrote in one step with no Bronze. When a deploy corrupted every timestamp, why was the history unrecoverable?",
      options: [
        "The corrupted data was too large to fit in memory for a rollback",
        "There was no immutable raw layer to reprocess from — Bronze exists so that when a transform breaks, you can re-run it against the original data",
        "Streaming pipelines can't be rolled back by design",
        "The bug was in Gold, and Gold tables are never backed up"
      ],
      answer: 1,
      explain: "Bronze is the immutable replay log. Fusing ingest and transform means the raw copy is overwritten the moment it's cleaned, so a bug in the cleaning step has no source to recover from. Keeping Bronze separate is cheap insurance against exactly this — the transform is code, and code has bugs."
    }
  ],
  fieldNotes: `A logistics analytics team ran a "lean" pipeline that read raw courier pings, cleaned them, and wrote only the cleaned Silver table — no Bronze, to save roughly 30% on storage. It worked for eight months. Then a library upgrade changed how a timezone-naive timestamp was parsed, and for nine days every ping's event_ts silently shifted by the cluster's UTC offset, quietly corrupting the on-time-delivery KPI that a customer SLA was billed against. When they went to reprocess correctly, there was nothing to reprocess: the raw pings had been transformed-in-place and discarded. They reconstructed nine days of history from a partial application-log backup over a frantic weekend, and the "30% storage savings" was erased many times over by one incident. The permanent fix was one line of architecture — land Bronze raw and immutable, always — which is the whole point of separating ingest from transform.`
};
