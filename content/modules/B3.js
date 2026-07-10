// B3 — Medallion Architecture (T3 trace: b3-medallion-flow)
// Trace facts: one order_events batch — Bronze 48,214 (append-all) → Silver 46,161 clean
//   (1,206 dupes dropped, 847 quarantined) → Gold order_funnel_daily + fraud_review_queue (431).
export default {
  id: "B3",
  track: "B",
  title: "Medallion Architecture",
  minutes: 22,
  coldOpen: "An analyst files a bug: the executive funnel dashboard shows 3% more 'checkout_started' events than 'cart_added', which is physically impossible. The data engineer traces it back and finds the dashboard is reading straight off the raw ingestion table — duplicates from at-least-once delivery, a null-order_id row that a COUNT still tallied, and a schema-drift column the query silently fanned out on. Nothing was wrong with the data. Everything was wrong with which layer the dashboard trusted.",
  concept: [
    { type: "prose", html: `
<p>The Medallion architecture is one rule wearing three coats: <strong>raw data and business-ready data must not live in the same table, and there is a disciplined station in between.</strong> Bronze, Silver, Gold — the names are just medal tiers, but each is a <em>contract</em> about what a table guarantees, and the entire value comes from never letting one layer's mess leak into the next.</p>
<ul>
<li><strong>Bronze — raw, append-only.</strong> A faithful, replayable copy of exactly what the source sent: every column, every row, every duplicate, every drifted field, plus lineage metadata (<code>_ingested_at</code>, <code>_source_file</code>). Bronze validates nothing and drops nothing. Its promise is fidelity, not cleanliness.</li>
<li><strong>Silver — cleaned, conformed.</strong> Typed, deduplicated, contract-checked. Bad rows are <em>quarantined</em> (not silently discarded), columns get real types, duplicates collapse to one row per entity. Silver's promise: every row satisfies the schema and means what it says.</li>
<li><strong>Gold — business-ready.</strong> Joined, aggregated, shaped for a consumer — a funnel mart, a revenue cube, the fraud review queue. Gold's promise: a stakeholder can query it without knowing a single thing about drift, dedup, or nulls.</li>
</ul>` },
    { type: "prose", html: `
<p>The direction of flow is the whole discipline: <strong>mess flows downstream, never up.</strong> Duplicates and drift are <em>legal</em> in Bronze and <em>illegal</em> in Gold. A business aggregate is legal in Gold and forbidden in Bronze. This gives you two properties that the cold-open dashboard threw away by reading raw:</p>
<ul>
<li><strong>Replayability.</strong> Because Bronze is an untouched copy, you can rebuild Silver and Gold from scratch when you find a bug in your cleaning logic — the source truth is still sitting there. If you'd cleaned data on ingestion, that truth would be gone.</li>
<li><strong>A single quarantine boundary.</strong> Bad rows get caught at exactly one place — the Bronze→Silver promotion — and land in a queryable reject table with a reason, not in a log nobody reads. You always know what you dropped and why, and you can backfill it once the producer is fixed.</li>
</ul>
<p>The costly anti-patterns are the two boundary violations: cleaning in Bronze (you've destroyed the source of truth) and serving raw as Gold (you've handed drift and duplicates to the CFO). The layers exist precisely so neither happens.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 250" font-family="var(--mono)" font-size="12">
<defs><marker id="b3arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0 L10 5 L0 10 z" fill="var(--ink2)"/></marker></defs>
<rect x="24" y="60" width="190" height="120" rx="10" fill="var(--paper2)" stroke="var(--rust)"/>
<text x="119" y="84" text-anchor="middle" fill="var(--rust)" font-weight="bold">BRONZE</text>
<text x="119" y="104" text-anchor="middle" fill="var(--ink2)" font-size="10">receiving dock</text>
<text x="119" y="128" text-anchor="middle" fill="var(--ink)" font-size="11">48,214 rows</text>
<text x="119" y="146" text-anchor="middle" fill="var(--ink2)" font-size="9">append-all · keep drift</text>
<text x="119" y="162" text-anchor="middle" fill="var(--ink2)" font-size="9">reject nothing</text>
<rect x="265" y="60" width="190" height="120" rx="10" fill="var(--paper2)" stroke="var(--accent)"/>
<text x="360" y="84" text-anchor="middle" fill="var(--accent)" font-weight="bold">SILVER</text>
<text x="360" y="104" text-anchor="middle" fill="var(--ink2)" font-size="10">QC station</text>
<text x="360" y="128" text-anchor="middle" fill="var(--ink)" font-size="11">46,161 clean</text>
<text x="360" y="146" text-anchor="middle" fill="var(--ink2)" font-size="9">typed · deduped (−1,206)</text>
<text x="360" y="162" text-anchor="middle" fill="var(--ink2)" font-size="9">847 → quarantine</text>
<rect x="506" y="60" width="190" height="120" rx="10" fill="var(--paper2)" stroke="var(--green)"/>
<text x="601" y="84" text-anchor="middle" fill="var(--green)" font-weight="bold">GOLD</text>
<text x="601" y="104" text-anchor="middle" fill="var(--ink2)" font-size="10">showroom</text>
<text x="601" y="128" text-anchor="middle" fill="var(--ink)" font-size="11">funnel + review queue</text>
<text x="601" y="146" text-anchor="middle" fill="var(--ink2)" font-size="9">joined · aggregated</text>
<text x="601" y="162" text-anchor="middle" fill="var(--ink2)" font-size="9">431 in review</text>
<line x1="214" y1="120" x2="263" y2="120" stroke="var(--ink2)" stroke-width="2" marker-end="url(#b3arr)"/>
<line x1="455" y1="120" x2="504" y2="120" stroke="var(--ink2)" stroke-width="2" marker-end="url(#b3arr)"/>
<text x="360" y="212" text-anchor="middle" fill="var(--rust)" font-size="10">847 quarantined rows exit here ↓ — held, queryable, backfillable — never silently dropped</text>
<line x1="360" y1="180" x2="360" y2="200" stroke="var(--rust)" stroke-dasharray="4 3" marker-end="url(#b3arr)"/>
<text x="360" y="236" text-anchor="middle" fill="var(--ink2)" font-size="10">mess flows → downstream only; a business aggregate never flows ← back into Bronze</text>
</svg>`, caption: "Three contracts, one direction of flow: Bronze keeps everything, Silver conforms and quarantines, Gold serves." },
    { type: "code", lang: "python", code: `# The Bronze→Silver promotion in miniature: type, dedup, and QUARANTINE (don't drop).
from pyspark.sql import functions as F

bronze = spark.read.table("bronze_order_events")     # 48,214 raw rows, all columns, all dupes

typed = (bronze
    .withColumn("event_ts", F.to_timestamp("event_ts"))          # string -> real timestamp
    .withColumn("device", F.coalesce("device", F.lit("unknown"))) # drift null -> explicit sentinel
    .dropDuplicates(["event_id"]))                                # at-least-once dupes collapse

good = typed.filter(F.col("order_id").isNotNull() & F.col("event_ts").isNotNull())
bad  = typed.filter(F.col("order_id").isNull()  | F.col("event_ts").isNull()) \\
            .withColumn("_reject_reason", F.lit("null_order_id_or_bad_ts"))

good.write.saveAsTable("silver_order_events")          # 46,161 conformed rows
bad.write.saveAsTable("quarantine_order_events")       # 847 rows, queryable, backfillable`, caption: "Silver quarantines rejects into a dead-letter table with a reason — it never uses a silent .filter() to make bad rows vanish." },
    { type: "analogy", title: "Receiving dock, QC station, showroom", html: `
<p>NimbusMart's warehouse is literally built as three zones, and the Medallion layers are just their data twins.</p>
<p>The <strong>receiving dock</strong> (Bronze) accepts every truck. A pallet arrives shrink-wrapped, mislabeled, with a damaged corner — the dock takes it anyway, logs the truck, the time, the seal number, and sets it down. Rejecting at the dock would mean losing the record that the supplier <em>sent</em> it. The dock's only job is: nothing that arrived goes unrecorded.</p>
<p>The <strong>QC station</strong> (Silver) unwraps each pallet, checks it against the spec, relabels to the warehouse standard, and pulls the damaged unit onto a clearly-marked <em>quarantine</em> shelf — not the bin, but not the bin either. It's set aside <em>with a tag saying why</em>, so it can be inspected or returned. What leaves QC is uniform and trustworthy.</p>
<p>The <strong>showroom</strong> (Gold) is arranged for the customer, not the forklift: products grouped by what a shopper asks for, priced, faced-out. No shrink-wrap, no quarantine shelf, no receiving log in sight. A customer should never see the dock — and a stakeholder should never query Bronze.</p>` },
    { type: "javaBridge", html: `
<p>You've built this shape before without the medal names. A robust ingestion service almost always separates <strong>raw capture</strong> from <strong>validated domain objects</strong>:</p>
<ul>
<li>Bronze is your <strong>raw inbound payload log</strong> — the <code>String</code> body (or Kafka <code>ConsumerRecord</code>) you persist <em>before</em> parsing, so that when the parser has a bug you can replay from the original bytes. Every senior engineer who has lost data to &ldquo;we transformed on ingest and the transform was wrong&rdquo; keeps this log. That instinct <em>is</em> Bronze.</li>
<li>Silver is the boundary where you deserialize into typed domain objects and run Bean Validation — <code>@NotNull</code>, <code>@Pattern</code>. But instead of throwing a <code>ConstraintViolationException</code> that drops the message on the floor, Silver routes the failures to a <strong>dead-letter queue</strong> you can inspect and reprocess. Quarantine table = DLQ, made queryable.</li>
<li>Gold is the <strong>read model / DTO</strong> you expose to the API or dashboard — a CQRS projection shaped for the consumer, never the raw aggregate. You'd never serialize your JPA entity graph straight to the client; you don't serve Bronze to the exec either.</li>
</ul>` },
  ],
  lab: {
    tier: "T3",
    understand: {
      engine: "trace",
      trace: "b3-medallion-flow",
      task: `<p>Scrub one night's <code>order_events</code> batch through all three layers and watch the row counts move. Things to track as you step:</p><ul><li><strong>Bronze (t0–t6):</strong> the count that stays flat — 48,214 in, 48,214 kept. Note that the schema-drift rows (missing <code>device</code>, extra <code>app_version</code>) ride straight through untouched. Ask yourself why keeping them is correct.</li><li><strong>Silver (t9–t18):</strong> two different subtractions — 1,206 <em>duplicates dropped</em> vs 847 <em>rows quarantined</em>. Watch that the 847 go to a reject table, not to <code>/dev/null</code>. Confirm the arithmetic reconciles: 48,214 − 1,206 − 847 = 46,161.</li><li><strong>Gold (t21–t27):</strong> how 46,161 clean rows collapse to a handful of business rows, and where <code>FRAUD_REVIEW_THRESHOLD</code> gets applied — on clean data, once, at the very end.</li></ul><p>The badge says <em>simulation</em>: the counts are illustrative, but the flow and the contracts are exactly how a real medallion pipeline behaves.</p>`
    },
    buildWithAI: `I'm learning Medallion architecture (Bronze/Silver/Gold) and I want to build a real local three-layer pipeline in PySpark, not just watch the animation. Assume a fresh machine, Python 3.10+, nothing else installed.

1. Create a project folder \`nimbusmart-medallion\` with a venv, and install pyspark (pin any recent 3.5.x) and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator writing \`data/raw_order_events.jsonl\` — newline-delimited JSON mimicking a drifty clickstream, ~4000 rows of order_events with fields: event_id, order_id, event_type (from cart_add, checkout_start, payment_submitted, fraud_check, fulfillment_hold, shipped_scan), event_ts (ISO string), device (ios/android/web). Inject realistic mess ON PURPOSE: ~6% of rows omit the device field entirely; ~8% add an unexpected app_version field (schema drift); duplicate ~1 in 20 rows verbatim (at-least-once delivery); make ~1% have a null order_id and ~0.5% have a non-parseable event_ts. Print the exact counts of each defect so tests can reference them.

3. Create \`pipeline.py\` with three functions:
   - bronze(spark): read the raw JSONL with schema-on-read (permissive), append _ingested_at and _source_file, write Delta/parquet table 'bronze' — DROP NOTHING, keep drift columns via mergeSchema.
   - silver(spark): from bronze, cast event_ts to timestamp, coalesce missing device to 'unknown', dropDuplicates(['event_id']); split into 'silver' (order_id not null AND event_ts parsed) and 'quarantine' (the rest, tagged with a _reject_reason). Write both.
   - gold(spark): from silver, build 'gold_order_funnel' = count of distinct order_id per event_type, and 'gold_fraud_review_queue' = orders whose fraud_check outcome is at/above a named constant FRAUD_REVIEW_THRESHOLD = 0.80 (synthesize a fraud_score per fraud_check event deterministically). Write both.

4. Create \`test_pipeline.py\` (pytest) asserting, computed from the generator's printed defect counts (do NOT hardcode blindly):
   - bronze row count == raw row count (nothing dropped)
   - silver row count == bronze - duplicates - quarantined; and silver + quarantine + duplicates reconciles to bronze
   - quarantine contains exactly the null-order_id and bad-event_ts rows, each with a _reject_reason
   - no row in silver has a null order_id; every silver device is non-null
   - gold_order_funnel has one row per event_type and no event_type outside the known enum

5. Run generator, run each layer, run pytest. Then write a short comment explaining which single defect would corrupt a dashboard if you skipped Silver and queried Bronze directly. Windows-friendly paths please.`
  },
  check: [
    {
      type: "mcq",
      q: "The cold-open dashboard showed <em>more</em> checkout_started events than cart_added — impossible. Which Medallion principle would have prevented it?",
      options: [
        "Serve stakeholders from Gold (deduplicated, conformed), never from Bronze — the raw table still holds at-least-once duplicates and null-key rows that inflate naive counts",
        "Bronze should have rejected the duplicate rows on ingestion",
        "The dashboard should COUNT DISTINCT on every column defensively",
        "Gold should append to Bronze so the layers stay in sync"
      ],
      answer: 0,
      explain: "Bronze deliberately keeps duplicates and drift for fidelity and replay — it is not meant to be queried by consumers. Deduplication and validation happen at the Bronze→Silver boundary; Gold is the layer shaped for reading. The dashboard's bug was trusting the wrong layer, not bad data. Bronze must NOT reject dupes (that would break replayability) — it's simply not the serving layer."
    },
    {
      type: "predict",
      q: "In the trace, Bronze holds 48,214 rows. Silver drops 1,206 duplicate event_ids and quarantines 847 rows that fail the contract. How many rows does the clean Silver table hold?",
      code: `silver_clean = bronze_rows - duplicates_dropped - quarantined
# bronze_rows = 48214, duplicates_dropped = 1206, quarantined = 847
print(silver_clean)`,
      options: ["48,214", "47,008", "46,161", "45,314"],
      answer: 2,
      explain: "48,214 − 1,206 − 847 = 46,161. Note the two subtractions are different in kind: duplicates are truly removed (they were redundant copies), while the 847 quarantined rows are set aside in a reject table with a reason — recoverable, not destroyed. The arithmetic reconciles, which is exactly the audit property Silver is supposed to give you."
    },
    {
      type: "mcq",
      q: "A well-meaning engineer adds <code>.dropDuplicates()</code> and a <code>NOT NULL</code> filter to the Bronze ingestion job 'to keep Bronze clean.' What has this broken?",
      options: [
        "Replayability and fidelity — Bronze is no longer a faithful copy of the source, so you can never rebuild Silver/Gold from raw truth when you find a cleaning bug, and you've lost the record of what the source actually sent",
        "Nothing — cleaning earlier is strictly better",
        "Only performance, because dropDuplicates triggers a shuffle in Bronze",
        "Gold, because Gold reads directly from Bronze"
      ],
      answer: 0,
      explain: "Bronze's contract is fidelity: an untouched, replayable copy. The moment you clean in Bronze you destroy the source of truth — if your dedup logic later turns out to be wrong, there is nothing to rebuild from, and you can no longer answer 'what did the producer actually send?'. Cleaning belongs at the Bronze→Silver boundary, where rejects are quarantined, not vanished."
    },
    {
      type: "mcq",
      q: "Why does Gold apply <code>FRAUD_REVIEW_THRESHOLD</code> to produce the review queue, rather than Bronze or Silver doing it?",
      options: [
        "Applying a business rule is Gold's job: it acts on clean, conformed data once, producing a decision-ready mart — doing it earlier would bake a business decision into a layer meant to stay business-agnostic",
        "Bronze can't do comparisons because it has no schema",
        "The threshold must be applied in all three layers for consistency",
        "Silver already applied it, so Gold is just copying the result"
      ],
      answer: 0,
      explain: "Bronze and Silver are business-agnostic — they capture and conform data without encoding what the business does with it. FRAUD_REVIEW_THRESHOLD is a business rule, so it lives in Gold, applied once to clean data. Push it into Silver and every consumer inherits one team's policy; a second consumer with a different threshold now has to unwind it."
    }
  ],
  fieldNotes: `A fintech team I advised had no Bronze layer — their ingestion job parsed, validated, and typed each event in one pass, writing only the clean result, &ldquo;to save storage.&rdquo; It worked until a vendor silently changed a timestamp format from epoch-millis to ISO-8601 mid-morning. The parser didn't crash; it just produced nulls for the new format, and the validation step dropped those rows as invalid — so for four hours, a growing fraction of transactions simply <em>disappeared</em>, with no record they had ever arrived. There was nothing to replay from, because the raw bytes were never stored. Recovery meant begging the vendor for a re-send of a window they could only estimate. After that incident they added a dead-simple Bronze: write the raw payload to object storage first, parse second, always. The next format change — and there's always a next one — cost them a ten-minute reprocess from Bronze instead of a data-loss incident and an apology email to compliance. Bronze storage is the cheapest insurance in the building; the thing it insures is your ability to say &ldquo;we still have exactly what they sent us.&rdquo;`
};
