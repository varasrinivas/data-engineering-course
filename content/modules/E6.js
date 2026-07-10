// E6 — Incremental Processing & Delta Lake (Track E) — T2 sparksim
// Verified facts (data/nimbusmart/generate.py, seed 42):
//   customer_updates = 8 rows over 7 distinct customer_ids (C-0042 appears twice).
//   Keep-latest-per-key dedup => 7 rows. C-0042 moved Munich (day 35) -> Hamburg (day 52);
//   ordering by updated_at DESC keeps Hamburg.
export default {
  id: "E6",
  track: "E",
  title: "Incremental Processing & Delta Lake",
  minutes: 26,
  coldOpen: "NimbusMart's customer dimension is rebuilt from scratch every night — read all 60 million rows from the OLTP export, overwrite the table. It worked at launch. Two years on it takes four hours, burns a cluster to reprocess data that didn't change, and customer C-0042, who moved from Munich to Hamburg mid-quarter, flickers between the two cities depending on which nightly run you catch. The fix isn't a bigger cluster. It's to stop reprocessing history and start MERGEing only what changed.",
  concept: [
    { type: "prose", html: `
<p><strong>Incremental processing</strong> is the discipline of touching only the rows that changed. Instead of overwriting the whole customer table nightly, you take a <em>change feed</em> — the OLTP export's inserts, updates, and deletes since the last run (CDC) — and fold just those into the target. The operation that folds them is an <strong>upsert</strong>: update the keys that exist, insert the keys that don't, atomically.</p>
<p>Plain Parquet can't do this. It has no notion of "update row where key = X" — you can only rewrite files. <strong>Delta Lake</strong> adds a transaction log on top of Parquet that gives you three things you've been missing:</p>
<ul>
<li><code>MERGE</code> — atomic upsert (and delete) against a keyed table.</li>
<li><strong>Time travel</strong> — read the table as of a previous version or timestamp (<code>VERSION AS OF 41</code>), so "what did this look like before last night's run?" is a query, not an archaeology project.</li>
<li><strong>Schema evolution</strong> — add a column to the feed and let the target absorb it (<code>mergeSchema</code>) instead of failing the write.</li>
</ul>` },
    { type: "code", lang: "python", code: `from delta.tables import DeltaTable

# 'latest' is ONE row per customer_id (deduped — see below).
target = DeltaTable.forName(spark, "customers")

(target.alias("t")
  .merge(latest.alias("s"), "t.customer_id = s.customer_id")
  .whenMatchedUpdate(set={
      "city":       "s.city",
      "country":    "s.country",
      "updated_at": "s.updated_at"})
  .whenNotMatchedInsert(values={
      "customer_id": "s.customer_id",
      "city":        "s.city",
      "country":     "s.country",
      "updated_at":  "s.updated_at"})
  .execute())

# Time travel: what did the dimension look like 40 versions ago?
prev = spark.read.option("versionAsOf", 41).table("customers")`, caption: "Update the keys that match, insert the keys that don't — one atomic transaction." },
    { type: "prose", html: `
<p>There's a landmine in <code>MERGE</code> that catches everyone once: <strong>the source must have at most one row per target key.</strong> If two source rows match the same <code>customer_id</code>, Delta throws <em>"Cannot perform Merge as multiple source rows matched"</em> — because it can't decide which update wins. And a CDC feed almost always violates this: C-0042 moved twice this quarter, so <code>customer_updates</code> has two rows for that key.</p>
<p>So every real MERGE pipeline begins with the exact idiom from E4: <strong>dedup to the latest row per key</strong>. Partition by the natural key, order by the change timestamp <em>descending</em>, keep <code>row_number() == 1</code>. Get the order direction wrong and you upsert the <em>stale</em> version — C-0042 lands back in Munich, and the bug is silent because the row count is identical either way.</p>` },
    { type: "code", lang: "python", code: `from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Collapse the CDC feed to one winning row per key BEFORE the merge.
w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())

latest = (spark.read.table("customer_updates")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)          # newest change per customer wins
    .drop("rn"))
# latest now has one row per customer_id — safe to MERGE.`, caption: "The upsert-prep step: row_number()==1 over the key, newest first. This is what makes the MERGE legal." },
    { type: "svg", svg: `<svg viewBox="0 0 720 230" font-family="var(--mono)" font-size="12">
<defs><marker id="e6arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0 L10 5 L0 10 z" fill="var(--ink2)"/></marker></defs>
<text x="20" y="22" fill="var(--ink2)" font-size="11">CDC feed (customer_updates) — C-0042 changed twice</text>
<rect x="20" y="34" width="170" height="24" rx="4" fill="var(--paper2)" stroke="var(--rust)"/><text x="30" y="50" fill="var(--ink)" font-size="10">C-0042 · Munich · day35</text>
<rect x="20" y="62" width="170" height="24" rx="4" fill="var(--paper2)" stroke="var(--accent)"/><text x="30" y="78" fill="var(--ink)" font-size="10">C-0042 · Hamburg · day52</text>
<rect x="20" y="90" width="170" height="24" rx="4" fill="var(--paper2)" stroke="var(--line)"/><text x="30" y="106" fill="var(--ink)" font-size="10">C-0007 · Denver · day40</text>
<line x1="196" y1="74" x2="250" y2="74" stroke="var(--ink2)" marker-end="url(#e6arr)"/>
<text x="223" y="66" text-anchor="middle" fill="var(--ink2)" font-size="9">dedup</text>
<text x="223" y="94" text-anchor="middle" fill="var(--ink2)" font-size="9">rn==1</text>
<rect x="256" y="48" width="170" height="24" rx="4" fill="var(--paper2)" stroke="var(--accent)"/><text x="266" y="64" fill="var(--ink)" font-size="10">C-0042 · Hamburg (latest)</text>
<rect x="256" y="90" width="170" height="24" rx="4" fill="var(--paper2)" stroke="var(--line)"/><text x="266" y="106" fill="var(--ink)" font-size="10">C-0007 · Denver</text>
<text x="256" y="134" fill="var(--ink2)" font-size="9">one row per key — MERGE is now legal</text>
<line x1="432" y1="74" x2="486" y2="74" stroke="var(--ink2)" marker-end="url(#e6arr)"/>
<text x="459" y="66" text-anchor="middle" fill="var(--ink2)" font-size="9">MERGE</text>
<rect x="492" y="34" width="210" height="120" rx="8" fill="var(--paper2)" stroke="var(--line)"/>
<text x="597" y="52" text-anchor="middle" fill="var(--ink2)" font-size="10">customers (Delta target)</text>
<rect x="506" y="62" width="182" height="24" rx="4" fill="none" stroke="var(--accent)"/><text x="516" y="78" fill="var(--ink)" font-size="10">C-0042 → UPDATE city</text>
<rect x="506" y="92" width="182" height="24" rx="4" fill="none" stroke="var(--line)"/><text x="516" y="108" fill="var(--ink)" font-size="10">C-0099 → INSERT (new)</text>
<text x="597" y="140" text-anchor="middle" fill="var(--ink2)" font-size="9">atomic · versioned · re-runnable</text>
<text x="20" y="176" fill="var(--rust)" font-size="10">Skip the dedup and MERGE throws: two C-0042 rows, no way to pick a winner.</text>
<text x="20" y="196" fill="var(--ink2)" font-size="10">Order ascending instead of descending and you upsert Munich — the stale value — with the same 7-row shape.</text>
</svg>`, caption: "Two changes for one key collapse to the newest; only then can MERGE atomically update or insert against the target." },
    { type: "analogy", title: "The master ledger, restocked from the manifest", html: `
<p>The NimbusMart warehouse keeps a <strong>master inventory ledger</strong>: one line per SKU, its bin location and count. Deliveries arrive with a <em>manifest</em> of changes — new SKUs, moved bins, restocks. The stock clerk doesn't rewrite the entire ledger every morning; they walk the manifest and, for each line, either <em>update</em> the SKU's existing entry or <em>add</em> a new one. That's a <code>MERGE</code>: upsert the changes, leave the untouched 99% of the ledger alone.</p>
<p>Two habits make it safe. First, if a single SKU appears twice on today's manifest (restocked, then moved), the clerk reconciles to the <em>latest</em> instruction before touching the ledger — that's the dedup. Second, the ledger is idempotent: <strong>re-scanning the same barcode</strong> from a re-run manifest updates the line to the same value, never double-counts. That's why a failed MERGE can simply be re-run — same input, same result — where the old "overwrite everything" rebuild had no such guarantee if it died halfway.</p>` },
    { type: "javaBridge", html: `
<p>You already know MERGE — it's <code>entityManager.merge()</code> / JPA <code>saveOrUpdate</code> / SQL <code>INSERT ... ON CONFLICT DO UPDATE</code>. Same contract: match on the key, update if present, insert if not. Two upgrades for the data-engineering scale:</p>
<ul>
<li><strong>It's a set operation over billions of rows, not one entity at a time.</strong> JPA merges one managed entity per call inside a transaction; a Delta <code>MERGE</code> takes a whole <em>DataFrame</em> of changes and applies them in one distributed, ACID transaction against a file-based table. No row-by-row round trips — the "batch the crossing" instinct from E5 applies here too.</li>
<li><strong>The uniqueness constraint moves upstream.</strong> Your database enforced one row per primary key, so an ambiguous upsert was impossible. A Delta table has no such guardrail on the <em>source</em>: if your change set has two rows for one key, MERGE fails at runtime. The dedup-before-merge step is you doing, in the pipeline, the job the database's primary-key index used to do for you.</li>
</ul>` },
  ],
  lab: {
    tier: "T2",
    understand: {
      engine: "sparksim",
      datasets: ["customer_updates"],
      task: `<p><strong>Prepare the CDC feed for an upsert: one winning row per customer.</strong> <code>customer_updates</code> is the change feed from the OLTP export, and customer <code>C-0042</code> appears twice — they moved from Munich to Hamburg mid-quarter. The starter deduplicates to one row per <code>customer_id</code>, but it orders the window by <code>updated_at</code> <em>ascending</em>, so it keeps the <strong>oldest</strong> change and C-0042 lands back in Munich. Flip the window's ordering to <code>F.col("updated_at").desc()</code> so the newest change wins, then Run.</p><p>The row count is 7 either way — that's the trap. Only the values change: with <code>.desc()</code>, C-0042's surviving row reads Hamburg. This deduped frame is exactly what a Delta MERGE consumes.</p>`,
      starterCode: `updates = spark.read.table("customer_updates")

w = Window.partitionBy("customer_id").orderBy("updated_at")

latest = (updates
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1))

latest.select(
    "customer_id", "city", "country", "updated_at"
).show()`,
      solutionCode: `updates = spark.read.table("customer_updates")

w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())

latest = (updates
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1))

latest.select(
    "customer_id", "city", "country", "updated_at"
).show()`,
      expect: { rows: 7, cols: ["customer_id", "city", "country", "updated_at"] },
      dagNotes: `<p>Eight change rows collapse to 7 — one per distinct customer_id — because only C-0042 changed twice. That the row count is identical whether you order ascending or descending is exactly why this bug is dangerous: the shape looks right, the reconciliation looks done, and the data is silently stale. With <code>.desc()</code>, row_number()==1 selects C-0042's Hamburg change (day 52) over Munich (day 35). Feed the ascending version into a MERGE and you'd overwrite the customer dimension with last quarter's address.</p>`
    },
    buildWithAI: `I'm learning incremental processing with Delta Lake (MERGE upsert, time travel, and the dedup-before-merge pattern). Build me a real local project on my own machine. Assume nothing beyond Python 3.10+.

1. Create a folder \`nimbusmart-delta\` with a venv; install pyspark (recent 3.5.x), delta-spark (matching your pyspark), and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) writer emitting two CSVs into \`data/\`:
   - customers: 60 rows — customer_id (C-0001..C-0060), city, country, updated_at (ISO string). This is the initial dimension.
   - customer_updates: a CDC feed of 8 change rows over 7 distinct customer_ids, where C-0042 appears TWICE (Munich with an earlier updated_at, then Hamburg with a later one) — customer_id, city, country, updated_at.

3. Create \`delta_lab.py\` that builds a SparkSession configured for Delta (spark.sql.extensions + the DeltaCatalog), then:
   - writes customers as a Delta table (format("delta").saveAsTable("customers")) — this is version 0
   - dedups the feed: w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc()); keep row_number()==1 — print it and confirm C-0042 shows Hamburg, not Munich
   - runs DeltaTable.forName(...).merge(latest, "t.customer_id = s.customer_id").whenMatchedUpdate(...).whenNotMatchedInsert(...).execute()
   - demonstrates time travel: read the table versionAsOf 0 and show C-0042's city BEFORE the merge, then current to show it AFTER
   - re-runs the SAME merge a second time and shows the table is unchanged (idempotent)

4. Create \`test_delta_lab.py\` (pytest) asserting — deriving expected values from the raw files, never hardcoding:
   - the deduped feed has exactly one row per customer_id (count == distinct customer_id count == 7)
   - after the merge, C-0042's city is Hamburg (the latest change won)
   - versionAsOf 0 still shows C-0042's pre-merge city (time travel works)
   - running the merge twice yields the same table state as running it once (idempotency)

5. Run the generator, the lab, and pytest. Show me C-0042 before and after via time travel, and prove the second merge changed nothing. Windows-friendly paths please.`
  },
  check: [
    {
      type: "predict",
      q: "customer_updates has 8 rows over 7 distinct customer_ids (C-0042 changed twice). How many rows does this keep-latest dedup produce?",
      code: `w = Window.partitionBy("customer_id").orderBy(
    F.col("updated_at").desc())
latest = (customer_updates
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1))
print(latest.count())`,
      options: ["7", "8", "60", "1"],
      answer: 0,
      explain: "row_number()==1 keeps one row per partition, and there is one partition per distinct customer_id. Eight change rows over seven distinct customers collapse to 7 — C-0042's two changes reduce to its single latest one. This is the shape a MERGE requires."
    },
    {
      type: "mcq",
      q: "Why must you dedup the CDC feed to one row per key <em>before</em> the MERGE?",
      options: [
        "MERGE fails with \"multiple source rows matched\" if two source rows hit the same target key — it can't decide which update wins",
        "MERGE is slower on unsorted input, so dedup is purely a performance optimization",
        "Delta tables cannot store duplicate keys, so the write would silently drop rows",
        "Dedup is optional; MERGE automatically keeps the row with the latest timestamp"
      ],
      answer: 0,
      explain: "A MERGE with two source rows matching one target key is ambiguous, so Delta raises an error rather than guess. The database's primary-key index used to prevent this upstream; in a pipeline, the dedup-to-latest step is you doing that job explicitly before the upsert."
    },
    {
      type: "predict",
      q: "C-0042 moved Munich (day 35) then Hamburg (day 52). What city does the deduped row show?",
      code: `w = Window.partitionBy("customer_id").orderBy(
    F.col("updated_at").desc())
latest = (customer_updates
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1))
latest.filter(F.col("customer_id") == "C-0042").select("city").show()`,
      options: ["Hamburg", "Munich", "Both rows survive (Munich and Hamburg)", "Berlin"],
      answer: 0,
      explain: "Ordering by updated_at descending puts the day-52 Hamburg change first, so row_number()==1 keeps Hamburg. Order ascending instead and you'd keep the stale Munich row — same 7-row shape, wrong value. Direction of the sort is the whole game in CDC dedup."
    },
    {
      type: "mcq",
      q: "The old pipeline overwrote the whole customer table nightly; the new one MERGEs only changes. Beyond speed, what does the Delta MERGE approach give you that the overwrite didn't?",
      options: [
        "Idempotent, atomic re-runs and time travel — a failed MERGE can be safely re-run, and you can query the table as of any previous version",
        "Automatic schema inference, so you never have to declare types again",
        "Elimination of shuffles, since MERGE never moves data between partitions",
        "Guaranteed uniqueness of source keys, so dedup is no longer needed"
      ],
      answer: 0,
      explain: "Delta's transaction log makes the MERGE atomic (it fully applies or not at all, so a re-run after a failure is safe and idempotent) and versioned (time travel to any prior version). The old overwrite had no such guarantees — a job that died halfway left the table in an unknown state."
    }
  ],
  fieldNotes: `A team migrated a nightly full-overwrite customer dimension to an incremental Delta MERGE and cut a 4-hour job to 9 minutes. Three weeks later, addresses started reverting to old values at random. The cause: their CDC feed occasionally delivered a key's changes out of order across micro-batches, and their dedup ordered by the feed's arrival sequence instead of the business updated_at timestamp — so a late-arriving stale change sometimes won row_number()==1 and got merged over the fresh one. The row counts were always correct, so monitoring never flinched. The fix was ordering the dedup by the event timestamp (with a tiebreaker), not ingestion order, and adding a check that no merged updated_at ever moved backwards. The lesson they wrote down: in incremental pipelines, 'latest' must mean latest in business time, and a dedup is only as correct as the key you sort it by.`
};
