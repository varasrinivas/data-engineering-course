// B4 — Warehouse, Lake, Lakehouse (T3 trace: b4-table-formats)
// Trace facts: plain Parquet dir (128 files) grows a _delta_log; atomic commits v0→v5,
//   optimistic-concurrency conflict resolved, reader pinned to v2 while writers advance, time travel to v2.
export default {
  id: "B4",
  track: "B",
  title: "Warehouse, Lake, Lakehouse",
  minutes: 20,
  coldOpen: "Two NimbusMart jobs write the orders table at 2am — the nightly compaction and a late-arriving hourly append — and a dashboard query runs straight through the middle of both. It returns a total that's off by a few thousand orders: it listed the directory mid-write and caught some new Parquet files, some half-deleted old ones, and no way to tell which set was 'the table.' Nobody wrote bad data. The storage layer simply had no idea what a 'commit' was.",
  concept: [
    { type: "prose", html: `
<p>The ten-minute history, because the present only makes sense as the answer to two failures.</p>
<p>First came the <strong>data warehouse</strong> (Teradata, later Redshift, Snowflake, BigQuery): a database tuned for analytics — columnar, ACID, SQL, fast. Its price was rigidity and cost. Data had to be structured and loaded on the warehouse's terms; storage and compute were fused and expensive; your JSON clickstream and your ML features didn't fit. It was a beautiful showroom you could only stock with pre-approved goods.</p>
<p>The reaction was the <strong>data lake</strong> (Hadoop, then S3/ADLS/GCS + Parquet): dump <em>any</em> file, cheap, at any scale, and bring your own engine — Spark, Presto, whatever. Storage and compute finally decoupled. But a lake is <em>just files in a bucket</em>, and files in a bucket have no transactions, no schema enforcement, no atomic commit — exactly the cold open. The industry's own name for the failure mode was the <strong>data swamp</strong>: infinite cheap storage with no guarantees you could trust.</p>` },
    { type: "prose", html: `
<p>The <strong>lakehouse</strong> is the synthesis: keep the lake's cheap, open, decoupled object storage, and add back the warehouse's transactional guarantees — <em>without</em> a database in the middle. The trick that makes it work is the <strong>open table format</strong>: Delta Lake, Apache Iceberg, Apache Hudi. And the whole idea, stripped down, is one sentence:</p>
<p><strong>Put a transaction log next to your Parquet files, and read the log instead of listing the directory.</strong></p>
<p>The data files stay plain, immutable Parquet — anyone's engine can read them. Alongside them sits an ordered log (Delta's <code>_delta_log/</code>, Iceberg's <code>metadata/</code>) where each entry is a <em>commit</em>: &ldquo;as of version N, the table is exactly these files.&rdquo; A reader never guesses from a directory listing again — it asks the log which files are live. That single indirection buys you everything the cold open lacked:</p>
<ul>
<li><strong>Atomic commits</strong> — a write becomes visible only when its one log entry lands; no torn reads.</li>
<li><strong>Snapshots &amp; time travel</strong> — every version is still described by its log entry, so you can read the table <em>as of</em> any past version or timestamp.</li>
<li><strong>ACID over object storage</strong> — atomicity, consistency, isolation, durability, on a bucket, with no server.</li>
</ul>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 250" font-family="var(--mono)" font-size="12">
<text x="24" y="24" fill="var(--ink2)" font-size="11">PLAIN PARQUET — the &ldquo;table&rdquo; is whatever the directory lists right now</text>
<rect x="24" y="34" width="300" height="70" rx="8" fill="var(--paper2)" stroke="var(--rust)"/>
<text x="40" y="56" fill="var(--ink)" font-size="11">orders/  part-0001 … part-0128</text>
<text x="40" y="76" fill="var(--rust)" font-size="10">two writers mid-flight → reader sees a torn set</text>
<text x="40" y="92" fill="var(--ink2)" font-size="9">no commit · no snapshot · no isolation</text>
<text x="396" y="24" fill="var(--ink2)" font-size="11">OPEN TABLE FORMAT — read the log, not the listing</text>
<rect x="396" y="34" width="300" height="70" rx="8" fill="var(--paper2)" stroke="var(--green)"/>
<text x="412" y="56" fill="var(--ink)" font-size="11">orders/  part-*.parquet  (immutable)</text>
<text x="412" y="74" fill="var(--green)" font-size="11">orders/_delta_log/  000…N.json</text>
<text x="412" y="92" fill="var(--ink2)" font-size="9">each entry = a commit: &ldquo;version N is these files&rdquo;</text>
<text x="24" y="140" fill="var(--ink2)" font-size="11">the log is a git history for the table:</text>
<rect x="24" y="152" width="120" height="40" rx="8" fill="var(--paper2)" stroke="var(--line)"/>
<text x="84" y="170" text-anchor="middle" fill="var(--ink)">v0</text>
<text x="84" y="184" text-anchor="middle" fill="var(--ink2)" font-size="9">128 files</text>
<rect x="176" y="152" width="120" height="40" rx="8" fill="var(--paper2)" stroke="var(--line)"/>
<text x="236" y="170" text-anchor="middle" fill="var(--ink)">v1</text>
<text x="236" y="184" text-anchor="middle" fill="var(--ink2)" font-size="9">+6 −3 files</text>
<rect x="328" y="152" width="120" height="40" rx="8" fill="var(--paper2)" stroke="var(--accent)" stroke-width="2"/>
<text x="388" y="170" text-anchor="middle" fill="var(--accent)">v2 ← reader pinned</text>
<text x="388" y="184" text-anchor="middle" fill="var(--ink2)" font-size="9">stable snapshot</text>
<rect x="480" y="152" width="120" height="40" rx="8" fill="var(--paper2)" stroke="var(--line)"/>
<text x="540" y="170" text-anchor="middle" fill="var(--ink)">v5 ← writers</text>
<text x="540" y="184" text-anchor="middle" fill="var(--ink2)" font-size="9">head advances</text>
<line x1="144" y1="172" x2="174" y2="172" stroke="var(--ink2)"/>
<line x1="296" y1="172" x2="326" y2="172" stroke="var(--ink2)"/>
<line x1="448" y1="172" x2="478" y2="172" stroke="var(--ink2)"/>
<text x="24" y="228" fill="var(--ink2)" font-size="10">time travel = check out an old version; snapshot isolation = the pinned reader never sees the writers' churn</text>
</svg>`, caption: "Immutable Parquet + an ordered commit log = versions, atomic commits, and readers pinned to a stable snapshot." },
    { type: "code", lang: "python", code: `# The lakehouse gestures that a plain Parquet directory simply cannot make.
orders = spark.read.format("delta").load("s3://nimbus/orders")   # reads the log, not a listing

# Atomic commit: visible all-at-once when its single log entry lands (here, version N+1)
(new_hour.write.format("delta").mode("append").save("s3://nimbus/orders"))

# Time travel: read the table AS OF a past version or wall-clock time
v2 = spark.read.format("delta").option("versionAsOf", 2).load("s3://nimbus/orders")
prev = (spark.read.format("delta")
    .option("timestampAsOf", "2026-07-08T00:00:00")
    .load("s3://nimbus/orders"))        # yesterday's snapshot, reproduced exactly

# Housekeeping the log needs — the tax for keeping history
spark.sql("VACUUM delta.\`s3://nimbus/orders\` RETAIN 168 HOURS")  # reclaim files past 7-day retention`, caption: "format('delta') + versionAsOf / timestampAsOf: the same bucket, now with commits and history." },
    { type: "analogy", title: "Git for the warehouse ledger", html: `
<p>Picture NimbusMart's master inventory as a shared folder of spreadsheets on a network drive — that's the plain data lake. Two clerks open it, both save at once, and whoever saves last wins; a third clerk reading mid-save gets a mix of both edits and no way to know it. This is precisely how teams lose data, and it's why nobody sane keeps <em>source code</em> on a shared drive.</p>
<p>They keep it in <strong>Git</strong> — and the open table format brings Git's exact discipline to the warehouse ledger. The Parquet files are the working tree; the transaction log is the commit history. A <strong>commit</strong> is atomic: it lands whole or not at all, so no reader ever sees a half-write. Two writers who touch the table concurrently hit the same thing two developers hit — one commits first, the other gets a conflict and <em>rebases</em> their change onto the new head and re-commits, rather than clobbering. And <code>git checkout &lt;sha&gt;</code> becomes <code>VERSION AS OF 2</code>: the whole table, exactly as it stood at any past commit. Same model, same guarantees — applied to a 12 GB table on a bucket instead of a codebase.</p>` },
    { type: "javaBridge", html: `
<p>The whole lakehouse trick is one you already trust your career to: <strong>an append-only log is the source of truth, and current state is a projection of it.</strong></p>
<ul>
<li>It's the <strong>write-ahead log</strong> under every database you've used. Postgres doesn't mutate a table file in place and hope; it appends the change to the WAL and treats the WAL as authoritative. Delta's <code>_delta_log</code> is a WAL you can read, sitting in a bucket. &ldquo;Read the log to know the true state&rdquo; is the same sentence.</li>
<li>The concurrency model is <strong>optimistic locking</strong> — the JPA <code>@Version</code> column you've used to stop two transactions silently overwriting each other. A conflicting commit is rejected and retried against the new version, not blindly merged. No table-wide lock is ever held; writers race and the loser rebases.</li>
<li>Time travel is <strong>event sourcing's</strong> replay: because the log is the truth and files are immutable, any past state is reconstructable by replaying entries up to version N. You've built this by hand for an audit trail; the table format gives it to you as <code>versionAsOf</code>.</li>
</ul>` },
  ],
  lab: {
    tier: "T3",
    understand: {
      engine: "trace",
      trace: "b4-table-formats",
      task: `<p>Scrub a plain Parquet directory as it grows a transaction log and becomes a real table. What to watch for as you step:</p><ul><li><strong>The problem (t0–t3):</strong> two concurrent writers and a reader listing the directory mid-write — the torn read the cold open described. Convince yourself no single writer did anything wrong.</li><li><strong>The log arrives (t6–t12):</strong> data files become immutable; a <em>commit</em> is a single atomic append to the log. Note that until the commit lands, readers still see the previous version whole — never a partial one.</li><li><strong>Concurrency &amp; isolation (t15–t18):</strong> two writers targeting the same next version — optimistic concurrency lets one win and the other retry, with zero lost updates. Then watch the reader <em>pinned to v2</em> hold its stable snapshot while writers race the head to v5.</li><li><strong>Time travel &amp; the tax (t21–t27):</strong> reading <code>VERSION AS OF 2</code>, and the housekeeping (VACUUM / retention) that keeping history costs.</li></ul><p>Badge: <em>simulation</em> — file and version counts are illustrative; the commit/snapshot/isolation mechanics are exactly how Delta and Iceberg behave.</p>`
    },
    buildWithAI: `I'm learning open table formats (the lakehouse: Delta/Iceberg over Parquet) and I want to SEE ACID, snapshots, and time travel on my own machine. Assume a fresh machine, Python 3.10+, nothing else installed.

1. Create a project folder \`nimbusmart-lakehouse\` with a venv, and install pyspark (pin any recent 3.5.x), delta-spark (matching version), and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator that writes \`data/orders.csv\` — 240 rows: order_id (O-10001..O-10240), customer_id (C-0001..C-0060), seller_id, total_amount (8..950, 2dp), status, country. Also emit a second file \`data/orders_late.csv\` with 30 additional 'late-arriving' orders (O-10241..O-10270) to append later.

3. Create \`lakehouse_demo.py\` that (SparkSession configured for Delta via configure_spark_with_delta_pip, local[*]):
   - writes orders.csv as a Delta table at \`./delta/orders\` — call this version 0; print DESCRIBE HISTORY
   - performs an UPDATE (e.g. set status='review' where total_amount > 900) — version 1
   - APPENDS orders_late.csv — version 2
   - prints the row count at each of versions 0, 1, 2 using .option('versionAsOf', v) to prove TIME TRAVEL (v0 has 240, v2 has 270)
   - demonstrates ATOMICITY narratively in comments: explain that the append became visible only when its _delta_log entry committed
   - runs OPTIMIZE (if available) and then VACUUM with RETAIN 168 HOURS, printing what retention means

4. Create \`test_lakehouse.py\` (pytest) asserting — recompute expected counts from the CSVs, don't hardcode:
   - version 0 row count == rows in orders.csv (240)
   - version 2 row count == orders.csv + orders_late.csv (270)
   - reading versionAsOf 0 still returns 240 AFTER the append (history is immutable)
   - the number of committed versions in DESCRIBE HISTORY is exactly 3 (v0, v1, v2)
   - the _delta_log directory exists and contains one JSON commit file per version

5. Run generator, run lakehouse_demo.py, run pytest. Then add a comment explaining what a plain Parquet directory (no _delta_log) could NOT do among: atomic append, time travel, safe concurrent write. Windows-friendly paths please (use ./delta/orders relative paths).`
  },
  check: [
    {
      type: "mcq",
      q: "The cold-open dashboard read a wrong total while two jobs wrote the plain-Parquet orders table. What did the open table format add that would have prevented it?",
      options: [
        "Atomic commits via a transaction log — a write becomes visible only when its single log entry lands, so a reader sees version N or N+1 whole, never a torn mix of files",
        "It would compress the Parquet files so writes finish faster and don't overlap",
        "It would lock the entire table so no read can run during any write",
        "It moves the data into a warehouse, which is the only place ACID is possible"
      ],
      answer: 0,
      explain: "Plain Parquet has no notion of a commit: a reader lists the directory and catches whatever files happen to be there mid-write. The table format's log makes each write atomic — readers resolve the table from the log's latest complete version, never a half-written directory. No table-wide lock is needed; that's the point of snapshot isolation over a whole-table lock."
    },
    {
      type: "predict",
      q: "Using the Delta orders table from the trace: version 0 was written with 240 orders, then 30 late orders were appended as version 2. What does this time-travel read print?",
      code: `# after the append (table head is now version 2, 270 rows)
v0 = spark.read.format("delta").option("versionAsOf", 0).load(path)
print(v0.count())`,
      options: ["270", "240", "30", "an error — version 0 was overwritten"],
      answer: 1,
      explain: "Time travel reconstructs the table as it stood at that version. Version 0's snapshot is described by its log entry and its data files are immutable, so reading versionAsOf 0 returns the original 240 rows even though the current head (v2) has 270. Appending never rewrites old versions — it adds a new commit, so history stays intact."
    },
    {
      type: "mcq",
      q: "Two writers both read version 1 and try to commit the next version concurrently. How does the table format keep them from corrupting each other, and what happens?",
      options: [
        "Optimistic concurrency: one writer wins and commits version 2; the other's commit is rejected because the log moved, so it re-reads the new head, re-applies its change, and commits version 3 — no lost update, no lock held",
        "Both commits are blindly merged into one version, combining the two file sets",
        "The second writer silently overwrites the first, and the first's rows are lost",
        "A table-wide lock blocks the second writer until the first fully finishes and releases it"
      ],
      answer: 0,
      explain: "Table formats use optimistic concurrency (the same idea as a JPA @Version column). The loser of the commit race doesn't clobber or block — it detects that the log advanced, rebases its change onto the new version, and retries. Two writers, zero lost updates, and no long-held table lock. Blindly merging file sets would be exactly the corruption the log exists to prevent."
    },
    {
      type: "mcq",
      q: "A colleague says 'a lakehouse is just a data lake, so keeping unlimited version history is free.' What's the correction?",
      options: [
        "Time travel keeps superseded data files alive until you expire them, so history costs storage — you set a retention window and run VACUUM / snapshot expiration, the same housekeeping a Git repo needs",
        "Correct — snapshots are pointers only and never consume storage",
        "History is free but only readable from a warehouse, not the lake",
        "There is no history in a lakehouse; time travel re-derives old data on the fly"
      ],
      answer: 0,
      explain: "Every commit that removes or replaces files keeps the old files around so past versions remain readable — that's real storage. Retention policies plus VACUUM (Delta) or snapshot expiration (Iceberg) reclaim files older than your window. It's the lakehouse's version of `git gc`: history is cheap to keep and not free to keep forever."
    }
  ],
  fieldNotes: `A data platform team migrated a 40 TB event table from plain Parquet-on-S3 to Delta mostly for time travel, and got a bill they didn't budget for: three months in, storage had grown 2.3× even though the logical table was flat. The cause was innocent — a compaction job ran hourly, and every compaction rewrites files, and every rewrite leaves the superseded files behind for time travel. With the default retention nobody had touched, they were keeping the full file history of 24 rewrites a day. The fix was two lines: set a sane retention window for their actual audit need (they needed 7 days of history, not unbounded) and schedule VACUUM to reclaim anything older. Storage dropped back to 41 TB overnight. The lesson the senior engineer wrote in the runbook: a table format gives you Git-like history on object storage, and like a Git repo it will happily hoard every version you never expire — decide your retention window <em>on purpose</em>, because the default is 'forever,' and forever has a monthly invoice.`
};
