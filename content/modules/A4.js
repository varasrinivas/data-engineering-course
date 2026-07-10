// A4 — Partitioning, Compression & the Small-Files Problem (Track A, story-sim T3)
// Reuses the existing flagship trace engine/traces/a4-small-files.json (do NOT recreate).
// Verified arithmetic used in checks: 168 MB / 12,000 files ≈ 14 KB avg;
// healthy Parquet target 96–128 MB per file.
export default {
  id: "A4",
  track: "A",
  title: "Partitioning, Compression & the Small-Files Problem",
  minutes: 22,
  coldOpen: "The NimbusMart courier-pings stream commits a micro-batch every 30 seconds, and each commit writes one tiny Parquet file. Nothing ever fails: every write succeeds, checkpoints advance, dashboards stay green. But by midnight a single day's partition holds 12,000 files averaging 14 KB — 168 MB of data stored as twelve thousand objects, when two files would have held it. The next morning's batch job, reading that partition, spends 38 seconds listing files and 4 seconds reading data. The directory quietly filled with confetti, and confetti is expensive to sweep.",
  concept: [
    { type: "prose", html: `
<p>A partitioned table is just a <strong>directory layout used as a coarse index</strong>. When you write NimbusMart orders partitioned by date and country, Spark physically arranges the files like this:</p>
<pre style="font-family:var(--mono);font-size:11px;line-height:1.5;color:var(--ink2);margin:0 0 4px 0;">orders/
  ds=2026-05-14/country=DE/part-0000.parquet
  ds=2026-05-14/country=US/part-0000.parquet
  ds=2026-05-15/country=DE/part-0000.parquet   ...</pre>
<p>The magic is what a query does with it. <code>WHERE ds = '2026-05-15' AND country = 'DE'</code> doesn't scan the table and filter — the planner reads the <em>directory paths</em>, prunes every folder that can't match, and opens only <code>ds=2026-05-15/country=DE/</code>. That's <strong>partition pruning</strong>: skipping data by never opening the files, one level coarser than the row-group predicate pushdown from A3. Directory layout <em>is</em> the index; the folder names <em>are</em> the keys.</p>
<p>Which makes the choice of partition column the whole game. Partition by something <strong>low-cardinality and frequently filtered</strong> — date, country, status — and each partition is a healthy chunk of data queries actually target. Partition by something <strong>high-cardinality</strong> — <code>order_id</code>, <code>customer_id</code>, a timestamp to the second — and you get the disaster this module is named after.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 250" font-family="var(--mono)" font-size="11">
<text x="16" y="20" fill="var(--ink2)" font-size="11">GOOD: partition by low-cardinality key → pruning skips whole folders unopened</text>
<g>
<rect x="16" y="30" width="150" height="30" rx="4" fill="var(--paper2)" stroke="var(--line)" stroke-dasharray="4 3"/><text x="91" y="49" text-anchor="middle" fill="var(--ink2)">ds=05-14 (skipped)</text>
<rect x="176" y="30" width="150" height="30" rx="4" fill="var(--paper2)" stroke="var(--green)"/><text x="251" y="49" text-anchor="middle" fill="var(--green)">ds=05-15 · country=DE</text>
<rect x="336" y="30" width="150" height="30" rx="4" fill="var(--paper2)" stroke="var(--line)" stroke-dasharray="4 3"/><text x="411" y="49" text-anchor="middle" fill="var(--ink2)">ds=05-16 (skipped)</text>
<text x="500" y="49" fill="var(--green)" font-size="10">WHERE ds='05-15' AND country='DE'</text>
<text x="16" y="78" fill="var(--green)" font-size="10">→ open 1 folder, one ~110 MB file. Read what you need, ignore the rest.</text>
</g>
<line x1="16" y1="92" x2="704" y2="92" stroke="var(--line)"/>
<text x="16" y="114" fill="var(--ink2)" font-size="11">BAD: partition by high-cardinality key (or commit every 30s) → the small-files tax</text>
<g>
<rect x="16" y="124" width="672" height="70" rx="6" fill="var(--paper2)" stroke="var(--rust)"/>
<text x="352" y="146" text-anchor="middle" fill="var(--ink)" font-size="10">12,000 × 14 KB files in one partition — the confetti pile</text>
<g fill="none" stroke="var(--rust)">
<rect x="30" y="156" width="10" height="10"/><rect x="44" y="156" width="10" height="10"/><rect x="58" y="156" width="10" height="10"/><rect x="72" y="156" width="10" height="10"/><rect x="86" y="156" width="10" height="10"/><rect x="100" y="156" width="10" height="10"/><rect x="114" y="156" width="10" height="10"/><rect x="128" y="156" width="10" height="10"/><rect x="142" y="156" width="10" height="10"/><rect x="156" y="156" width="10" height="10"/><rect x="170" y="156" width="10" height="10"/><rect x="184" y="156" width="10" height="10"/><rect x="198" y="156" width="10" height="10"/><rect x="212" y="156" width="10" height="10"/><rect x="226" y="156" width="10" height="10"/><rect x="240" y="156" width="10" height="10"/><rect x="254" y="156" width="10" height="10"/><rect x="268" y="156" width="10" height="10"/><rect x="282" y="156" width="10" height="10"/><rect x="296" y="156" width="10" height="10"/><rect x="310" y="156" width="10" height="10"/><rect x="324" y="156" width="10" height="10"/><rect x="338" y="156" width="10" height="10"/><rect x="352" y="156" width="10" height="10"/><rect x="366" y="156" width="10" height="10"/><rect x="380" y="156" width="10" height="10"/><rect x="394" y="156" width="10" height="10"/><rect x="408" y="156" width="10" height="10"/><rect x="422" y="156" width="10" height="10"/><rect x="436" y="156" width="10" height="10"/><rect x="450" y="156" width="10" height="10"/><rect x="464" y="156" width="10" height="10"/><rect x="478" y="156" width="10" height="10"/><rect x="492" y="156" width="10" height="10"/><rect x="506" y="156" width="10" height="10"/><rect x="520" y="156" width="10" height="10"/><rect x="534" y="156" width="10" height="10"/><rect x="548" y="156" width="10" height="10"/><rect x="562" y="156" width="10" height="10"/><rect x="576" y="156" width="10" height="10"/><rect x="590" y="156" width="10" height="10"/><rect x="604" y="156" width="10" height="10"/><rect x="618" y="156" width="10" height="10"/><rect x="632" y="156" width="10" height="10"/><rect x="646" y="156" width="10" height="10"/><rect x="660" y="156" width="10" height="10"/></g>
<text x="352" y="185" text-anchor="middle" fill="var(--rust)" font-size="10">read cost: 12,000 file opens + 12,000 tasks for 168 MB — 38s listing, 4s reading</text>
</g>
<text x="16" y="216" fill="var(--ink2)" font-size="10">healthy target: 96–128 MB per file (one HDFS block). 14 KB is ~0.01% of that — all overhead, no payload.</text>
<text x="16" y="234" fill="var(--accent)" font-size="10">fix: compact closed partitions to 96 MB targets · widen the streaming trigger · never partition on a high-cardinality key</text>
</svg>`, caption: "Partitioning is a directory-level index: good keys let queries skip whole folders; bad keys (or too-frequent commits) shatter a partition into thousands of tiny files that cost far more to open than to read." },
    { type: "prose", html: `
<p><strong>Compression</strong> rides alongside partitioning and has one trap worth knowing: <em>splittability</em>. Spark parallelizes by handing each executor core a slice of a file. <strong>Snappy</strong> (the Parquet default) and LZ4 are splittable — a 1 GB Snappy-Parquet file can be read by many tasks at once. Whole-file <strong>gzip</strong> is <em>not</em> splittable: one gzip file = one task, no matter how big, so a 10 GB gzipped CSV is read single-threaded while your 200-core cluster watches. Prefer Snappy for lake storage; reach for gzip/zstd only for cold archives you won't scan in parallel.</p>
<p>Now the headline failure — the <strong>small-files problem</strong>. It's counterintuitive because the <em>total</em> data is small; the cost isn't bytes, it's <em>bookkeeping per file</em>:</p>
<ul>
<li><strong>Metadata storms.</strong> The object store LISTs keys ~1,000 per call and issues one footer GET per Parquet file. 12,000 files = 12 LIST round-trips + 12,000 metadata reads <em>before a single row is decoded</em>.</li>
<li><strong>One task per file.</strong> Spark schedules roughly one task per file. 12,000 files → 12,000 tasks for 168 MB. Median task: ~4 ms of real work wrapped in ~120 ms of scheduling, launch, and commit overhead. The cluster is busy being idle.</li>
<li><strong>Driver / metastore pressure.</strong> The driver tracks every task; the metastore tracks every partition and file. Push into the millions and the driver OOMs or the metastore query times out — the job dies at planning, before reading anything.</li>
</ul>
<p>The healthy target is <strong>96–128 MB per file</strong> (roughly one HDFS block). You cause small files two ways: <em>writing too often</em> (a streaming sink committing one file every 30 seconds) and <em>partitioning too finely</em> (a high-cardinality key splitting each partition into slivers). The fix is <strong>compaction</strong> — periodically rewrite closed partitions to the target size (<code>OPTIMIZE</code> in Delta, or <code>coalesce</code>/<code>repartition</code> and rewrite) — plus not creating the problem at the source: widen the trigger, choose a coarse partition key.</p>` },
    { type: "code", lang: "python", code: `# GOOD partition key: low-cardinality, frequently filtered → healthy partitions + pruning
(orders.write
    .partitionBy("ds", "country")               # date + country, not order_id
    .mode("overwrite")
    .parquet("s3://nimbus/lake/orders"))

# Partition pruning: the planner opens only ds=2026-05-15/country=DE/, never lists the rest
de_day = (spark.read.parquet("s3://nimbus/lake/orders")
    .filter((F.col("ds") == "2026-05-15") & (F.col("country") == "DE")))

# The FIX for a partition already shattered into tiny files: compact to ~96 MB targets.
# Read the closed partition, coalesce to few big files, rewrite it.
target_files = 2   # 168 MB / ~96 MB ≈ 2 files, not 12,000
(spark.read.parquet("s3://nimbus/lake/orders/ds=2026-07-08")
    .coalesce(target_files)
    .write.mode("overwrite")
    .parquet("s3://nimbus/lake/orders/ds=2026-07-08"))
# In Delta this is one line: OPTIMIZE nimbus.orders WHERE ds = '2026-07-08'`, caption: "Partition by a coarse key (date/country), let the planner prune whole folders, and compact closed partitions back to 96–128 MB — the three moves that keep a lake fast." },
    { type: "analogy", title: "Aisle-and-bin labels, and the day the shelves filled with confetti", html: `
<p>Partitioning is the NimbusMart warehouse's <strong>aisle-and-bin labelling scheme</strong>. Stock isn't dumped in one heap — it's filed by a coarse, sensible key: <em>aisle by department, bin by date received</em>. When an order calls for "electronics received on the 15th," a picker walks straight to that aisle-and-bin and ignores the other forty aisles. They never search the warehouse; the label <em>is</em> the index. That's partition pruning, and it's why you label by department (a handful of aisles), not by individual SKU (which would demand one aisle per product and a map to find anything).</p>
<p>The small-files problem is the day someone reconfigures the labeller to open a <strong>brand-new bin for every single item that arrives</strong> — one glove here, one glove there, ten thousand single-item bins by nightfall. The total stock is trivial, a few boxes' worth. But now a stock-take means walking to twelve thousand bins, opening each, reading a label, closing it — hours of opening and closing to count what fits in two crates. The warehouse didn't run out of space; it drowned in <em>bin-opening overhead</em>. Compaction is the night crew consolidating those ten thousand single-item bins back into two well-packed crates before the morning stock-take arrives.</p>` },
    { type: "javaBridge", html: `
<p>Two things you already have scar tissue for map cleanly onto this module.</p>
<ul>
<li><strong>Partition pruning ≈ hitting an index instead of a full table scan.</strong> <code>WHERE ds = '2026-05-15'</code> on a partitioned table is the data-lake version of a <code>WHERE</code> on an indexed column: the engine uses structure to avoid reading rows it can prove don't match. Choosing a high-cardinality partition key is like indexing a column with a distinct value per row — technically an index, practically useless, and expensive to maintain.</li>
<li><strong>The small-files problem is the N+1 query problem, at the filesystem.</strong> You know the pain of a loop that fires one query per row instead of one query for the batch — the per-call overhead dwarfs the work. Twelve thousand tiny files is exactly that: one <code>open()</code> + footer read + scheduled task <em>per file</em>, where the fixed cost per file swamps the 14 KB of actual payload. And one-task-per-file is the same trap as spinning up a thread per trivial unit of work: your scheduler spends all its time dispatching, none of it computing. Compaction is the "batch the calls" fix you'd reach for instinctively in application code.</li>
</ul>` },
  ],
  lab: {
    tier: "T3",
    understand: {
      engine: "trace",
      trace: "a4-small-files",
      task: `<p><strong>Watch a healthy stream quietly manufacture a performance disaster, then watch one compaction job undo it.</strong> Scrub the timeline and hold these in view:</p>
<ul>
<li><em>The accrual</em> (t=0 → t=9): one ~14 KB file every 30 seconds, all evening, until a single day's partition holds <strong>12,000 files for 168 MB</strong> — data that fits in two properly-sized files. Note that <em>nothing fails</em> the whole time; that's what makes it dangerous.</li>
<li><em>The bill</em> (t=12 → t=18): the morning read spends 38 s listing files and 4 s reading bytes, then schedules 12,000 tasks — median 4 ms of work under 120 ms of overhead. Watch the executor bars: none is doing meaningful work.</li>
<li><em>The fix</em> (t=21 → t=27): compaction rewrites the partition to a 96 MB target — 12,000 files → 12 — and the read drops from 42 s to 6 s. Then the senior move: stop <em>creating</em> the problem (widen the trigger, auto-compact on partition close).</li>
</ul>
<p>The lesson to carry: small files are a tax collected on <em>every</em> read, forever, until someone compacts — so you either pay it nightly or cancel it at the source.</p>`
    },
    buildWithAI: `I'm learning partitioning and the small-files problem for data engineering. Build me a real local PySpark project that reproduces the pain and then fixes it, on my own machine. Assume nothing beyond Python 3.10+; I'm on Windows (use pathlib, no bash-isms).

1. Create a folder \`nimbusmart-small-files\` with a venv, and install pyspark (pin a recent 3.5.x) and pytest.

2. Create \`generate_pings.py\` — deterministic (random.seed(42)) — that writes \`data/courier_pings.csv\` modeling NimbusMart courier pings: 500,000 rows. Columns: ping_id, courier_id (K-01..K-12), order_id (O-100001..), status (picked_up/in_transit/delivered), zone (north/south/east/west/central), event_ts (ISO across one day 2026-07-08), ingested_at (event_ts plus a random 1–130 minute lag so some are >1h late). Print the row count.

3. Create \`make_small_files.py\` that reproduces the anti-pattern: read the CSV, then write it to \`lake/bad/\` partitioned by event_ts truncated to the MINUTE (a deliberately high-cardinality key) so you get hundreds+ of tiny files. After writing, walk the output directory and print: total file count, total bytes, and average file size in KB. It should be alarmingly small.

4. Create \`compact.py\` that fixes it: read \`lake/bad/\`, repartition/coalesce so target file size is ~96–128 MB (for this dataset that's just a handful of files), and write to \`lake/good/\` partitioned by a COARSE key (event_ts truncated to the DAY). Print the before/after file counts and average sizes side by side.

5. Create \`bench.py\` that times a full .count() (or a groupBy zone .count()) reading \`lake/bad/\` vs reading \`lake/good/\`, and prints both wall-clock times plus the file counts each read had to list. The good layout should be clearly faster despite identical data.

6. Create \`test_small_files.py\` (pytest) asserting:
   - bad and good layouts contain the SAME total row count (compaction is lossless).
   - the good layout has at least 20x FEWER files than the bad layout.
   - the good layout's average file size is at least 10x larger than the bad layout's.
   - a query filtering one day on the good (day-partitioned) layout reads fewer partitions than the same filter on a naive single-folder layout (demonstrate partition pruning).

7. Run generator, make_small_files, compact, bench, then pytest. Explain in comments WHY 168 MB spread across thousands of tiny files reads slower than the same 168 MB in a few big ones — name the metadata-per-file and one-task-per-file costs.`
  },
  check: [
    {
      type: "mcq",
      q: "You need queries filtered on delivery date to be fast on a large orders table. Which partition key is the right choice, and why?",
      options: [
        "Partition by <code>ds</code> (date) — a low-cardinality, frequently-filtered key so pruning opens only the matching day's folder and each partition stays a healthy size",
        "Partition by <code>order_id</code> — the most unique key gives the finest-grained index and therefore the fastest lookups",
        "Partition by <code>total_amount</code> — numeric columns partition more efficiently than strings",
        "Don't partition at all — partitioning always slows writes more than it helps reads"
      ],
      answer: 0,
      explain: "Date is low-cardinality and matches the query's filter, so partition pruning opens one folder and each partition holds a healthy chunk of data. Partitioning by order_id (the misconception that 'more unique = better index') creates one tiny partition per order — millions of slivers — which is the small-files disaster, not a fast index."
    },
    {
      type: "predict",
      q: "A single day's partition holds 168 MB of courier pings, written as 12,000 files by a streaming sink. What is the average file size?",
      code: `total_mb = 168
files = 12000
avg_kb = total_mb * 1024 / files
print(round(avg_kb))   # KB per file`,
      options: ["14", "168", "96", "1024"],
      answer: 0,
      explain: "168 MB × 1024 KB/MB ÷ 12,000 files ≈ 14 KB per file — roughly 0.01% of the healthy 96–128 MB target. That tiny average is the whole problem: each file still costs a full metadata read and a full scheduled task, so 168 MB of data carries 12,000 files' worth of overhead."
    },
    {
      type: "mcq",
      q: "The partition holds only 168 MB total, yet the read takes 42 seconds — 38 s of it before any row is decoded. What is actually slow?",
      options: [
        "The 168 MB is too large to fit in executor memory, forcing spill to disk",
        "Per-file bookkeeping: listing 12,000 keys and reading 12,000 Parquet footers, then scheduling ~12,000 tasks (each ~4 ms of work under ~120 ms of overhead) — the cost scales with file count, not data size",
        "Snappy decompression of 12,000 files is CPU-bound and saturates the cores",
        "The cluster is too small; doubling the executors would make it read in ~21 s"
      ],
      answer: 1,
      explain: "The bottleneck is file-count overhead, not bytes: thousands of LIST/footer metadata reads plus one scheduled task per file, where fixed per-task cost dwarfs 14 KB of real work. 168 MB is trivial to hold in memory, and adding executors doesn't help when the driver is drowning in task bookkeeping — compaction (fewer, bigger files) is the fix."
    },
    {
      type: "mcq",
      q: "A streaming sink commits one file every 30 seconds and the partition is drowning in small files. Which pair of fixes actually addresses it?",
      options: [
        "Increase the number of shuffle partitions and cache the table in memory",
        "Widen the streaming trigger (e.g. 30 s → 5 min) to write ~10× fewer files, and schedule compaction (OPTIMIZE / coalesce-and-rewrite) to ~96–128 MB once each partition closes",
        "Switch the files from Snappy to gzip so each file compresses smaller",
        "Partition more finely (by minute) so each folder holds fewer rows per file"
      ],
      answer: 1,
      explain: "You fix small files at both ends: write less often (a wider trigger produces fewer, larger files) and compact closed partitions to the 96–128 MB target. Smaller-per-file compression doesn't reduce the file count, and partitioning by minute makes it dramatically worse — more folders, more slivers, more per-file overhead."
    }
  ],
  fieldNotes: `A streaming team ran a Structured Streaming job writing courier-ping events to a Delta table with a 20-second trigger, partitioned by event-hour. It hummed for months. Then the downstream nightly reconciliation job — which read the last 14 days — started missing its 06:00 SLA, creeping from 8 minutes to over 90. Nobody had touched its code. The cause: 14 days × ~24 hours × ~180 files/hour had accumulated into roughly 600,000 tiny Parquet files, and the job now spent almost all its time listing S3 keys and opening footers, not reading data. The first 'fix' — throwing a bigger cluster at it — did nothing, because the bottleneck was the driver planning 600,000 tasks, not executor throughput. The real fix was two changes: a scheduled OPTIMIZE compacting closed hours to 128 MB files, and widening the trigger to 2 minutes. Read time fell back under 10 minutes. The line the team wrote in the postmortem: 'the cluster was never the problem; the file count was, and the file count is a write-side decision we'd been ignoring for months.'`
};
