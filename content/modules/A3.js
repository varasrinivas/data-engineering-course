// A3 — Files That Scale (Track A, T2 SparkSim)
// Verified facts used by lab + checks (from data/nimbusmart/generate.py, seed 42):
//   orders = 240 rows, 9 columns (order_id, customer_id, seller_id, order_ts,
//   status, total_amount, item_count, country, channel)
//   Lab: select 3 of 9 columns → projection pruning; scan reads only the 3.
export default {
  id: "A3",
  track: "A",
  title: "Files That Scale",
  minutes: 22,
  coldOpen: "NimbusMart's orders export lands nightly as one 40 GB gzipped CSV. An analyst's query needs three columns — order_id, total_amount, country — and takes nine minutes. A platform engineer rewrites the same export as Parquet, changes nothing else, and the identical query returns in eleven seconds. No bigger cluster, no cache, no tuning. The only thing that changed was the shape of the bytes on disk — and that shape is a decision you make on the way in, not a knob you turn later.",
  concept: [
    { type: "prose", html: `
<p>Every file format is a bet about how the data will be read. Pick the wrong one and no amount of cluster does more than pay interest on that bet. There's a rough ladder data engineers climb:</p>
<ul>
<li><strong>CSV</strong> — text, row-per-line, no types, no schema, no metadata. Human-readable and universally accepted, which is exactly why it's everywhere and exactly why it doesn't scale: to read one column you must parse every character of every row, and "<code>42</code>" is a string until something guesses otherwise.</li>
<li><strong>JSON</strong> — text, self-describing, nested. Solves CSV's "what is this field" problem and handles nesting (NimbusMart's <code>products.category.aisle</code>), but pays for it: the schema is repeated in full on <em>every single record</em>, and it's still row-oriented text you must scan end to end.</li>
<li><strong>Parquet / ORC</strong> — binary, <strong>columnar</strong>, typed, with embedded statistics. Built for analytics: store each column together, write the schema once, keep min/max/count per chunk so the reader can skip whole blocks. This is where scans get fast.</li>
<li><strong>Avro</strong> — binary, typed, but <strong>row-oriented</strong>. The odd one out: it's the write-and-stream format. Compact, superb schema evolution, ideal for a Kafka topic or a landing zone where you append whole records — but a poor fit for "read 3 columns out of 30."</li>
</ul>
<p>The single axis that explains most of this table is <strong>row vs columnar</strong> — how the bytes are grouped on disk. It's the same distinction from A1 (OLTP row store vs OLAP columnar), now as a file format you choose.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 250" font-family="var(--mono)" font-size="11">
<text x="16" y="20" fill="var(--ink2)" font-size="11">ROW LAYOUT (CSV / JSON / Avro): records stored whole, one after another</text>
<g>
<rect x="16" y="30" width="150" height="24" rx="3" fill="var(--paper2)" stroke="var(--line)"/><text x="24" y="46" fill="var(--ink)">o1: id·amt·country·+6</text>
<rect x="172" y="30" width="150" height="24" rx="3" fill="var(--paper2)" stroke="var(--line)"/><text x="180" y="46" fill="var(--ink)">o2: id·amt·country·+6</text>
<rect x="328" y="30" width="150" height="24" rx="3" fill="var(--paper2)" stroke="var(--line)"/><text x="336" y="46" fill="var(--ink)">o3: id·amt·country·+6</text>
<text x="16" y="74" fill="var(--rust)" font-size="10">need "amount"? → read every byte of every row, parse, discard 8 of 9 fields</text>
</g>
<line x1="16" y1="90" x2="704" y2="90" stroke="var(--line)"/>
<text x="16" y="112" fill="var(--ink2)" font-size="11">COLUMNAR LAYOUT (Parquet / ORC): each column stored contiguously, with stats</text>
<g>
<rect x="16" y="122" width="120" height="30" rx="3" fill="var(--paper2)" stroke="var(--line)"/><text x="76" y="141" text-anchor="middle" fill="var(--ink2)">order_id ▓▓▓</text>
<rect x="142" y="122" width="120" height="30" rx="3" fill="var(--paper2)" stroke="var(--accent)"/><text x="202" y="141" text-anchor="middle" fill="var(--accent)">total_amount ▓</text>
<rect x="268" y="122" width="120" height="30" rx="3" fill="var(--paper2)" stroke="var(--line)"/><text x="328" y="141" text-anchor="middle" fill="var(--ink2)">country ▓▓</text>
<rect x="394" y="122" width="300" height="30" rx="3" fill="none" stroke="var(--line)" stroke-dasharray="4 3"/><text x="544" y="141" text-anchor="middle" fill="var(--ink2)" font-size="10">status · seller · ts · item_count · channel · … (never read)</text>
<text x="16" y="172" fill="var(--green)" font-size="10">need "amount"? → seek to that one column's chunk, read it, done</text>
</g>
<rect x="16" y="188" width="335" height="48" rx="8" fill="var(--paper2)" stroke="var(--accent)"/>
<text x="183" y="208" text-anchor="middle" fill="var(--accent)" font-weight="bold">PROJECTION PRUNING</text>
<text x="183" y="226" text-anchor="middle" fill="var(--ink2)" font-size="10">read only the columns the query selects</text>
<rect x="369" y="188" width="335" height="48" rx="8" fill="var(--paper2)" stroke="var(--green)"/>
<text x="536" y="208" text-anchor="middle" fill="var(--green)" font-weight="bold">PREDICATE PUSHDOWN</text>
<text x="536" y="226" text-anchor="middle" fill="var(--ink2)" font-size="10">use per-chunk min/max stats to skip blocks that can't match</text>
</svg>`, caption: "Row layout stores whole records together; columnar stores each column together. Columnar unlocks two skips a row format cannot: read fewer columns (projection pruning) and skip blocks whose stats can't match the filter (predicate pushdown)." },
    { type: "prose", html: `
<p>Columnar isn't just "the fast one" — it wins for four compounding, specific reasons, and it's worth being able to name them:</p>
<ul>
<li><strong>Projection pruning.</strong> Select 3 of 30 columns and the reader physically fetches only those 3 column chunks. A row format has no choice but to read whole rows and throw 27/30 away. This is the lever you'll pull in the lab.</li>
<li><strong>Better compression.</strong> A column holds one type with low cardinality — <code>country</code> is 7 distinct values across millions of rows. Store them together and dictionary + run-length encoding crushes them in a way you can never achieve when <code>country</code> is interleaved with <code>total_amount</code> and a timestamp.</li>
<li><strong>Predicate pushdown via statistics.</strong> Parquet/ORC keep min/max/null-count per row-group. A filter like <code>order_ts &gt;= '2026-06-01'</code> lets the reader skip entire row-groups whose max timestamp is earlier — without decoding a single row inside them.</li>
<li><strong>Typed, schema-once.</strong> The schema lives in the footer, once. No per-row type guessing, no <code>inferSchema</code> lottery, no "was that column an int or a string this week."</li>
</ul>
<p>So when do you <em>not</em> reach for Parquet? When you're writing, not reading. <strong>Avro</strong> earns its place at the landing edge — streaming ingest, a Bronze append zone, anywhere records arrive whole and schema evolves often — because row formats are cheap to append and Avro's schema-evolution story is excellent. The rule of thumb: <em>Avro to land it, Parquet/ORC to analyze it.</em> Parquet and ORC are near-twins (both columnar, both with stats); Parquet is the lakehouse default, ORC the historically Hive-native one.</p>` },
    { type: "code", lang: "python", code: `from pyspark.sql import types as T

# Explicit schema — no inferSchema guessing games (A3's whole point is knowing your bytes)
orders_schema = T.StructType([
    T.StructField("order_id",     T.StringType()),
    T.StructField("customer_id",  T.StringType()),
    T.StructField("seller_id",    T.StringType()),
    T.StructField("order_ts",     T.TimestampType()),
    T.StructField("status",       T.StringType()),
    T.StructField("total_amount", T.DoubleType()),
    T.StructField("item_count",   T.IntegerType()),
    T.StructField("country",      T.StringType()),
    T.StructField("channel",      T.StringType()),
])

# Land it once as columnar Parquet...
(spark.read.schema(orders_schema).csv("s3://nimbus/raw/orders.csv", header=True)
    .write.mode("overwrite").parquet("s3://nimbus/lake/orders_parquet"))

# ...then this analytics query touches only 3 of the 9 columns on disk.
summary = (spark.read.parquet("s3://nimbus/lake/orders_parquet")
    .select("order_id", "total_amount", "country"))   # projection pruning happens HERE
summary.explain()   # physical plan → ReadSchema shows 3 columns, not 9`, caption: "Same rows, two on-disk shapes. The .select() is what triggers projection pruning: the Parquet reader fetches 3 column chunks and never touches the other 6." },
    { type: "analogy", title: "The handwritten packing slip vs the aisle-indexed manifest", html: `
<p>Two ways NimbusMart's warehouse can record what's on a pallet. The first is a <strong>handwritten packing slip</strong>, one line per item, top to bottom: <em>SKU, qty, weight, bin, supplier, cost…</em> all on the same line. To answer "what's the total weight on this pallet?" a worker reads every full line and mentally ignores five fields out of six. That's CSV — a row format. Fine for one pallet, ruinous across ten thousand.</p>
<p>The second is an <strong>aisle-indexed manifest</strong>: one page lists <em>all</em> the SKUs, the next page <em>all</em> the weights, the next <em>all</em> the bins — each field filed together, with a summary box at the top of each page ("weights range 0.2–9.5 kg"). Now "total weight?" means turning to one page and adding a single column. And "any item over 9 kg?" — you read the summary box; if the max is 8.4, you skip the whole page unread. That's Parquet: columns filed together (projection pruning) with per-page stats (predicate pushdown).</p>
<p>Avro is the third artifact: the <strong>arrival log at the dock</strong>, where you scribble each whole pallet as it rolls in, fast, one complete record at a time. Perfect for <em>recording</em> arrivals; terrible for <em>analyzing</em> them. You keep the arrival log to capture, and you build the aisle-indexed manifest to answer questions. Different jobs, different formats.</p>` },
    { type: "javaBridge", html: `
<p>You already know the row-oriented serialization cost intimately — it's called <strong>Jackson</strong>. When you <code>objectMapper.writeValueAsString(order)</code>, every record carries its own field names as text: <code>{"order_id":"O-10188","total_amount":42.5,...}</code>, then the next record repeats <code>"order_id"</code>, <code>"total_amount"</code> all over again. To read one field back you parse the whole object graph. That's JSON — self-describing, flexible, and expensive exactly the way NimbusMart's clickstream is expensive: schema-per-record, row-at-a-time, text.</p>
<ul>
<li><strong>Parquet is the anti-Jackson.</strong> The schema is written <em>once</em> in the file footer, not on every record. Values are stored as typed binary grouped by column, not as re-labeled text grouped by object. Deserializing "give me the <code>total_amount</code> column" is a typed columnar read, not 10 million JSON parses.</li>
<li><strong>Avro is Jackson done right for streaming.</strong> Still row-oriented (whole records), but binary with the schema referenced by id, not inlined per record — the compact, schema-evolving wire format you'd actually want behind Kafka. Think of it as the efficient POJO-serialization you wished <code>ObjectMapper</code> produced.</li>
<li><strong>The mental upgrade:</strong> in backend code you serialize one object per request, so Jackson's per-record overhead is invisible. In data engineering you serialize billions, and that overhead <em>is</em> the bill. Columnar encoding is what happens when serialization cost becomes the dominant cost.</li>
</ul>` },
  ],
  lab: {
    tier: "T2",
    understand: {
      engine: "sparksim",
      datasets: ["orders"],
      task: `<p><strong>Watch projection pruning happen.</strong> The starter reads <code>orders</code> (stored columnar) and selects all 9 columns, so the scan reads all 9. But this report only needs three: <code>order_id</code>, <code>total_amount</code>, <code>country</code>.</p>
<p>Narrow the <code>.select(...)</code> to just those three columns, then Run. Watch the plan view: the scan's <strong>ReadSchema</strong> should drop from 9 columns to 3, and a <em>pruned-columns</em> note should appear listing the 6 columns the reader now skips entirely. That gap — 9 columns read vs 3 — is the same win that turned the cold open's 9-minute CSV query into 11 seconds on Parquet.</p>`,
      starterCode: `orders = spark.read.table("orders")

report = orders.select(
    "order_id", "customer_id", "seller_id", "order_ts",
    "status", "total_amount", "item_count", "country", "channel")

report.show()`,
      solutionCode: `orders = spark.read.table("orders")

report = orders.select("order_id", "total_amount", "country")

report.show()`,
      expect: { rows: 240, cols: ["order_id", "total_amount", "country"] },
      dagNotes: `<p>The scan node's <code>ReadSchema</code> now lists 3 columns, not 9 — the Parquet reader seeks to just those three column chunks and never decodes <code>seller_id</code>, <code>order_ts</code>, <code>status</code>, <code>item_count</code>, <code>channel</code>, or <code>customer_id</code>. On the seed data that's 3/9 of the columns; on a real 30-column orders table selecting 3, it's an order-of-magnitude less I/O for the identical answer. A row format (CSV/JSON) can't do this — it must read whole rows and discard the columns you didn't ask for <em>after</em> paying to read them.</p>`
    },
    buildWithAI: `I'm learning file formats for data engineering (row vs columnar, projection pruning). Build me a real local PySpark project that lets me measure the difference on my own machine. Assume nothing beyond Python 3.10+; I'm on Windows (use pathlib, no bash-isms).

1. Create a folder \`nimbusmart-formats\` with a venv, and install pyspark (pin a recent 3.5.x) and pytest.

2. Create \`generate_orders.py\` — deterministic (random.seed(42)) — that writes \`data/orders.csv\` matching NimbusMart's orders: 100,000 rows so timings are visible. Columns: order_id (O-100001..), customer_id (C-0001..C-0060), seller_id (make 'S-777' ~35% of rows), order_ts (ISO), status, total_amount (8..950, 2dp), item_count (1..6), country (DE,US,IN,BR,JP,FR,AU), channel (web/app). Print the row count.

3. Create \`convert.py\` that reads orders.csv with an EXPLICIT StructType (no inferSchema — I want to see the types I chose), then writes the SAME data three ways under \`out/\`: as CSV, as JSON, and as Parquet. Print the on-disk byte size of each output folder so I can see Parquet is dramatically smaller (columnar + compression).

4. Create \`prune.py\` that:
   - reads the Parquet version and runs report = df.select("order_id", "total_amount", "country"); calls report.explain(True) and prints the physical plan.
   - reads the CSV version and runs the identical select + explain.
   - times a .count() on each after the select.
   - highlights the ReadSchema line in the Parquet plan (should list exactly the 3 selected columns) versus the CSV plan (must read all 9). Print a one-line summary: "Parquet scan read 3/9 columns; CSV read 9/9."

5. Create \`test_formats.py\` (pytest) asserting:
   - all three formats round-trip to the SAME row count (100,000) and the SAME sum(total_amount) within a cent — proving the conversion is lossless.
   - the Parquet output folder is at least 3x smaller on disk than the CSV output folder.
   - the Parquet physical plan string for the 3-column select contains only order_id, total_amount, country in its ReadSchema (projection pruning happened); assert the CSV plan does not prune.

6. Run generator, convert, prune, then pytest. Then explain in comments WHY the Parquet select touched 3 columns while CSV had to read all 9 — tie it back to row-vs-columnar layout. Windows-friendly paths throughout.`
  },
  check: [
    {
      type: "mcq",
      q: "A nightly analytics query reads 3 columns out of a 30-column orders table across billions of rows. Which storage format serves it best, and why?",
      options: [
        "Parquet — columnar layout means the scan physically reads only the 3 columns it needs and skips the other 27, plus per-row-group stats let it skip non-matching blocks",
        "CSV — it's the smallest format once gzipped, so there are fewer bytes to move",
        "JSON — being self-describing, the reader can jump straight to the 3 fields it needs in each record",
        "Avro — as a binary format it's always faster to scan than any text format"
      ],
      answer: 0,
      explain: "Columnar Parquet gives projection pruning (read 3 of 30 columns) and predicate pushdown (skip blocks via stats). Gzipped CSV (the misconception) may be small but is still row-oriented text — the reader must decompress and parse whole rows to get any column. JSON re-reads whole records too, and Avro, though binary, is row-oriented — great for writing, wrong for a 3-of-30 scan."
    },
    {
      type: "predict",
      q: "<code>orders</code> is stored as Parquet with 9 columns and 240 rows. When this query runs, what does the physical scan actually read from disk?",
      code: `spark.read.table("orders").select("order_id", "total_amount").show()`,
      options: [
        "Only the order_id and total_amount column chunks — projection pruning skips the other 7 columns entirely",
        "All 9 columns for all 240 rows, then drops 7 columns in memory after reading",
        "Only the first 20 rows, because show() defaults to 20 and pruning follows the row limit",
        "Nothing — select is lazy, so no columns are ever read even when show() runs"
      ],
      answer: 0,
      explain: "Parquet's columnar layout lets the reader fetch just the 2 selected column chunks; the other 7 are never decoded. Reading all 9 then dropping 7 (option 2) is precisely what a ROW format like CSV is forced to do — and the contrast is the whole lesson. show() does trigger the read (select is lazy, but show() is the action), so 'nothing is read' is wrong."
    },
    {
      type: "mcq",
      q: "Your team lands a high-volume Kafka stream of whole order-event records into a Bronze zone, with a schema that changes often. Which format fits the <em>write/land</em> side best?",
      options: [
        "Avro — row-oriented and compact with excellent schema-evolution support, ideal for appending whole records as they stream in",
        "Parquet — always use columnar everywhere for consistency across the platform",
        "CSV — schema changes are easy because you can just add a column to the header",
        "ORC — it's the only format that supports streaming appends"
      ],
      answer: 0,
      explain: "Avro is the row-oriented, binary, schema-evolving format built for the landing/streaming edge where you append whole records. Parquet/ORC are columnar — superb for reading, but you'd typically compact Bronze Avro into Parquet for the analytics (Silver/Gold) side. 'Columnar everywhere' (the misconception) ignores that writing whole records favors a row format."
    },
    {
      type: "mcq",
      q: "\"We gzip our CSVs, so they're as small as Parquet — the format doesn't matter.\" What's wrong with this reasoning?",
      options: [
        "Gzipped CSV is actually larger than Parquet, so the premise is simply false in all cases",
        "Even if the compressed size matched, gzipped CSV is still row-oriented and un-splittable text: every query must decompress and parse whole rows, with no projection pruning or predicate pushdown — so scans stay slow regardless of file size",
        "Gzip corrupts numeric columns, so the data wouldn't round-trip correctly",
        "CSV can't be compressed with gzip; it requires Snappy like Parquet does"
      ],
      answer: 1,
      explain: "File size is not the point — access pattern is. A gzipped CSV must be fully decompressed and every row parsed to extract any column, and gzip makes it un-splittable on top of that. Parquet's advantage is structural (columnar + stats), not merely smaller bytes. Two files can be the same size and worlds apart on scan cost."
    }
  ],
  fieldNotes: `A retail-analytics team inherited a warehouse fed by daily 60 GB gzipped-CSV order exports, queried by a dashboard layer that scanned them directly. A typical "revenue by region, last 30 days" query took 6–8 minutes and the cluster autoscaled to 40 nodes every morning to keep up — a five-figure monthly bill driven almost entirely by re-parsing text. One engineer added a single conversion step: land the CSV, immediately rewrite it as Parquet partitioned by date. Same cluster, same queries, untouched dashboards. Query times fell to 10–15 seconds, the morning autoscale dropped from 40 nodes to 6, and the monthly compute bill fell by roughly 70%. Nothing about the data or the questions changed — only the shape of the bytes on disk. The lesson that stuck with the team: choosing CSV for an analytics store isn't a small default, it's a recurring tax you pay on every single query until someone stops paying it.`
};
