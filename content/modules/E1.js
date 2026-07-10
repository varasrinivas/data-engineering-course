// E1 — Reading & Writing at Scale (Track E, PySpark in Practice)
// Verified facts (data/nimbusmart/generate.py, seed 42):
//   orders = 240 rows; columns: order_id, customer_id, seller_id, order_ts,
//   status, total_amount, item_count, country, channel.
//   A projection to 4 columns still returns 240 rows (row count is not a projection concern).
export default {
  id: "E1",
  track: "E",
  title: "Reading & Writing at Scale",
  minutes: 24,
  coldOpen: "An analyst points a fresh notebook at four terabytes of NimbusMart order JSON with inferSchema=true and Runs the read cell. Forty minutes later — before a single transformation — the cell is still going. When it finally lands, order_ts is typed as a string and total_amount comes back as a string too, because one file three weeks ago wrote \"49.90 USD\" into the amount field. Every downstream SUM is now a runtime error.",
  concept: [
    { type: "prose", html: `
<p>Reading a file in Spark is not I/O — it's <em>planning</em> I/O. <code>spark.read</code> returns a DataFrame with a schema and a recipe; the bytes stay on disk until an action pulls them. But a plan needs a schema up front, and that's the fork in the road that decides whether your read is cheap or ruinous.</p>
<p>You get the schema one of two ways:</p>
<ul>
<li><strong>Declare it</strong> — hand Spark an explicit <code>StructType</code>. The read is a pure plan: zero bytes touched until an action, and every column has exactly the type you promised.</li>
<li><strong>Infer it</strong> — pass <code>inferSchema=true</code> (CSV) or let JSON sniff types. Convenient in a notebook, a trap in a pipeline.</li>
</ul>
<p>For Parquet and Delta the schema is embedded in the file footer, so the read is already cheap and typed — the inference tax is a CSV/JSON problem. But those are exactly the formats your raw <em>ingest</em> lands in.</p>` },
    { type: "code", lang: "python", code: `from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType)

orders_schema = StructType([
    StructField("order_id",     StringType(),    False),
    StructField("customer_id",  StringType(),    False),
    StructField("seller_id",    StringType(),    False),
    StructField("order_ts",     TimestampType(), False),
    StructField("status",       StringType(),    False),
    StructField("total_amount", DoubleType(),    False),
    StructField("item_count",   IntegerType(),   False),
    StructField("country",      StringType(),    False),
    StructField("channel",      StringType(),    True),
])

orders = (spark.read
    .schema(orders_schema)          # declared, not inferred
    .json("s3://nimbus-bronze/orders/"))   # nothing read yet — this is a plan`, caption: "Nine fields, nine promises. Spark reads none of it here." },
    { type: "prose", html: `
<p><code>inferSchema</code> lies in two distinct ways, and both bite in production, not in the notebook.</p>
<p><strong>1. It costs an extra full pass.</strong> To guess types, Spark must <em>read the data before it reads the data</em> — it launches a separate job that scans the source (the whole file set for JSON; a configurable sample for CSV) just to sample values, then launches your real job on top. On 4 TB of JSON that first pass <em>is</em> the forty minutes. A declared schema skips it entirely.</p>
<p><strong>2. It guesses from what it happened to see.</strong> Types are inferred from sampled rows, so they're a function of <em>which</em> rows and <em>which</em> files existed at read time — non-deterministic across runs. A column that's all integers today infers <code>long</code>; the day a file writes <code>"N/A"</code> the whole column silently widens to <code>string</code>, and every arithmetic op downstream breaks. <code>order_ts</code> almost always infers as <code>string</code> — ISO text looks like text — so your date filters do lexicographic comparison and your partition-by-day writes garbage.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 250" font-family="var(--mono)" font-size="12">
<defs><marker id="e1arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0 L10 5 L0 10 z" fill="var(--ink2)"/></marker></defs>
<text x="20" y="26" fill="var(--rust)" font-size="11">inferSchema = true — TWO passes over the same 4 TB</text>
<rect x="20" y="38" width="150" height="44" rx="8" fill="var(--paper2)" stroke="var(--rust)"/><text x="95" y="58" text-anchor="middle" fill="var(--ink)">scan to guess</text><text x="95" y="73" text-anchor="middle" fill="var(--ink2)" font-size="10">a whole extra job</text>
<line x1="170" y1="60" x2="214" y2="60" stroke="var(--ink2)" marker-end="url(#e1arr)"/>
<rect x="216" y="38" width="150" height="44" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="291" y="58" text-anchor="middle" fill="var(--ink)">scan again to read</text><text x="291" y="73" text-anchor="middle" fill="var(--ink2)" font-size="10">your real job</text>
<line x1="366" y1="60" x2="410" y2="60" stroke="var(--ink2)" marker-end="url(#e1arr)"/>
<rect x="412" y="38" width="170" height="44" rx="8" fill="none" stroke="var(--rust)"/><text x="497" y="58" text-anchor="middle" fill="var(--rust)">types you didn't choose</text><text x="497" y="73" text-anchor="middle" fill="var(--ink2)" font-size="10">order_ts → string</text>
<text x="20" y="140" fill="var(--accent)" font-size="11">explicit StructType — ONE pass, types guaranteed</text>
<rect x="20" y="152" width="150" height="44" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="95" y="172" text-anchor="middle" fill="var(--ink)">read (planned)</text><text x="95" y="187" text-anchor="middle" fill="var(--ink2)" font-size="10">schema already known</text>
<line x1="170" y1="174" x2="214" y2="174" stroke="var(--ink2)" marker-end="url(#e1arr)"/>
<rect x="216" y="152" width="366" height="44" rx="10" fill="var(--paper2)" stroke="var(--accent)"/><text x="399" y="172" text-anchor="middle" fill="var(--ink)">one scan → typed columns → straight into your job</text><text x="399" y="187" text-anchor="middle" fill="var(--ink2)" font-size="10">total_amount is a double because you said so</text>
<text x="20" y="228" fill="var(--ink2)" font-size="10">Same bytes, same cluster. The only difference is whether Spark had to read them twice to find out what they were.</text>
</svg>`, caption: "The inference tax is a second full scan — plus types chosen by whichever rows happened to be sampled." },
    { type: "analogy", title: "The manifest, not the crowbar", html: `
<p>Every pallet arriving at the NimbusMart receiving dock carries a <strong>manifest</strong> taped to the shrink-wrap: SKU, count, weight, hazard class. A dock worker reads the manifest and routes the pallet in one pass — the manifest <em>is</em> an explicit <code>StructType</code>, the pallet spec both sides agreed on.</p>
<p><code>inferSchema</code> is the worker who ignores the manifest and pries open every crate to guess what's inside before deciding where it goes — a complete extra unload of the truck before the real unload begins. Worse, they guess from the top layer: a pallet that's all textbooks on top gets labelled "books", and the bowling balls underneath crush the shelf. Declaring the schema is trusting the manifest; inferring it is reaching for the crowbar on every single pallet, every single morning.</p>` },
    { type: "javaBridge", html: `
<p>A DataFrame's <code>StructType</code> is your <strong>POJO plus Jackson annotations</strong> — with one upgrade and one twist.</p>
<ul>
<li><strong>The upgrade:</strong> when you deserialize into a typed class — <code>mapper.readValue(json, Order.class)</code> — Jackson infers <em>nothing</em>; the target class is the schema, fields land in the right types, and unknown fields are handled by policy. That's an explicit <code>StructType</code>. <code>inferSchema</code> is <code>readValue(json, Map.class)</code>: everything becomes <code>Object</code>, you've paid to parse the whole payload, and you still have to figure out what the values are.</li>
<li><strong>The twist — columnar.</strong> A POJO stores one record's fields together (row-major); a stream of them is an array of objects. Spark stores each <em>column</em> contiguously off-heap. That's why a projection to 4 of 9 columns can skip reading the other 5 entirely — impossible with a row of Java objects where the fields you don't want sit physically between the ones you do.</li>
</ul>` },
  ],
  lab: {
    tier: "T2",
    understand: {
      engine: "sparksim",
      datasets: ["orders"],
      task: `<p><strong>Build the lean orders export for the fraud team.</strong> They join on <code>seller_id</code> and rank by <code>total_amount</code> within a <code>country</code> — they need <em>four</em> columns, not the whole row. The starter selects all nine, so the scan hauls every column off disk. Narrow the <code>.select()</code> to exactly <code>order_id</code>, <code>seller_id</code>, <code>total_amount</code>, <code>country</code>, then Run.</p><p>Watch the scan node: with a columnar source, a projection is <em>pushed down</em> — Spark reads only the 4 column chunks you asked for and never touches the other 5. Same 240 rows either way; far less data leaves the disk.</p>`,
      starterCode: `orders = spark.read.table("orders")

export = orders.select(
    "order_id", "customer_id", "seller_id", "order_ts",
    "status", "total_amount", "item_count", "country", "channel")

export.show()`,
      solutionCode: `orders = spark.read.table("orders")

export = orders.select(
    "order_id", "seller_id", "total_amount", "country")

export.show()`,
      expect: { rows: 240, cols: ["order_id", "seller_id", "total_amount", "country"] },
      dagNotes: `<p>The row count is unchanged — 240 in, 240 out. Projection narrows the row, never the set. In the physical plan the <code>ReadSchema</code> on the scan node now lists 4 fields instead of 9: that's projection pushdown, and it's free only because the store is columnar. On a row-oriented format (CSV, JSON) Spark still reads the whole line off disk and drops the columns after — the win is real only when the file layout lets you skip bytes.</p>`
    },
    buildWithAI: `I'm learning PySpark I/O (explicit schemas vs inferSchema, and projection pushdown). Set up a real local project that proves the difference on my own machine. Assume nothing beyond Python 3.10+.

1. Create a project folder \`nimbusmart-io\` with a venv; install pyspark (pin a recent 3.5.x) and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) writer that emits NimbusMart orders as BOTH JSON and Parquet into \`data/\`:
   - 20,000 order rows: order_id (O-10001..), customer_id (C-0001..C-0060), seller_id from [S-101,S-204,S-355,S-410,S-777,S-812,S-903] with S-777 given ~35% of rows (the skew seller), order_ts as an ISO-8601 string, status from [placed,shipped,delivered,cancelled,returned], total_amount (8..950, 2dp), item_count (1..6), country from [DE,US,IN,BR,JP,FR,AU], channel from [web,app].
   - IMPORTANT: in a few hundred JSON rows, write total_amount as a STRING like "49.90" so inference has something to trip on.

3. Create \`read_lab.py\` that:
   - builds a SparkSession (local[*])
   - reads the JSON with inferSchema and prints df.schema — show me total_amount inferred as string and order_ts as string
   - defines an explicit StructType (order_ts TimestampType, total_amount DoubleType, item_count IntegerType) and reads the JSON again with .schema(...); print that schema
   - times both reads with a wall clock so I can see inference costs an extra pass
   - reads the Parquet with .select("order_id","seller_id","total_amount","country") and calls .explain(True); point me at the ReadSchema line proving only 4 columns are read (projection pushdown)

4. Create \`test_read_lab.py\` (pytest) asserting:
   - the explicit-schema DataFrame's total_amount field is DoubleType and order_ts is TimestampType
   - df.select(4 cols).count() equals df.count() (projection changes columns, not row count)
   - the Parquet explain() output contains a ReadSchema listing exactly those 4 columns

5. Run the generator, the lab, and pytest. Show me the two schemas side by side and the ReadSchema line. Windows-friendly paths please.`
  },
  check: [
    {
      type: "mcq",
      q: "The read cell runs for 40 minutes <em>before</em> any transformation. With <code>inferSchema=true</code> on 4 TB of JSON, what is that time?",
      options: [
        "A separate job that scans the entire source to sample and guess column types, run before your real job starts",
        "The JVM warming up its JIT compiler on the first read of the session",
        "Spark writing the inferred schema back to the source files as metadata",
        "Network latency fetching the files from object storage into executor memory"
      ],
      answer: 0,
      explain: "Inference cannot happen without data, so Spark launches a full extra pass over the source just to sample types, then runs your actual job on top. A declared StructType removes that first pass entirely — the read becomes a pure plan."
    },
    {
      type: "predict",
      q: "On the NimbusMart seed data, how many rows does this print?",
      code: `export = (spark.read.table("orders")
    .select("order_id", "country"))
print(export.count())`,
      options: ["240", "2", "480", "225"],
      answer: 0,
      explain: "There are 240 orders, and a projection narrows the row (fewer columns), never the set (same rows). Column pruning changes how many bytes are read per row, not how many rows exist — the count stays 240."
    },
    {
      type: "mcq",
      q: "Why does <code>inferSchema</code> often type <code>order_ts</code> as a string, and why does that matter?",
      options: [
        "ISO timestamps are text, so inference reads them as strings; date filters then compare lexicographically and partition-by-day writes are wrong",
        "Spark always types the first column as a string regardless of its contents",
        "Timestamps are stored as longs, and inference rounds them to strings to save memory",
        "It doesn't matter — Spark auto-casts string timestamps back to dates on any comparison"
      ],
      answer: 0,
      explain: "An ISO timestamp like \"2026-05-14T09:31:00\" looks like text, so inference guesses string. Lexicographic ordering happens to match time order for ISO strings — until you subtract dates, filter a range, or partition by day, where a real TimestampType is required."
    },
    {
      type: "mcq",
      q: "A pipeline reads the same daily CSV with <code>inferSchema=true</code>. It runs fine for months, then one morning every <code>SUM(total_amount)</code> throws. Most likely cause?",
      options: [
        "A new file contained a non-numeric value in total_amount, so inference widened the whole column to string and arithmetic now fails",
        "Spark upgraded and changed its default inference algorithm overnight",
        "The cluster ran out of memory and silently downgraded doubles to strings",
        "CSV files cannot be summed in Spark without a GROUP BY clause"
      ],
      answer: 0,
      explain: "Inferred types are a function of the data that happens to be present. One row with \"N/A\" or \"49.90 USD\" is enough to make Spark infer string for the whole column, and every downstream numeric op breaks. An explicit DoubleType would have rejected the bad row loudly at read time instead."
    }
  ],
  fieldNotes: `A payments team ran a nightly Parquet-to-CSV export that other teams re-ingested with inferSchema. For fourteen months a customer_id like \"C-0042\" was inferred as string — correct. Then marketing onboarded a partner whose IDs were pure digits, one file wrote customer_id as \"88123\", and inference flipped the column to long across the whole read. Leading zeros vanished, \"C-\" prefixes were absent, and 2.1 million rows joined to nothing — the dashboard showed a 30% overnight drop in known customers and paged the on-call. Root cause: three months earlier someone had deleted the explicit StructType to make a demo notebook shorter. The fix was eight lines of StructField. The lesson the team wrote on the wall: inferSchema is a convenience for exploring data you don't own yet, never a contract for data you ship.`
};
