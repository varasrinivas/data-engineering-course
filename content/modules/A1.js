// A1 — Welcome to NimbusMart: Why Data Engineering (Track A, story-sim T3)
// Verified facts used by lab + checks (from data/nimbusmart/generate.py, seed 42):
//   orders = 240; fraud_scores = 225 (15 unscored); inner join = 225
//   fraud_score >= FRAUD_REVIEW_THRESHOLD (0.80) = 43 rows; exactly 0.80 = 4 rows
//   S-777 owns 80/240 orders (skew, introduced here, used hard in E3)
export default {
  id: "A1",
  track: "A",
  title: "Welcome to NimbusMart: Why Data Engineering",
  minutes: 18,
  coldOpen: "A new analyst at NimbusMart wires a revenue dashboard straight at the production orders database — the same Postgres every checkout writes to — and hits refresh. The query takes forty seconds. For those forty seconds, checkout p99 climbs from 45 ms past the app's three-second timeout, and roughly 7% of in-flight carts fail at the payment step. A read broke the writes. That gap — between the database that runs the business and the one that answers questions about it — is the entire reason your new job exists.",
  concept: [
    { type: "prose", html: `
<p>You have spent your career on the <strong>OLTP</strong> side: <em>online transaction processing</em>. The order service, the payments service, the inventory table — systems tuned to do millions of tiny things correctly and fast. Insert one order. Read one customer by id. Decrement one stock count, atomically, under a row lock, with an index making every lookup a few milliseconds. That database is the <strong>system of record</strong>: one authoritative copy, optimized for point reads and writes.</p>
<p>Analytics asks the opposite kind of question. Not "what is order O-10188?" but "revenue by country this quarter, and how many orders are sitting in the fraud-review queue right now?" That question doesn't touch one row — it <em>scans all of them</em>, reads a couple of columns out of thirty, and collapses 240 million rows into a seven-row answer. That workload is <strong>OLAP</strong>: <em>online analytical processing</em>. Same data, opposite shape of access.</p>
<p>The founding mistake — the one in the cold open, the one every backend team makes once — is assuming one database can be good at both. It cannot, and the reason is physical.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 260" font-family="var(--mono)" font-size="11">
<text x="16" y="22" fill="var(--ink2)" font-size="11">OLTP — ROW STORE (system of record): a row's columns live glued together on disk</text>
<g>
<rect x="16" y="34" width="330" height="26" rx="4" fill="var(--paper2)" stroke="var(--line)"/>
<text x="24" y="51" fill="var(--ink)">[ id · cust · seller · ts · status · amount · … 31 cols ]</text>
<rect x="16" y="64" width="330" height="26" rx="4" fill="var(--paper2)" stroke="var(--line)"/>
<text x="24" y="81" fill="var(--ink)">[ id · cust · seller · ts · status · amount · … 31 cols ]</text>
<rect x="16" y="94" width="330" height="26" rx="4" fill="var(--paper2)" stroke="var(--line)"/>
<text x="24" y="111" fill="var(--ink)">[ id · cust · seller · ts · status · amount · … 31 cols ]</text>
<text x="16" y="146" fill="var(--rust)" font-size="10">read 3 columns → still drag all 31 off disk, 240M times</text>
</g>
<text x="384" y="22" fill="var(--ink2)" font-size="11">OLAP — COLUMNAR (system of analysis): each column stored contiguously</text>
<g>
<rect x="384" y="34" width="60" height="86" rx="4" fill="var(--paper2)" stroke="var(--accent)"/><text x="414" y="80" text-anchor="middle" fill="var(--accent)">country</text>
<rect x="450" y="34" width="60" height="86" rx="4" fill="var(--paper2)" stroke="var(--accent)"/><text x="480" y="80" text-anchor="middle" fill="var(--accent)">amount</text>
<rect x="516" y="34" width="60" height="86" rx="4" fill="var(--paper2)" stroke="var(--accent)"/><text x="546" y="80" text-anchor="middle" fill="var(--accent)">fraud</text>
<rect x="582" y="34" width="122" height="86" rx="4" fill="none" stroke="var(--line)" stroke-dasharray="4 3"/><text x="643" y="80" text-anchor="middle" fill="var(--ink2)" font-size="10">28 cols skipped</text>
<text x="384" y="146" fill="var(--green)" font-size="10">read 3 columns → touch only those 3, compressed</text>
</g>
<line x1="16" y1="166" x2="704" y2="166" stroke="var(--line)"/>
<text x="16" y="190" fill="var(--ink2)" font-size="11">SAME QUESTION, TWO ENGINES</text>
<rect x="16" y="200" width="330" height="46" rx="8" fill="var(--paper2)" stroke="var(--rust)"/>
<text x="181" y="222" text-anchor="middle" fill="var(--ink)">OLTP scan: 41 s · holds locks · starves checkout</text>
<text x="181" y="238" text-anchor="middle" fill="var(--ink2)" font-size="10">the report degrades the business it reports on</text>
<rect x="374" y="200" width="330" height="46" rx="8" fill="var(--paper2)" stroke="var(--green)"/>
<text x="539" y="222" text-anchor="middle" fill="var(--ink)">OLAP scan: 1.1 s · separate cluster · checkout untouched</text>
<text x="539" y="238" text-anchor="middle" fill="var(--ink2)" font-size="10">answers questions without touching the till</text>
</svg>`, caption: "OLTP glues a row's columns together for fast single-row work; OLAP splits columns apart for fast wide scans. Opposite shapes, opposite jobs." },
    { type: "prose", html: `
<p>Three concrete failures happen when you run analytics on the OLTP primary, and it's worth naming them because you will diagnose each in the wild:</p>
<ul>
<li><strong>Row-store scans read everything.</strong> A row store keeps all of a row's columns physically together. To read 3 of 31 columns it still pulls all 31 off disk, for every row — no index helps a full aggregate. Ten times the bytes for the answer you wanted.</li>
<li><strong>Lock and buffer contention.</strong> A 40-second scan holds its read snapshot and thrashes the buffer pool the whole time. Checkout <code>INSERT</code>s and inventory <code>UPDATE</code>s now fight it for the same pages and I/O — and lose.</li>
<li><strong>Replica lag.</strong> The long scan pins WAL cleanup, so the read replica that other services depend on falls behind — 200 ms of lag walks to 38 s. One report degrades three unrelated systems.</li>
</ul>
<p>NimbusMart's spine of a business rule lives right in the middle of this. Every order gets a <code>fraud_score</code> from a model. The rule: <strong>orders below <code>FRAUD_REVIEW_THRESHOLD = 0.80</code> auto-fulfill; orders at or above it go to a human review queue.</strong> It's inclusive — <em>at</em> the threshold counts as "review." That one constant is the recurring thread of this entire course; you'll meet it in SQL, in joins, in windows, in the capstone. In the seed dataset it selects exactly 43 of 240 orders (4 of them sitting precisely on the line), and answering "how big is the queue right now?" is precisely the analytical question the OLTP database chokes on.</p>` },
    { type: "code", lang: "sql", code: `-- The business rule, as the analyst wrote it — pointed at the wrong database.
-- On orders-primary this is a 240M-row heap scan that fights every live checkout.
SELECT o.country,
       COUNT(*)                                   AS orders,
       SUM(o.total_amount)                         AS revenue,
       COUNT(*) FILTER (WHERE f.fraud_score >= 0.80 /* FRAUD_REVIEW_THRESHOLD */)
                                                   AS in_review_queue
FROM   orders o
JOIN   fraud_scores f ON f.order_id = o.order_id
GROUP  BY o.country
ORDER  BY revenue DESC;`, caption: "A correct query in the wrong place. The fix is never a better index — it's a different engine." },
    { type: "analogy", title: "Never run the stock-take by locking the warehouse", html: `
<p>NimbusMart's fulfillment warehouse runs on one live count. Pickers scan barcodes, forklifts move pallets, the receiving dock logs every truck — and while a picker is pulling from bin 14, that bin is <em>locked</em> so two people don't grab the same unit. This is OLTP: one authoritative count, fast small operations, correctness under contention. It is the freight line actually running.</p>
<p>Now the head of finance wants a full quarterly stock-take: count every unit in every aisle. If you do that by <strong>walking the live floor and freezing each aisle as you tally it</strong>, you've halted shipping to produce a report — pickers stall, trucks back up at the dock, and the count is stale by the time you reach aisle 40 anyway. That's the OLTP dashboard query in the cold open.</p>
<p>What a real warehouse does instead: photograph the floor at 2 a.m. into a <strong>separate stock-take ledger</strong> — a copy laid out by aisle for fast tallying — and count <em>that</em> all day without a single picker noticing. That copy is OLAP. Data engineering is the night-shift crew that builds the copy, keeps it faithful, and makes sure the count is never run on the live floor again.</p>` },
    { type: "javaBridge", html: `
<p>You already run this split without naming it. Your transactional service — JPA entities, <code>@Transactional</code> methods, a normalized schema, an index behind every <code>findById</code> — is OLTP. It exists to mutate a few rows correctly under concurrency. You would never dream of running <code>SELECT SUM(amount) GROUP BY country</code> across the whole table on your primary in the request path; you already feel that it's the wrong tool.</p>
<ul>
<li><strong>The read replica is not the answer.</strong> Backend teams reach for "just point the report at the read replica." It helps isolation a little, but a replica is <em>still a row store</em> — same 10× bytes, same lag risk, still the wrong shape. It's a smaller version of the same mistake, not a fix.</li>
<li><strong>The real move is a second system of a different shape.</strong> Snapshot the OLTP data into a columnar analytics store on a schedule. That ETL/ELT job — extract from the system of record, reshape it for scanning, serve it to analysts — is the thing you are now paid to build. "Data engineering" is mostly the disciplined, repeatable version of the copy job your warehouse does at 2 a.m.</li>
</ul>` },
  ],
  lab: {
    tier: "T3",
    understand: {
      engine: "trace",
      trace: "a1-oltp-vs-olap",
      task: `<p><strong>Watch a single dashboard query take down the checkout — then watch the same question run harmlessly on the right engine.</strong> Scrub the timeline and hold three numbers in view:</p>
<ul>
<li><em>Columns needed vs columns read</em> (t=3, t=15) — this is the row-store-vs-columnar gap made literal: 3 needed, 31 read on OLTP.</li>
<li><em>Checkout p99</em> (t=6 → t=12 → t=21) — the fulfillment impact. Note the exact moment it crosses the 3-second app timeout and carts start failing.</li>
<li><em>Query latency</em> (t=18): 41 s on the row store vs 1.1 s on the columnar store — same rows, same aggregate.</li>
</ul>
<p>The point to internalize: the OLAP win isn't only speed, it's <strong>isolation</strong> — the checkout database never even feels the columnar query, because it runs somewhere else entirely.</p>`
    },
    buildWithAI: `I'm a backend developer starting a data engineering course. Build me a real local PySpark project that makes the OLTP-vs-OLAP difference concrete on my own machine. Assume nothing is installed beyond Python 3.10+, and I'm on Windows (use os.path.join / pathlib, no bash-isms).

1. Create a folder \`nimbusmart-oltp-olap\` with a Python venv, and install pyspark (pin a recent 3.5.x) and pytest.

2. Create \`generate_data.py\` — deterministic (random.seed(42)) — that mirrors NimbusMart's engineered facts and writes CSVs into \`data/\`:
   - \`orders.csv\`: 240 rows. Columns: order_id (O-10001..O-10240), customer_id (C-0001..C-0060), seller_id (make 'S-777' own ~35% of rows so there's skew), country (from DE,US,IN,BR,JP,FR,AU), status, total_amount (8..950, 2dp), plus ~24 filler columns (col_07..col_30 of random small strings) so a row is genuinely "wide".
   - \`fraud_scores.csv\`: scores for exactly 225 of the 240 orders (15 deliberately unscored). With FRAUD_REVIEW_THRESHOLD = 0.80, engineer it so that EXACTLY 43 orders have fraud_score >= the threshold and EXACTLY 4 sit exactly on it. Columns: order_id, fraud_score (2dp), model_version.
   Add asserts at the bottom of generate_data.py proving 43 and 4 and 15, so the data can't drift.

3. Create \`compare.py\` that:
   - defines FRAUD_REVIEW_THRESHOLD = 0.80 as a named constant.
   - starts a local SparkSession.
   - reads orders.csv with an EXPLICIT StructType (no inferSchema), writes it once as row-ish CSV and once as Parquet under \`warehouse/\`.
   - runs the SAME aggregate both ways: revenue by country + a count of orders joined to fraud_scores with fraud_score >= FRAUD_REVIEW_THRESHOLD (the review queue).
   - for the Parquet read, selects ONLY country, total_amount, fraud_score, calls .explain(True), and prints the physical plan so I can see column pruning ("ReadSchema" lists 3 columns, not 31).
   - prints wall-clock timing for both reads so the columnar one is visibly cheaper.

4. Create \`test_compare.py\` (pytest) that:
   - recomputes the expected review-queue size from the raw CSV with the plain csv module (do NOT hardcode 43) and asserts the Spark result equals it.
   - asserts the inner join orders×fraud_scores has 225 rows (15 unscored orders drop out).
   - asserts the Parquet physical plan string contains only the 3 projected columns in its ReadSchema, proving the columnar store didn't read the 28 filler columns.

5. Run generate_data.py, then compare.py, then pytest. Show me the explain() output and point at the ReadSchema line that proves the columnar read touched 3 columns while the row store would have read all 31. Explain in comments why this is exactly why analytics must not run on the OLTP primary.`
  },
  check: [
    {
      type: "mcq",
      q: "The analyst's <code>GROUP BY country</code> dashboard query makes NimbusMart's checkout p99 spike from 45 ms to over 3 seconds. What is the primary mechanism?",
      options: [
        "The full-table aggregate scan holds its read snapshot and buffer pool for ~40 s, so concurrent checkout writes contend for the same pages and I/O on the one primary",
        "The query is CPU-bound; adding a B-tree index on <code>country</code> would let it skip the scan and remove the contention",
        "PostgreSQL escalates the read to a table-level exclusive lock that blocks all writes until the query finishes",
        "The dashboard opens too many connections and exhausts the connection pool"
      ],
      answer: 0,
      explain: "A full aggregate is a heap scan — no index helps it, and an index on country wouldn't change that (the common misconception). The damage is contention: one long-held read snapshot thrashing the shared buffer pool that live checkout writes also need. Standard MVCC reads don't take a table exclusive lock, so it isn't lock escalation either."
    },
    {
      type: "predict",
      q: "On the NimbusMart seed data (240 orders; 15 with no fraud score), how many rows land in the review queue — the analytical count the OLTP database struggles to produce?",
      code: `SELECT COUNT(*)
FROM   orders o
JOIN   fraud_scores f ON f.order_id = o.order_id
WHERE  f.fraud_score >= FRAUD_REVIEW_THRESHOLD;   -- 0.80, inclusive`,
      options: ["47", "43", "225", "4"],
      answer: 1,
      explain: "Exactly 43 orders have fraud_score >= FRAUD_REVIEW_THRESHOLD, and the threshold is inclusive so the 4 orders sitting exactly on 0.80 are counted. 225 is the full inner join (all scored orders); 4 is just the boundary rows. The queue is 43."
    },
    {
      type: "mcq",
      q: "Same query, same rows: 41 s on the OLTP row store, 1.1 s on the OLAP columnar store. Where does the ~37× come from?",
      options: [
        "The columnar store keeps the whole table in RAM on SSDs, so it never touches disk",
        "The columnar store reads only the 3 columns the query references (skipping 28) and compresses each column, so it moves ~10× fewer bytes — and it runs on a separate cluster, so there's no contention",
        "The columnar store uses a faster SQL dialect that the row store can't parse",
        "The row store re-parses the query for every row, while the columnar store parses it once"
      ],
      answer: 1,
      explain: "Columnar layout means a 3-column query touches only those 3 columns' data, compressed — an order of magnitude fewer bytes than dragging all 31 columns off a row store. The 'it's all in RAM' answer is the misconception: the win is the storage layout and workload isolation, not a magic memory tier."
    },
    {
      type: "mcq",
      q: "A well-meaning engineer says: \"Just point the dashboard at the read replica instead of the primary.\" Why is this a smaller version of the same mistake, not a real fix?",
      options: [
        "Read replicas can't run <code>GROUP BY</code> queries at all",
        "A replica is still a row store — same 10× bytes per scan — and long analytical reads on it grow replica lag, degrading the services that depend on the replica being fresh",
        "Replicas are always slower than the primary, so the query would take even longer",
        "It's actually the correct fix and fully resolves the problem"
      ],
      answer: 1,
      explain: "Isolating reads onto a replica helps a little, but the replica is the same OLTP shape (row store, full 31-column scans) and long scans there inflate replica lag for everyone else. The real fix is a different engine of a different shape — a columnar OLAP store fed on a schedule."
    }
  ],
  fieldNotes: `A fintech-adjacent marketplace (details filed off) shipped an internal "live ops" dashboard that ran a revenue-and-risk rollup directly against the primary orders Postgres, refreshing every 30 seconds on a wall-mounted TV in the ops room. It was fine in staging with 50k orders. In production, at ~180M orders, each refresh was a 35-second heap scan; every refresh, checkout p99 jumped past the 3-second gateway timeout and a slice of payments failed and retried. Because it "worked" and nobody connected a dashboard to a checkout dip, it ran for eleven days — an estimated €40k in abandoned carts — before someone correlated the failure spikes with the 30-second refresh cadence and pulled the plug. The permanent fix was a nightly snapshot into a columnar warehouse and a hard rule: no analyst query ever touches orders-primary. That rule, and the pipeline that makes it possible, is the job this course teaches.`
};
