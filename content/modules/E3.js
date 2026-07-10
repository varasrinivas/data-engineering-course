// E3 — Joins: Broadcast, Sort-Merge & Skew (Track E) — T3, reuses engine/traces/e3-skew-salting.json
// Verified facts (data/nimbusmart/generate.py, seed 42):
//   orders = 240; fraud_scores = 225 (15 unscored).
//   inner join orders x fraud_scores on order_id = 225 rows; LEFT join = 240 (15 null scores).
//   seller S-777 owns 80/240 orders — the skew hot key. 43 orders have fraud_score >= FRAUD_REVIEW_THRESHOLD.
export default {
  id: "E3",
  track: "E",
  title: "Joins: Broadcast, Sort-Merge & Skew",
  minutes: 28,
  coldOpen: "The NimbusMart fraud pipeline joins every order to a seller-level risk rollup and it's been fine for a year. Then MegaDeals (seller S-777) runs a flash sale, and the nightly job that took 6 minutes now runs for 41. The Spark UI shows the same story every night: 199 of 200 tasks green in seven seconds, one task grinding alone for another half-minute. No error, no data growth to speak of — one seller just got popular, and one hash bucket is paying for it.",
  concept: [
    { type: "prose", html: `
<p>A join has one physical problem to solve: for two rows to be matched on a key, they have to be <em>in the same place at the same time</em>. The order row and its fraud-score row might start life on different machines; Spark has to get them onto the same executor before it can pair them. There are two ways to do that, and choosing the wrong one is the difference between a 3-second join and a 40-second one.</p>
<ul>
<li><strong>Broadcast hash join</strong> — if one side is small, copy it <em>whole</em> to every executor and keep the big side exactly where it is. No shuffle of the big table at all.</li>
<li><strong>Sort-merge join</strong> — if both sides are large, shuffle <em>both</em> by the join key so matching keys land in the same partition, sort each partition, then merge-walk them together.</li>
</ul>
<p>The fraud-score join is the easy case. <code>fraud_scores</code> is 225 rows — kilobytes. You never shuffle 240 million orders to meet a table that fits in a coffee cup; you broadcast the small side.</p>` },
    { type: "code", lang: "python", code: `from pyspark.sql import functions as F

FRAUD_REVIEW_THRESHOLD = 0.80

orders = spark.read.table("orders")
fraud  = spark.read.table("fraud_scores")   # 225 rows — tiny

# LEFT join: keep all 240 orders; the 15 unscored ones get a null score.
# F.broadcast ships the small side to every executor — no shuffle of orders.
review = (orders
    .join(F.broadcast(fraud), "order_id", "left")
    .withColumn("needs_review",
                (F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)
                | F.col("fraud_score").isNull())      # null score = review it
    .filter(F.col("needs_review")))

review.show()`, caption: "Broadcast the tiny side, left-join so no order vanishes, treat a missing score as 'review'." },
    { type: "prose", html: `
<p>Spark auto-broadcasts a side it can prove is under <code>spark.sql.autoBroadcastJoinThreshold</code> (10 MB by default); <code>F.broadcast(df)</code> is you forcing the issue when the optimizer's size estimate is too pessimistic. The moment <em>both</em> sides are genuinely large — orders joined to a seller rollup that's itself derived from all orders — broadcast is off the table and you're in <strong>sort-merge</strong>: two shuffles, two sorts, one merge. Correct, general, and shuffle-bound.</p>
<p>Sort-merge is where <strong>skew</strong> becomes lethal. Shuffle routes each row by <code>hash(key) % numPartitions</code>. <code>hash("S-777")</code> is a single value, so every one of S-777's <strong>80 of 240</strong> orders — a third of the table — lands in the same partition. Seven tasks breeze through ~23M rows each; one task must sort-merge 80M alone. And it's worse than 3.5x: that partition's sort no longer fits in the task's execution memory, so it <em>spills</em> to disk, and 3.5x the data becomes ~7x the time. The stage can't finish until its slowest task does — the flat line at 199/200 that pages you at 3am.</p>` },
    { type: "prose", html: `
<p>The fix is <strong>salting</strong>: make the hot key stop being one key. Append a random suffix <code>0..N-1</code> to the join key on the big side, so <code>S-777</code> becomes <code>S-777_0 … S-777_7</code> — eight keys that hash to eight partitions, ~10M rows each instead of 80M in one. For the join to stay <em>correct</em>, the small side must speak the same language: every seller row is <strong>exploded once per salt value</strong>, so <code>S-777_3</code> still finds its match. Salt only one side and the join silently drops rows.</p>
<p>The trade is real but usually trivial: the small side grows N-fold (8x here), and any downstream aggregate needs a second pass to merge the salted partials back to the true key. You pay that to turn a 41-second straggler into a 7-second stage — a <strong>5.9x</strong> win on the trace. Salt surgically, only for keys you've measured as hot, and check whether Spark 3's AQE skew-join handles it before you hand-roll the suffix.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 250" font-family="var(--mono)" font-size="12">
<text x="20" y="22" fill="var(--rust)" font-size="11">RAW KEY — hash(S-777) sends 80/240 to one partition</text>
<g>
<rect x="20" y="34" width="52" height="24" rx="3" fill="var(--paper2)" stroke="var(--line)"/><text x="46" y="51" text-anchor="middle" fill="var(--ink)" font-size="10">p0 24</text>
<rect x="80" y="34" width="52" height="23" rx="3" fill="var(--paper2)" stroke="var(--line)"/><text x="106" y="51" text-anchor="middle" fill="var(--ink)" font-size="10">p1 23</text>
<rect x="140" y="34" width="52" height="22" rx="3" fill="var(--paper2)" stroke="var(--line)"/><text x="166" y="51" text-anchor="middle" fill="var(--ink)" font-size="10">p2 22</text>
<rect x="200" y="34" width="52" height="112" rx="3" fill="var(--rust)" opacity="0.85"/><text x="226" y="96" text-anchor="middle" fill="var(--paper)" font-size="10">p3</text><text x="226" y="112" text-anchor="middle" fill="var(--paper)" font-size="10">80</text>
<text x="226" y="164" text-anchor="middle" fill="var(--rust)" font-size="9">S-777</text>
<rect x="260" y="34" width="52" height="23" rx="3" fill="var(--paper2)" stroke="var(--line)"/><text x="286" y="51" text-anchor="middle" fill="var(--ink)" font-size="10">p4 23</text>
<rect x="320" y="34" width="52" height="23" rx="3" fill="var(--paper2)" stroke="var(--line)"/><text x="346" y="51" text-anchor="middle" fill="var(--ink)" font-size="10">p5 23</text>
</g>
<text x="20" y="200" fill="var(--accent)" font-size="11">SALTED — S-777_0..7 spread the hot key evenly (~30 each)</text>
<g>
<rect x="20" y="212" width="52" height="30" rx="3" fill="var(--accent)" opacity="0.75"/><text x="46" y="231" text-anchor="middle" fill="var(--paper)" font-size="10">30</text>
<rect x="80" y="210" width="52" height="31" rx="3" fill="var(--accent)" opacity="0.75"/><text x="106" y="230" text-anchor="middle" fill="var(--paper)" font-size="10">31</text>
<rect x="140" y="213" width="52" height="29" rx="3" fill="var(--accent)" opacity="0.75"/><text x="166" y="231" text-anchor="middle" fill="var(--paper)" font-size="10">29</text>
<rect x="200" y="212" width="52" height="30" rx="3" fill="var(--accent)" opacity="0.75"/><text x="226" y="231" text-anchor="middle" fill="var(--paper)" font-size="10">30</text>
<rect x="260" y="212" width="52" height="30" rx="3" fill="var(--accent)" opacity="0.75"/><text x="286" y="231" text-anchor="middle" fill="var(--paper)" font-size="10">30</text>
<rect x="320" y="210" width="52" height="31" rx="3" fill="var(--accent)" opacity="0.75"/><text x="346" y="230" text-anchor="middle" fill="var(--paper)" font-size="10">31</text>
</g>
<text x="430" y="130" fill="var(--ink2)" font-size="11">41s stage  →  7s stage</text>
<text x="430" y="150" fill="var(--ink2)" font-size="11">one indivisible task  →  8 even tasks</text>
<text x="430" y="170" fill="var(--rust)" font-size="11" font-weight="bold">5.9x — same data, same answer</text>
</svg>`, caption: "One key owns a third of the table; salting splits it into eight so the partitions — and the tasks — even out." },
    { type: "analogy", title: "The pocket reference card", html: `
<p>Every picker on the NimbusMart floor carries a laminated <strong>pocket reference card</strong>: the 200 seller risk ratings, printed small enough to fit in an apron. When a parcel needs its seller's rating, the picker glances at the card — no walk, no queue. That's a <strong>broadcast join</strong>: the small table copied to everyone, so the lookup happens locally wherever the big data already is.</p>
<p>The alternative, when the reference list is too big for a card, is to haul both the parcels <em>and</em> the master ledger to a central sorting table, line them up by seller, and walk them together — the <strong>sort-merge join</strong>, and every parcel had to make the trip. And on a flash-sale night, when one seller (S-777) accounts for a third of all parcels, that one seller's pile at the sorting table is taller than everyone else's combined — the whole table waits on the one clerk stuck working it. Salting is quietly relabelling that seller's parcels into eight sub-piles so eight clerks can share the load.</p>` },
    { type: "javaBridge", html: `
<p>A broadcast join is a distributed <code>HashMap</code> lookup — the exact pattern you'd reach for by hand.</p>
<pre style="font-size:11px;overflow-x:auto"><code>// small side, loaded once, held in memory:
Map&lt;String, Double&gt; sellerRisk = loadSellerRisk();   // ~200 entries
// big side, streamed:
orders.forEach(o -&gt; enrich(o, sellerRisk.get(o.sellerId)));</code></pre>
<p>Spark does exactly this, distributed: it ships that <code>Map</code> — the broadcast variable — to every executor's heap <em>once</em>, and each task probes its local copy while the big side streams past. No shuffle, because the lookup table travelled to the data instead of the data travelling to a common point. Two things to carry over:</p>
<ul>
<li><strong>It only works while the Map fits in heap.</strong> A 200-entry card is nothing; a 5 GB "small" side broadcast to 100 executors is 500 GB of duplicated memory and an OOM. That's precisely what <code>autoBroadcastJoinThreshold</code> guards, and why forcing <code>F.broadcast</code> on a not-actually-small table is a classic self-inflicted outage.</li>
<li><strong>Skew has no Java analogue you'd notice</strong> — a single-machine <code>HashMap</code> doesn't care that one key has a million values. Partitioned across a cluster, that one key is one task, and one task is the unit of parallelism you can't subdivide. Hence salting.</li>
</ul>` },
  ],
  lab: {
    tier: "T3",
    understand: {
      engine: "trace",
      trace: "e3-skew-salting",
      task: `<p><strong>Scrub the A/B timeline and watch one key melt a stage.</strong> Both sides run the <em>same</em> seller-stats sort-merge join; the left uses the raw <code>seller_id</code>, the right salts the hot key with a random 0–7 suffix. Things to catch as you drag the scrubber:</p><ul><li>At the shuffle write (t=4), where do S-777's rows go on the left — and how does salting spread them on the right?</li><li>Around t=12, why does partition <strong>p3</strong> fall behind <em>faster</em> than 3.5x the rows would predict? (Look for the spill.)</li><li>At t=16, the salted stage is already <strong>done at 7s</strong> while the raw stage sits parked at 7/8 tasks — the Spark UI pattern you're learning to smell.</li><li>The final tally: <strong>41s vs 7s, a 5.9x win</strong> — and the price on the right (the small side exploded 8x). Was it worth it?</li></ul>`
    },
    buildWithAI: `I'm learning about join skew and salting in Spark. Build me a real local project that reproduces a hot-key straggler and fixes it, on my own machine. Assume nothing beyond Python 3.10+.

1. Create a folder \`nimbusmart-skew\` with a venv; install pyspark (recent 3.5.x) and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) writer emitting two Parquet datasets into \`data/\`:
   - orders: 2,000,000 rows — order_id, seller_id from [S-101,S-204,S-355,S-410,S-777,S-812,S-903] where S-777 is deliberately given ~35% of all rows (the skew hot key), total_amount (8..950, 2dp), country from [DE,US,IN,BR,JP,FR,AU].
   - seller_stats: one row per seller_id — seller_id, risk_rating (0..1, 2dp), lifetime_orders.

3. Create \`skew_lab.py\` that builds a SparkSession (local[*]) with spark.sql.autoBroadcastJoinThreshold set to -1 (force sort-merge so we can SEE the skew), then:
   - runs the naive join: orders.join(seller_stats, "seller_id") and times an action (count) — print wall clock
   - prints the per-seller row counts so I can see S-777 dominates
   - runs the SALTED version: add salt = floor(rand()*8) to orders, build key = concat(seller_id, '_', salt); explode seller_stats across salt values 0..7 into the same key shape; join on the salted key; time it
   - prints both wall-clock numbers and the speedup ratio

4. Create \`test_skew_lab.py\` (pytest) asserting:
   - the salted join returns the SAME row count as the naive join (salting must not drop or duplicate matches)
   - per-key row counts after salting are far more even than before (max/min partition ratio drops sharply)
   - S-777's share of orders is > 0.30 (confirm the skew was actually generated)

5. Run the generator, the lab, and pytest. Show me the two wall-clock times, the speedup, and the before/after per-key distribution. Also tell me whether enabling AQE (spark.sql.adaptive.skewJoin.enabled=true) fixes it without manual salting. Windows-friendly paths please.`
  },
  check: [
    {
      type: "predict",
      q: "On the seed data (240 orders, 15 unscored), how many rows does this LEFT join produce?",
      code: `enriched = orders.join(
    F.broadcast(fraud_scores), "order_id", "left")
print(enriched.count())`,
      options: ["240", "225", "255", "215"],
      answer: 0,
      explain: "A left join keeps every left row. All 240 orders survive; the 15 with no matching fraud_scores row get a null fraud_score rather than disappearing. (An inner join would drop those 15 and return 225 — which is exactly why the fraud pipeline uses left and treats null as 'needs review'.)"
    },
    {
      type: "mcq",
      q: "Why does broadcasting <code>fraud_scores</code> avoid a shuffle entirely?",
      options: [
        "The small table is copied to every executor, so each task looks matches up locally while the big table stays exactly where it is",
        "Broadcasting compresses the small table so it fits in the shuffle buffer",
        "Broadcast joins skip the sort phase, which is what the shuffle actually does",
        "Spark caches the join result, so the shuffle only happens on the first run"
      ],
      answer: 0,
      explain: "Broadcast ships the small side whole to every executor's memory; the big side is never moved. The match becomes a local hash-map probe wherever the big data already lives — no exchange, no sort-merge. It works only while the small side fits in heap, which is what autoBroadcastJoinThreshold guards."
    },
    {
      type: "predict",
      q: "How many orders belong to the skew hot key on the seed data?",
      code: `hot = orders.filter(
    F.col("seller_id") == "S-777")
print(hot.count())`,
      options: ["80", "240", "35", "43"],
      answer: 0,
      explain: "Seller S-777 (MegaDeals) owns 80 of the 240 orders — a third of the table. In a sort-merge join keyed on seller_id, all 80 hash to one partition, so one task does a third of the work alone. That is the straggler salting exists to break up."
    },
    {
      type: "mcq",
      q: "The Spark UI shows a stage at 199/200 tasks for 30 seconds, then finishes. Salting the join key made almost no difference. Most likely reason?",
      options: [
        "The salt applied to only one side of the join, or the hot key wasn't actually the join key — so the skewed partition never got split",
        "200 partitions is too few; salting needs at least 1000 partitions to help",
        "Salting only helps broadcast joins, and this was a sort-merge join",
        "The straggler was a slow disk, which salting cannot address"
      ],
      answer: 0,
      explain: "Salting works only if the big side's salt and the exploded small side share the same key shape, and only if the hot key is the join key. Salt one side but not the other and matches are lost; salt a key that isn't causing the skew and the melting partition is untouched. Measure which key is hot first."
    }
  ],
  fieldNotes: `A logistics platform chased a nightly join that stalled at 3am for weeks. Every dashboard said the cluster was healthy: CPU low, memory fine, 4,999 of 5,000 tasks done in under a minute. The last task ran for 34 minutes, spilling 14 GB to disk, because a single test merchant account had been wired to every synthetic order from a load-testing suite that never got turned off — one seller_id owned 60% of the fact table. Nobody had added capacity or changed the query; a QA fixture had quietly become the hottest key in production. The permanent fix was salting that join, but the real fix was a monitor on per-key partition size, because skew is invisible in every cluster-level metric — it hides inside one task while the averages look perfect. They now alert when any shuffle partition exceeds 5x the median, and catch the next hot key before it pages anyone.`
};
