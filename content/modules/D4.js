// D4 — Jobs, Stages, Tasks & the Shuffle (Track D, reading the DAG)
// T2 sparksim. Verified facts (data/nimbusmart/generate.py, seed 42):
//   orders = 240 rows across 7 countries; groupBy("country") -> 7 rows.
//   Order counts by country: FR 58, JP 41, BR 35, US 33, DE 31, IN 22, AU 20 (FR tops orderBy desc).
export default {
  id: "D4",
  track: "D",
  title: "Jobs, Stages, Tasks & the Shuffle",
  minutes: 26,
  coldOpen: "A NimbusMart analyst asked for the country-revenue report 'sorted nicely, biggest first.' The engineer added one line — `.orderBy(F.col(\"revenue\").desc())` — shipped it, and the nightly job's runtime went from 6 minutes to 11. Nothing about the data changed. That one cosmetic sort added a second full shuffle across the cluster, and the DAG had been trying to warn him the whole time.",
  concept: [
    { type: "prose", html: `
<p>When you call an action, the driver turns your plan into a <strong>DAG</strong> — a directed graph of the work. Reading that graph is the single most useful Spark skill, because it tells you, before you spend a cent of compute, where the expensive parts are. The whole graph is built from three nested units:</p>
<ul>
<li>A <strong>job</strong> = everything one action triggers.</li>
<li>A <strong>stage</strong> = a run of work that needs <em>no data movement between machines</em>. The driver cuts a new stage exactly where data must be reshuffled across the cluster.</li>
<li>A <strong>task</strong> = one stage's work on one partition. Stages are the graph's nodes; tasks are how each node fans out across executors.</li>
</ul>
<p>Everything hinges on one distinction between two kinds of transformation:</p>
<ul>
<li><strong>Narrow</strong> transformations — <code>select</code>, <code>filter</code>, <code>withColumn</code> — compute each output partition from a <em>single</em> input partition. No data leaves its machine. These chain together inside one stage, for free.</li>
<li><strong>Wide</strong> transformations — <code>groupBy</code>, <code>join</code>, <code>orderBy</code>, <code>distinct</code> — need rows from <em>many</em> input partitions to land together. That forces a <strong>shuffle</strong>: every executor writes its rows out keyed by the grouping column, and every executor reads back the keys it now owns. The shuffle is the stage boundary.</li>
</ul>
<p>So the cold-open regression writes itself. <code>groupBy("country")</code> was already one shuffle. Adding <code>orderBy</code> added a <em>second</em> — a global sort can't be done partition-by-partition, so all the rows cross the network again. Two shuffles, not one; roughly double the expensive part.</p>` },
    { type: "code", lang: "python", code: `# Country revenue report — count the shuffles before you run it.
report = (
    spark.read.table("orders")                 # stage 1: scan (narrow)
        .filter(F.col("status") != "cancelled")# stage 1: filter (narrow) — same stage, free
        .groupBy("country")                    # ── SHUFFLE ── rows regroup by country
        .agg(
            F.count("*").alias("orders"),       # stage 2: aggregate each country's rows
            F.sum("total_amount").alias("revenue"),
        )
        .orderBy(F.col("orders").desc())        # ── SHUFFLE ── global sort across machines
)
report.show()   # one job, THREE stages, TWO shuffle boundaries

# Read it as: scan+filter | shuffle | aggregate | shuffle | sort
# The two "|" shuffles are the whole cost. The narrow steps are noise beside them.`, caption: "Two wide transformations (groupBy, orderBy) = two shuffles = three stages. The narrow steps ride along for free." },
    { type: "prose", html: `
<p>Why is a shuffle the thing to fear? Because it is the only step that writes every row to disk and pushes it across the network. Narrow work streams through memory; a shuffle <em>materializes</em> — each executor spills its keyed output to local disk (the 'shuffle write'), then every executor fetches its share from every other executor (the 'shuffle read'). For NimbusMart's 240-row toy that's nothing, but the shape is identical at a terabyte, where it's the difference between a 6-minute job and an 11-minute one.</p>
<p>This is also where <strong>skew</strong> lives, and why D4 sets up E3. After <code>groupBy("country")</code>, all rows for one country sit in one task. NimbusMart's countries are lopsided — France carries 58 orders, Australia 20 — so the France task does nearly 3× the work of the Australia task, and the stage is only as fast as its slowest task. Reading the DAG tells you a shuffle happened; reading the <em>task durations within a stage</em> tells you whether one hot key is melting it. You will fix that in E3 by salting the key; here, you just learn to see it.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 250" font-family="var(--mono)" font-size="11">
<defs><marker id="d4arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0 L10 5 L0 10 z" fill="var(--ink2)"/></marker></defs>
<text x="20" y="22" fill="var(--ink2)" font-size="11">ONE JOB · THREE STAGES · TWO SHUFFLE BOUNDARIES (rust = cross-dock)</text>
<rect x="20" y="40" width="170" height="150" rx="10" fill="var(--paper2)" stroke="var(--line)"/>
<text x="105" y="58" text-anchor="middle" fill="var(--ink2)" font-size="9">STAGE 1 (narrow)</text>
<rect x="36" y="70" width="138" height="26" rx="5" fill="var(--paper)" stroke="var(--line)"/><text x="105" y="87" text-anchor="middle" fill="var(--ink)">scan orders</text>
<rect x="36" y="104" width="138" height="26" rx="5" fill="var(--paper)" stroke="var(--line)"/><text x="105" y="121" text-anchor="middle" fill="var(--ink)">filter status</text>
<text x="105" y="150" text-anchor="middle" fill="var(--ink2)" font-size="8">8 tasks · one per partition</text>
<text x="105" y="176" text-anchor="middle" fill="var(--ink2)" font-size="8">no data crosses machines</text>
<line x1="190" y1="115" x2="262" y2="115" stroke="var(--rust)" stroke-width="3" stroke-dasharray="6 4" marker-end="url(#d4arr)"/>
<text x="226" y="105" text-anchor="middle" fill="var(--rust)" font-size="9">SHUFFLE</text>
<text x="226" y="132" text-anchor="middle" fill="var(--rust)" font-size="8">regroup by country</text>
<rect x="265" y="40" width="170" height="150" rx="10" fill="var(--paper2)" stroke="var(--line)"/>
<text x="350" y="58" text-anchor="middle" fill="var(--ink2)" font-size="9">STAGE 2 (aggregate)</text>
<rect x="281" y="80" width="138" height="26" rx="5" fill="var(--paper)" stroke="var(--line)"/><text x="350" y="97" text-anchor="middle" fill="var(--ink)">count + sum / country</text>
<text x="350" y="132" text-anchor="middle" fill="var(--ink2)" font-size="8">7 tasks · one per country</text>
<text x="350" y="152" text-anchor="middle" fill="var(--rust)" font-size="8">FR=58 rows ≫ AU=20 → skew</text>
<line x1="435" y1="115" x2="507" y2="115" stroke="var(--rust)" stroke-width="3" stroke-dasharray="6 4" marker-end="url(#d4arr)"/>
<text x="471" y="105" text-anchor="middle" fill="var(--rust)" font-size="9">SHUFFLE</text>
<text x="471" y="132" text-anchor="middle" fill="var(--rust)" font-size="8">global sort</text>
<rect x="510" y="40" width="170" height="150" rx="10" fill="var(--paper2)" stroke="var(--line)"/>
<text x="595" y="58" text-anchor="middle" fill="var(--ink2)" font-size="9">STAGE 3 (sort)</text>
<rect x="526" y="80" width="138" height="26" rx="5" fill="var(--paper)" stroke="var(--line)"/><text x="595" y="97" text-anchor="middle" fill="var(--ink)">orderBy desc</text>
<rect x="526" y="120" width="138" height="26" rx="5" fill="none" stroke="var(--accent)"/><text x="595" y="137" text-anchor="middle" fill="var(--accent)">show() → 7 rows</text>
<text x="20" y="222" fill="var(--ink2)" font-size="10">Delete the orderBy and STAGE 3 and its shuffle vanish — that is the cold-open's 5 lost minutes.</text>
<text x="20" y="240" fill="var(--ink2)" font-size="10">Narrow steps fuse into one stage for free; every wide step buys another stage and another network crossing.</text>
</svg>`, caption: "The DAG for the report: two wide transformations draw two cross-dock lines and three stages. The narrow scan+filter fuse into stage 1." },
    { type: "analogy", title: "The cross-dock transfer", html: `
<p>Picking items that already sit in your own aisle is cheap — you walk your zone and never leave it. That's a <strong>narrow</strong> transformation: each picker works their own partition, in place. The NimbusMart floor is built so most work stays local.</p>
<p>But 'gather every order by destination country onto one pallet per country' is a different animal. Orders for France are scattered across every zone in the building, so you fire up the forklifts and run the expensive <strong>cross-dock transfer</strong>: every zone sends its France items to the France staging bay, its Japan items to the Japan bay, and so on — every zone talking to every bay at once. That forklift storm is the <strong>shuffle</strong>, and it's the stage boundary: nothing on the far side can start until the transfer lands.</p>
<p>Now the cold-open. Asking for the pallets 'sorted biggest first' means <em>another</em> full cross-dock — you can't globally rank pallets without moving them together again. Two 'gather-by' requests, two forklift storms. And when one bay (France) gets 58 items while another (Australia) gets 20, the whole transfer waits on the France crew — the crowded aisle on sale day, which you'll learn to unclog in E3.</p>` },
    { type: "javaBridge", html: `
<p>The narrow-vs-wide split is one you already feel in the <code>Stream</code> API, even if you've never named it. <code>stream().map(f).filter(p)</code> is embarrassingly parallel — split the source, run each chunk independently, done. That's a stage of narrow transformations: no coordination between chunks.</p>
<p>A <code>Collectors.groupingBy(Order::country)</code> on a <em>parallel</em> stream is the other kind. To build the per-country groups, partial results from every worker thread have to be merged together — the framework does a combine step where threads exchange data. That merge is a shuffle in miniature, happening in shared memory on one JVM.</p>
<ul>
<li>On one machine, that exchange is a memory copy between threads — cheap enough to ignore.</li>
<li>In Spark, the same 'bring the same keys together' step writes every row to local disk and pulls it across the network between machines. Same logical operation, but now it's the most expensive thing in your job.</li>
</ul>
<p>So the instinct to carry over is: a <code>groupingBy</code>/<code>sorted</code>/<code>distinct</code> that was free on a parallel stream is a <strong>shuffle</strong> in Spark — and every one you add is another stage and another network crossing. Count them before you run.</p>` },
  ],
  lab: {
    tier: "T2",
    understand: {
      engine: "sparksim",
      datasets: ["orders"],
      task: `<p><strong>Predict the cross-dock lines before you Run.</strong> The starter groups <code>orders</code> by country and counts them — one wide transformation, so one shuffle. Your job: add <code>.orderBy(F.col("orders").desc())</code> to sort the countries busiest-first, then, <em>before running</em>, decide out loud how many rust cross-dock lines the DAG will draw and where.</p>
<p>Now Run and check yourself. Count the shuffle boundaries and the stages. Look at the aggregate stage's tasks: one country per task, and the France task carries 58 rows while Australia carries 20 — that lopsidedness is skew, and the stage waits on its slowest task. This is the exact shape you'll optimize in E3; here, just learn to <em>see</em> two shuffles where you wrote two wide transformations.</p>`,
      starterCode: `orders = spark.read.table("orders")

report = (orders
    .groupBy("country")
    .agg(F.count("*").alias("orders"), F.sum("total_amount").alias("revenue")))

report.show()`,
      solutionCode: `orders = spark.read.table("orders")

report = (orders
    .groupBy("country")
    .agg(F.count("*").alias("orders"), F.sum("total_amount").alias("revenue"))
    .orderBy(F.col("orders").desc()))

report.show()`,
      expect: { rows: 7, cols: ["country", "orders", "revenue"] },
      dagNotes: `<p>Two wide transformations, so <strong>two rust cross-dock lines</strong> and <strong>three stages</strong>. Stage 1 scans <code>orders</code> (narrow, one task per partition). The first shuffle regroups rows by <code>country</code>; stage 2 aggregates — now <strong>one task per country</strong>, seven tasks, and they're uneven: France's task chews 58 rows, Australia's 20, so the stage is only as fast as the France task (that's skew, and it's what E3 fixes). The second shuffle does the global <code>orderBy</code> sort; stage 3 emits the 7 sorted rows. Delete the <code>orderBy</code> and stage 3 <em>and</em> its shuffle disappear — the cold-open's regression, in reverse.</p>`
    },
    buildWithAI: `I'm learning to read a Spark DAG: jobs, stages, tasks, and where shuffles draw stage boundaries. Set up a real local PySpark project that makes the shuffles countable. Assume Python 3.10+ and nothing else installed.

1. Create a project folder \`nimbusmart-dag\` with a venv, and install pyspark (pin a recent 3.5.x) and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator writing \`data/orders.csv\` with 240 rows matching NimbusMart: order_id (O-10001..O-10240), customer_id (C-0001..C-0060), status, total_amount (8..950, 2dp), and country drawn so the mix is deliberately skewed — roughly FR 58, JP 41, BR 35, US 33, DE 31, IN 22, AU 20 (7 countries, France the hot one). Print the per-country counts.

3. Create \`dag_lab.py\` that:
   - builds a local SparkSession, master "local[4]", spark.sql.shuffle.partitions=8
   - reads orders.csv with an EXPLICIT StructType (no inferSchema)
   - builds report = df.groupBy("country").agg(F.count("*").alias("orders"), F.sum("total_amount").alias("revenue")).orderBy(F.col("orders").desc())
   - calls report.explain(mode="formatted") and prints it — I want to COUNT the "Exchange" nodes (there should be TWO: one for the groupBy, one for the orderBy)
   - runs report.show() and prints the top country
   - prints spark.sparkContext.uiWebUrl and sleeps 30s so I can open the SQL/Jobs tab and see 3 stages + the two shuffles

4. Create \`test_dag_lab.py\` (pytest) asserting:
   - report has exactly 7 rows (compute 7 as the distinct country count from the CSV via the plain csv module, do NOT hardcode)
   - the top row by orders is the most-common country in the CSV (compute it from the CSV and assert it matches — should be France)
   - the formatted explain() string contains exactly TWO "Exchange" occurrences (one shuffle per wide transformation); then remove the .orderBy and assert it drops to ONE

5. Run the generator, the lab, and pytest. Point me at the two Exchange nodes in explain() and explain why removing the orderBy deletes an entire stage. Windows-friendly paths please.`
  },
  check: [
    {
      type: "predict",
      q: "On the NimbusMart seed data, how many rows does <code>report.show()</code> print, and which country is on top?",
      code: `report = (orders
    .groupBy("country")
    .agg(F.count("*").alias("orders"))
    .orderBy(F.col("orders").desc()))
report.show()`,
      options: [
        "7 rows; France (FR) on top with 58 orders",
        "240 rows; one per order, unsorted",
        "7 rows; Australia (AU) on top — orderBy defaults to ascending",
        "60 rows; one per customer"
      ],
      answer: 0,
      explain: "groupBy('country') collapses 240 orders to one row per country — 7 countries, 7 rows. orderBy(...desc()) puts the busiest first: France with 58. (Australia's 20 is the smallest, and desc() means it lands last, not first.)"
    },
    {
      type: "mcq",
      q: "The cold-open engineer added <code>.orderBy(...)</code> to a report that already had a <code>groupBy(...)</code>, and runtime jumped ~80%. What did that one line actually add?",
      options: [
        "A second job — orderBy is an action that re-runs the whole plan",
        "A second shuffle and therefore a third stage — a global sort can't be done partition-by-partition, so every row crosses the network again",
        "Nothing structural — the slowdown was random cluster noise",
        "A cache miss that forced the groupBy to recompute from disk"
      ],
      answer: 1,
      explain: "orderBy is a wide transformation: a global ordering requires bringing rows together across machines, which is a full shuffle and a new stage. It's still one job (one action, show()), but now with two shuffles instead of one — roughly doubling the expensive part."
    },
    {
      type: "mcq",
      q: "Which chain runs entirely in a <em>single stage</em>, with no shuffle boundary?",
      options: [
        "orders.groupBy(\"country\").count()",
        "orders.join(fraud_scores, \"order_id\")",
        "orders.filter(F.col(\"total_amount\") > 100).withColumn(\"big\", F.lit(True)).select(\"order_id\", \"big\")",
        "orders.orderBy(\"order_ts\")"
      ],
      answer: 2,
      explain: "filter, withColumn, and select are all narrow — each output partition comes from one input partition, no data crosses machines, so they fuse into one stage. groupBy, join, and orderBy are all wide: each forces a shuffle and a stage boundary."
    },
    {
      type: "mcq",
      q: "After <code>groupBy(\"country\")</code>, the aggregate stage runs one task per country, and the France task (58 orders) takes far longer than the Australia task (20 orders). Why does that make the whole stage slow, and what is it called?",
      options: [
        "It doesn't — Spark auto-balances tasks so all finish together",
        "Skew: a stage finishes only when its slowest task does, so the one hot partition (France) sets the stage's wall-clock while other executors sit idle",
        "Backpressure: the France task throttles the others to conserve memory",
        "Small files: France has more files, so its task lists more objects"
      ],
      answer: 1,
      explain: "A stage completes when its last task does, so an uneven key distribution (France 58 vs Australia 20) means everyone waits on the hot task while other cores idle. That's skew — visible here as lopsided task durations, and fixed in E3 by salting the hot key."
    }
  ],
  fieldNotes: `A retail-analytics team chased a nightly job that had crept from 18 minutes to 90 over a quarter, blaming 'data growth' — but the row count had barely moved. Reading the DAG in the Spark UI told the real story: the job had accreted six wide transformations, mostly from well-meaning readability edits — a <code>distinct()</code> here 'to be safe', a <code>groupBy</code> that could have been a window, two <code>orderBy</code>s for report cosmetics, a <code>join</code> that a broadcast would have avoided. Six shuffles, seven stages, and one of the shuffles was slammed by a single hot merchant key doing 40× the rows of the median task — the stage sat at 100% for 22 minutes on one core while 63 others idled. The fix wasn't more hardware; it was reading the graph: they collapsed two shuffles into one, dropped a needless <code>distinct</code>, broadcast the small side of the join, and salted the hot key. Runtime: 16 minutes. The team's new rule made it into the style guide: 'every wide transformation is a line item on the bill — before you add one, know what it costs and whether the DAG already pays for it.'`
};
