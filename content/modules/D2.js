// D2 — Driver, Executors, Cluster Managers (Track D, runtime anatomy)
// T2 sparksim. Verified facts (data/nimbusmart/generate.py, seed 42):
//   orders = 240 rows; status=="delivered" = 108 rows (the lab's count() result).
export default {
  id: "D2",
  track: "D",
  title: "Driver, Executors, Cluster Managers",
  minutes: 24,
  coldOpen: "A new engineer's fraud-export job passed every test on the 240-row sample and OOM-killed the driver ten minutes into its first production run against the full orders history. The offending line looked innocent: `for row in orders.collect():`. On the sample, `collect()` pulled 240 rows to one machine. In production it tried to pull 190 million rows off forty executors into the driver's heap — and where your code runs turned out to matter more than what it did.",
  concept: [
    { type: "prose", html: `
<p>A Spark application is not one program on one machine pretending to be fast. It is three kinds of process with three different jobs, and almost every confusing Spark behavior comes from not knowing which one your line of code is talking to.</p>
<ul>
<li><strong>The driver</strong> runs your <code>main</code>. It executes your Python line by line, builds the logical plan out of your transformations, cuts that plan into <em>jobs → stages → tasks</em>, and hands tasks out. Crucially, it is the <em>only</em> process that runs your top-level code. There is exactly one driver.</li>
<li><strong>The executors</strong> are worker JVMs — usually one per machine, several across the cluster. They do not run your script; they run the <em>tasks</em> the driver ships them, each task chewing through one partition of data. There are many, and they come and go.</li>
<li><strong>The cluster manager</strong> (YARN, Kubernetes, or Spark Standalone) owns the machines. The driver asks it 'give me 32 executors, 4 cores and 16 GB each'; the manager finds the hardware, launches the executors, and relaunches them when they die.</li>
</ul>
<p>So the cold-open bug is now obvious. <code>collect()</code> is an action that says 'gather every partition from every executor <em>back to the driver's heap</em> and hand me a Python list.' On 240 rows, fine. On the full history, you asked one machine to hold what forty machines were built to spread out. The driver isn't a bigger executor — it's a coordinator, and treating it like a data sink is how you page yourself at 02:00.</p>` },
    { type: "code", lang: "python", code: `# WHERE DOES EACH LINE RUN?

spark = SparkSession.builder.getOrCreate()   # DRIVER: sets up, negotiates executors

orders = spark.read.table("orders")          # DRIVER: builds the plan, reads nothing yet
delivered = orders.filter(F.col("status") == "delivered")   # DRIVER: plan grows

n = delivered.count()                        # ACTION → a JOB:
#   DRIVER   cuts the plan into tasks (one per partition) and schedules them
#   EXECUTORS each scan their partition, count matching rows  -> a partial count
#   DRIVER   sums the partial counts into one number: 108

# DANGER: this brings every row to the driver's heap, not a count:
rows = delivered.collect()   # fine on 240 rows, fatal on 190 million`, caption: "The same DataFrame; the action decides whether the driver gets a number (count) or the whole dataset (collect)." },
    { type: "prose", html: `
<p>The vocabulary the driver uses to break work down is the spine of the rest of Track D, so meet it here in its simplest form:</p>
<ul>
<li>A <strong>job</strong> is triggered by one action. <code>count()</code> above is one job.</li>
<li>A <strong>stage</strong> is a run of work that needs no data movement between machines. Reading and filtering are <em>narrow</em> — each partition is handled independently — so the whole count fits in a single stage. (When a step <em>does</em> need to move data across machines — a <code>groupBy</code>, a <code>join</code> — the driver draws a new stage boundary. That's D4.)</li>
<li>A <strong>task</strong> is one stage's work on one partition. If <code>orders</code> has 8 partitions, the count stage is 8 tasks, and the driver hands them to whichever executors have a free core.</li>
</ul>
<p>Read that last point again: <strong>tasks are the unit of scheduling, partitions are the unit of data, and the driver is the thing that maps one onto the other.</strong> An executor with 4 cores runs 4 tasks at once. Give the job more partitions than you have cores and tasks queue; give it fewer and cores sit idle. Everything you will later tune — parallelism, skew, shuffle — is a story about how well this mapping goes.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 250" font-family="var(--mono)" font-size="12">
<defs><marker id="d2arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0 L10 5 L0 10 z" fill="var(--ink2)"/></marker></defs>
<rect x="20" y="90" width="150" height="70" rx="10" fill="none" stroke="var(--accent)" stroke-width="2"/>
<text x="95" y="118" text-anchor="middle" fill="var(--accent)" font-weight="bold">DRIVER</text>
<text x="95" y="136" text-anchor="middle" fill="var(--ink2)" font-size="9">your main() + plan</text>
<text x="95" y="150" text-anchor="middle" fill="var(--ink2)" font-size="9">schedules tasks</text>
<text x="300" y="34" text-anchor="middle" fill="var(--ink2)" font-size="10">EXECUTORS — one JVM per machine, run tasks on partitions</text>
<g>
<rect x="230" y="46" width="140" height="54" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="300" y="66" text-anchor="middle" fill="var(--ink)" font-size="10">executor 1 · 4 cores</text><text x="300" y="86" text-anchor="middle" fill="var(--ink2)" font-size="9">task p0  p1  p2  p3</text>
<rect x="230" y="110" width="140" height="54" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="300" y="130" text-anchor="middle" fill="var(--ink)" font-size="10">executor 2 · 4 cores</text><text x="300" y="150" text-anchor="middle" fill="var(--ink2)" font-size="9">task p4  p5  p6  p7</text>
</g>
<line x1="170" y1="112" x2="228" y2="80" stroke="var(--ink2)" marker-end="url(#d2arr)"/>
<line x1="170" y1="130" x2="228" y2="136" stroke="var(--ink2)" marker-end="url(#d2arr)"/>
<text x="198" y="108" fill="var(--ink2)" font-size="8">tasks</text>
<line x1="370" y1="73" x2="405" y2="107" stroke="var(--accent)" stroke-dasharray="4 3" marker-end="url(#d2arr)"/>
<line x1="370" y1="137" x2="405" y2="120" stroke="var(--accent)" stroke-dasharray="4 3" marker-end="url(#d2arr)"/>
<rect x="410" y="98" width="150" height="54" rx="8" fill="none" stroke="var(--accent)"/><text x="485" y="122" text-anchor="middle" fill="var(--ink)" font-size="10">partial counts</text><text x="485" y="140" text-anchor="middle" fill="var(--ink2)" font-size="9">summed on driver → 108</text>
<rect x="600" y="70" width="100" height="110" rx="10" fill="var(--paper2)" stroke="var(--rust)"/>
<text x="650" y="118" text-anchor="middle" fill="var(--rust)" font-size="10">CLUSTER</text><text x="650" y="132" text-anchor="middle" fill="var(--rust)" font-size="10">MANAGER</text>
<text x="650" y="152" text-anchor="middle" fill="var(--ink2)" font-size="8">grants machines</text>
<line x1="600" y1="125" x2="372" y2="73" stroke="var(--rust)" stroke-dasharray="2 3"/>
<line x1="600" y1="140" x2="372" y2="137" stroke="var(--rust)" stroke-dasharray="2 3"/>
<text x="20" y="210" fill="var(--ink2)" font-size="11">ONE JOB (count) · ONE STAGE (no shuffle) · 8 TASKS (one per partition)</text>
<text x="20" y="230" fill="var(--ink2)" font-size="10">driver plans and sums · executors scan partitions · cluster manager owns the hardware</text>
</svg>`, caption: "count() as three roles: the driver schedules and sums, executors scan partitions, the cluster manager supplies the machines." },
    { type: "analogy", title: "The supervisor, the pickers, and the staffing agency", html: `
<p>On the NimbusMart warehouse floor, the <strong>shift supervisor</strong> never picks an order. They hold the clipboard, break the night's work into slips ('count aisles 1–4', 'count aisles 5–8'), hand slips to whoever's free, and tally the numbers the pickers shout back. That's the <strong>driver</strong>: it plans and coordinates, it doesn't move boxes.</p>
<p>The <strong>pickers</strong> are the <strong>executors</strong> — interchangeable, many, each working the aisles on their slip and reporting a subtotal. And the <strong>staffing agency</strong> that sends thirty pickers tonight and replaces the one who sprains an ankle is the <strong>cluster manager</strong>: the supervisor requests bodies, the agency supplies and covers them.</p>
<p>Now the cold-open bug in warehouse terms: <code>collect()</code> is the supervisor telling every picker to carry every box they counted back to the supervisor's tiny desk. On a sample shelf, fine. On the real warehouse, the desk collapses. A supervisor asks for <em>counts</em>, not the inventory.</p>` },
    { type: "javaBridge", html: `
<p>You've built this shape before with <code>ExecutorService pool = Executors.newFixedThreadPool(32)</code>. Your <code>main</code> thread submits <code>Callable</code> tasks, the pool's worker threads run them, and you gather results with <code>Future.get()</code>. Map it straight across:</p>
<ul>
<li>Your <code>main</code> thread submitting and joining ↔ the <strong>driver</strong>.</li>
<li>The pool's worker threads ↔ the <strong>executors</strong> (each a JVM on another machine, not a thread in yours).</li>
<li><code>newFixedThreadPool(32)</code> sizing the pool ↔ the <strong>cluster manager</strong> granting executors.</li>
</ul>
<p>Two upgrades. First, <code>future.get()</code> that returns a <code>List&lt;Row&gt;</code> of everything is exactly <code>collect()</code> — harmless on a small result, an OOM on a big one; you already know not to buffer an unbounded stream into one list, and the driver is that one list's home. Second, in a thread pool a captured variable is shared memory; in Spark it is <em>serialized and shipped over the wire</em> to each executor, so a task that closes over a 2 GB lookup map copies 2 GB to every executor. Same submit-and-gather instinct — but the tasks now run in other JVMs, and that changes what's cheap.</p>` },
  ],
  lab: {
    tier: "T2",
    understand: {
      engine: "sparksim",
      datasets: ["orders"],
      task: `<p><strong>Trace one action through the runtime.</strong> The starter reads <code>orders</code> and counts <em>all</em> of them. Change it to count only delivered orders: add a <code>.filter(F.col("status") == "delivered")</code> before the <code>count()</code>, then Run.</p>
<p>The result is one number — but watch the DAG and the badges around it. How many <em>jobs</em> did one <code>count()</code> trigger? How many <em>stages</em>? How many <em>tasks</em>, and what decides that number? Notice there is no red cross-dock line anywhere: filter and count are narrow work, so the driver never has to move data between machines. Every task counts its own partition; the driver just sums the partials.</p>`,
      starterCode: `orders = spark.read.table("orders")

n = orders.count()
print(n)`,
      solutionCode: `orders = spark.read.table("orders")

delivered = orders.filter(F.col("status") == "delivered")

n = delivered.count()
print(n)`,
      expect: { rows: 108 },
      dagNotes: `<p>One action, one <strong>job</strong>. Because <code>filter</code> is narrow (each partition handled alone, no data crosses machines), the whole thing is a <strong>single stage</strong> — no shuffle boundary, no cross-dock line. That stage fans out into one <strong>task per partition</strong>: each executor scans its slice, counts the delivered rows locally, and reports a partial count. The <em>driver</em> does none of the scanning — it builds the plan, schedules the tasks, and sums the partials into the final <strong>108</strong>. Swap <code>count()</code> for <code>collect()</code> and the shape is identical except the last step ships all 108 rows to the driver instead of one integer; on this dataset harmless, at production scale the cold-open OOM.</p>`
    },
    buildWithAI: `I'm learning the Spark runtime: driver vs executors vs cluster manager, and how one action becomes jobs/stages/tasks. Set up a real local PySpark project that makes the split visible. Assume Python 3.10+ and nothing else installed.

1. Create a project folder \`nimbusmart-runtime\` with a venv, and install pyspark (pin a recent 3.5.x) and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator writing \`data/orders.csv\` with 240 rows matching NimbusMart: order_id (O-10001..O-10240), customer_id (C-0001..C-0060), status drawn so that exactly 108 rows are "delivered" (the rest spread over placed/shipped/cancelled/returned), total_amount (8..950, 2dp), country (DE,US,IN,BR,JP,FR,AU). Print the status counts so I can see 108 delivered.

3. Create \`runtime_lab.py\` that:
   - builds a local SparkSession with master "local[4]" and sets spark.sql.shuffle.partitions=8
   - reads orders.csv with an EXPLICIT StructType (no inferSchema)
   - repartitions orders to 8 and prints df.rdd.getNumPartitions()
   - defines delivered = df.filter(F.col("status") == "delivered")
   - runs delivered.count() and prints it
   - calls delivered.explain(mode="formatted") and prints it, so I can see there is NO Exchange (shuffle) node — filter+count is one stage
   - prints the Spark UI URL (spark.sparkContext.uiWebUrl) and tells me to open the Jobs tab to see 1 job / 1 stage / 8 tasks while the script sleeps 30s at the end

4. Create \`test_runtime_lab.py\` (pytest) asserting:
   - delivered.count() == the number of "delivered" rows computed by re-reading the CSV with the plain csv module (do NOT hardcode 108)
   - the formatted explain() string for delivered contains NO "Exchange" (proving no shuffle / single stage)
   - df.repartition(8).rdd.getNumPartitions() == 8 (tasks in the count stage = partitions)

5. Run the generator, the lab, and pytest. Point me at the line in explain() that proves the count ran in a single stage, and explain why collect() here would route every row to the driver while count() routes only a partial integer per partition. Windows-friendly paths please.`
  },
  check: [
    {
      type: "mcq",
      q: "The cold-open job OOM-killed the <em>driver</em> (not an executor) on <code>for row in orders.collect():</code>. Why the driver specifically?",
      options: [
        "collect() runs entirely on the driver, so the filter never reached the executors",
        "collect() gathers every partition from every executor back into the driver's single heap; at production row counts that one machine can't hold what the cluster was built to spread out",
        "The driver has less memory than executors by design, so any action OOMs it first",
        "Python for-loops can only run on the driver, forcing all the data there to iterate"
      ],
      answer: 1,
      explain: "collect() is an action that pulls the full result set into the driver's heap so your Python can iterate it. The executors do the scanning; the driver becomes the sink — and one sink can't hold forty machines' worth of rows. Use count()/aggregations, or write() to storage, when the result is large."
    },
    {
      type: "predict",
      q: "On the NimbusMart seed data (240 orders, 108 of them status=='delivered'), what does this print — and how many Spark jobs did it trigger?",
      code: `orders = spark.read.table("orders")
delivered = orders.filter(F.col("status") == "delivered")
print(delivered.count())`,
      options: [
        "108, and one job (count is the only action)",
        "240, and one job (filter is ignored until collect)",
        "108, and two jobs (one for filter, one for count)",
        "108, and zero jobs (count is lazy)"
      ],
      answer: 0,
      explain: "filter is a transformation (no job); count is the single action, so exactly one job runs, and it returns 108. filter is not deferred-until-collect — it's part of the plan the count job executes; and count is emphatically not lazy, it's the thing that starts the meter."
    },
    {
      type: "mcq",
      q: "<code>orders</code> has 8 partitions and runs on 2 executors with 4 cores each. When the <code>count()</code> stage executes, how many tasks are there, and how many run at once?",
      options: [
        "1 task (count returns one number), running on the driver",
        "8 tasks (one per partition), and all 8 run concurrently because 2×4 = 8 cores are available",
        "2 tasks (one per executor), running one at a time",
        "240 tasks (one per row), queued across the 8 cores"
      ],
      answer: 1,
      explain: "A task is one stage's work on one partition, so 8 partitions → 8 tasks. Each core runs one task at a time, and 2 executors × 4 cores = 8 slots, so all 8 tasks run in parallel. Tasks map to partitions (not rows or executors); cores decide how many run at once."
    },
    {
      type: "mcq",
      q: "Which statement about the cluster manager (YARN / Kubernetes / Standalone) is correct?",
      options: [
        "It builds the logical plan and cuts it into stages and tasks",
        "It runs the tasks on the data partitions",
        "It owns the machines: it launches the executors the driver requests and relaunches them when they fail, but it doesn't touch your data or your plan",
        "It is the same process as the driver, just under a different name"
      ],
      answer: 2,
      explain: "Planning and scheduling belong to the driver; running tasks belongs to executors. The cluster manager is the resource layer — it grants and replaces executor machines. That separation is why the same PySpark job runs unchanged on YARN, Kubernetes, or Standalone: only the manager swaps out."
    }
  ],
  fieldNotes: `A payments team shipped a reconciliation job whose final step was <code>results = df.collect()</code> feeding a Python loop that wrote one summary row per merchant to Postgres. It ran clean for months — the pilot had 900 merchants. The night onboarding flipped the merchant table to the full book, the driver tried to <code>collect()</code> 41 million reconciliation rows into an 8 GB driver heap, and the process died with a GC overhead error before a single Postgres write happened; the on-call spent two hours convinced an executor was the problem because 'the cluster has 600 GB of RAM' — none of which was the driver's. The fix was one line: replace the <code>collect()</code>-and-loop with <code>df.write.jdbc(...)</code>, which lets the <em>executors</em> write their own partitions in parallel and never routes the data through the driver at all. The durable lesson: driver memory is sized for plans and coordination, not for data. The moment a result has to fit 'on one machine,' ask which machine — because in Spark the answer is almost never the one you want.`
};
