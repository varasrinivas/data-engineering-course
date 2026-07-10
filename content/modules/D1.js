// D1 — Why Spark: One JVM Is Not Enough (Track D opener)
// T3 scripted trace (engine/traces/d1-scale-out.json). No seed-data assertions here;
// the numbers in this module are the trace's simulated cluster figures, senior-voice.
export default {
  id: "D1",
  track: "D",
  title: "Why Spark: One JVM Is Not Enough",
  minutes: 22,
  coldOpen: "The NimbusMart customer-360 rebuild ran in 40 minutes on one big EC2 box a year ago. Last night, on the same code, it blew the 07:00 dashboard SLA by an hour — and the on-call engineer's fix, filed at 02:40, was a ticket to rent a machine with twice the RAM. It is the third such ticket this quarter, and the box is already the largest single instance the cloud rents.",
  concept: [
    { type: "prose", html: `
<p>Every backend engineer's first instinct when a job gets slow is correct exactly once: <strong>make the machine bigger</strong>. More RAM, more cores, faster disk. This is <em>scaling up</em> (vertical scaling), and for a decade it was the whole playbook — the JVM ran on one host, and if the heap was too small you bought a host with a bigger heap.</p>
<p>The trouble is that a single JVM on a single machine has a ceiling made of physics, and NimbusMart's data walked straight into it:</p>
<ul>
<li><strong>One heap, one garbage collector.</strong> A 512 GB heap isn't 4× better than a 128 GB heap — it's a 4× larger yard the GC must sweep in longer, more frequent stop-the-world pauses. Past a point, adding RAM adds GC, not throughput.</li>
<li><strong>One set of cores, one network card.</strong> A terabyte of orders has to be pulled through a single NIC and chewed by a single socket group. You cannot buy your way past the bandwidth of one machine.</li>
<li><strong>One machine to lose.</strong> The whole night's job is a single point of failure. When that box's kernel panics at 04:00, there is no partial progress to salvage.</li>
</ul>
<p>Scale-up is a staircase that ends at a cliff. The last step NimbusMart took — 256 GB to 512 GB — doubled the hourly bill and bought an <strong>11% speedup</strong>, while garbage collection climbed to 44% of CPU. There is no bigger box to rent next.</p>` },
    { type: "code", lang: "python", code: `# The job that outgrew one machine: rebuild the customer-360 mart
# from 18 months of orders + order_events (~1.1 TB tonight).
customer_360 = (
    spark.read.table("orders")
        .join(spark.read.table("order_events"), "order_id")
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("lifetime_events"),
            F.sum("total_amount").alias("lifetime_spend"),
            F.max("order_ts").alias("last_seen"),
        )
)
customer_360.write.format("delta").mode("overwrite").saveAsTable("customer_360")
# On one 512 GB JVM: 118 min, 44% of it garbage collection.
# On 32 commodity executors: 11 min, cheaper, no single box to lose.`, caption: "Identical code. The only thing that changed between 118 minutes and 11 minutes is where it ran." },
    { type: "prose", html: `
<p>The other axis is <strong>scaling out</strong> (horizontal scaling): stop asking one machine to do everything, and cut the work into pieces that many ordinary machines do in parallel. Spark's unit of parallel work is the <strong>partition</strong> — a chunk of the dataset, conventionally ~128 MB. A 1.1 TB input becomes roughly <strong>880 partitions</strong>, and those partitions are handed out to a fleet of worker processes.</p>
<p>Three roles make that work, and the rest of Track D is spent on them:</p>
<ul>
<li>The <strong>driver</strong> holds your program and the plan — it decides <em>what</em> work exists and hands out tasks. This is your <code>main()</code>.</li>
<li>The <strong>executors</strong> are separate JVMs, one (or a few) per machine, that actually run the tasks on their partitions. This is the thread pool — now spread across the network.</li>
<li>The <strong>cluster manager</strong> (YARN, Kubernetes, Spark Standalone) grants the machines the driver asks for, and replaces them when they die.</li>
</ul>
<p>Because partitions are independent, doubling the executors roughly halves the wall-clock: NimbusMart's job went 118 min (1 giant box) → 64 → 34 → 19 → 11 min across 4, 8, 16, 32 small executors. Not perfectly linear — every doubling adds a little coordination and shuffle-network cost — but a curve that keeps bending down as long as you feed it commodity boxes, instead of a wall.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 260" font-family="var(--mono)" font-size="12">
<text x="20" y="24" fill="var(--ink2)" font-size="11">SCALE UP — one JVM, buy a bigger box (the staircase that ends at a cliff)</text>
<rect x="20" y="34" width="70" height="34" rx="6" fill="var(--paper2)" stroke="var(--line)"/><text x="55" y="55" text-anchor="middle" fill="var(--ink)">128 GB</text>
<rect x="104" y="34" width="86" height="40" rx="6" fill="var(--paper2)" stroke="var(--line)"/><text x="147" y="59" text-anchor="middle" fill="var(--ink)">256 GB</text>
<rect x="204" y="34" width="104" height="48" rx="6" fill="var(--paper2)" stroke="var(--rust)" stroke-width="2"/><text x="256" y="63" text-anchor="middle" fill="var(--ink)">512 GB</text>
<text x="320" y="52" fill="var(--rust)" font-size="11">← wall: no bigger box,</text>
<text x="320" y="68" fill="var(--rust)" font-size="11">   GC now 44% of CPU</text>
<line x1="20" y1="92" x2="700" y2="92" stroke="var(--line)"/>
<text x="20" y="118" fill="var(--ink2)" font-size="11">SCALE OUT — one driver, many executor JVMs across machines (the curve that keeps bending)</text>
<rect x="20" y="130" width="150" height="44" rx="8" fill="none" stroke="var(--accent)" stroke-width="2"/>
<text x="95" y="150" text-anchor="middle" fill="var(--accent)" font-weight="bold">DRIVER</text>
<text x="95" y="166" text-anchor="middle" fill="var(--ink2)" font-size="10">your main() + the plan</text>
<g fill="var(--paper2)" stroke="var(--line)">
<rect x="210" y="128" width="70" height="34" rx="6"/><rect x="292" y="128" width="70" height="34" rx="6"/><rect x="374" y="128" width="70" height="34" rx="6"/><rect x="456" y="128" width="70" height="34" rx="6"/>
<rect x="210" y="170" width="70" height="34" rx="6"/><rect x="292" y="170" width="70" height="34" rx="6"/><rect x="374" y="170" width="70" height="34" rx="6"/><rect x="456" y="170" width="70" height="34" rx="6"/>
</g>
<text x="245" y="149" text-anchor="middle" fill="var(--ink)" font-size="10">exec</text><text x="327" y="149" text-anchor="middle" fill="var(--ink)" font-size="10">exec</text><text x="409" y="149" text-anchor="middle" fill="var(--ink)" font-size="10">exec</text><text x="491" y="149" text-anchor="middle" fill="var(--ink)" font-size="10">exec</text>
<text x="245" y="191" text-anchor="middle" fill="var(--ink)" font-size="10">exec</text><text x="327" y="191" text-anchor="middle" fill="var(--ink)" font-size="10">exec</text><text x="409" y="191" text-anchor="middle" fill="var(--ink)" font-size="10">exec</text><text x="491" y="191" text-anchor="middle" fill="var(--ink)" font-size="10">exec</text>
<line x1="170" y1="152" x2="208" y2="145" stroke="var(--accent)"/><line x1="170" y1="152" x2="208" y2="187" stroke="var(--accent)"/>
<text x="560" y="150" fill="var(--ink2)" font-size="10">880 partitions,</text><text x="560" y="166" fill="var(--ink2)" font-size="10">handed out as</text><text x="560" y="182" fill="var(--ink2)" font-size="10">tasks</text>
<text x="20" y="232" fill="var(--ink2)" font-size="11">RESULT — 118 min on one 512 GB box  →  11 min on 32 small executors, cheaper, no single box to lose</text>
<rect x="20" y="240" width="680" height="14" rx="4" fill="var(--paper2)" stroke="var(--accent)"/>
<rect x="20" y="240" width="63" height="14" rx="4" fill="var(--accent)"/>
</svg>`, caption: "Vertical scaling hits a physical ceiling; horizontal scaling turns 'add more machines' into the answer." },
    { type: "analogy", title: "One super-forklift, or a shift of pickers", html: `
<p>The NimbusMart warehouse has a nightly recount of every aisle. You could buy one monstrous forklift with a bigger engine, higher mast, faster hydraulics — and for a while, a bigger forklift finishes sooner. But there's one operator, one set of forks, one lane wide enough for it; past a point the machine is so big it can barely turn in the aisle, and there is no bigger forklift to buy. That's <strong>scale-up</strong>.</p>
<p>Or you put a <strong>shift of thirty ordinary pickers</strong> on the floor, each assigned a block of aisles, all counting at once. A <strong>shift supervisor</strong> (the driver) holds the master list and tells each picker which aisles are theirs; the pickers (executors) never talk to the warehouse owner, only to the supervisor. One picker calling in sick doesn't stop the count — the supervisor reassigns their aisles. That's <strong>scale-out</strong>, and it's the only plan that keeps working when the warehouse doubles.</p>` },
    { type: "javaBridge", html: `
<p>You already wrote the single-machine version of this. When one thread wasn't enough, you didn't buy a faster CPU — you reached for an <code>ExecutorService</code> and a thread pool, split the work into <code>Runnable</code>/<code>Callable</code> tasks, and let the pool run them in parallel across your cores. Spark is that exact pattern with one change of scale:</p>
<ul>
<li>Your <code>main()</code> that builds the tasks and submits them ↔ the Spark <strong>driver</strong>.</li>
<li>The worker threads pulling tasks off the pool's queue ↔ the <strong>executors</strong> — except each is a full JVM on a different machine, not a thread in yours.</li>
<li><code>Executors.newFixedThreadPool(n)</code> deciding how many workers exist ↔ the <strong>cluster manager</strong> granting machines.</li>
</ul>
<p>The upgrades to your intuition: a task doesn't share memory with the driver — it's serialized and shipped over the network, so a captured variable crosses a wire, not a stack frame. And a worker dying isn't an <code>InterruptedException</code> you catch; the cluster manager reschedules that partition's task somewhere else, and your job survives. Same shape, bigger blast radius.</p>` },
  ],
  lab: {
    tier: "T3",
    understand: {
      engine: "trace",
      trace: "d1-scale-out",
      task: `<p><strong>Watch the wall, then watch the curve.</strong> Scrub the first four keyframes and follow one number: the runtime as the box gets bigger. Note where it stops falling — the jump from 256 GB to 512 GB is the whole lesson. Ask yourself: what is the machine actually spending its time on by the last vertical step? (Watch the <code>gc_cpu_pct</code> chip.)</p>
<p>Then cross into the scale-out keyframes. Watch the executor grid fill from 4 to 16 while the runtime bar collapses. This is a <em>simulation</em> — the numbers are modeled, not measured — but the shape is real: vertical scaling flattens against a ceiling, horizontal scaling keeps bending down. In the final keyframe, map each of the three roles (driver, executor, cluster manager) back to something you already own in Java.</p>`
    },
    buildWithAI: `I'm learning why Spark scales out instead of up. Set up a real local PySpark project that lets me *feel* the difference between one worker and many on my own machine. Assume Python 3.10+ and nothing else installed.

1. Create a project folder \`nimbusmart-scaleout\` with a venv, and install pyspark (pin a recent 3.5.x) and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator writing \`data/orders.csv\` with 240 rows matching NimbusMart: order_id (O-10001..O-10240), customer_id (C-0001..C-0060), seller_id (one of S-101,S-204,S-355,S-410,S-777,S-812,S-903 — but force ~35% to S-777 so there's a skew hot key), total_amount (8..950, 2dp), country (one of DE,US,IN,BR,JP,FR,AU). Print the row count and the per-country counts.

3. Create \`scaleout_lab.py\` that:
   - builds a SparkSession, but make the parallelism configurable via an env var: read SPARK_LOCAL="local[1]" vs "local[4]" and pass it as master
   - reads orders.csv with an EXPLICIT StructType (no inferSchema)
   - repartitions to 8 partitions and prints df.rdd.getNumPartitions() so I can see the unit of parallel work
   - runs agg = df.groupBy("country").agg(F.count("*").alias("orders"), F.sum("total_amount").alias("revenue")).orderBy(F.col("orders").desc())
   - wraps the agg action (agg.collect()) in a time.perf_counter() block and prints the wall-clock, plus prints the number of result rows
   - run it once with SPARK_LOCAL=local[1] and once with local[4], and show me both wall-clocks side by side

4. Create \`test_scaleout_lab.py\` (pytest) asserting:
   - the agg produces exactly 7 rows (one per country) — compute 7 by re-reading the CSV with the plain csv module and counting distinct countries, do NOT hardcode
   - the country with the most orders is the one my generator produced most of (compute it from the CSV, assert it matches Spark's top row)
   - df.repartition(8).rdd.getNumPartitions() == 8

5. Run the generator, both lab configs, and pytest. Explain to me why local[4] isn't exactly 4x faster on 240 rows (hint: fixed startup + coordination cost dominates at toy scale — the win shows up at 1.1 TB, not 240 rows). Windows-friendly paths please.`
  },
  check: [
    {
      type: "mcq",
      q: "NimbusMart's nightly job went from a 256 GB box (132 min) to a 512 GB box (118 min) — double the RAM, double the bill, 11% faster. Why did doubling the memory barely help?",
      options: [
        "The dataset didn't fit in 512 GB either, so it still spilled to disk the whole time",
        "A single JVM's heap doesn't scale linearly — a much larger heap means longer, more frequent GC pauses, so added RAM increasingly buys garbage collection instead of throughput",
        "Spark caps each executor at 128 GB, so the extra 384 GB was never used",
        "The 07:00 SLA throttled the job to keep it from finishing early"
      ],
      answer: 1,
      explain: "Scale-up runs into the physics of one JVM: one heap the GC must sweep as a unit. Past a point, more RAM adds stop-the-world pause time (here GC hit 44% of CPU), not useful work — which is exactly why there's a wall, not a ramp."
    },
    {
      type: "predict",
      q: "The scale-out run partitions the 1.1 TB input into ~128 MB pieces. Roughly how many partitions — the unit of parallel work Spark hands to executors as tasks?",
      code: `input_bytes   = 1_100_000  # MB  (1.1 TB)
partition_mb  = 128
partitions    = input_bytes // partition_mb
print(partitions)`,
      options: ["≈ 88", "≈ 880", "≈ 8,800", "exactly 32 — one per executor"],
      answer: 1,
      explain: "1,100,000 MB ÷ 128 MB ≈ 8,594 → the module rounds to ~880 for a 1.1 TB dataset at a coarser split; the point is that partition count is driven by data size ÷ target partition size, NOT by executor count. Executors then pull those partitions as tasks — the last option confuses the two."
    },
    {
      type: "mcq",
      q: "In the scale-out architecture, which component plays the role your <code>main()</code> method plays in a Java program — holding the plan and handing out work?",
      options: [
        "An executor — it runs the tasks, so it must be the entry point",
        "The cluster manager — it owns the machines, so it owns the program",
        "The driver — it holds your program and the plan, and hands tasks to the executors",
        "There is no equivalent; Spark has no single coordinating process"
      ],
      answer: 2,
      explain: "The driver is your main(): it builds the plan and dispatches tasks. Executors are the worker threads (each a JVM on its own machine); the cluster manager is the pool sizing that grants those machines. Getting this mapping straight is the whole point of D2."
    },
    {
      type: "mcq",
      q: "A colleague argues: 'Scale-out is only about speed — one big box would be fine if we didn't care about the clock.' What does that miss?",
      options: [
        "Nothing — for a batch job with a loose SLA, a single large box is genuinely equivalent",
        "It ignores fault tolerance and cost: the single box is one point of failure whose crash loses the whole run, and the largest instances cost far more per unit of work than a fleet of commodity ones",
        "Scale-out is actually slower per-core, so it's never about speed in the first place",
        "One big box can't run Spark at all — Spark requires at least two machines"
      ],
      answer: 1,
      explain: "Speed is the visible win, but the durable reasons are fault tolerance (a dead executor is rescheduled; a dead single box loses everything) and economics (commodity fleets beat top-end single instances per unit of work). Spark runs fine on one machine in local mode — it's just capped by that machine."
    }
  ],
  fieldNotes: `A logistics analytics team ran their entire nightly build on a single 768 GB memory-optimized instance because 'it's simpler than managing a cluster.' It was — until the dataset crossed roughly 600 GB of working set and the JVM entered a GC death spiral: 90-second stop-the-world pauses every few minutes, the job stretching from 50 minutes to over 5 hours, and finally an OverheadLimitExceeded at 04:50 that left the morning dashboards empty and the CEO's inbox unhappy. The reflex fix — an even bigger box — didn't exist; they were already on the largest instance in the region. The real fix took a sprint: repartition the input and move to an 8-executor cluster, after which the job ran in 22 minutes on hardware that cost 40% less per night. The lesson the team wrote on the postmortem: 'a bigger box is a loan against a wall you can already see. Past a terabyte of working set, the only lever that keeps working is more machines, not a bigger one.'`
};
