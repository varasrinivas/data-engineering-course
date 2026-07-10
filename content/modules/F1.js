// F1 — From Script to Pipeline (Track F, T2 SparkSim)
// Verified facts used by lab + checks (NimbusMart seed 42, per docs/04-ENGINE-CONTRACT.md):
//   fraud_scores = 225 rows; fraud_score >= FRAUD_REVIEW_THRESHOLD (0.80) = 43 rows (4 exactly 0.80)
//   Lab: run1 = 43 rows. Naive union(run1) simulates a retried append = 86 rows.
//   dropDuplicates(["order_id"]) makes the write idempotent = 43 rows no matter how many reruns.
export default {
  id: "F1",
  track: "F",
  title: "From Script to Pipeline",
  minutes: 24,
  coldOpen: "The fraud-review job ran fine for months as a notebook someone triggered by hand each morning. Then it moved to a scheduler with a retry policy, and the first time a task timed out and retried, the review queue came back with 86 orders instead of 43 — every flagged order listed twice. Nothing errored. The scheduler did exactly what it was told: run it again.",
  concept: [
    { type: "prose", html: `
<p>A <strong>script</strong> assumes it runs once, by a human, who watches it finish. A <strong>pipeline</strong> assumes the opposite: it will be run unattended, on a schedule, and — this is the part that breaks scripts — it will sometimes be run <em>again</em>. A task times out and the scheduler retries it. A worker dies mid-write and the orchestrator reruns the task. A colleague clears a failed run and re-triggers the day. The single most important property that separates a pipeline from a script that happens to be scheduled is <strong>idempotency</strong>:</p>
<blockquote><p>Running the task once and running it five times must leave the output in the <em>same</em> state.</p></blockquote>
<p>Idempotency is not a nice-to-have you add later. The moment your code runs behind a retry policy, you have opted into <em>at-least-once</em> execution — the framework guarantees your task runs to completion at least once, and explicitly does <strong>not</strong> guarantee it runs <em>only</em> once. Exactly-once is a fairy tale you buy back yourself, by making the second run a no-op.</p>` },
    { type: "code", lang: "python", code: `from pyspark.sql import functions as F

FRAUD_REVIEW_THRESHOLD = 0.80

# The NAIVE pipeline — a script wearing a scheduler's uniform.
def build_review_queue(spark):
    fraud = spark.read.table("fraud_scores")
    review = (fraud
        .filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)
        .select("order_id", "fraud_score"))
    # append = "add these rows to whatever is already there"
    review.write.format("delta").mode("append").saveAsTable("review_queue")

# Run 1: table now has 43 rows.  Task times out, scheduler retries.
# Run 2 (the retry): append runs AGAIN -> 86 rows. Every order, twice.`, caption: "mode('append') is not idempotent: a retry adds a second full copy." },
    { type: "svg", svg: `<svg viewBox="0 0 720 260" font-family="var(--mono)" font-size="12">
<defs><marker id="f1arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0 L10 5 L0 10 z" fill="var(--ink2)"/></marker></defs>
<text x="20" y="24" fill="var(--ink2)" font-size="11">AT-LEAST-ONCE: the scheduler may run your task more than once</text>
<rect x="20" y="36" width="120" height="38" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="80" y="60" text-anchor="middle" fill="var(--ink)">run 1</text>
<rect x="20" y="82" width="120" height="38" rx="8" fill="var(--paper2)" stroke="var(--rust)" stroke-dasharray="5 4"/><text x="80" y="106" text-anchor="middle" fill="var(--rust)">run 2 (retry)</text>
<line x1="140" y1="55" x2="196" y2="70" stroke="var(--ink2)" marker-end="url(#f1arr)"/>
<line x1="140" y1="101" x2="196" y2="86" stroke="var(--rust)" marker-end="url(#f1arr)"/>
<text x="150" y="150" fill="var(--rust)" font-size="11">NAIVE append</text>
<rect x="200" y="60" width="150" height="52" rx="10" fill="var(--paper2)" stroke="var(--rust)"/>
<text x="275" y="82" text-anchor="middle" fill="var(--ink)">review_queue</text>
<text x="275" y="100" text-anchor="middle" fill="var(--rust)" font-weight="bold">86 rows (43×2)</text>
<text x="410" y="150" fill="var(--accent)" font-size="11">IDEMPOTENT overwrite / dedupe on key</text>
<line x1="140" y1="55" x2="486" y2="70" stroke="var(--ink2)" stroke-opacity="0.35" marker-end="url(#f1arr)"/>
<line x1="140" y1="101" x2="486" y2="86" stroke="var(--ink2)" stroke-opacity="0.35" marker-end="url(#f1arr)"/>
<rect x="490" y="60" width="150" height="52" rx="10" fill="var(--paper2)" stroke="var(--accent)" stroke-width="2"/>
<text x="565" y="82" text-anchor="middle" fill="var(--ink)">review_queue</text>
<text x="565" y="100" text-anchor="middle" fill="var(--accent)" font-weight="bold">43 rows (once or five times)</text>
<text x="20" y="196" fill="var(--ink2)" font-size="11">The retry is identical either way. Only the write semantics differ.</text>
<rect x="20" y="208" width="620" height="40" rx="10" fill="var(--paper2)" stroke="var(--line)"/>
<text x="330" y="233" text-anchor="middle" fill="var(--ink)">idempotency = make the second run a no-op:  overwrite the partition, or dedupe on the natural key</text>
</svg>`, caption: "The framework's at-least-once retry is a given; the output's correctness is your write semantics." },
    { type: "analogy", title: "The re-scanned barcode", html: `
<p>On the NimbusMart receiving dock, when a pallet arrives the picker scans its barcode and stock goes up. Scanners are flaky; radios drop. So the handhelds are built to be scanned <em>twice</em> without fear: the scan doesn't say “add 40 units,” it says “this pallet, ID <code>P-88214</code>, is now received.” Scan it once, stock reflects that pallet. Scan it again because you weren't sure the beep registered — stock is unchanged, because the system keys on the pallet ID, not on the act of scanning.</p>
<p>An <code>append</code> write is a scanner that says “add 40 units” every time you pull the trigger — double-scan a pallet and your inventory is now wrong, silently. An idempotent write keys on the identity of the thing (<code>order_id</code>, the partition date) so the second scan lands you in the same place as the first. Design every pipeline task like a dock scanner: safe to re-fire, because it declares <em>state</em>, not <em>deltas</em>.</p>` },
    { type: "javaBridge", html: `
<p>You have shipped this exact problem in a message consumer. A queue (SQS, Kafka, RabbitMQ) gives you <strong>at-least-once delivery</strong>: if your handler doesn't ack in time, the broker redelivers. A handler that does <code>balance += amount</code> on each delivery double-charges the customer on a redelivery. The senior fix is never “make the broker deliver exactly once” (it can't, cheaply) — it's an <strong>idempotent consumer</strong>: dedupe on a message/idempotency key, or make the operation a set-to-value rather than an increment.</p>
<ul>
<li>Broker redelivery ↔ scheduler retry. Same guarantee, same trap.</li>
<li>Idempotency key on the message ↔ <code>dropDuplicates(["order_id"])</code> or overwrite-by-partition on the write.</li>
<li>“Set the balance to X” instead of “add X” ↔ <code>mode("overwrite")</code> instead of <code>mode("append")</code>.</li>
</ul>
<p>You already know retries are the framework's job and de-duplication is yours. Spark just moves it from one row at a time to a whole partition at a time.</p>` },
  ],
  lab: {
    tier: "T2",
    understand: {
      engine: "sparksim",
      datasets: ["fraud_scores"],
      task: `<p><strong>Make the review-queue write survive a retry.</strong> The starter builds today's review queue (orders at or above <code>FRAUD_REVIEW_THRESHOLD</code>), then simulates what a scheduler retry does to a naive <code>append</code>: it <code>union</code>s the run's output with a second identical copy of itself — the retry re-ran the task and appended the same rows again. Run it and read the row count: <strong>86</strong>, every flagged order duplicated.</p>
<p>Now fix it. Add <code>.dropDuplicates(["order_id"])</code> after the union so the write is idempotent on the natural key. Re-run: <strong>43</strong> — the retry is now a no-op, exactly as if the task had run once. That one line is the entire difference between a script and a pipeline.</p>`,
      starterCode: `fraud = spark.read.table("fraud_scores")

run1 = (fraud
    .filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)
    .select("order_id", "fraud_score"))

# the scheduler timed out and retried: the append ran twice
review_queue = run1.union(run1)

review_queue.show()`,
      solutionCode: `fraud = spark.read.table("fraud_scores")

run1 = (fraud
    .filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)
    .select("order_id", "fraud_score"))

# the retry still appends twice, but the write dedupes on the key
review_queue = run1.union(run1).dropDuplicates(["order_id"])

review_queue.show()`,
      expect: { rows: 43, cols: ["order_id", "fraud_score"] },
      dagNotes: `<p>The <code>union</code> is cheap — it just concatenates two branches into one logical relation, no shuffle. The <code>dropDuplicates(["order_id"])</code> is where the work happens: it draws a shuffle boundary, hashing rows by <code>order_id</code> so every copy of an order lands on the same partition, then keeps one. That shuffle is the price of idempotency at scale — and it is almost always cheaper than the incident an <code>append</code> retry causes. Note the count fell from 86 to 43, not to 82: every one of the 43 rows had an exact duplicate.</p>`
    },
    buildWithAI: `I'm learning to turn a one-shot PySpark script into an idempotent pipeline task. Build me a real local project that proves the difference. Assume a clean machine with Python 3.10+ and nothing else installed.

1. Create a folder \`nimbusmart-idempotent\` with a venv, and install pyspark (pin a recent 3.5.x), pytest, and chispa.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator writing \`data/fraud_scores.csv\` with 225 rows — columns order_id (O-10001..), fraud_score (0.01..0.99, 2dp). Seed it so that EXACTLY 43 rows have fraud_score >= FRAUD_REVIEW_THRESHOLD (0.80) (compute and print that count at the end so I can see it).

3. Create \`pipeline.py\` defining FRAUD_REVIEW_THRESHOLD = 0.80 as a named constant and two functions that both write a Delta-style output table to a local path:
   - build_review_queue_naive(spark): reads fraud_scores, filters fraud_score >= FRAUD_REVIEW_THRESHOLD, selects order_id + fraud_score, and writes with mode("append").
   - build_review_queue_idempotent(spark): identical, but writes with mode("overwrite") AND, before writing, dropDuplicates(["order_id"]).
   Use an explicit StructType schema on the read (no inferSchema).

4. Create \`test_idempotency.py\` (pytest + chispa) that, for EACH function, runs it TWICE against the same output path and then reads the result back:
   - assert the naive table has 86 rows after two runs (double-counted) — proving it is NOT idempotent.
   - assert the idempotent table has 43 rows after two runs, identical to after one run — use chispa's assert_df_equality to compare the one-run and two-run DataFrames.
   - do NOT hardcode 43/86: derive the expected flagged count by reading the CSV with the plain csv module and counting fraud_score >= FRAUD_REVIEW_THRESHOLD.

5. Run the generator, then pytest -v. Show me both assertions passing and explain in one paragraph why the retry doubled the naive table but left the idempotent one unchanged. Windows-friendly paths please.`
  },
  check: [
    {
      type: "predict",
      q: "The naive pipeline ran, then the scheduler retried the task after a timeout. On the NimbusMart seed data, what does the final <code>review_queue</code> row count read?",
      code: `# run 1 appended 43 rows; the retry appended the same rows again
run1 = (spark.read.table("fraud_scores")
    .filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)
    .select("order_id", "fraud_score"))

review_queue = run1.union(run1)   # append + retry-append
print(review_queue.count())`,
      options: ["43", "86", "225", "0"],
      answer: 1,
      explain: "43 orders sit at or above FRAUD_REVIEW_THRESHOLD (0.80). A naive append writes them, the retry appends the identical 43 again, so the table holds 86 — every flagged order duplicated. Nothing errored; the retry did precisely what at-least-once promises."
    },
    {
      type: "mcq",
      q: "Which change makes <code>build_review_queue</code> idempotent with the least ceremony?",
      options: [
        "Wrap the write in a try/except so the retry is swallowed and never runs the append",
        "Switch the write to <code>mode(\"overwrite\")</code>, or dedupe on <code>order_id</code> before an append — so a second run lands on the same state",
        "Configure the scheduler with <code>retries=0</code> so the task can never run twice",
        "Add a timestamp column so each run's rows are distinguishable from the last run's"
      ],
      answer: 1,
      explain: "Idempotency is a property of the write, not of the retry policy. Overwrite-by-key (or dedupe-then-append) makes the second run a no-op. Setting retries=0 just trades a double-count for an unrecovered failure, and try/except can't un-append rows already committed by a run that later died."
    },
    {
      type: "mcq",
      q: "A scheduler offers <em>at-least-once</em> task execution. In practice, what does that guarantee — and not guarantee?",
      options: [
        "It guarantees the task runs exactly once; retries are only simulated for logging",
        "It guarantees the task completes at least once, and explicitly does NOT guarantee it runs only once — so re-runs are your problem to absorb",
        "It guarantees the task runs at most once, so you may lose a run but never duplicate one",
        "It guarantees idempotency automatically, because Spark writes are transactional"
      ],
      answer: 1,
      explain: "At-least-once means the framework keeps trying until the task succeeds — which is why the same task can run more than once. Exactly-once behavior is something you construct on top, by making the operation idempotent. It is never a free property of the engine."
    },
    {
      type: "predict",
      q: "After adding <code>.dropDuplicates([\"order_id\"])</code>, the task is retried not twice but four times (four appended copies). What does <code>count()</code> read?",
      code: `run1 = (spark.read.table("fraud_scores")
    .filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)
    .select("order_id", "fraud_score"))

# four at-least-once deliveries of the same output
four = run1.union(run1).union(run1).union(run1)
review_queue = four.dropDuplicates(["order_id"])
print(review_queue.count())`,
      options: ["172", "86", "43", "4"],
      answer: 2,
      explain: "That is the whole point of idempotency: the count is invariant to the number of re-runs. Four copies collapse to one per order_id, so 43 — the same answer as one run, or five, or fifty."
    }
  ],
  fieldNotes: `A payments team ran a nightly 'chargeback reconciliation' job that appended matched rows into a summary table. It was idempotent-by-luck for a year because it never retried — until a cluster autoscaling event killed a worker at 02:40 and the orchestrator, doing its job, reran the task. The append fired a second time. Finance's Monday dashboard showed chargebacks up 98% week-over-week; three analysts spent a day chasing a fraud spike that did not exist before someone diffed the row counts and found every reconciled record present exactly twice. The fix was one line — <code>mode('overwrite')</code> partitioned by settlement date instead of <code>mode('append')</code> — plus a lesson the team wrote on the wall: the day you put a script behind a retry policy is the day 'it only runs once' stops being true, and every non-idempotent write becomes a scheduled outage waiting for its worker to die.`
};
