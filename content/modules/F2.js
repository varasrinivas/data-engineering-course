// F2 — Airflow DAGs (Track F, T3 scripted trace: f2-airflow-dag)
// Concept: operators, sensors, scheduling; the shift-supervisor's clipboard.
// Trace facts: nightly DAG, 6 tasks, sensor waits 14 min on late orders export,
//   build_fraud_review_queue fails try 1, retries, produces 43 rows >= FRAUD_REVIEW_THRESHOLD (0.80; 4 exactly at it).
export default {
  id: "F2",
  track: "F",
  title: "Airflow DAGs",
  minutes: 24,
  coldOpen: "For two years the NimbusMart nightly ran as a 300-line bash script that called eleven Python jobs in sequence with `&&`. It worked until the orders export was 20 minutes late one night; the script had already moved on, built the fraud queue on yesterday's orders, and shipped it. No error, no retry, no idea which of the eleven steps ran on stale input. The replacement wasn't a better script. It was a graph.",
  concept: [
    { type: "prose", html: `
<p>A <strong>DAG</strong> — directed acyclic graph — is how you stop describing your nightly as a <em>sequence of commands</em> and start describing it as a <em>set of dependencies</em>. You don't say “run ingest, then silver, then the fraud queue.” You say “the fraud queue <em>depends on</em> silver orders and the fraud scores; silver orders <em>depends on</em> the orders ingest.” From those edges, the scheduler derives the order itself — and, crucially, derives what can run <em>at the same time</em> and what must <em>wait</em>.</p>
<p>Airflow gives you three primitives, and almost every pipeline is built from just these:</p>
<ul>
<li><strong>Operators</strong> — a unit of work. <code>PythonOperator</code> runs a function, <code>BashOperator</code> a command, <code>SparkSubmitOperator</code> a Spark job. One operator = one box on the clipboard.</li>
<li><strong>Sensors</strong> — a task that does nothing but <em>wait</em> for a condition: a file to land, a partition to appear, an upstream table to be ready. It's how a DAG refuses to start work on inputs that aren't there yet.</li>
<li><strong>Scheduling</strong> — the DAG runs per <code>execution_date</code> on a cron-like interval (<code>@daily</code>, <code>0 2 * * *</code>). Each run is stamped with the logical date it represents, not the wall-clock time it happens to execute.</li>
</ul>
<p>The bash script failed because it encoded order but not <em>readiness</em>. A DAG with a sensor on the orders export simply would not have built the fraud queue until the export landed — late or not.</p>` },
    { type: "code", lang: "python", code: `from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import pendulum

with DAG(
    dag_id="nimbusmart_nightly",
    schedule="0 2 * * *",                       # 02:00 every day
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=2)},
) as dag:

    wait_for_orders = FileSensor(                # do nothing until the export lands
        task_id="wait_for_orders_export",
        filepath="/exports/orders/{{ ds }}/_SUCCESS",
        poke_interval=60, timeout=45 * 60, mode="reschedule",
    )
    ingest_orders = PythonOperator(task_id="ingest_orders_bronze", python_callable=load_orders)
    ingest_fraud  = PythonOperator(task_id="ingest_fraud_scores_bronze", python_callable=load_fraud)
    silver_orders = PythonOperator(task_id="build_silver_orders", python_callable=build_silver)
    review_queue  = PythonOperator(task_id="build_fraud_review_queue", python_callable=build_queue)
    gold          = PythonOperator(task_id="refresh_gold_dashboard", python_callable=refresh_gold)

    # the edges ARE the schedule — Airflow derives order and parallelism from them
    wait_for_orders >> [ingest_orders, ingest_fraud]
    ingest_orders >> silver_orders
    [silver_orders, ingest_fraud] >> review_queue >> gold`, caption: "You declare dependencies (>>); the scheduler derives order, parallelism, and what to hold." },
    { type: "svg", svg: `<svg viewBox="0 0 720 250" font-family="var(--mono)" font-size="11.5">
<defs><marker id="f2arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0 L10 5 L0 10 z" fill="var(--ink2)"/></marker></defs>
<rect x="16" y="96" width="150" height="42" rx="8" fill="var(--paper2)" stroke="var(--accent)" stroke-width="2"/>
<text x="91" y="113" text-anchor="middle" fill="var(--ink)">wait_for_orders</text>
<text x="91" y="129" text-anchor="middle" fill="var(--accent)" font-size="10">SENSOR · pokes 60s</text>
<rect x="216" y="48" width="150" height="40" rx="8" fill="var(--paper2)" stroke="var(--line)"/>
<text x="291" y="72" text-anchor="middle" fill="var(--ink)">ingest_orders</text>
<rect x="216" y="150" width="150" height="40" rx="8" fill="var(--paper2)" stroke="var(--line)"/>
<text x="291" y="174" text-anchor="middle" fill="var(--ink)">ingest_fraud</text>
<rect x="410" y="48" width="150" height="40" rx="8" fill="var(--paper2)" stroke="var(--line)"/>
<text x="485" y="72" text-anchor="middle" fill="var(--ink)">build_silver</text>
<rect x="410" y="150" width="150" height="40" rx="8" fill="var(--paper2)" stroke="var(--rust)"/>
<text x="485" y="167" text-anchor="middle" fill="var(--ink)">review_queue</text>
<text x="485" y="182" text-anchor="middle" fill="var(--rust)" font-size="10">retries=2</text>
<rect x="588" y="99" width="120" height="40" rx="8" fill="var(--paper2)" stroke="var(--line)"/>
<text x="648" y="123" text-anchor="middle" fill="var(--ink)">refresh_gold</text>
<line x1="166" y1="110" x2="214" y2="72" stroke="var(--ink2)" marker-end="url(#f2arr)"/>
<line x1="166" y1="124" x2="214" y2="166" stroke="var(--ink2)" marker-end="url(#f2arr)"/>
<line x1="366" y1="68" x2="408" y2="68" stroke="var(--ink2)" marker-end="url(#f2arr)"/>
<line x1="560" y1="72" x2="470" y2="148" stroke="var(--ink2)" marker-end="url(#f2arr)"/>
<line x1="366" y1="170" x2="408" y2="170" stroke="var(--ink2)" marker-end="url(#f2arr)"/>
<line x1="560" y1="170" x2="600" y2="136" stroke="var(--ink2)" marker-end="url(#f2arr)"/>
<text x="16" y="228" fill="var(--ink2)" font-size="10.5">the sensor gates the whole graph · two ingests run in parallel · review_queue waits on BOTH parents · gold is last</text>
</svg>`, caption: "The clipboard as a graph: edges encode order and parallelism; the sensor holds everything until inputs are ready." },
    { type: "analogy", title: "The shift-supervisor's clipboard", html: `
<p>The NimbusMart night shift doesn't run on a memorized checklist in one person's head — that person going home sick would end the shift. It runs on the supervisor's <strong>clipboard</strong>: every task written down, with arrows showing what must finish before what can start. Unloading can't begin until the truck is at the dock (a <em>sensor</em>: wait for the arrival). Two crews can unload two trucks at once (parallel tasks, no arrow between them). QC can't start on a pallet until it's unloaded (a dependency edge).</p>
<p>And when a forklift operator calls in sick mid-task, the supervisor doesn't cancel the night — the clipboard says that task is unfinished, so it gets reassigned and retried, while everything downstream waits and everything unrelated proceeds. The clipboard is not the work. It's the thing that knows the <em>order</em> of the work, what can overlap, what's blocked, and what to re-do when a step falls over. That is exactly, and only, what Airflow is.</p>` },
    { type: "javaBridge", html: `
<p>You have already built and read this graph — it's a <strong>Jenkins pipeline, but the artifact flowing through it is data, not a <code>.jar</code></strong>. Map it straight across:</p>
<ul>
<li>A Jenkins <code>stage</code> ↔ an Airflow <strong>task</strong> (an operator). A <code>parallel { }</code> block ↔ two tasks with no edge between them.</li>
<li><code>when { branch 'main' }</code> / waiting on an upstream job ↔ a <strong>sensor</strong> gating on a condition before the stage runs.</li>
<li>Jenkins' cron trigger (<code>triggers { cron('H 2 * * *') }</code>) ↔ the DAG's <code>schedule</code> — except Airflow stamps each run with an <code>execution_date</code> and can backfill past dates, which Jenkins has no first-class notion of.</li>
<li>A flaky stage with <code>retry(3)</code> ↔ <code>default_args={"retries": 2}</code>.</li>
</ul>
<p>The one genuinely new idea: a Jenkins pipeline builds “the current code, now.” An Airflow DAG run represents “the pipeline <em>as of</em> a specific logical date,” and that date is a parameter you can replay. Hold that thought — it's the entire subject of the next module.</p>` },
  ],
  lab: {
    tier: "T3",
    understand: {
      engine: "trace",
      trace: "f2-airflow-dag",
      task: `<p>Scrub the <code>nimbusmart_nightly</code> DAG through one real night (<code>execution_date=2026-07-08</code>). Watch four things in order:</p>
<ul>
<li><strong>The sensor waits, it doesn't fail.</strong> The orders export is 14 minutes late — follow the sensor bar poking until the <code>_SUCCESS</code> flag lands. A hard-coded sleep would have broken; the sensor absorbs it.</li>
<li><strong>Parallelism from the graph.</strong> When the sensor clears, both ingest tasks fire at once — they share no edge. Note their durations run concurrently, not back-to-back.</li>
<li><strong>The sick task.</strong> <code>build_fraud_review_queue</code> loses its executor on try 1 (watch the red bar), then the retry policy re-dispatches it and it succeeds — producing <strong>43</strong> orders at or above FRAUD_REVIEW_THRESHOLD (0.80).</li>
<li><strong>Downstream is held, not failed.</strong> <code>refresh_gold_dashboard</code> stays scheduled through the retry, then runs once its parent goes green. Total: 35 min, of which only 14 was compute.</li>
</ul>
<p>The badge says <em>simulation</em>: durations are illustrative, but the state machine — sensor → parallel fan-out → retry → held downstream — is exactly Airflow's.</p>`
    },
    buildWithAI: `I'm learning Airflow. Build me a real, runnable local DAG that mirrors a NimbusMart nightly pipeline, so I can see sensors, parallelism, dependency order, and retries for myself. Assume Python 3.10+ and nothing else installed.

1. Create a folder \`nimbusmart-airflow\` with a venv. Install apache-airflow (constrained install for the matching Python version), pyspark (recent 3.5.x), and pytest. Set AIRFLOW_HOME to a local ./airflow dir and run \`airflow db migrate\` (standalone/SQLite is fine).

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator writing \`data/exports/orders/2026-07-08/orders.csv\` (240 rows: order_id, customer_id, total_amount, status) and \`data/fraud_scores.csv\` (225 rows: order_id, fraud_score 0.01..0.99), seeded so EXACTLY 43 rows have fraud_score >= FRAUD_REVIEW_THRESHOLD (0.80). Do NOT write the export's _SUCCESS flag yet — I want to trigger the sensor by hand.

3. Create \`dags/nimbusmart_nightly.py\` with dag_id "nimbusmart_nightly", schedule "0 2 * * *", catchup=False, and default_args retries=2 / retry_delay 2 minutes. Tasks: a FileSensor wait_for_orders_export (poke_interval 15s, mode="reschedule") on the export's _SUCCESS path; two PythonOperator ingests (orders, fraud) that run in parallel after the sensor; build_silver_orders after the orders ingest; build_fraud_review_queue after BOTH silver + fraud ingest that reads fraud_scores, defines FRAUD_REVIEW_THRESHOLD = 0.80 as a named constant, filters fraud_score >= FRAUD_REVIEW_THRESHOLD (0.80), and writes the count to data/out/review_count.txt; refresh_gold_dashboard last. Wire edges with >>.

4. Create \`test_dag.py\` (pytest) that imports the DAG via DagBag and asserts: it loaded with zero import errors; it has exactly 6 tasks; wait_for_orders_export has no upstream and build_fraud_review_queue lists BOTH build_silver_orders and ingest_fraud_scores_bronze as upstream task_ids; refresh_gold_dashboard is a leaf (no downstream).

5. Show me how to run \`airflow dags test nimbusmart_nightly 2026-07-08\`, then touch the _SUCCESS flag mid-run so I watch the sensor clear, and confirm review_count.txt reads 43. Windows-friendly paths (or WSL note if Airflow needs it).`
  },
  check: [
    {
      type: "mcq",
      q: "The orders export is 14 minutes late. The DAG's first task is a <code>FileSensor</code> on the export's <code>_SUCCESS</code> flag with <code>timeout=45m</code>. What happens?",
      options: [
        "The DAG fails immediately, because the export missed its 02:00 schedule",
        "The sensor keeps poking every 60s and succeeds when the flag lands at 02:14; downstream tasks stay held until then, no page",
        "The downstream ingest runs anyway on whatever data is present, as the old bash script did",
        "The sensor blocks a worker slot for the full 45 minutes regardless of when the file lands"
      ],
      answer: 1,
      explain: "A sensor's job is to wait for readiness, not to fail on lateness. It pokes until the condition is met or the timeout expires; everything downstream is held behind it. (With mode='reschedule' it even frees its worker slot between pokes.) This is precisely the failure mode the bash script had — it encoded order but not readiness."
    },
    {
      type: "mcq",
      q: "In the DAG, <code>ingest_orders_bronze</code> and <code>ingest_fraud_scores_bronze</code> both run right after the sensor clears, at the same time. Why?",
      options: [
        "Airflow always runs exactly two tasks in parallel to balance the workers",
        "There is no dependency edge between them, so the scheduler is free to dispatch both once their shared upstream (the sensor) is green",
        "They were declared in the same Python list, and list order forces parallelism",
        "PythonOperators always run in parallel; only BashOperators run in sequence"
      ],
      answer: 1,
      explain: "Parallelism is derived from the absence of an edge, not declared. Both ingests depend only on the sensor and not on each other, so the scheduler runs them concurrently. Order and concurrency both fall out of the graph's edges."
    },
    {
      type: "predict",
      q: "<code>build_fraud_review_queue</code> is configured as below. Its first attempt dies to a lost executor after 2 minutes. What does the scheduler do, and what does the queue ultimately contain?",
      code: `default_args = {"retries": 2, "retry_delay": timedelta(minutes=2)}
# build_fraud_review_queue try 1: ExecutorLostFailure at 02:29
# the task is idempotent (overwrites its output partition)`,
      options: [
        "It marks the whole DAG failed and skips the remaining tasks",
        "It waits the 2-minute retry_delay, re-dispatches the same task instance, and on success the queue holds 43 orders — the same result try 1 would have produced",
        "It reruns from the very first task (the sensor) to keep the run consistent",
        "It appends try 1's partial output to try 2's, so the queue double-counts"
      ],
      answer: 1,
      explain: "retries=2 means the scheduler re-dispatches the failed task after the delay, not the whole DAG. Because the task is idempotent (it overwrites its partition), the retry reproduces the correct 43-row queue — a transient infra failure costs minutes, not the night. Downstream refresh_gold stays held, not failed, until this task is green."
    },
    {
      type: "mcq",
      q: "Why is the DAG a strict improvement over the old bash script that chained the same jobs with <code>&&</code>?",
      options: [
        "It runs faster, because Airflow compiles the Python tasks to native code",
        "It encodes readiness (sensors) and dependencies (edges) explicitly, so it waits on late inputs, runs independent work in parallel, and retries a fallen task while holding — not failing — downstream",
        "It removes the need for idempotency, because Airflow guarantees each task runs exactly once",
        "It eliminates the orders export entirely by reading straight from the OLTP database"
      ],
      answer: 1,
      explain: "The bash script encoded order but not readiness or recovery: one late input and it silently built on stale data. The DAG makes dependencies and preconditions first-class, so lateness is absorbed, independent steps overlap, and a transient failure is retried in isolation. (Idempotency is still your job — retries make it necessary, not optional; that was module F1.)"
    }
  ],
  fieldNotes: `A logistics analytics team ran their nightly as a chained shell script for eighteen months. The failure that finally forced the migration wasn't dramatic: the upstream orders export slipped from its usual 01:50 to 02:25 one Tuesday because the source DB was vacuuming. The script, kicked off at 02:00, found an empty export directory, and — because \`cp\` of nothing still exits 0 — happily built the entire Silver and Gold layer on the *previous* day's orders. The dashboards were green and completely wrong; the discrepancy surfaced four days later when a regional manager asked why Tuesday's revenue exactly matched Monday's to the cent. The Airflow rewrite was mostly one FileSensor on the export's _SUCCESS flag and letting the scheduler derive the rest from >> edges. The next time the export was late — and it was late again within the month — the DAG simply waited 31 minutes, then ran clean. The team's takeaway: the expensive bug wasn't the late export, it was a pipeline that couldn't tell 'not ready yet' from 'nothing to do.'`
};
