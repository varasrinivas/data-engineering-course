// H4 — Orchestrate, Document, Demo (capstone finale)
// T3 trace: engine/traces/h4-platform-run.json — one full nightly run of the NimbusMart platform.
// Facts referenced (verified against seed.js, seed 42):
//   6 sources · 3 layers · 21 tasks · Bronze rows 1432 · Silver orders 225 · quarantine 18 (15 orders + 3 customers)
//   review queue = 43 (FRAUD_REVIEW_THRESHOLD 0.80) · S-777 owns 80/240 and 19/43 · revenue total €114,847 · FR top €22,655
export default {
  id: "H4",
  track: "H",
  title: "Orchestrate, Document, Demo",
  minutes: 26,
  coldOpen: "It's demo day. The pipeline runs perfectly when you run it — cell by cell, in order, babysitting each step. But the VP doesn't want to watch you run notebooks at 2am; she wants to arrive at 6am to a dashboard that's already right. The last thing the capstone asks is the hardest: make the whole platform run itself, on schedule, unattended — and prove, every single night, that the review queue still says 43.",
  concept: [
    { type: "prose", html: `
<p>You have all the parts: Bronze ingest (H2), Silver gates and quarantine (H2), SCD2 dimensions and Gold marts (H3), and the acceptance test that pins the answer (H1). Orchestration is the wiring that turns a pile of correct scripts into a <strong>platform</strong> — something that runs unattended, in the right order, recovers from failure, and tells you when it's wrong before a human notices.</p>
<p>Three jobs, and the capstone isn't done until all three exist:</p>
<ul>
<li><strong>Orchestrate.</strong> Express the pipeline as a DAG: tasks with dependencies. Sensors wait on the six source exports; ingest tasks fan out; Silver gates run once their Bronze parents succeed; Gold marts wait on Silver; the dashboard refresh is a leaf that fires only when everything upstream is green. The orchestrator's core promise: <em>a task runs only when all its parents succeeded</em>, so one red upstream short-circuits its whole subtree instead of computing on broken data.</li>
<li><strong>Document.</strong> Lineage: every dashboard tile traces back through its Gold mart, its Silver table, its Bronze source, to a logical date. When finance asks "where did the 43 come from?", the answer is a lineage graph, not an archaeology dig.</li>
<li><strong>Demo.</strong> The payoff: at 06:00 the dashboard lights up on its own — revenue by country, the review queue at 43, quarantine counts, freshness green — and it reconciles to the acceptance test you wrote in H1. That reconciliation, run automatically every night, is what separates a demo from a platform.</li>
</ul>` },
    { type: "code", lang: "python", code: `# The platform DAG, sketched as dependencies (Airflow-style).
# The orchestrator runs each task only after ALL its upstreams succeed.
FRAUD_REVIEW_THRESHOLD = 0.80

wait_for_sources  >> ingest_bronze          # 6 sensors → 6 Bronze appends
ingest_bronze     >> silver_gate            # type, dedupe, quarantine failures
silver_gate       >> [build_scd2_dim, build_fact_orders]
[build_scd2_dim,
 build_fact_orders] >> [revenue_by_country, fraud_review_queue]   # Gold marts
fraud_review_queue >> assert_queue_is_43    # the H1 acceptance test, as a task
assert_queue_is_43 >> refresh_dashboard     # leaf: fires only if the assert passed

# assert_queue_is_43 is not optional decoration — it is a gate.
# If the nightly run produces anything but 43, the dashboard never refreshes,
# and on-call is paged with "review queue = <n>, expected 43" instead of
# finance discovering a wrong number in a board meeting.`, caption: "The acceptance test is a task in the DAG — the dashboard can't go green unless the queue is 43." },
    { type: "prose", html: `
<p>One idea makes the whole thing reproducible, and it's the same one from the backfill lab (F3): <strong>the pipeline is a function of its logical date, not of <code>now()</code></strong>. Every task takes an <code>execution_date</code> parameter and computes <em>f(code, inputs, execution_date)</em>. Re-run last Tuesday's pipeline today and it must produce byte-identical marts to what it produced last Tuesday. Any <code>CURRENT_DATE</code> or <code>now()</code> buried in transformation logic silently breaks that — and breaks your ability to backfill, because a rerun would no longer reproduce the original.</p>
<p>This is why the acceptance test is worth wiring in as a DAG task rather than a manual check. On the seed data the answer is 43; in production the number moves daily, but the <em>invariant</em> holds: the count the pipeline computes must equal the count an independent query computes over the same inputs. The night an upstream schema change drops the fraud-score column, the review queue collapses toward zero — and the assert task catches it at 02:09 and refuses to light the dashboard, six hours before the VP would have seen a suspiciously empty queue.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 260" font-family="var(--mono)" font-size="10">
<defs><marker id="h4a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0 0 L10 5 L0 10 z" fill="var(--ink2)"/></marker></defs>
<rect x="12" y="100" width="96" height="40" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="60" y="118" text-anchor="middle" fill="var(--ink)">sensors</text><text x="60" y="132" text-anchor="middle" fill="var(--ink2)" font-size="8">6 sources</text>
<rect x="140" y="100" width="96" height="40" rx="8" fill="var(--paper2)" stroke="var(--rust)"/><text x="188" y="118" text-anchor="middle" fill="var(--rust)">Bronze</text><text x="188" y="132" text-anchor="middle" fill="var(--ink2)" font-size="8">append</text>
<rect x="268" y="100" width="96" height="40" rx="8" fill="var(--paper2)" stroke="var(--accent)"/><text x="316" y="118" text-anchor="middle" fill="var(--accent)">Silver</text><text x="316" y="132" text-anchor="middle" fill="var(--ink2)" font-size="8">gate+quarantine</text>
<rect x="396" y="56" width="96" height="40" rx="8" fill="var(--paper2)" stroke="var(--gold)"/><text x="444" y="74" text-anchor="middle" fill="var(--gold)">SCD2 dim</text><text x="444" y="88" text-anchor="middle" fill="var(--ink2)" font-size="8">C-0042</text>
<rect x="396" y="144" width="96" height="40" rx="8" fill="var(--paper2)" stroke="var(--gold)"/><text x="444" y="162" text-anchor="middle" fill="var(--gold)">fact + marts</text><text x="444" y="176" text-anchor="middle" fill="var(--ink2)" font-size="8">revenue · queue</text>
<rect x="524" y="100" width="86" height="40" rx="8" fill="var(--paper2)" stroke="var(--rust)" stroke-width="2"/><text x="567" y="118" text-anchor="middle" fill="var(--rust)" font-weight="bold">assert</text><text x="567" y="132" text-anchor="middle" fill="var(--ink)" font-size="8">queue == 43</text>
<rect x="640" y="100" width="72" height="40" rx="8" fill="var(--paper2)" stroke="var(--green)" stroke-width="2"/><text x="676" y="118" text-anchor="middle" fill="var(--green)" font-weight="bold">dashboard</text><text x="676" y="132" text-anchor="middle" fill="var(--ink2)" font-size="8">green</text>
<line x1="108" y1="120" x2="138" y2="120" stroke="var(--ink2)" marker-end="url(#h4a)"/>
<line x1="236" y1="120" x2="266" y2="120" stroke="var(--ink2)" marker-end="url(#h4a)"/>
<line x1="364" y1="115" x2="394" y2="80" stroke="var(--ink2)" marker-end="url(#h4a)"/>
<line x1="364" y1="125" x2="394" y2="160" stroke="var(--ink2)" marker-end="url(#h4a)"/>
<line x1="492" y1="80" x2="522" y2="112" stroke="var(--ink2)" marker-end="url(#h4a)"/>
<line x1="492" y1="164" x2="522" y2="128" stroke="var(--ink2)" marker-end="url(#h4a)"/>
<line x1="610" y1="120" x2="638" y2="120" stroke="var(--green)" stroke-width="2" marker-end="url(#h4a)"/>
<text x="16" y="30" fill="var(--ink)" font-size="11">The platform DAG — one nightly run, 02:00 → 06:00</text>
<text x="16" y="220" fill="var(--ink2)" font-size="9">Each task fires only when every parent is green. The assert gate sits between the marts and the</text>
<text x="16" y="234" fill="var(--ink2)" font-size="9">dashboard: if the queue isn't 43, the dashboard stays dark and on-call is paged — not the VP.</text>
</svg>`, caption: "The whole course, wired into one DAG. The acceptance test is the gate the dashboard must pass through." },
    { type: "analogy", title: "The shift supervisor's clipboard", html: `
<p>A NimbusMart fulfilment centre doesn't run because each worker is individually competent; it runs because the <strong>shift supervisor's clipboard</strong> says who does what, in what order, and what happens when someone calls in sick. Receiving can't start until the trucks are confirmed at the dock (sensors). QC can't inspect a pallet that receiving hasn't logged (Silver waits on Bronze). The showroom can't be stocked from a QC bay that's still empty (Gold waits on Silver). And the store doesn't open its doors until the floor manager walks the acceptance checklist (the assert gate) — if the count is wrong, the doors stay shut and someone gets a call, rather than customers wandering a half-stocked floor.</p>
<p>The orchestrator is that clipboard, executed by a machine at 2am. The DAG is the ordering; the sensors are "confirm the trucks"; the retry policy is "what happens when someone calls in sick"; and the dashboard opening on time is the store opening on time — because every prior line on the clipboard was checked off, in order, unattended.</p>` },
    { type: "javaBridge", html: `
<p>An Airflow DAG is a <strong>Jenkins pipeline for data</strong>, and you already know the shape: stages with dependencies, a stage runs only if its upstreams passed, a red stage fails the build and blocks deploy. Map it straight across:</p>
<ul>
<li><strong>Stages → tasks</strong>, <strong>stage dependencies → the DAG edges</strong>, <strong>the build agent → the executor</strong>. A sensor is a pipeline step that polls until an artifact is available — the same as waiting on an upstream job's artifact before the next stage.</li>
<li><strong>The acceptance-test task is your deploy gate.</strong> You don't let a build promote to production because it compiled; it promotes because the integration suite is green. The <code>assert queue == 43</code> task is exactly that gate — the dashboard is the deploy target, and it doesn't "ship" (refresh) unless the test passes.</li>
<li><strong>execution_date is your build's immutable parameters.</strong> A reproducible pipeline run is a reproducible build: pin the inputs (the logical date), forbid ambient state (<code>now()</code> is the data equivalent of reading the wall clock or a mutable env var mid-build), and any rerun reproduces the artifact. That's what makes a backfill safe and a green demo trustworthy.</li>
</ul>` },
  ],
  lab: {
    tier: "T3",
    understand: {
      engine: "trace",
      trace: "h4-platform-run",
      task: `<p><strong>Watch the whole NimbusMart platform run once, end to end — the capstone's payoff.</strong> Scrub the trace from 02:00 to 06:00 and follow one nightly run: six source sensors clearing, Bronze appends landing 1,432 raw rows, Silver's quality gates routing 18 rows to quarantine (15 unscored orders + 3 null-city customers), the SCD2 dimension versioning C-0042's Munich → Hamburg move, and the Gold marts building.</p>
<p>Three moments to watch for. First, the <strong>quality gate</strong> at t=9: kept + quarantined conserves the row count — nothing vanishes. Second, the <strong>skew</strong> at t=18–21: seller S-777's partition (80 of 240 orders) is the fat one every executor waits on, and it concentrates 19 of the 43 review orders — E3's hot key, live in production. Third, the finale: at t=24 the review queue lands at exactly <strong>43</strong>, reconciling to your H1 acceptance test, and at t=30 the dashboard tiles light up green — revenue €114,847, review queue 43, freshness green. Every module you took is one task on this clipboard.</p>`
    },
    buildWithAI: `I want to orchestrate my NimbusMart capstone as a real DAG and prove it every night with an acceptance test. Set up a runnable local project. Assume Python 3.10+ and nothing installed.

1. Create \`nimbusmart-platform\` with a venv; install apache-airflow (or, if you prefer zero services, a tiny custom DAG runner is fine — see step 4), pyspark (recent 3.5.x), and pytest.

2. Reuse the generator from earlier modules: a deterministic (random.seed(42)) \`generate_data.py\` writing ALL SIX NimbusMart sources into \`data/\` (orders 240, order_events ~361 with drift, customers 60 + customer_updates 8 with C-0042 moving cities, products 40 nested, fraud_scores 225 with 15 unscored / 43 >= FRAUD_REVIEW_THRESHOLD which is 0.80 / 4 == 0.80, payments ~228, couriers 12 + courier_pings ~278 late).

3. Create task modules mirroring the medallion pipeline: \`bronze.py\` (append raw), \`silver.py\` (type + dropDuplicates + isNotNull gate + quarantine table), \`gold.py\` (SCD2 dim_customer, fact_orders, revenue_by_country, fraud_review_queue). Define FRAUD_REVIEW_THRESHOLD = 0.80 as a named constant in a shared \`config.py\`.

4. Create \`platform_dag.py\`: wire the tasks with dependencies — six source sensors (FileSensor or a poll loop on the expected files) >> bronze ingest >> silver gate >> [scd2_dim, fact_orders] >> [revenue_by_country, fraud_review_queue] >> assert_queue_is_43 >> refresh_dashboard. Every task must accept an execution_date parameter and use it instead of now(). The refresh_dashboard task must be DOWNSTREAM of assert_queue_is_43 so the dashboard cannot update unless the assert passed.

5. Create \`assert_queue.py\` (called by the assert task AND by pytest): recompute the review-queue count independently (plain csv module over data/) and compare it to the Gold fraud_review_queue count. Raise if they differ; the message must read "review queue = <n>, expected <m>".

6. Create \`test_platform.py\` (pytest) asserting: the full DAG runs top to bottom with 0 task failures; the review queue == 43; deliberately corrupting one input (drop the fraud_score column) makes assert_queue_is_43 FAIL and leaves refresh_dashboard un-run (prove the gate blocks the dashboard); and re-running the DAG for the same execution_date twice yields identical Gold marts (idempotency / reproducibility).

7. Run generator → trigger the DAG once → pytest. Print the final dashboard tiles (revenue by country, review queue = 43, quarantine count) and confirm the run is green. Windows-friendly paths.`
  },
  check: [
    {
      type: "mcq",
      q: "In the platform DAG, why is <code>refresh_dashboard</code> placed <em>downstream</em> of the <code>assert_queue_is_43</code> task rather than run in parallel with it?",
      options: [
        "To save cluster resources by running them sequentially",
        "So the dashboard can only refresh if the acceptance test passed — a wrong review-queue count blocks the update and pages on-call instead of surfacing a bad number to executives",
        "Because Airflow can't run two tasks at the same time",
        "Because the dashboard needs the assert task's output columns"
      ],
      answer: 1,
      explain: "The orchestrator runs a task only when all its parents succeeded, so making the dashboard a child of the assert gate means a failed acceptance test short-circuits the refresh. The dashboard stays on last-known-good, on-call gets 'review queue = n, expected 43', and the wrong number never reaches a board meeting. That's the difference between a demo and a platform: the correctness check is wired into the flow, not run by hand."
    },
    {
      type: "mcq",
      q: "At t=18–21 the trace shows three executors idle while executor-3 sits at 96% on seller S-777's partition. What is this, and why is it tolerated here?",
      options: [
        "A cluster hardware failure — executor-3 should be restarted",
        "Data skew: S-777 owns 80 of 240 orders, so its shuffle partition is ~3.6× the others and the stage waits on the one fat task — tolerated at this scale because it costs seconds, but you'd salt it at 10× the data",
        "A network partition between the driver and executor-3",
        "The optimizer chose a broadcast join by mistake"
      ],
      answer: 1,
      explain: "This is E3's hot key, live in the nightly run. The join shuffles rows by seller, and one seller holds a third of them, so a single task carries far more than its share and the stage is only as fast as that task. On NimbusMart's small data it's a few extra seconds — not worth salting. At production scale the same shape becomes the outage E3 taught you to pre-empt with salting or an adaptive skew join."
    },
    {
      type: "predict",
      q: "The Silver gate at t=9 keeps 225 orders and quarantines 15; it also keeps 57 customers and quarantines 3. What total row count must Silver-plus-quarantine equal for orders, and what invariant does that check?",
      code: `silver_orders      = 225   # passed the isNotNull(fraud_score) gate
quarantine_orders  = 15    # unscored → routed, not dropped
print(silver_orders + quarantine_orders)   # must equal Bronze orders`,
      options: ["225 — quarantine isn't counted", "240 — kept plus quarantined equals ingested; the conservation invariant proving nothing was silently dropped", "43 — only the review queue counts", "255 — quarantine adds new rows"],
      answer: 1,
      explain: "225 + 15 = 240, the Bronze order count. Kept + quarantined = ingested is the conservation invariant that makes Silver auditable: every row is either trusted or explicitly held with a reason, and none vanished. An inner join would have produced 225 Silver rows with no trace of the other 15 — the same number of good rows, but a broken invariant and an invisible data-loss bug."
    },
    {
      type: "predict",
      q: "At t=24 the pipeline's review queue lands. What number must it be to match the H1 acceptance test, and what happens at t=27–30 if it does?",
      code: `# end-to-end pipeline output, computed by the Gold layer:
review_queue = scored_orders.filter(
    F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)   # 0.80
assert review_queue.count() == 43        # the H1 contract, as a DAG task`,
      options: [
        "225 — the assert fails and the dashboard still refreshes",
        "43 — the assert passes, the leaf tasks turn green, and at 06:00 the dashboard lights up (revenue €114,847, review queue 43, freshness green)",
        "39 — the boundary orders are excluded and the run is marked green anyway",
        "0 — the pipeline never produces a queue"
      ],
      answer: 1,
      explain: "The full Bronze→Silver→Gold run reproduces the exact 43 that H1's SQL acceptance test pinned on the same seed — that reconciliation is the whole point of the capstone. With the assert green, its downstream leaves (dashboard, freshness, reconciliation) fire, and the executive dashboard lights up on schedule. Six messy sources became one number that three independent systems now agree on: 43."
    }
  ],
  fieldNotes: `The gap between "my notebook produces the right answer" and "the platform produces the right answer at 6am unattended" is where most data projects quietly die. A team I worked with had a flawless analysis that took an engineer 40 minutes of manual cell-running each morning; it was correct every time she ran it, and it ran maybe three mornings a week when she wasn't in meetings. It was never a platform — it was a person. Orchestrating it took a week and felt like a step backward (more code, more infrastructure, for an answer they "already had"), right up until the morning the fraud-score feed shipped an hour late. The old manual process would have silently computed on stale data; the new DAG's sensor blocked, the run waited, the assert gate held the dashboard, and on-call got one clear page instead of finance getting a wrong board deck. That was the moment the value landed: the orchestration wasn't for the days everything worked — it was for the one day in thirty that didn't, when the platform caught what a tired human at 6am would have missed. The acceptance test wired into the DAG is the cheapest insurance in data engineering: one query, run every night, that turns "the number looks off" into a page at 02:09 instead of an apology at the quarterly review.`
};
