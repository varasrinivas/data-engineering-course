// D5 — Catalyst & Tungsten (Track D, the optimizer)
// T2 sparksim. Verified facts (data/nimbusmart/generate.py, seed 42):
//   orders 240 × fraud_scores 225 inner join = 225; fraud_score >= FRAUD_REVIEW_THRESHOLD (0.80) = 43.
//   Lab result (join → filter >= threshold → select 3 cols) = 43 rows.
export default {
  id: "D5",
  track: "D",
  title: "Catalyst & Tungsten",
  minutes: 26,
  coldOpen: "During review, a NimbusMart engineer flagged what looked like a bug: 'The fraud-review query filters after the join, but explain() shows the filter running *below* the join, inside the fraud_scores scan. Did someone secretly rewrite my code?' Nobody had. Catalyst had — silently, correctly, and for the better. The question is why you'd ever want a runtime that ignores the order you wrote.",
  concept: [
    { type: "prose", html: `
<p>You do not hand Spark an execution plan. You hand it <em>intent</em> — a DataFrame recipe — and a compiler called <strong>Catalyst</strong> turns that intent into an execution plan, rewriting it aggressively on the way. It works in four passes, and <code>explain(True)</code> shows you all four:</p>
<ul>
<li><strong>Parsed logical plan</strong> — your code, transcribed literally. Filter after join, every column you mentioned, nothing moved.</li>
<li><strong>Analyzed logical plan</strong> — column names and types resolved against the catalog. Now Spark knows <code>fraud_score</code> is a double living in <code>fraud_scores</code>.</li>
<li><strong>Optimized logical plan</strong> — the interesting one. Catalyst applies rule-based rewrites: it <em>pushes predicates down</em>, <em>prunes columns</em> you never use, folds constants, collapses projections. Your filter migrates from after the join to inside the scan.</li>
<li><strong>Physical plan</strong> — the runnable one. Catalyst picks concrete operators (broadcast vs sort-merge join, hash vs sort aggregate) using cost estimates, and marks the shuffle boundaries.</li>
</ul>
<p>Two rewrites carry most of the win, and both are visible in the fraud-review query:</p>
<ul>
<li><strong>Predicate pushdown.</strong> You wrote <code>.filter(fraud_score &gt;= FRAUD_REVIEW_THRESHOLD)</code> <em>after</em> the join. Catalyst moves it to the <code>fraud_scores</code> scan — so the rows below the threshold are dropped <em>before</em> they're read into the join, and the join processes 43 rows instead of 225.</li>
<li><strong>Projection pruning.</strong> The query ends in <code>.select("order_id", "customer_id", "fraud_score")</code>. Catalyst walks that requirement back to the scans and reads <em>only</em> those columns off disk — <code>seller_id</code>, <code>total_amount</code>, <code>model_version</code> are never loaded. On columnar Parquet, unread columns are unpaid-for I/O.</li>
</ul>` },
    { type: "code", lang: "python", code: `FRAUD_REVIEW_THRESHOLD = 0.80

review = (
    spark.read.table("orders")
        .join(spark.read.table("fraud_scores"), "order_id")
        .filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)   # written AFTER the join
        .select("order_id", "customer_id", "fraud_score"))        # only 3 columns wanted

# What you wrote (parsed logical plan):
#   Scan orders → Scan fraud_scores → Join → Filter(>= threshold) → Project(3 cols)
#
# What Catalyst RUNS (optimized plan) — same answer, far less work:
#   Scan orders    (project: order_id, customer_id)          <- pruned
#   Scan fraud_scores (project: order_id, fraud_score;
#                      PushedFilter: fraud_score >= threshold) <- filter pushed INTO the scan
#   → Join (now over 43 rows, not 225) → Project
review.show()   # 43 rows — the review queue, identical either way`, caption: "You wrote filter-after-join; Catalyst runs filter-inside-scan. Same 43 rows, a fraction of the work." },
    { type: "prose", html: `
<p>Once Catalyst has the physical plan, <strong>Tungsten</strong> takes over — Spark's execution engine, and the reason the plan runs fast rather than merely being clever. Two moves matter. First, <strong>whole-stage code generation</strong>: instead of interpreting the operator tree row by row (a virtual call per operator per row), Tungsten generates a single tight Java method for the whole stage — scan, filter, project collapse into one loop the JVM's JIT can then compile to machine code. Second, <strong>off-heap columnar memory</strong>: Tungsten stores rows in a compact binary format outside the JVM heap, so it sidesteps object overhead and the garbage collector you met melting the single box in D1.</p>
<p>The division of labor is clean and worth memorizing: <strong>Catalyst decides <em>what</em> to run</strong> (the optimized, physical plan), <strong>Tungsten decides <em>how</em> to run it fast</strong> (codegen, memory layout). Together they're why idiomatic DataFrame code you didn't hand-tune routinely beats the 'clever' version — and why the one thing that defeats them, a Python UDF, is the subject of E5: a UDF is a black box Catalyst can't see into and Tungsten can't fold into its generated loop.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 260" font-family="var(--mono)" font-size="11">
<defs><marker id="d5arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0 L10 5 L0 10 z" fill="var(--ink2)"/></marker></defs>
<text x="20" y="20" fill="var(--ink2)" font-size="11">WHAT YOU WROTE (parsed logical plan) — filter sits above the join, all columns read</text>
<g font-size="10">
<rect x="20" y="30" width="120" height="24" rx="5" fill="var(--paper2)" stroke="var(--line)"/><text x="80" y="46" text-anchor="middle" fill="var(--ink)">Project (3 cols)</text>
<rect x="20" y="60" width="120" height="24" rx="5" fill="var(--paper2)" stroke="var(--rust)"/><text x="80" y="76" text-anchor="middle" fill="var(--rust)">Filter ≥ threshold</text>
<rect x="20" y="90" width="120" height="24" rx="5" fill="var(--paper2)" stroke="var(--line)"/><text x="80" y="106" text-anchor="middle" fill="var(--ink)">Join (225 rows)</text>
<rect x="8" y="122" width="118" height="24" rx="5" fill="var(--paper)" stroke="var(--line)"/><text x="67" y="138" text-anchor="middle" fill="var(--ink)">Scan orders (all)</text>
<rect x="132" y="122" width="130" height="24" rx="5" fill="var(--paper)" stroke="var(--line)"/><text x="197" y="138" text-anchor="middle" fill="var(--ink)">Scan fraud (all)</text>
<line x1="80" y1="60" x2="80" y2="56" stroke="var(--ink2)" marker-end="url(#d5arr)"/>
<line x1="80" y1="90" x2="80" y2="86" stroke="var(--ink2)" marker-end="url(#d5arr)"/>
<line x1="67" y1="122" x2="72" y2="116" stroke="var(--ink2)" marker-end="url(#d5arr)"/>
<line x1="197" y1="122" x2="140" y2="116" stroke="var(--ink2)" marker-end="url(#d5arr)"/>
</g>
<line x1="290" y1="90" x2="360" y2="90" stroke="var(--accent)" stroke-width="2" marker-end="url(#d5arr)"/>
<text x="325" y="82" text-anchor="middle" fill="var(--accent)" font-size="10">Catalyst</text>
<text x="325" y="104" text-anchor="middle" fill="var(--ink2)" font-size="8">rewrites</text>
<text x="380" y="20" fill="var(--ink2)" font-size="11">WHAT CATALYST RUNS (optimized) — filter pushed INTO the scan, columns pruned</text>
<g font-size="10">
<rect x="380" y="30" width="120" height="24" rx="5" fill="var(--paper2)" stroke="var(--line)"/><text x="440" y="46" text-anchor="middle" fill="var(--ink)">Project (3 cols)</text>
<rect x="380" y="66" width="120" height="24" rx="5" fill="var(--paper2)" stroke="var(--line)"/><text x="440" y="82" text-anchor="middle" fill="var(--ink)">Join (43 rows)</text>
<rect x="380" y="108" width="130" height="40" rx="5" fill="var(--paper)" stroke="var(--line)"/><text x="445" y="123" text-anchor="middle" fill="var(--ink)">Scan orders</text><text x="445" y="139" text-anchor="middle" fill="var(--accent)" font-size="8">project: 2 cols</text>
<rect x="524" y="108" width="176" height="40" rx="5" fill="var(--paper)" stroke="var(--rust)" stroke-width="2"/><text x="612" y="121" text-anchor="middle" fill="var(--ink)" font-size="9">Scan fraud_scores</text><text x="612" y="133" text-anchor="middle" fill="var(--rust)" font-size="8">PushedFilter ≥ threshold</text><text x="612" y="144" text-anchor="middle" fill="var(--accent)" font-size="8">project: 2 cols</text>
<line x1="440" y1="66" x2="440" y2="54" stroke="var(--ink2)" marker-end="url(#d5arr)"/>
<line x1="445" y1="108" x2="442" y2="90" stroke="var(--ink2)" marker-end="url(#d5arr)"/>
<line x1="612" y1="108" x2="500" y2="84" stroke="var(--ink2)" marker-end="url(#d5arr)"/>
</g>
<text x="380" y="176" fill="var(--rust)" font-size="10">↑ the pushed-down node: the filter now lives in the fraud_scores scan, so the join sees 43 rows, not 225</text>
<line x1="20" y1="196" x2="700" y2="196" stroke="var(--line)"/>
<text x="20" y="216" fill="var(--ink2)" font-size="11">THEN TUNGSTEN: fuse scan+filter+join+project into one generated loop (whole-stage codegen),</text>
<text x="20" y="234" fill="var(--ink2)" font-size="11">store rows off-heap in columnar binary — no per-row virtual calls, no GC pressure.</text>
<text x="20" y="252" fill="var(--ink2)" font-size="10">Catalyst decides WHAT to run · Tungsten decides HOW to run it fast.</text>
</svg>`, caption: "Catalyst pushes the filter into the fraud_scores scan and prunes columns; Tungsten then fuses the operators into one generated loop." },
    { type: "analogy", title: "The dispatcher who rewrites your pick list", html: `
<p>You hand the NimbusMart dispatcher a pick list in the order it occurred to you: 'walk the whole warehouse, gather every order, <em>then</em> throw away the ones under the review threshold, and by the way I only care about three fields.' A literal-minded runner would do exactly that — haul everything, then discard most of it at the desk.</p>
<p>A good dispatcher reads the <em>whole</em> list first and rewrites your route without changing the outcome: 'don't gather what you'll only discard — check the fraud tag <em>at the shelf</em> and leave the low-score orders where they are (that's <strong>predicate pushdown</strong>); and since you only want three fields, don't photograph the whole label, just those three (that's <strong>projection pruning</strong>).' Same 43 orders arrive at the desk; a fraction of the walking happens.</p>
<p>Catalyst is that dispatcher. The order you wrote is a <em>request</em>, not a route — and the reason you don't hand-optimize is that the dispatcher has already read the whole list and knows the floor better than you do.</p>` },
    { type: "javaBridge", html: `
<p>You already trust a runtime to rewrite what you wrote: the <strong>JIT compiler</strong>. You write clear, naive Java — a loop, a method call, a bounds check — and the JIT, watching it run, inlines the call, hoists the invariant out of the loop, elides the redundant check, unrolls, vectorizes. You did not ask for any of it, and you'd be a fool to hand-write the result. <strong>Catalyst is the JIT for your data plan.</strong></p>
<ul>
<li>JIT inlines a method call ↔ Catalyst collapses adjacent projections.</li>
<li>JIT hoists an invariant out of a loop ↔ Catalyst pushes a predicate down to the scan, so the filter runs once at the source instead of after the join.</li>
<li>JIT is <em>profile-guided</em> — it uses runtime facts to choose ↔ Catalyst is <em>cost-based</em> for the physical plan — it uses table statistics to pick, say, a broadcast join over a sort-merge join.</li>
</ul>
<p>And Tungsten completes the parallel literally: its <strong>whole-stage code generation</strong> emits a single Java method for an entire stage, which the JVM's JIT then compiles to machine code — Spark generating source for the JIT to optimize. The upgrade to your instinct: just as fighting the JIT with hand-rolled 'clever' bytecode usually loses, hand-reordering your DataFrame ops usually does nothing, because Catalyst already did it. Write the clear version; read <code>explain()</code> to confirm the rewrite; spend your cleverness only where the optimizer is blind — a UDF (E5).</p>` },
  ],
  lab: {
    tier: "T2",
    understand: {
      engine: "sparksim",
      datasets: ["orders", "fraud_scores"],
      task: `<p><strong>Make Catalyst move your filter, then find where it went.</strong> The starter joins <code>orders</code> to <code>fraud_scores</code> and selects five columns — no filter, so all 225 joined rows flow through. Two edits: add <code>.filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)</code> after the join, and narrow the <code>.select(...)</code> to just <code>order_id</code>, <code>customer_id</code>, <code>fraud_score</code>. Run.</p>
<p>Now open the plan view and compare the three levels. In the <em>parsed</em> plan your filter sits above the join, where you wrote it. In the <em>optimized</em> and <em>physical</em> plans, hunt for the <strong>pushed-down node</strong>: the filter has migrated <em>into</em> the <code>fraud_scores</code> scan as a <code>PushedFilter</code>, and the scans now read only the columns you kept — <code>seller_id</code> and <code>total_amount</code> are gone. You didn't reorder anything; Catalyst did, and the result is the same 43-row review queue for less work.</p>`,
      starterCode: `orders = spark.read.table("orders")
fraud = spark.read.table("fraud_scores")

review = (orders
    .join(fraud, "order_id")
    .select("order_id", "customer_id", "seller_id", "fraud_score", "total_amount"))

review.show()`,
      solutionCode: `orders = spark.read.table("orders")
fraud = spark.read.table("fraud_scores")

review = (orders
    .join(fraud, "order_id")
    .filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)
    .select("order_id", "customer_id", "fraud_score"))

review.show()`,
      expect: { rows: 43, cols: ["order_id", "customer_id", "fraud_score"] },
      dagNotes: `<p>Read the plan top to bottom. <strong>Parsed:</strong> Project → Filter → Join → two full Scans — exactly your source order. <strong>Optimized:</strong> the <code>fraud_score >= FRAUD_REVIEW_THRESHOLD</code> predicate is <em>pushed down</em> into the <code>fraud_scores</code> scan (the rust-outlined node), so the join operates on 43 rows instead of 225; and projection pruning walks your 3-column <code>select</code> back to the scans, which now read only the columns actually needed — <code>seller_id</code> and <code>total_amount</code> are never loaded. <strong>Physical:</strong> same shape with a concrete join operator and a <code>PushedFilters: [fraud_score &gt;= FRAUD_REVIEW_THRESHOLD]</code> line on the scan. The result grid is 43 rows either way — Catalyst changed the route, not the destination.</p>`
    },
    buildWithAI: `I'm learning Catalyst (predicate pushdown, projection pruning) and Tungsten (whole-stage codegen). Set up a real local PySpark project where I can SEE the optimizer rewrite my plan. Assume Python 3.10+ and nothing else installed.

1. Create a project folder \`nimbusmart-catalyst\` with a venv, and install pyspark (pin a recent 3.5.x) and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator writing TWO Parquet files into \`data/\` (Parquet matters — it's columnar, so projection pruning is visible):
   - \`orders\`: 240 rows — order_id (O-10001..O-10240), customer_id (C-0001..C-0060), seller_id (force ~35% to S-777), total_amount (8..950, 2dp), country
   - \`fraud_scores\`: scores for exactly 225 of those orders (15 unscored) — order_id, fraud_score (0.01..0.99, 2dp), model_version. Make exactly 43 rows have fraud_score >= 0.80 (that's FRAUD_REVIEW_THRESHOLD).
   Print how many fraud rows are >= the threshold so I can confirm 43.

3. Create \`catalyst_lab.py\` that:
   - defines FRAUD_REVIEW_THRESHOLD = 0.80 as a named constant
   - builds a local SparkSession, reads both Parquet files (no inferSchema needed — Parquet carries its schema)
   - builds review = orders.join(fraud, "order_id").filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD).select("order_id", "customer_id", "fraud_score")
   - prints review.explain(True) — ALL FOUR plans (parsed, analyzed, optimized, physical)
   - prints review.count()

4. Create \`test_catalyst_lab.py\` (pytest) asserting:
   - review.count() equals the number of fraud rows >= FRAUD_REVIEW_THRESHOLD computed by re-reading the Parquet with pandas/pyarrow (do NOT hardcode 43)
   - the physical-plan string contains "PushedFilters" AND mentions fraud_score (proof the predicate reached the scan)
   - the physical-plan string does NOT read seller_id or total_amount for the fraud path (proof of projection pruning) — assert those column names don't appear in the ReadSchema of the scans

5. Run the generator, the lab, and pytest. Point me at the PushedFilters line and the pruned ReadSchema in the physical plan, and explain how whole-stage codegen (the "*(1)" markers with a WholeStageCodegen node) fuses the scan+filter+join into one generated loop. Windows-friendly paths please.`
  },
  check: [
    {
      type: "mcq",
      q: "The cold-open engineer wrote <code>.filter(...)</code> after the join but <code>explain()</code> shows it running inside the <code>fraud_scores</code> scan. What happened, and is it a bug?",
      options: [
        "A bug — the filter is running in the wrong place and will drop the wrong rows",
        "Not a bug — Catalyst applied predicate pushdown, moving the filter to the scan so fewer rows enter the join; the result is identical, the work is less",
        "Not a bug, but pointless — pushdown changes the plan's shape without changing performance",
        "A bug in explain() — the display is wrong; the filter really does run after the join"
      ],
      answer: 1,
      explain: "Predicate pushdown is a correctness-preserving rewrite: filtering fraud_scores before the join yields the same 43 matched rows as filtering after, but the join processes far fewer rows. Catalyst treats your written order as intent, not instruction — and here the rewrite is a strict win."
    },
    {
      type: "predict",
      q: "On the NimbusMart seed data, how many rows does this print — and how many <code>fraud_scores</code> columns does the scan actually read?",
      code: `FRAUD_REVIEW_THRESHOLD = 0.80
review = (orders
    .join(fraud_scores, "order_id")
    .filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)
    .select("order_id", "customer_id", "fraud_score"))
print(review.count())`,
      options: [
        "43 rows; the fraud_scores scan reads only order_id and fraud_score (model_version pruned)",
        "225 rows; the scan reads all fraud_scores columns",
        "240 rows; the filter is applied after count()",
        "43 rows; but the scan must read every column to evaluate the filter"
      ],
      answer: 0,
      explain: "43 orders have fraud_score >= FRAUD_REVIEW_THRESHOLD, so count() is 43. Projection pruning means the scan reads only the columns the plan needs downstream — order_id (join key) and fraud_score (filter + select); model_version and scored_at are never loaded. The filter needs fraud_score, not every column."
    },
    {
      type: "mcq",
      q: "Which task belongs to <strong>Tungsten</strong>, not Catalyst?",
      options: [
        "Pushing the fraud-score predicate down into the scan",
        "Pruning unused columns so the scan reads fewer bytes",
        "Choosing a broadcast join over a sort-merge join based on table size",
        "Generating a single Java method for the whole stage (whole-stage codegen) and laying rows out off-heap in columnar binary"
      ],
      answer: 3,
      explain: "Catalyst decides WHAT to run — pushdown, pruning, join-strategy selection all produce the optimized/physical plan. Tungsten decides HOW to run it fast: whole-stage code generation and off-heap columnar memory. Plan rewrites are Catalyst; codegen and memory layout are Tungsten."
    },
    {
      type: "mcq",
      q: "A teammate proposes hand-reordering the DataFrame ops — manually filtering before the join and selecting columns early — 'to help Spark.' On idiomatic DataFrame code, what's the likely effect?",
      options: [
        "A large speedup — Catalyst never reorders operations, so you must do it yourself",
        "Essentially no change — Catalyst already pushes predicates down and prunes projections, so the hand-tuning reproduces a rewrite it had already made",
        "A slowdown — manual reordering disables Catalyst entirely",
        "It breaks correctness — filtering before a join changes the result"
      ],
      answer: 1,
      explain: "For relational DataFrame ops, Catalyst already does predicate pushdown and projection pruning, so hand-reordering usually reproduces the optimized plan and buys nothing — like hand-inlining a method the JIT already inlines. Cleverness pays off only where the optimizer is blind, e.g. inside a Python UDF (E5)."
    }
  ],
  fieldNotes: `A data team spent a sprint 'optimizing' a slow enrichment job by hand — manually reordering filters ahead of joins, splitting selects, rewriting a groupBy as a window and back — and shaved off almost nothing, because Catalyst had already produced that plan; the diffs were cosmetic. The actual culprit surfaced only when someone read the physical plan instead of guessing: a single Python UDF, <code>def risk_bucket(score): ...</code>, sat in the middle of the pipeline. Catalyst can't see inside a UDF, so it couldn't push the fraud filter through it — the predicate stalled above the UDF and the job scanned and deserialized every row into Python before discarding 80% of them, one row at a time across the JVM/Python boundary. Replacing the UDF with a native <code>F.when(...).otherwise(...)</code> expression let the filter push all the way to the scan and let Tungsten fold the whole stage into generated code: 47 minutes to 9. The lesson the team taped to the wall: 'don't out-think Catalyst; read its plan. Your job isn't to reorder the operations — it's to never hand it an operator it can't see through.'`
};
