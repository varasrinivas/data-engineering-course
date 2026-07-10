// C3 — Pandas as the Gateway (Track C, Tier 1 / Pyodide)
// Verified facts used by lab + checks (data/nimbusmart/generate.py, seed 42):
//   orders=240 across 7 countries; fraud_scores=225 (15 unscored)
//   orders.merge(fraud_scores, on="order_id", how="inner") -> 225 rows
//   orders.groupby("country")["total_amount"].sum(): FR highest at 22655.38
//   orders.groupby("country") -> 7 groups (DE US IN BR JP FR AU)
export default {
  id: "C3",
  track: "C",
  title: "Pandas as the Gateway",
  minutes: 24,
  coldOpen: "An analyst ships a revenue-by-country notebook that runs in two seconds on a 10,000-row sample and is declared production-ready. On the full export it hits an OOM kill at 3 a.m. — the whole frame, plus every intermediate copy pandas made along the way, has to fit in one machine's RAM at once, and it didn't. The logic was perfect. The assumption — that one machine's memory is infinite — was the bug, and it's the exact assumption Spark exists to remove.",
  concept: [
    { type: "prose", html: `
<p><strong>pandas is the DataFrame you learn the mental model on, before you distribute it.</strong> A pandas <code>DataFrame</code> is a table with labeled columns and an index, held entirely in one process's memory, with operations that run <em>eagerly</em> — the moment you write them. Almost everything you'll do to data reduces to three verbs, and they're the same three you'll meet in Spark:</p>
<ul>
<li><strong>groupBy → aggregate</strong> — <code>df.groupby("country")["total_amount"].sum()</code>: split rows into groups, compute one number per group, combine into a result. This is “split-apply-combine.”</li>
<li><strong>merge (join)</strong> — <code>orders.merge(fraud, on="order_id")</code>: line up two frames on a key. An inner merge keeps only matching keys; a left merge keeps all left rows and fills the misses with <code>NaN</code>.</li>
<li><strong>filter / project</strong> — boolean masks (<code>df[df.total_amount &gt;= 500]</code>) and column selection.</li>
</ul>
<p>Learn these on one machine where you can <code>print()</code> the whole frame and see what happened. The Spark you meet in Track D has the <em>same three verbs with the same names</em> — the only thing that changes is that the rows are spread across a cluster and nothing runs until you ask for a result.</p>` },
    { type: "code", lang: "python", code: `import pandas as pd

FRAUD_REVIEW_THRESHOLD = 0.80

# 1. merge — inner keeps only orders that HAVE a fraud score (15 unscored drop out)
scored = orders.merge(fraud_scores, on="order_id", how="inner")   # 240 -> 225 rows

# 2. filter — a boolean mask, evaluated eagerly, right now
flagged = scored[scored["fraud_score"] >= FRAUD_REVIEW_THRESHOLD]  # the review candidates

# 3. groupBy -> aggregate — split by country, sum within each, combine
revenue = (orders
    .groupby("country")["total_amount"]
    .sum()
    .sort_values(ascending=False)
    .round(2))
# country
# FR    22655.38   <- one row per country; the individual orders are gone
# US    19234.03
# ...

# .agg() runs several aggregates at once — like SELECT COUNT(*), SUM(x) ... GROUP BY
summary = orders.groupby("country").agg(
    orders=("order_id", "count"),
    revenue=("total_amount", "sum"),
)`, caption: "merge, mask, groupby-agg — eager, in-memory, one machine. Same verbs you'll call on a Spark DataFrame in Track D." },
    { type: "svg", svg: `<svg viewBox="0 0 720 210" font-family="var(--mono)" font-size="12">
<text x="20" y="22" fill="var(--ink2)" font-size="11">SPLIT · APPLY · COMBINE — what groupby("country")["total_amount"].sum() actually does</text>
<rect x="20" y="36" width="150" height="120" rx="8" fill="var(--paper2)" stroke="var(--line)"/>
<text x="30" y="56" fill="var(--ink)">FR  120</text><text x="30" y="74" fill="var(--ink)">US  300</text><text x="30" y="92" fill="var(--ink)">FR  400</text><text x="30" y="110" fill="var(--ink)">US  210</text><text x="30" y="128" fill="var(--ink)">FR  80</text>
<text x="30" y="150" fill="var(--ink2)" font-size="10">one frame, all rows</text>
<line x1="176" y1="96" x2="214" y2="96" stroke="var(--ink2)" stroke-dasharray="4 3"/><text x="176" y="88" fill="var(--ink2)" font-size="9">SPLIT</text>
<rect x="220" y="42" width="120" height="46" rx="8" fill="var(--paper2)" stroke="var(--accent)"/><text x="230" y="60" fill="var(--accent)">FR 120</text><text x="230" y="80" fill="var(--accent)">FR 400 · FR 80</text>
<rect x="220" y="104" width="120" height="46" rx="8" fill="var(--paper2)" stroke="var(--accent)"/><text x="230" y="122" fill="var(--accent)">US 300</text><text x="230" y="142" fill="var(--accent)">US 210</text>
<line x1="346" y1="96" x2="384" y2="96" stroke="var(--ink2)" stroke-dasharray="4 3"/><text x="346" y="88" fill="var(--ink2)" font-size="9">APPLY sum</text>
<rect x="392" y="42" width="90" height="46" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="402" y="70" fill="var(--ink)">FR 600</text>
<rect x="392" y="104" width="90" height="46" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="402" y="132" fill="var(--ink)">US 510</text>
<line x1="488" y1="96" x2="526" y2="96" stroke="var(--rust)" stroke-dasharray="4 3"/><text x="488" y="88" fill="var(--rust)" font-size="9">COMBINE</text>
<rect x="534" y="66" width="160" height="60" rx="8" fill="var(--paper2)" stroke="var(--rust)"/><text x="544" y="90" fill="var(--rust)">FR 600</text><text x="544" y="112" fill="var(--rust)">US 510</text>
<text x="534" y="144" fill="var(--ink2)" font-size="10">one row per group</text>
</svg>`, caption: "Split rows into per-country buckets, apply sum to each, combine into one row per country — the shape of every aggregation." },
    { type: "analogy", title: "Sorting the belt into bins, then weighing each bin", html: `
<p>Picture the NimbusMart sortation belt. A <code>groupby("country")</code> is the diverter that pushes each parcel down the chute for its destination country — <strong>split</strong>. At the end of each chute sits a scale that totals whatever landed there — <strong>apply</strong>. A clerk copies each chute's total onto one line of a manifest — <strong>combine</strong>. What you hand upstairs is the manifest: one number per country, not the parcels.</p>
<p>A <code>merge</code> is the manifest-matching desk: two stacks of paper, one keyed by tracking number, and a clerk pairing them up. An <em>inner</em> match sets aside anything without a partner in both stacks (the 15 orders with no fraud score); a <em>left</em> match keeps every order and just leaves the fraud column blank where no score arrived. And the whole warehouse fits under one roof — that's the pandas assumption. Track D is what happens when the parcels no longer fit in one building.</p>` },
    { type: "javaBridge", html: `
<p>You've done all of this with collections; pandas makes it columnar, and Spark makes it distributed:</p>
<ul>
<li><code>df.groupby("country").agg(...)</code> ↔ <code>Collectors.groupingBy(Order::country, ...)</code>. Same split-apply-combine — but pandas stores each column as one typed array, so the sum is a vectorized sweep, not a loop over boxed objects.</li>
<li><code>orders.merge(fraud, on="order_id")</code> ↔ building a <code>Map&lt;String, Fraud&gt;</code> and looking each order up. The <code>how="inner"/"left"</code> flag is just which side you keep when the lookup misses.</li>
<li>The upgrade to hold onto for Track D: a <strong>pandas DataFrame is your single-machine <code>ExecutorService</code> running one task; a Spark DataFrame is the same API over a cluster.</strong> The method names (<code>groupBy</code>, <code>join</code>, <code>filter</code>, <code>agg</code>) are deliberately the same. What changes is eager→lazy and one heap→many machines — so the mental model you build here transfers wholesale, and only the failure modes are new.</li>
</ul>` },
  ],
  lab: {
    tier: "T1",
    understand: {
      engine: "pyodide",
      datasets: ["orders"],
      task: `<p><strong>Revenue by country — the split-apply-combine you'll repeat a thousand times.</strong> The <code>orders</code> DataFrame is loaded. Group it by <code>country</code>, sum <code>total_amount</code> within each group, and sort so the biggest market is on top.</p><p>The starter uses <code>.count()</code> — it counts orders per country instead of summing their value, so <code>top_rev</code> comes out as an order <em>count</em>, not revenue. Change the aggregation to <code>.sum()</code> and Run. The check expects the top market to be <strong>FR</strong> at <strong>22,655.38</strong>.</p><p><em>Foreshadow:</em> this is the identical API you'll call on a Spark DataFrame in Track D — same <code>groupby</code>, same <code>sum</code> — the only difference there is that the rows live across a cluster and nothing computes until you ask.</p>`,
      starterCode: `# orders is a pandas DataFrame already loaded for you.
# Goal: total revenue (sum of total_amount) per country, biggest first.
# The starter COUNTS orders instead of SUMMING their value — fix the aggregation.
rev_by_country = (orders
    .groupby("country")["total_amount"]
    .count()
    .sort_values(ascending=False)
    .round(2))

top_country = rev_by_country.index[0]
top_rev = round(float(rev_by_country.iloc[0]), 2)

print(rev_by_country)
print(f"top market: {top_country} at {top_rev}")`,
      solutionCode: `rev_by_country = (orders
    .groupby("country")["total_amount"]
    .sum()
    .sort_values(ascending=False)
    .round(2))

top_country = rev_by_country.index[0]
top_rev = round(float(rev_by_country.iloc[0]), 2)

print(rev_by_country)
print(f"top market: {top_country} at {top_rev}")`,
      assertCode: `assert top_country == "FR", f"expected FR on top, got {top_country}"
assert top_rev == 22655.38, f"expected FR revenue 22655.38, got {top_rev} (are you summing total_amount, not counting rows?)"`
    },
    buildWithAI: `I'm a Java/backend developer learning the pandas DataFrame model (groupby/agg/merge) as the on-ramp to Spark. Scaffold a real, runnable local project on my own machine (assume Python 3.10+ only).

1. Create a folder \`nimbusmart-pandas\` with a venv (\`python -m venv .venv\`) and install pandas and pytest (pin recent versions). Give me the Windows (\`.venv\\Scripts\\activate\`) and macOS/Linux activation lines.

2. \`generate_data.py\` — deterministic (\`random.seed(42)\`) generator writing two CSVs into \`data/\`:
   - \`orders.csv\`: 240 rows — order_id (O-10001..O-10240), customer_id (C-0001..C-0060), total_amount (8..950, 2dp), country from [DE, US, IN, BR, JP, FR, AU], channel from [web, app, app]
   - \`fraud_scores.csv\`: fraud scores for exactly 225 of those orders (15 unscored, chosen with the same seed) — order_id, fraud_score (0.01..0.99, 2dp)
   Print the row counts.

3. \`analyze.py\` — read both CSVs with pandas (explicit dtypes), then:
   - revenue_by_country: \`orders.groupby("country")["total_amount"].sum().sort_values(ascending=False).round(2)\` — print it and the top market
   - a multi-aggregate summary with \`.agg(orders=("order_id","count"), revenue=("total_amount","sum"))\`
   - a merge: \`orders.merge(fraud_scores, on="order_id", how="inner")\` and \`how="left"\`; print both row counts and explain the difference (inner drops the 15 unscored; left keeps them with NaN fraud_score)

4. \`test_analyze.py\` (pytest) — re-derive expectations independently from the CSVs (do NOT hardcode): assert the inner merge has 225 rows and the left merge has 240; assert revenue_by_country sums to the grand total of total_amount within 0.01; assert there are exactly 7 country groups.

5. Run \`python generate_data.py\`, then \`python analyze.py\`, then \`pytest -q\`. Finally, write me a 3-line note on which of these calls would look identical in PySpark and which one behavioral thing (eager vs lazy execution) changes when the same code runs on a cluster. Windows-friendly paths throughout.`
  },
  check: [
    {
      type: "predict",
      q: "The seed has 240 orders; 15 have no fraud score. How many rows does this inner merge produce?",
      code: `merged = orders.merge(fraud_scores, on="order_id", how="inner")
print(len(merged))`,
      options: ["225", "240", "255", "43"],
      answer: 0,
      explain: "An inner merge keeps only order_ids present in BOTH frames. 15 orders have no fraud_scores row, so they drop: 240 − 15 = 225. Switch to how=\"left\" and you'd get all 240, with NaN in the fraud_score column for the 15 unscored — which is the version you actually want when a missing score should still be visible."
    },
    {
      type: "mcq",
      q: "What is the difference between what <code>groupby(\"country\").sum()</code> returns and what a window function (or no aggregation) returns?",
      options: [
        "<code>groupby().sum()</code> collapses each country's rows into one summed row; the per-order detail is gone unless you keep the original frame",
        "They return identical results; <code>groupby</code> is just a faster spelling",
        "<code>groupby().sum()</code> keeps every original row and adds a total column to each",
        "<code>groupby().sum()</code> returns one row total for the whole frame, ignoring country"
      ],
      answer: 0,
      explain: "groupby-aggregate is split-apply-COMBINE: many rows in, one row per group out. The individual orders are collapsed into the group total. If you need per-row detail alongside the group total, you keep the original frame (or use a transform/window) rather than the aggregated result."
    },
    {
      type: "mcq",
      q: "Your pandas revenue notebook runs perfectly on a 10k-row sample, then OOM-kills on the full export. What has to change, and what carries over to Spark?",
      options: [
        "The <em>execution model</em> changes — Spark spreads the rows across a cluster and runs lazily — but the <code>groupby</code>/<code>merge</code>/<code>agg</code> API and your split-apply-combine reasoning carry over unchanged",
        "Nothing carries over — Spark uses a completely different set of operations you must relearn from scratch",
        "You just need a machine with more RAM; the pandas code is already the distributed solution",
        "Spark removes the need for groupby entirely, since distribution handles aggregation automatically"
      ],
      answer: 0,
      explain: "pandas holds the entire frame (plus intermediate copies) in one machine's memory — that's the wall you hit. Spark's job is to remove the single-machine assumption by partitioning rows across executors and deferring work until an action. The API is deliberately near-identical, so the concepts transfer; what's new is the failure modes (shuffles, skew) you'll meet in Tracks D and E."
    },
    {
      type: "predict",
      q: "Orders span 7 countries (DE, US, IN, BR, JP, FR, AU). How many rows does this print?",
      code: `by_country = orders.groupby("country")["total_amount"].sum()
print(len(by_country))`,
      options: ["7", "240", "1", "225"],
      answer: 0,
      explain: "groupby produces one aggregated row per distinct group key. There are 7 distinct countries, so the resulting Series has 7 entries — one revenue total each. The 240 input rows were split into 7 buckets and combined back to 7 rows."
    }
  ],
  fieldNotes: `A retail-analytics team ran their entire weekly reporting stack on a single 64 GB pandas box for two years, and it worked — until a Black Friday export doubled row counts and the Monday report simply never appeared, the process OOM-killed somewhere inside a <code>merge</code> that transiently materialized both frames plus the join buffer. The uncomfortable finding in the postmortem was that pandas had been silently making full copies on operations the team assumed were cheap (chained filters, <code>merge</code>, <code>sort_values</code>), so peak memory was several times the frame's nominal size. They didn't rewrite in Spark that week — they bought time by switching the heaviest joins to run in chunks — but it forced the real lesson: pandas' one-machine, eager, copy-happy model is a feature at 10 GB and a liability at 100, and the groupby/merge muscle they'd built transferred to Spark intact when they finally made the jump. The API was familiar; only the memory model had been lying to them.`
};
