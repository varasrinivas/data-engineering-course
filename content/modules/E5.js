// E5 — UDFs vs Built-ins (Track E) — T3, reuses engine/traces/e5-udf-tax.json
// Verified facts (data/nimbusmart/generate.py, seed 42):
//   fraud_scores = 225; exactly 43 have fraud_score >= FRAUD_REVIEW_THRESHOLD (0.80).
//   Trace story: uppercasing country on 40M rows costs 1.2s built-in / 38s Python UDF / 4.5s pandas UDF.
export default {
  id: "E5",
  track: "E",
  title: "UDFs vs Built-ins",
  minutes: 24,
  coldOpen: "Three NimbusMart engineers each need the same trivial thing — uppercase the country column before a join — and each reaches for a different tool. In the dev notebook, on 40,000 rows, all three finish instantly and every code review waves them through. On the first full-volume run, 40 million rows, one of the three takes 38 seconds where another takes 1.2, and the nightly SLA blows at 3am. Same output, same cluster, a 30x gap — and it was invisible until the data got big.",
  concept: [
    { type: "prose", html: `
<p>Here's the thing nobody tells you until it pages you: in PySpark, the <em>logic</em> of a transformation is almost never the cost. The cost is <strong>how many times your data has to cross the boundary between the JVM (where Spark lives) and a Python process (where your function lives)</strong>. Three tools, three answers to that question, and it decides everything.</p>
<ul>
<li><strong>Built-in / <code>F.expr</code></strong> — <code>F.upper(...)</code>, <code>F.col(...) >= ...</code>. The work is compiled by Catalyst and run by Tungsten on off-heap columnar buffers. It <em>never leaves the JVM</em>. Zero boundary crossings.</li>
<li><strong>Python UDF</strong> — <code>@udf def up(s): return s.upper()</code>. Functionally identical, and on the trace <strong>30x slower</strong>: every single row is serialized, shipped to a Python worker, deserialized, processed, and shipped back. Forty million round trips.</li>
<li><strong>pandas UDF</strong> — <code>@pandas_udf</code> over a <code>pd.Series</code>. Same Python logic, but rows cross in <strong>Arrow batches of ~10,000</strong>, vectorized and zero-copy. ~4,000 crossings instead of 40 million.</li>
</ul>` },
    { type: "code", lang: "python", code: `from pyspark.sql import functions as F
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType
import pandas as pd

FRAUD_REVIEW_THRESHOLD = 0.80

# --- Option A: built-in. Stays in the JVM. ~1.2s on 40M rows.
df1 = orders.withColumn("cc", F.upper(F.col("country")))

# --- Option B: row-at-a-time Python UDF. ~38s. Same answer, 30x the bill.
@udf(StringType())
def up(s): return s.upper() if s else s
df2 = orders.withColumn("cc", up(F.col("country")))

# --- Option C: pandas UDF. Arrow batches, vectorized. ~4.5s.
@pandas_udf(StringType())
def up_vec(s: pd.Series) -> pd.Series: return s.str.upper()
df3 = orders.withColumn("cc", up_vec(F.col("country")))

# The fraud rule needs no UDF at all — it's a comparison:
review = fraud_scores.filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)`, caption: "Four ways to the same result; only the first and last are ones you'd ship." },
    { type: "prose", html: `
<p><strong>Why the row UDF collapses.</strong> For each of 40 million rows Spark must pickle the value, push it over a local socket to a Python worker, unpickle it, run your one line, pickle the result, and unpickle it back into the JVM. The <code>.upper()</code> itself is a rounding error; <strong>serialization is ~80% of the wall clock</strong>. Watch the executors in the trace during that stage and the JVM cores are <em>starved</em> — stalled at ~19% utilization, waiting for the Python side to hand rows back one at a time. You rented 16 cores to watch them idle.</p>
<p><strong>Why the pandas UDF recovers most of it.</strong> Same boundary, but crossed ~10,000x less often, and each crossing moves a whole Arrow column with no per-value pickling. The Python worker does real vectorized work on entire Series; the JVM keeps pace feeding batches. Not built-in-fast, but the same order of magnitude — 4.5s, not 38.</p>
<p><strong>The decision rule writes itself:</strong> reach for a built-in or <code>F.expr</code> first (there's one for far more than people expect — string ops, dates, JSON, regex, hashing). If the logic genuinely can't be expressed in built-ins, use a <strong>pandas UDF</strong>. Reach for a plain row-at-a-time <code>@udf</code> almost never.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 220" font-family="var(--mono)" font-size="12">
<text x="20" y="22" fill="var(--ink2)" font-size="11">same transform · 40,000,000 rows · wall clock</text>
<text x="20" y="58" fill="var(--ink)">F.upper (built-in)</text>
<rect x="230" y="44" width="20" height="20" rx="3" fill="var(--accent)"/><text x="262" y="59" fill="var(--accent)">1.2s</text>
<text x="330" y="58" fill="var(--ink2)" font-size="10">0 boundary crossings — stays in Tungsten</text>
<text x="20" y="98" fill="var(--ink)">Python UDF (row)</text>
<rect x="230" y="84" width="380" height="20" rx="3" fill="var(--rust)"/><text x="622" y="99" fill="var(--rust)">38s</text>
<text x="330" y="126" fill="var(--ink2)" font-size="10">40,000,000 crossings — pickle every row, both ways</text>
<text x="20" y="158" fill="var(--ink)">pandas UDF (Arrow)</text>
<rect x="230" y="144" width="45" height="20" rx="3" fill="var(--accent)" opacity="0.7"/><text x="287" y="159" fill="var(--accent)">4.5s</text>
<text x="330" y="158" fill="var(--ink2)" font-size="10">~4,000 crossings — Arrow batches of ~10,000, zero-copy</text>
<line x1="230" y1="180" x2="230" y2="196" stroke="var(--line)"/>
<text x="20" y="196" fill="var(--ink2)" font-size="10">The bar length is the boundary tax, not the logic — the .upper() is identical in all three.</text>
</svg>`, caption: "Identical output; the bar is the price of crossing JVM↔Python, and how often you pay it." },
    { type: "analogy", title: "The inspection booth across the yard", html: `
<p>On the NimbusMart line, most relabelling happens <strong>inline on the conveyor</strong> — a print head stamps the new country code as the parcel rolls past, never slowing down. That's a built-in: the work happens right where the parcels already are, in the system's own hands.</p>
<p>A row-at-a-time UDF is a rule that says <em>every parcel must be hand-carried out the loading door, across the yard to an external contractor's inspection booth, stamped, and carried back</em> — one parcel at a time. The stamp takes a second; the round trip across the yard takes a minute, and you do it 40 million times while the conveyor sits idle. The pandas UDF is the same contractor, same booth — but now you wheel a whole <strong>cage of 10,000 parcels</strong> over at once, they stamp the lot, and you wheel it back. The booth didn't move; you just stopped making 40 million separate trips to it.</p>` },
    { type: "javaBridge", html: `
<p>You've felt this exact cost curve before: it's the <strong>JNI boundary</strong>. A single native call through JNI is cheap in isolation, but call it per element in a tight loop and the marshalling — copying arguments across the managed/native fence, pinning memory, crossing the call gate — dwarfs the native work itself. The fix was always the same: batch the crossing (pass an array, do the loop on the native side), never cross per element.</p>
<ul>
<li><strong>A Python UDF is JNI per row.</strong> Spark's JVM talks to a Python worker over a socket (pickle instead of JNI marshalling, but the same shape): cheap logic, ruinous crossing cost, paid once per element. The row-at-a-time UDF is the anti-pattern you already know not to write in a JNI hot loop.</li>
<li><strong>A pandas UDF is the batched native call.</strong> Cross once per ~10,000 rows with a columnar Arrow buffer — the "pass the whole array across the fence and loop on the far side" fix, applied to the JVM↔Python boundary. And a built-in is staying on the JVM side of the fence entirely, which is always the cheapest call: the one you don't make.</li>
</ul>` },
  ],
  lab: {
    tier: "T3",
    understand: {
      engine: "trace",
      trace: "e5-udf-tax",
      task: `<p><strong>Scrub the timeline and watch one boundary set three prices.</strong> The transform is the same in all three passes — uppercase <code>country</code> on 40 million rows. As you drag the scrubber:</p><ul><li>At t=3–6, the built-in (<strong>1.2s</strong>): note the executors near full and even — codegen streaming columnar batches, nobody waiting.</li><li>At t=9–12, the row UDF (<strong>38s</strong>): watch the JVM cores drop to ~19% while the Python workers peg at 99% doing nothing but pickling. The cluster is busy being idle.</li><li>At t=15–18, the pandas UDF (<strong>4.5s</strong>): utilization comes back — real vectorized work on Arrow batches, JVM and Python both busy.</li><li>At t=24, the trap: at 40k dev rows all three read 0.02–0.09s and look identical. The 30x gap only opens at production scale. That's why the row UDF passes review and detonates at 3am.</li></ul>`
    },
    buildWithAI: `I'm learning the cost of PySpark UDFs vs built-ins (the JVM↔Python serialization boundary). Build me a real local benchmark on my own machine that makes the gap visible. Assume nothing beyond Python 3.10+.

1. Create a folder \`nimbusmart-udf-tax\` with a venv; install pyspark (recent 3.5.x), pandas, pyarrow, and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) writer emitting a Parquet dataset into \`data/\`:
   - orders: make TWO sizes so I can see the trap — a dev set of 40,000 rows and a big set of ~4,000,000 rows (keep it runnable on a laptop). Columns: order_id, country from [DE,US,IN,BR,JP,FR,AU] (lowercase on purpose), total_amount (8..950, 2dp), fraud_score (0..1, 2dp).

3. Create \`udf_bench.py\` that builds a SparkSession (local[*]) and, for BOTH sizes, times three ways to uppercase country, forcing execution with an action each time:
   - built-in: F.upper(F.col("country"))
   - row UDF: @udf(StringType()) def up(s): return s.upper() if s else s
   - pandas UDF: @pandas_udf(StringType()) def up_vec(s): return s.str.upper()
   Print a small table: rows x method x wall_clock, and the slowdown ratio of each UDF vs the built-in. Also define FRAUD_REVIEW_THRESHOLD = 0.80 and show the review filter (fraud_score >= FRAUD_REVIEW_THRESHOLD) needs no UDF at all.

4. Create \`test_udf_bench.py\` (pytest) asserting:
   - all three methods produce identical output columns (collect and compare — same answer, different cost)
   - on the big set, the row UDF wall clock is at least several times the built-in's (the tax is real at scale)
   - on the 40k set, all three are within a small absolute margin (the tax is invisible in dev — this is the trap)

5. Run the generator and the benchmark, then pytest. Show me the timing table for both sizes and the slowdown ratios, and confirm the dev-size numbers look deceptively identical. Windows-friendly paths please.`
  },
  check: [
    {
      type: "mcq",
      q: "Two engineers uppercase <code>country</code> identically — one with <code>F.upper</code>, one with an <code>@udf</code>. Same output, but the UDF is ~30x slower. Where did the time go?",
      options: [
        "Serializing every row across the JVM↔Python boundary and back — 40 million round trips — while the JVM cores wait; the .upper() itself is negligible",
        "The UDF recompiles itself on every row, and JIT warmup dominates the run",
        "F.upper runs on the GPU while the Python UDF is stuck on the CPU",
        "The UDF forces a shuffle that the built-in avoids"
      ],
      answer: 0,
      explain: "A row UDF pickles each value, ships it to a Python worker over a socket, and ships the result back — per row. On 40M rows serialization is ~80% of the wall clock and the JVM sits at ~19% utilization waiting. The built-in never leaves the JVM, so it pays none of that."
    },
    {
      type: "predict",
      q: "The fraud rule needs no UDF — it's a built-in comparison. On the seed data, how many rows does the review filter return?",
      code: `FRAUD_REVIEW_THRESHOLD = 0.80
review = fraud_scores.filter(
    F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)
print(review.count())`,
      options: ["43", "225", "4", "240"],
      answer: 0,
      explain: "43 of the 225 scored orders are at or above FRAUD_REVIEW_THRESHOLD (0.80). This is a pure built-in comparison compiled by Catalyst — no Python UDF, no boundary crossing. Whenever a rule is expressible as a column expression, that's the version you ship."
    },
    {
      type: "mcq",
      q: "A pandas UDF runs the <em>same</em> Python <code>.upper()</code> logic as a row UDF, yet finishes in 4.5s instead of 38s. Why?",
      options: [
        "Rows cross the boundary in Arrow batches of ~10,000 — about 4,000 vectorized, zero-copy crossings instead of 40 million per-row pickles",
        "pandas UDFs run inside the JVM, so they never cross the boundary at all",
        "pandas is written in C, so the .upper() call itself is 30x faster",
        "Spark caches the pandas UDF result and skips recomputation"
      ],
      answer: 0,
      explain: "The boundary is the same; the frequency isn't. A pandas UDF ships whole columns as Arrow batches (~10,000 rows each) with zero-copy transfer and vectorized processing, cutting ~40 million per-row crossings to ~4,000. It still crosses — hence slower than the built-in's zero — but it's the same order of magnitude."
    },
    {
      type: "mcq",
      q: "Why does a row-at-a-time UDF sail through code review and staging, then blow the SLA on the first production run?",
      options: [
        "The cost is a property of the boundary, which is invisible at small scale: at 40k dev rows all options finish under 0.1s; the 30x gap only opens at 40M rows",
        "Staging always runs UDFs on faster hardware than production",
        "The UDF only serializes rows when more than one executor is involved, which never happens in dev",
        "Code review tools can't parse @udf decorators, so they skip them"
      ],
      answer: 0,
      explain: "At 40,000 rows the per-row tax is real but tiny in absolute terms, so all three tools look identical and the UDF passes every gate. The tax scales linearly with row count, so it stays hidden until the first full-volume run — where 40 million crossings turn 1.2s into 38s."
    }
  ],
  fieldNotes: `A fraud team shipped a 'risk band' feature as a five-line Python UDF: take a score, return 'low'/'medium'/'high'. It passed review, passed a staging run on a sampled day, and on the first full nightly run turned a 9-minute job into 51 minutes, missing the downstream dashboard's 6am SLA by an hour. The logic was three comparisons — expressible as a single F.when chain that Catalyst compiles and runs in the JVM. Rewriting the UDF as built-ins took twenty minutes and dropped the stage back under a minute; no cluster changes, no tuning. The postmortem line that stuck: 'a UDF is a Python process tax you pay per row, forever — always ask whether the boundary is necessary before you cross it.' Their review checklist now flags every @udf and asks the author to prove no built-in or F.expr can do the job first.`
};
