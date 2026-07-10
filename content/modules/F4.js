// F4 — CI/CD for Data Pipelines (Track F, T3 scripted trace: f4-cicd-pipeline)
// Concept: environments (dev/staging/prod), testing pipelines (chispa-style DataFrame
//   assertions), deploy gates.
// Trace facts (engine/traces/f4-cicd-pipeline.json): commit renames fraud_score->risk_score;
//   lint/unit/integration all green; the data-quality gate catches the schema-contract break
//   (review queue 0 rows vs expected 43) and blocks promotion.
export default {
  id: "F4",
  track: "F",
  title: "CI/CD for Data Pipelines",
  minutes: 24,
  coldOpen: "A commit titled 'silver_orders: tidy column names' renamed one column and updated the one unit test that referenced it. Lint passed. All 47 unit tests passed. The integration job ran to a clean exit. Every check a backend engineer trusts was green — and the change, had it shipped, would have produced a fraud-review queue of exactly zero orders in production, silently auto-fulfilling every high-risk order in the marketplace.",
  concept: [
    { type: "prose", html: `
<p>CI/CD for application code asks one question: <em>does the code work?</em> CI/CD for a data pipeline has to ask a second, harder one: <em>does the code still produce output that means what its consumers depend on?</em> Code can be correct — compiles, passes its tests, runs to a clean exit — and still emit data that is quietly, catastrophically wrong. So a data pipeline's CI ladder has an extra rung the backend one doesn't.</p>
<p>The ladder, cheapest gate first:</p>
<ul>
<li><strong>Lint</strong> — <code>ruff</code>, <code>black --check</code>. Style and obvious mistakes. Seconds. Catches form, never behavior.</li>
<li><strong>Unit tests</strong> — <code>chispa</code>-style DataFrame assertions: build a tiny input DataFrame, run one transform, assert the output DataFrame equals an expected one with <code>assert_df_equality</code>. Proves your logic is internally consistent.</li>
<li><strong>Integration</strong> — run the real pipeline end-to-end against a small, frozen golden sample (a fixed 240-order NimbusMart fixture). Proves the pieces compose and the job actually runs.</li>
<li><strong>Data-quality gate</strong> — the extra rung. Assert properties of the <em>output</em> against a contract: schema (exactly these columns and types), row expectations (on the golden sample, exactly 43 orders sit at or above <code>FRAUD_REVIEW_THRESHOLD</code>), null and uniqueness rules. This gate does not run your code — it interrogates your code's result.</li>
</ul>
<p>Across <strong>environments</strong> — dev → staging → prod — each promotion is gated on the one before it. And the deep reason the data-quality gate is non-negotiable: <strong>unit tests move with the code</strong>. The commit that renamed the column also edited the fixture, so the unit test happily passed. Only a gate pinned to the <em>contract</em> — not to the code — can catch a change the tests were edited to allow.</p>` },
    { type: "code", lang: "python", code: `import chispa
from pyspark.sql import functions as F

FRAUD_REVIEW_THRESHOLD = 0.80

# --- UNIT: chispa DataFrame assertion (moves with the code) ---
def test_review_queue_flags_high_risk(spark):
    fraud = spark.createDataFrame(
        [("O-1", 0.91), ("O-2", 0.42), ("O-3", 0.80)],   # O-3 sits exactly at FRAUD_REVIEW_THRESHOLD
        ["order_id", "fraud_score"])
    got = build_review_queue(fraud)                 # the transform under test
    expected = spark.createDataFrame(
        [("O-1", 0.91), ("O-3", 0.80)],             # >= FRAUD_REVIEW_THRESHOLD
        ["order_id", "fraud_score"])
    chispa.assert_df_equality(got, expected, ignore_row_order=True)

# --- DATA-QUALITY GATE: pinned to the contract, not the code ---
REQUIRED_COLS = {"order_id", "fraud_score"}          # the schema contract

def gate_review_queue(review_df):
    missing = REQUIRED_COLS - set(review_df.columns)
    assert not missing, f"schema contract broken: missing {missing}"
    n = review_df.filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD).count()
    assert n == 43, f"expected 43 orders >= FRAUD_REVIEW_THRESHOLD on golden sample, got {n}"`, caption: "The unit test edits with the code; the gate is pinned to the contract — which is why only the gate catches the rename." },
    { type: "svg", svg: `<svg viewBox="0 0 720 210" font-family="var(--mono)" font-size="11.5">
<defs><marker id="f4arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0 L10 5 L0 10 z" fill="var(--ink2)"/></marker></defs>
<text x="20" y="22" fill="var(--ink2)" font-size="11">commit a1c9f04 — renames fraud_score → risk_score (and edits its unit fixture to match)</text>
<rect x="16" y="40" width="118" height="44" rx="8" fill="var(--paper2)" stroke="var(--accent)"/><text x="75" y="60" text-anchor="middle" fill="var(--ink)">lint</text><text x="75" y="76" text-anchor="middle" fill="var(--accent)" font-size="10">PASS 11s</text>
<rect x="154" y="40" width="118" height="44" rx="8" fill="var(--paper2)" stroke="var(--accent)"/><text x="213" y="60" text-anchor="middle" fill="var(--ink)">unit (chispa)</text><text x="213" y="76" text-anchor="middle" fill="var(--accent)" font-size="10">PASS 47/47</text>
<rect x="292" y="40" width="118" height="44" rx="8" fill="var(--paper2)" stroke="var(--accent)"/><text x="351" y="60" text-anchor="middle" fill="var(--ink)">integration</text><text x="351" y="76" text-anchor="middle" fill="var(--accent)" font-size="10">PASS exit 0</text>
<rect x="430" y="40" width="130" height="44" rx="8" fill="var(--paper2)" stroke="var(--rust)" stroke-width="2"/><text x="495" y="60" text-anchor="middle" fill="var(--ink)">data-quality gate</text><text x="495" y="76" text-anchor="middle" fill="var(--rust)" font-size="10" font-weight="bold">FAIL</text>
<rect x="580" y="40" width="124" height="44" rx="8" fill="var(--paper2)" stroke="var(--line)" stroke-dasharray="5 4"/><text x="642" y="60" text-anchor="middle" fill="var(--ink2)">deploy(prod)</text><text x="642" y="76" text-anchor="middle" fill="var(--ink2)" font-size="10">BLOCKED</text>
<line x1="134" y1="62" x2="152" y2="62" stroke="var(--ink2)" marker-end="url(#f4arr)"/>
<line x1="272" y1="62" x2="290" y2="62" stroke="var(--ink2)" marker-end="url(#f4arr)"/>
<line x1="410" y1="62" x2="428" y2="62" stroke="var(--ink2)" marker-end="url(#f4arr)"/>
<line x1="560" y1="62" x2="578" y2="62" stroke="var(--rust)" stroke-dasharray="5 4" marker-end="url(#f4arr)"/>
<text x="20" y="122" fill="var(--ink2)" font-size="11">the gate is pinned to the OUTPUT contract, so the code change couldn't move it:</text>
<rect x="20" y="134" width="684" height="30" rx="6" fill="var(--paper2)" stroke="var(--rust)"/><text x="362" y="153" text-anchor="middle" fill="var(--rust)">required column fraud_score MISSING · review queue rows 0 ≠ 43 expected</text>
<text x="20" y="188" fill="var(--ink2)" font-size="10.5">three green checks proved the code runs; only the gate proved the output still means what consumers depend on</text>
</svg>`, caption: "Lint/unit/integration go green on a correctness bug; the contract-pinned data-quality gate is the only rung that catches it." },
    { type: "analogy", title: "The pallet spec at the loading gate", html: `
<p>NimbusMart's warehouse has an outbound loading gate, and before any pallet leaves for a customer it's checked against the <strong>pallet spec</strong>: right SKUs, right counts, shrink-wrapped, label in the right corner. The crew that <em>built</em> the pallet also has a checklist — but that checklist is theirs; they can and do amend it. The loading-gate spec is different: it belongs to the <em>receiving</em> customers downstream, and the build crew can't edit it. So when someone reorganizes the packing process and a required label ends up missing, the crew's own checklist might get updated to match the new process and pass — but the loading gate, holding the customer's spec, stops the pallet at the door.</p>
<p>Lint, unit, and integration are the build crew's checklist — they travel with whoever changes the process. The data-quality gate is the loading-gate spec: pinned to what consumers require, unowned by the code being changed. That independence is the entire reason it catches the pallet the build crew's own checklist waved through.</p>` },
    { type: "javaBridge", html: `
<p>You already run exactly this ladder on your services — you just don't have the fourth rung yet, because a REST response doesn't have a thousand downstream consumers silently trusting its shape.</p>
<ul>
<li><code>checkstyle</code> / <code>spotless</code> ↔ <strong>lint</strong>. Identical rung.</li>
<li>JUnit + AssertJ (<code>assertThat(actual).isEqualTo(expected)</code>) ↔ <strong>chispa</strong>'s <code>assert_df_equality</code> — the same assertion, but the unit under test is a DataFrame transform and the fixtures are little DataFrames.</li>
<li>Spinning up Testcontainers to run against a real Postgres ↔ the <strong>integration</strong> run against a frozen golden sample.</li>
<li>Your GitHub <strong>branch-protection required checks</strong> — the ones that make the merge button grey until CI is green ↔ <strong>deploy gates</strong>. A red required check blocks promotion, full stop. The data-quality gate is just one more required check, except it asserts on <em>data</em>, and it's owned by the contract rather than by the PR.</li>
</ul>
<p>The mental upgrade: in a service you gate on “the code is correct.” In a pipeline you also gate on “the output still honors its contract” — because the failure mode isn't a 500 someone notices, it's a table full of plausible, wrong numbers that nobody notices for a week.</p>` },
  ],
  lab: {
    tier: "T3",
    understand: {
      engine: "trace",
      trace: "f4-cicd-pipeline",
      task: `<p>Scrub commit <code>a1c9f04</code> ("tidy column names") through the pipeline: <strong>lint → unit → integration → data-quality gate → deploy</strong>. Watch each rung report, and watch where the ladder finally stops the bug:</p>
<ul>
<li><strong>Three green rungs on a real bug.</strong> Lint passes (11s — form, not behavior). All 47 chispa unit tests pass — because the same commit edited the fixture to the new column name. Integration exits 0 — renaming a column is a perfectly successful operation.</li>
<li><strong>The gate that can't be edited.</strong> The data-quality gate is pinned to the output contract, so it fails where the tests couldn't: required column <code>fraud_score</code> missing, and the review queue came out <strong>0 rows instead of 43</strong>.</li>
<li><strong>Promotion blocked.</strong> The deploy stage never starts; the author gets the failure on the commit in ~90 seconds, at their desk.</li>
<li><strong>The counterfactual.</strong> With no gate, this ships to prod and tonight's fraud queue flags 0 orders at or above FRAUD_REVIEW_THRESHOLD (0.80) — silent, unpaged. The final step reruns the fixed commit clean.</li>
</ul>
<p>Badge: <em>simulation</em> — stage durations are illustrative, but the "unit tests move with the code, the contract-pinned gate does not" dynamic is the whole point and is exactly real.</p>`
    },
    buildWithAI: `I'm learning CI/CD for data pipelines. Build me a real local project that shows the four-rung ladder — lint, chispa unit tests, integration on a sample, and a data-quality gate — and proves that only the gate catches a renamed column. Assume Python 3.10+ and nothing else installed.

1. Create a folder \`nimbusmart-cicd\` with a venv. Install pyspark (recent 3.5.x), pytest, chispa, and ruff.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator writing \`data/fraud_scores.csv\` (225 rows: order_id, fraud_score 0.01..0.99) seeded so EXACTLY 43 rows have fraud_score >= 0.80. Print that count.

3. Create \`pipeline.py\` with FRAUD_REVIEW_THRESHOLD = 0.80 as a named constant and build_review_queue(fraud_df) that filters fraud_score >= FRAUD_REVIEW_THRESHOLD and selects order_id + fraud_score (explicit StructType on any read).

4. Create the ladder:
   - \`test_unit.py\` (pytest + chispa): build a tiny 3-row input DataFrame and assert build_review_queue's output equals the expected 2-row DataFrame via chispa.assert_df_equality(ignore_row_order=True).
   - \`test_integration.py\`: run build_review_queue against the full generated CSV and assert the row count equals the count of fraud_score >= FRAUD_REVIEW_THRESHOLD derived from the CSV with the plain csv module (should be 43 — don't hardcode).
   - \`gate.py\`: a data-quality gate function asserting the output schema has EXACTLY {order_id, fraud_score} and that the >= FRAUD_REVIEW_THRESHOLD row count is 43. Add \`test_gate.py\` running it.

5. Now the demo: make a git branch, rename fraud_score to risk_score in build_review_queue AND update ONLY test_unit.py's fixture to match. Show me that ruff + test_unit pass, but gate.py fails on the missing required column. Then revert and show all four rungs green. Give me a one-paragraph explanation of why the unit test passed on a breaking change and the gate didn't. Windows-friendly paths, and a \`run_ci.sh\`/\`run_ci.ps1\` that runs the four rungs in order and stops at the first red.`
  },
  check: [
    {
      type: "mcq",
      q: "A commit renames <code>fraud_score</code> to <code>risk_score</code> in the transform and updates the one unit-test fixture to match. Lint, all 47 chispa unit tests, and integration all pass. How is that possible on a breaking change?",
      options: [
        "The change wasn't actually breaking; renaming a column is always safe",
        "Unit tests move with the code — the same commit edited the fixture, so the test asserts the new name and passes; lint checks form and integration only checks the job runs, neither validates the output contract",
        "The 47 tests were skipped because the fixture changed",
        "Integration caught it but reported a warning, not a failure"
      ],
      answer: 1,
      explain: "A unit test is only as honest as its fixture, and the fixture is editable by the same PR that breaks the contract. Lint validates form, integration validates that the job runs to a clean exit — neither asserts that the output still exposes the columns consumers depend on. That's precisely the gap the data-quality gate exists to close."
    },
    {
      type: "predict",
      q: "The data-quality gate runs against the pipeline output after the rename. Given the gate below and the fact that downstream logic keys on <code>fraud_score</code> (now gone), what does it report?",
      code: `REQUIRED_COLS = {"order_id", "fraud_score"}   # the contract

def gate_review_queue(review_df):
    missing = REQUIRED_COLS - set(review_df.columns)
    assert not missing, f"schema contract broken: missing {missing}"
    n = review_df.filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD).count()
    assert n == 43, f"expected 43, got {n}"`,
      options: [
        "It passes — risk_score is just fraud_score with a new name",
        "It fails on the schema contract: fraud_score is missing (renamed to risk_score), and the review queue collapsed to 0 rows instead of 43",
        "It passes the schema check but the count is 225",
        "It throws a SyntaxError because the column doesn't exist"
      ],
      answer: 1,
      explain: "The gate is pinned to the contract, not the code: REQUIRED_COLS still names fraud_score, so the first assert fires on the missing column. And because the downstream join keyed on fraud_score, the queue came out empty — 0 ≠ 43. Two red assertions, and promotion is blocked. The gate caught exactly what the editable unit fixture waved through."
    },
    {
      type: "mcq",
      q: "Why is the data-quality gate placed as a required check <em>before</em> promotion to prod, rather than as a monitoring alert <em>after</em> deploy?",
      options: [
        "Post-deploy alerts are impossible to build for data pipelines",
        "A blocked promotion costs the author ~90 seconds at their desk with full context; the same bug in prod silently produces a 0-row fraud queue that may go unnoticed for days — gate before, don't alert after",
        "Required checks run faster than monitoring alerts",
        "Monitoring alerts can't read the FRAUD_REVIEW_THRESHOLD constant"
      ],
      answer: 1,
      explain: "The failure mode of a data bug isn't a loud 500 — it's plausible, wrong output that nobody notices. Catching it as a pre-promotion required check (like GitHub branch protection blocking a merge) stops it at the cheapest possible place. An after-the-fact alert only tells you how long prod was already wrong."
    },
    {
      type: "mcq",
      q: "What does the data-quality gate assert that the chispa unit tests fundamentally cannot?",
      options: [
        "That the transform logic produces the right output for a hand-built input DataFrame",
        "That the output still honors a contract owned by consumers and NOT editable by the PR under review — schema shape and row expectations pinned independently of the code",
        "That the code compiles and passes lint",
        "That the pipeline runs end-to-end without throwing"
      ],
      answer: 1,
      explain: "Unit tests assert internal consistency between code and its own fixtures — both of which the PR can edit together. The gate asserts an external contract (columns, types, the 43-row expectation) that the code change cannot move. That independence is the entire reason it's a distinct, load-bearing rung and not redundant with the unit suite."
    }
  ],
  fieldNotes: `An ads-analytics team had a solid CI suite — lint, ~200 unit tests, an integration run on sample data — and shipped multiple times a day with confidence. The incident that added their first data-quality gate: a refactor split a 'revenue' column into 'gross_revenue' and 'net_revenue', and the PR dutifully updated every unit fixture, so all 200 tests stayed green and the integration job ran clean. What no test asserted was that the downstream attribution mart still found a column literally named 'revenue' — it didn't, so the mart's join silently produced nulls, and three days of the executive revenue dashboard read zero for the affected channels before anyone trusted the number enough to question it. The postmortem's action item was one rung: a gate that reads the mart's published schema contract (owned by the consuming team, pinned in a YAML file the producing PR can't edit) and asserts required columns exist and a canary row-count stays within bounds. It has fired four times since, each on a green-tests PR — every one a change the unit suite was edited, in good faith, to allow.`
};
