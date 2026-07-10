// G1 — Data Contracts & Schema Evolution (Track G, Quality/Governance/Ops)
// Verified facts used by lab + checks (from data/nimbusmart/seed.js, seed 42):
//   order_events = 361 rows; 17 rows MISSING `device`; 344 rows HAVE `device`;
//   25 rows CARRY the newer `app_version`; 336 rows do not; 180 distinct orders.
//   COUNT(device) = 344, so COUNT(*) - COUNT(device) = 17. COUNT(app_version) = 25.
export default {
  id: "G1",
  track: "G",
  title: "Data Contracts & Schema Evolution",
  minutes: 24,
  coldOpen: "The clickstream team ships a mobile release and, buried in the diff, the analytics event payload quietly loses its `device` field on 6% of events and gains a shiny new `app_version` on others. Nobody told the fraud pipeline. At 03:00 the Silver job doesn't crash — it just starts writing NULL devices into the risk features, and a week later a model retrain silently learns that 17 orders came from nowhere.",
  concept: [
    { type: "prose", html: `
<p>A <strong>data contract</strong> is the schema-plus-semantics agreement between whoever <em>produces</em> a dataset and whoever <em>consumes</em> it: the column names and types, which fields are required vs optional, the allowed value ranges, the freshness, and — the part teams always forget to write down — <em>the rules for changing any of it</em>. Without a contract, the producer's schema is an accident that consumers reverse-engineer by staring at yesterday's data. With one, a change is a negotiation instead of a 3 a.m. surprise.</p>
<p>The load-bearing distinction in this whole module is <strong>additive vs breaking</strong>:</p>
<ul>
<li><strong>Additive (backward-compatible):</strong> adding a new <em>optional</em> field, widening a type (<code>int</code> → <code>long</code>), adding an enum value a consumer already treats as "other". Old readers keep working because they can ignore what they don't know. Our clickstream adding <code>app_version</code> is additive.</li>
<li><strong>Breaking:</strong> removing or renaming a field, tightening a type, making an optional field required, changing units or semantics. Every consumer must change <em>before</em> the producer ships, or they break. Our clickstream dropping <code>device</code> on some events is breaking — even though nothing threw an exception.</li>
</ul>
<p>The trap the cold open lands on: <strong>a breaking change rarely announces itself with a stack trace.</strong> Spark reads JSON with a permissive schema, sees a missing key, and fills <code>NULL</code>. The pipeline stays green; the <em>meaning</em> silently rots. A contract exists precisely so that "field disappeared" is a rejected pull request, not a slow data-quality leak.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 260" font-family="var(--mono)" font-size="12">
<text x="20" y="24" fill="var(--ink2)" font-size="11">PRODUCER publishes a schema · CONSUMERS depend on it · the CONTRACT governs change</text>
<rect x="20" y="40" width="150" height="52" rx="10" fill="var(--paper2)" stroke="var(--accent)" stroke-width="2"/>
<text x="95" y="63" text-anchor="middle" fill="var(--accent)" font-weight="bold">producer</text>
<text x="95" y="80" text-anchor="middle" fill="var(--ink2)" font-size="10">clickstream · order_events</text>
<rect x="540" y="24" width="160" height="40" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="620" y="48" text-anchor="middle" fill="var(--ink)">fraud pipeline</text>
<rect x="540" y="72" width="160" height="40" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="620" y="96" text-anchor="middle" fill="var(--ink)">analytics dashboards</text>
<rect x="540" y="120" width="160" height="40" rx="8" fill="var(--paper2)" stroke="var(--line)"/><text x="620" y="144" text-anchor="middle" fill="var(--ink)">ML feature store</text>
<line x1="170" y1="66" x2="538" y2="44" stroke="var(--ink2)" stroke-width="1.5"/>
<line x1="170" y1="66" x2="538" y2="92" stroke="var(--ink2)" stroke-width="1.5"/>
<line x1="170" y1="66" x2="538" y2="140" stroke="var(--ink2)" stroke-width="1.5"/>
<rect x="120" y="176" width="480" height="66" rx="10" fill="var(--paper2)" stroke="var(--line)"/>
<text x="360" y="197" text-anchor="middle" fill="var(--ink2)" font-size="11">THE CONTRACT — what a change is allowed to do</text>
<rect x="140" y="206" width="210" height="26" rx="6" fill="none" stroke="var(--accent)"/>
<text x="245" y="223" text-anchor="middle" fill="var(--accent)" font-size="10">+ add optional app_version = ADDITIVE ✓</text>
<rect x="368" y="206" width="212" height="26" rx="6" fill="none" stroke="var(--rust)"/>
<text x="474" y="223" text-anchor="middle" fill="var(--rust)" font-size="10">− drop required device = BREAKING ✗</text>
</svg>`, caption: "One producer, many consumers. The contract is the rulebook for what a schema change may and may not do to them." },
    { type: "code", lang: "python", code: `# A data contract expressed as code the CI job can enforce (e.g. via a
# schema registry / Great Expectations / a plain dataclass + test).
FRAUD_REVIEW_THRESHOLD = 0.80   # consumer-side business rule, versioned with the contract

order_events_contract = {
    "version": "1.2.0",                 # semver: MINOR bump = additive, MAJOR = breaking
    "required": {
        "event_id":   "string",
        "order_id":   "string",
        "event_type": "string",         # enum, additive-only (new values allowed)
        "event_ts":   "timestamp",
        "device":     "string",         # REQUIRED — dropping it is a MAJOR/breaking change
    },
    "optional": {
        "app_version": "string",        # added in 1.2.0 — additive, old readers ignore it
    },
    "compat": "backward",               # new schema must still read old data
}

# The fraud_scores contract the review queue depends on downstream:
fraud_scores_contract = {
    "version": "3.2.0",
    "required": {"order_id": "string", "fraud_score": "double", "model_version": "string"},
    "checks": ["0.0 <= fraud_score <= 1.0"],   # range is part of the contract, not just the type
    # consumer rule: fraud_score >= FRAUD_REVIEW_THRESHOLD routes an order to human review
}`, caption: "A contract is code a CI gate can check — semver encodes the additive/breaking promise, ranges and required-ness are first-class." },
    { type: "analogy", title: "The pallet spec at the receiving dock", html: `
<p>Every supplier delivering to the NimbusMart warehouse gets the <strong>pallet spec</strong>: max height, weight, a scannable barcode in a fixed corner, a packing slip in a known format. The dock crew doesn't inspect trucks by vibes — they hold the spec against the pallet. A supplier who wants to <em>add</em> a second barcode for their own tracking? Fine, additive, the dock ignores it. A supplier who quietly <em>moves</em> the barcode to the other corner, or ships a taller pallet than the racking allows? Rejected at the gate — because <em>changing the spec is a conversation you have before the truck leaves, not a surprise the dock discovers at 3 a.m.</em></p>
<p>A data contract is that pallet spec. Additive changes flow through untouched. Breaking changes get stopped at the dock — which is the whole point of having a dock instead of a hole in the wall.</p>` },
    { type: "javaBridge", html: `
<p>You have shipped this discipline already — it's your <strong>REST API's OpenAPI contract</strong>. When you own an endpoint a dozen services call, you don't rename a JSON field on a Tuesday. You know the rules in your bones:</p>
<ul>
<li><strong>Adding an optional response field</strong> is safe — old clients that deserialize into a POJO with <code>@JsonIgnoreProperties(ignoreUnknown = true)</code> just skip it. That's an additive schema change, and it's why permissive readers exist on both sides.</li>
<li><strong>Removing a field, renaming it, or making a request param newly required</strong> is a breaking change — you bump the major version, run <code>/v1</code> and <code>/v2</code> side by side, and migrate consumers before sunsetting. Same playbook, byte-for-byte, for a Bronze table's schema.</li>
</ul>
<p>The one upgrade to your intuition: an HTTP client that gets a malformed field usually <em>fails loudly</em> (deserialization error, 4xx). A Spark reader handed a missing column usually <em>fails silently</em> (a column of <code>NULL</code>). So the data contract has to be enforced by an explicit CI gate — there's no framework throwing the exception for you.</p>` },
  ],
  lab: {
    tier: "T1",
    understand: {
      engine: "sql",
      datasets: ["order_events"],
      task: `<p><strong>Measure the schema drift the clickstream release introduced.</strong> The <code>order_events</code> table is the raw Bronze clickstream — and because it was ingested with a permissive reader, its rows literally don't all share the same columns. Some are missing <code>device</code>; some carry the newer <code>app_version</code>. Your job is to quantify both, so the contract review has numbers instead of adjectives.</p>
<p>Write one query returning three columns in this order: <code>total_events</code>, <code>missing_device</code> (rows where the required field is gone), and <code>carries_app_version</code> (rows on the newer, additive schema). Remember: <code>COUNT(*)</code> counts rows, but <code>COUNT(col)</code> skips <code>NULL</code>s — that difference <em>is</em> your drift meter.</p>`,
      starterQuery: `-- Starter: this only reports the total. Add the two drift columns.
SELECT
  COUNT(*) AS total_events
FROM order_events`,
      solutionQuery: `SELECT
  COUNT(*)                   AS total_events,
  COUNT(*) - COUNT(device)   AS missing_device,
  COUNT(app_version)         AS carries_app_version
FROM order_events`,
      hint: `COUNT(device) counts only rows where device is present, so COUNT(*) - COUNT(device) is exactly the rows that lost the required field — a <em>breaking</em> drift (17 of them). COUNT(app_version) counts the rows on the new optional field — an <em>additive</em> change old readers ignore (25 of them). Additive is safe; a required field going missing is the one to escalate.`
    },
    buildWithAI: `I'm learning data contracts and schema evolution in PySpark (additive vs breaking changes). Set up a real local project that makes the drift measurable. I'm on my own machine; assume nothing beyond Python 3.10+.

1. Create a project folder \`nimbusmart-contracts\` with a venv, and install pyspark (pin a recent 3.5.x) and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator that writes \`data/order_events.jsonl\` (JSON Lines, because the drift only makes sense in a schema-flexible format) modeling NimbusMart's clickstream:
   - 361 events across 180 distinct orders (order_id like O-10001..), event_id like E-90001..
   - event_type from [cart_add, checkout_start, payment_submitted, fraud_check, fulfillment_hold, shipped_scan]
   - event_ts ISO timestamps in May-June 2026
   - Every event normally has a \`device\` field (ios/android/web). Schema drift, on purpose: ~6% of events OMIT \`device\` entirely (breaking drop of a required field), and a separate ~7% ADD an optional \`app_version\` field (4.1.0/4.2.1) — additive. Make the exact counts come out to 17 missing device and 25 carrying app_version so my tests can assert them.

3. Create \`contract.py\` defining order_events_contract v1.2.0: required fields (event_id, order_id, event_type, event_ts, device) and optional (app_version), with a MAJOR/MINOR semver note on each. Include a named constant FRAUD_REVIEW_THRESHOLD = 0.80 with a comment that it's the downstream consumer rule.

4. Create \`drift_check.py\` that:
   - builds a SparkSession (local[*]), reads the JSONL with an EXPLICIT permissive StructType (device and app_version nullable)
   - computes total_events, missing_device = count(*) - count(device), carries_app_version = count(app_version)
   - prints a verdict: additive changes = OK, but any missing REQUIRED field is a BREAKING contract violation and should fail CI

5. Create \`test_contract.py\` (pytest) asserting: total_events == 361; missing_device == 17; carries_app_version == 25; and a test that FAILS (non-zero exit) when missing_device > 0, proving the gate would block the release. Do NOT hardcode 17/25 in drift_check.py — compute them; hardcode them only in the test as expected values.

6. Run the generator, drift_check, and pytest. Show me the failing-gate output and explain which drift was additive and which was breaking. Windows-friendly paths please.`
  },
  check: [
    {
      type: "mcq",
      q: "The clickstream release <em>adds</em> an optional <code>app_version</code> field and <em>drops</em> <code>device</code> from some events. How should the contract review classify these two changes?",
      options: [
        "Adding app_version is additive (backward-compatible); dropping the required device is breaking",
        "Both are additive, since Spark reads both without throwing an error",
        "Adding app_version is breaking (new field to parse); dropping device is additive (less data)",
        "Neither matters until a downstream job actually crashes on the change"
      ],
      answer: 0,
      explain: "Backward-compatibility is about whether OLD consumers still work. A new optional field they can ignore is additive; a required field vanishing silently poisons them with NULLs — that's breaking, precisely because Spark does NOT throw, so nothing forces the conversation."
    },
    {
      type: "predict",
      q: "The Bronze <code>order_events</code> rows don't all share the same columns (permissive ingestion). What does this print on the seed data?",
      code: `SELECT COUNT(*) - COUNT(device) AS lost_device
FROM order_events`,
      options: ["0", "17", "25", "361"],
      answer: 1,
      explain: "COUNT(*) counts all 361 rows; COUNT(device) skips the rows where device is NULL/absent, giving 344. 361 − 344 = 17 rows lost the required field — the breaking drift. (0 would mean 'no drift'; 25 is the app_version count; 361 is the total.)"
    },
    {
      type: "predict",
      q: "On the same table, how many events arrived on the newer, additive schema?",
      code: `SELECT COUNT(app_version) AS carries_app_version
FROM order_events`,
      options: ["361", "336", "25", "0"],
      answer: 2,
      explain: "COUNT(app_version) counts only rows where the optional field is present: 25. The other 336 predate the additive change and read back as NULL — which is exactly why the change is safe: old-schema rows and old consumers both just ignore the field."
    },
    {
      type: "mcq",
      q: "Why is enforcing a data contract in a CI gate more important for a Spark Bronze table than for a REST endpoint?",
      options: [
        "Spark tables are larger, so mistakes cost more storage",
        "A permissive Spark reader fills missing columns with NULL and keeps running, so a breaking change fails silently — there's no deserialization error to catch it",
        "REST endpoints never have breaking changes, so they don't need contracts",
        "CI can't run against REST endpoints, only against tables"
      ],
      answer: 1,
      explain: "A strict HTTP client usually rejects a malformed payload loudly. A permissive DataFrame reader turns a dropped column into a column of NULLs and stays green — so the only thing standing between a breaking change and a week of quiet corruption is an explicit contract check in CI."
    }
  ],
  fieldNotes: `A payments team I worked alongside had a producer rename <code>txn_amount</code> to <code>amount_cents</code> and switch the unit from dollars to cents in the same deploy — they'd added the new column additively for a week, then dropped the old one, and considered that a safe migration. It was, for every consumer they knew about. They did not know about a reconciliation job owned by finance that still read <code>txn_amount</code>; after the drop it read <code>NULL</code>, coalesced to 0, and reported that a quiet Tuesday had processed exactly zero dollars in three regions. No job failed. The dashboard was green for nine days until a controller noticed the flat line. The postmortem's one-line fix wasn't technical: register every consumer of the table in the catalog, and make "drop a field" require sign-off from each. The producer hadn't been reckless — they'd been additive-then-breaking without a contract that listed who was on the other end of the break.`
};
