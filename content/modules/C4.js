// C4 — Thinking in Data Quality (Track C, Tier 1 / SQL)
// Verified facts used by lab + checks (data/nimbusmart/generate.py, seed 42;
// run against engine/sqlrunner.js):
//   customers=60; 3 have NULL city; COUNT(*)=60 vs COUNT(city)=57
//   2 casing-duplicate email pairs (upper/title variants of another row's email):
//     GROUP BY LOWER(email) HAVING COUNT(*) > 1  -> 2 rows; GROUP BY email -> 0 rows
//   order_events schema drift: some rows drop 'device', some add 'app_version'
//   FRAUD_REVIEW_THRESHOLD=0.80 framed here as a tested invariant (43-row queue)
export default {
  id: "C4",
  track: "C",
  title: "Thinking in Data Quality",
  minutes: 24,
  coldOpen: "The weekly “active customers” number jumps 4% overnight with no marketing spend, and finance wants to know why. It didn't: two customers had signed up twice with the same email in different letter-casing — Wei.Kimura vs wei.kimura — and a dashboard that counted DISTINCT email treated the casings as two people. Meanwhile three customers with a NULL city silently vanished from the regional breakdown, because a GROUP BY drops the group nobody named. Nothing errored. Every number was wrong.",
  concept: [
    { type: "prose", html: `
<p>Data quality is not a phase that happens after the pipeline, in a QA sign-off. It is a set of <strong>properties you engineer into the pipeline</strong>, the same way you engineer idempotency or backpressure. Three failure classes cause most incidents, and each is a decision you either make on purpose or inherit by accident:</p>
<ul>
<li><strong>Nulls — absence.</strong> A missing value isn't zero and isn't empty string; in SQL it's <code>NULL</code>, and <code>NULL</code> propagates: any comparison with it is neither true nor false, and a <code>GROUP BY</code> quietly sets its group aside. You decide what absence <em>means</em> (unknown? not-applicable? default?) or the query decides for you.</li>
<li><strong>Dupes — identity.</strong> “The same customer” is a definition, not a given. If your identity key is raw <code>email</code>, then <code>Wei.Kimura@…</code> and <code>wei.kimura@…</code> are two people. Dedup is choosing a <em>normalized</em> key (lower-cased, trimmed) and enforcing it.</li>
<li><strong>Drift — shape.</strong> Producers change the payload: <code>order_events</code> here sometimes drops <code>device</code> and sometimes adds <code>app_version</code>. A reader that assumes a fixed schema breaks; a reader that takes the <em>union</em> of keys and tolerates absence survives. (Both this SQL engine and Spark take the union across drifting rows.)</li>
</ul>
<p>The discipline that ties them together: a quality rule you care about gets a <em>test</em>, not a comment. The <code>FRAUD_REVIEW_THRESHOLD</code> (0.80) queue is the canonical example — “exactly 43 orders should land in review” is an invariant you assert on every run, so the day a join or a boundary operator silently changes it, a test fails instead of an auditor noticing months later.</p>` },
    { type: "code", lang: "sql", code: `-- NULLS: COUNT(*) counts rows; COUNT(col) skips NULLs. The gap IS the null count.
SELECT COUNT(*) AS total,          -- 60 customers
       COUNT(city) AS with_city,   -- 57 — three cities are NULL
       COUNT(*) - COUNT(city) AS missing_city
FROM customers;

-- NULL is not a value you can '=' against — only IS [NOT] NULL tests it:
SELECT customer_id FROM customers WHERE city IS NULL;     -- the 3 rows
-- WHERE city = NULL  would return ZERO rows — NULL = NULL is not true.

-- DUPES: raw email hides casing duplicates. Normalize the identity key first.
SELECT LOWER(email) AS email_norm, COUNT(*) AS n
FROM customers
GROUP BY LOWER(email)     -- collapse casings onto one key
HAVING COUNT(*) > 1       -- keep only the collisions
ORDER BY email_norm;
-- GROUP BY email (no LOWER) finds NOTHING here — the casings never collide.`, caption: "Absence, identity, and the one-character difference (LOWER) between finding your duplicates and missing them." },
    { type: "svg", svg: `<svg viewBox="0 0 720 210" font-family="var(--mono)" font-size="12">
<text x="20" y="22" fill="var(--ink2)" font-size="11">THREE FAILURE CLASSES — each a design decision, not a QA afterthought</text>
<rect x="20" y="34" width="215" height="150" rx="10" fill="var(--paper2)" stroke="var(--line)"/>
<text x="34" y="56" fill="var(--accent)" font-weight="bold">NULL · absence</text>
<text x="34" y="80" fill="var(--ink)">city = NULL</text>
<text x="34" y="100" fill="var(--ink2)" font-size="10">NULL = NULL  → not true</text>
<text x="34" y="118" fill="var(--ink2)" font-size="10">GROUP BY drops the group</text>
<text x="34" y="142" fill="var(--rust)" font-size="10">fix: IS NULL, COALESCE,</text>
<text x="34" y="158" fill="var(--rust)" font-size="10">decide what absence means</text>
<rect x="252" y="34" width="215" height="150" rx="10" fill="var(--paper2)" stroke="var(--line)"/>
<text x="266" y="56" fill="var(--accent)" font-weight="bold">DUPE · identity</text>
<text x="266" y="80" fill="var(--ink)">Wei.K@…  vs  wei.k@…</text>
<text x="266" y="100" fill="var(--ink2)" font-size="10">raw email → 2 people</text>
<text x="266" y="118" fill="var(--ink2)" font-size="10">count inflates</text>
<text x="266" y="142" fill="var(--rust)" font-size="10">fix: normalized key</text>
<text x="266" y="158" fill="var(--rust)" font-size="10">LOWER(TRIM(email))</text>
<rect x="484" y="34" width="216" height="150" rx="10" fill="var(--paper2)" stroke="var(--line)"/>
<text x="498" y="56" fill="var(--accent)" font-weight="bold">DRIFT · shape</text>
<text x="498" y="80" fill="var(--ink)">event A: {device}</text>
<text x="498" y="100" fill="var(--ink)">event B: {app_version}</text>
<text x="498" y="118" fill="var(--ink2)" font-size="10">fixed-schema read breaks</text>
<text x="498" y="142" fill="var(--rust)" font-size="10">fix: union of keys,</text>
<text x="498" y="158" fill="var(--rust)" font-size="10">tolerate absent fields</text>
</svg>`, caption: "Nulls, dupes, drift — the three shapes of dirty data, and the design choice each one forces." },
    { type: "analogy", title: "The QC station, and the pallet spec you test on arrival", html: `
<p>On the Freight Line, quality lives at the <strong>QC station</strong> between the receiving dock and the showroom — you don't ship a pallet to the customer floor and inspect it there. A <strong>null</strong> is a parcel with a missing destination label: you can't just guess, so you decide the rule (hold it, route it to “unknown,” or reject it) before it hits the belt. A <strong>dupe</strong> is the same item scanned twice under two slightly different barcodes; if your scanner treats “ABC-1” and “abc-1” as different SKUs, your stock count lies — so you normalize the barcode before you trust it.</p>
<p><strong>Drift</strong> is a supplier who quietly changed the pallet spec — dropped a field here, added one there. The station survives it by reading against a tolerant contract, not a rigid template. And the rules that matter — the pallet spec, the fraud-review threshold — aren't taped to the wall as guidance; they're <em>gauges the station tests every pallet against</em>. A rule you merely document drifts out of true silently. A rule you test fails loudly the moment reality diverges.</p>` },
    { type: "javaBridge", html: `
<p>Your Java instincts are close, but three of them will bite on data at rest:</p>
<ul>
<li><strong>NULL is not Java <code>null</code>.</strong> In Java <code>null == null</code> is <code>true</code>; in SQL it isn't — <code>NULL = NULL</code> yields unknown, so equality-based dedup silently misses null keys. SQL forces <code>IS NULL</code>, three-valued logic, and an explicit <code>COALESCE</code> (your <code>Optional.orElse</code>) to decide meaning.</li>
<li><strong>Dedup is <code>equals()</code>/<code>hashCode()</code>, but you pick the fields.</strong> Two customers are “equal” only under the key you choose. Comparing raw email is comparing case-sensitive strings; the right key is <code>email.toLowerCase().trim()</code> — normalize before you hash.</li>
<li><strong>Drift is a lenient Jackson deserializer.</strong> A producer adding <code>app_version</code> to the JSON is fine if you ignore unknown fields; dropping <code>device</code> is fine if the field is nullable. The break comes when a downstream reader assumes a rigid POJO. Same negotiation as <code>@JsonIgnoreProperties(ignoreUnknown = true)</code> — just at table scale, and you'll formalize it as a data contract in Track G.</li>
</ul>` },
  ],
  lab: {
    tier: "T1",
    understand: {
      engine: "sql",
      datasets: ["customers"],
      task: `<p><strong>Find the casing-duplicate customers.</strong> Two of NimbusMart's 60 customers registered a second time with the same email in different letter-casing (an upper-cased and a title-cased variant of another row's address). Your job: surface every email that appears more than once <em>once you ignore case</em>.</p><p>The starter groups by the raw <code>email</code> — and finds <strong>nothing</strong>, because the casings never collide as exact strings. Normalize the identity key with <code>LOWER()</code> before grouping. The check expects <strong>2</strong> collision rows.</p>`,
      starterQuery: `SELECT email AS email_norm, COUNT(*) AS n
FROM customers
GROUP BY email          -- raw email: 'Wei.K' and 'wei.k' don't collide
HAVING COUNT(*) > 1
ORDER BY email_norm`,
      solutionQuery: `SELECT LOWER(email) AS email_norm, COUNT(*) AS n
FROM customers
GROUP BY LOWER(email)   -- normalize case so the duplicates land on one key
HAVING COUNT(*) > 1
ORDER BY email_norm`,
      hint: `The two spellings only look duplicate to a human — as raw strings they differ, so <code>GROUP BY email</code> puts them in separate groups and <code>HAVING COUNT(*) &gt; 1</code> finds none. Wrap the key in <code>LOWER(...)</code> in both the <code>SELECT</code> and the <code>GROUP BY</code> so the casings collapse onto one normalized key.`
    },
    buildWithAI: `I'm a Java/backend developer learning to treat data quality (nulls, duplicates, schema drift) as an engineering property, not a QA phase. Scaffold a real, runnable local project on my own machine (assume Python 3.10+ only).

1. Create a folder \`nimbusmart-quality\` with a venv (\`python -m venv .venv\`) and install pandas and pytest (pin recent versions). Give me the Windows and macOS/Linux activation lines.

2. \`generate_data.py\` — deterministic (\`random.seed(42)\`) generator that writes INTENTIONALLY MESSY data into \`data/\`:
   - \`customers.csv\`: 60 rows — customer_id (C-0001..C-0060), name, email (name-based), city, country. Then inject exactly these defects: set city to empty/NULL for 3 rows; make 2 rows' emails a differently-cased copy of two other rows' emails (one UPPER, one Title-case) so there are exactly 2 casing-duplicate pairs.
   - \`order_events.jsonl\`: ~360 event rows with schema drift — most rows have a "device" field, ~6% DROP it, ~8% ADD an "app_version" field. Write it as JSON lines.
   Assert the injected defect counts at the end so the generator fails loudly if they drift.

3. \`profile.py\` — a data-quality report using pandas:
   - null profile: count and percent of NULL city (expect 3)
   - dup profile: casing-duplicate emails via \`df.assign(norm=df.email.str.lower()).groupby("norm").size()\` filtered to > 1 (expect 2 groups)
   - drift profile: read order_events.jsonl and report which keys are present in what fraction of rows (device < 100%, app_version > 0%)
   Print a compact report.

4. \`test_quality.py\` (pytest) — these are the quality GATES: assert exactly 3 NULL cities; assert exactly 2 casing-duplicate email groups; assert 'device' is missing from at least one event and 'app_version' present in at least one. Re-derive from the files; do not hardcode beyond the injected invariants.

5. Run \`python generate_data.py\`, then \`python profile.py\`, then \`pytest -q\`. Then explain, in a short paragraph, why these assertions belong in CI and not in a one-time manual QA check. Windows-friendly paths throughout.`
  },
  check: [
    {
      type: "predict",
      q: "customers has 60 rows; 3 have a NULL city. What does this return?",
      code: `SELECT COUNT(*) AS total, COUNT(city) AS with_city
FROM customers;`,
      options: ["total = 60, with_city = 57", "total = 60, with_city = 60", "total = 57, with_city = 57", "total = 60, with_city = 3"],
      answer: 0,
      explain: "COUNT(*) counts every row (60). COUNT(city) counts only rows where city is non-NULL, skipping the 3 nulls (57). The gap — COUNT(*) − COUNT(col) — is a free, exact null count for any column, and one of the cheapest quality checks you can add."
    },
    {
      type: "mcq",
      q: "Why does <code>WHERE city = NULL</code> return zero rows even though three customers have a NULL city?",
      options: [
        "Because <code>NULL = NULL</code> is not true — NULL means “unknown,” and any comparison to it yields unknown, so the row is excluded; you must use <code>IS NULL</code>",
        "Because <code>=</code> only works on numbers, and city is text",
        "Because the three NULL cities were automatically converted to empty strings on load",
        "Because <code>WHERE</code> silently skips any row containing a NULL in any column"
      ],
      answer: 0,
      explain: "SQL uses three-valued logic: NULL is unknown, so `city = NULL` evaluates to unknown (not true), and WHERE keeps only rows that are true. This is the sharpest departure from Java, where `null == null` is true. Testing for absence requires the dedicated `IS NULL` / `IS NOT NULL` predicates."
    },
    {
      type: "mcq",
      q: "The dedup query uses <code>GROUP BY LOWER(email)</code> instead of <code>GROUP BY email</code>. What breaks if you drop the <code>LOWER()</code>?",
      options: [
        "The casing-duplicate pairs land in separate groups and <code>HAVING COUNT(*) &gt; 1</code> finds none — the duplicates go undetected",
        "The query errors, because <code>GROUP BY</code> requires a function around the column",
        "It returns every customer, because grouping by a unique column makes all counts 1 and passes HAVING",
        "Nothing — <code>GROUP BY</code> is case-insensitive by default, so both spellings collide anyway"
      ],
      answer: 0,
      explain: "Grouping on the raw string treats 'Wei.Kimura@…' and 'wei.kimura@…' as distinct keys, so each duplicate sits alone in its group with count 1 and HAVING filters it out — the collision is invisible. Identity for dedup is a normalized key you choose (here, lower-cased); the engine does not normalize case for you."
    },
    {
      type: "mcq",
      q: "The rule “exactly 43 orders are at or above FRAUD_REVIEW_THRESHOLD (0.80), and belong in the review queue” is written as an automated assertion that runs on every pipeline execution. Why there, rather than as a comment or a one-time QA check?",
      options: [
        "Because quality rules are invariants that silently rot — a changed join, a <code>&gt;</code>-for-<code>&gt;=</code> typo, or upstream data drift can move the count any day, and only a test that runs every time fails loudly when it does",
        "Because comments are not allowed in production SQL",
        "Because the review queue only needs to be correct on the first run; later runs reuse the cached result",
        "Because a one-time QA check is more thorough than an automated assertion and so is redundant to repeat"
      ],
      answer: 0,
      explain: "A documented rule is a hope; a tested rule is a guarantee that re-checks itself. The queue's size depends on the join, the boundary operator, and the freshness of fraud_scores — all of which can change unnoticed. Asserting the invariant every run turns a silent months-later audit finding into an immediate, located failure. This is exactly the validation-gate discipline formalized in Track G."
    }
  ],
  fieldNotes: `A subscription business discovered during an investor audit that its headline “monthly active accounts” had been overstated by roughly 6% for over a year, and the root cause was almost embarrassingly small: signups came from two front-ends, one lower-casing emails and one preserving whatever the user typed, and the identity key was the raw email string. <code>alex@x.com</code> and <code>Alex@x.com</code> were two accounts to every downstream count. No system errored; the number was simply built on the wrong definition of “the same person.” The fix was a one-line normalization (<code>LOWER(TRIM(email))</code>) applied at the Silver layer plus a backfill to merge the historical pairs — but the durable change was cultural: identity keys, null handling, and expected row counts became <em>tested</em> properties in CI, not tribal knowledge. The rule the team wrote on the whiteboard afterward: if a number matters, its definition is code, and its expected shape is an assertion — because dirty data never throws an exception, it just quietly lies.`
};
