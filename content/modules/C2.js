// C2 — SQL for Analytics (Track C, Tier 1 / SQL) — KEYSTONE
// Verified facts used by lab + checks (data/nimbusmart/generate.py, seed 42;
// run against engine/sqlrunner.js):
//   orders=240; fraud_scores=225 (15 unscored); FRAUD_REVIEW_THRESHOLD=0.80
//   orders LEFT JOIN fraud_scores WHERE fraud_score >= 0.80 (FRAUD_REVIEW_THRESHOLD) -> 43 rows
//   same WHERE but '>' (exclusive) -> 39 rows: drops the 4 scored exactly at the threshold
//   ROW_NUMBER() OVER (PARTITION BY country ORDER BY total_amount DESC), rn=1 -> 7 rows
export default {
  id: "C2",
  track: "C",
  title: "SQL for Analytics",
  minutes: 28,
  coldOpen: "Two analysts build “the same” fraud-review queue from the same tables and hand risk two different lists — one with 43 orders, one with 39. The four missing orders were the ones scored at exactly FRAUD_REVIEW_THRESHOLD (0.80). One analyst wrote `fraud_score > 0.80`, the other `>= 0.80`, and a boundary that reads as a rounding footnote quietly decided which orders a human ever looked at. Nobody was wrong about the SQL; someone was wrong about the policy the SQL encoded.",
  concept: [
    { type: "prose", html: `
<p>SQL is the one language every layer of a data platform speaks — the warehouse, the lakehouse query engine, and (as you'll see in Track D) Spark itself, which compiles DataFrame code into the same relational plan a <code>SELECT</code> would produce. Learn to think in sets and you've learned the substrate under all of it.</p>
<p>Three constructs carry analytics work, and this module is about wiring them together:</p>
<ul>
<li><strong>Aggregations</strong> — <code>GROUP BY</code> collapses many rows into one per group, with <code>COUNT / SUM / AVG / MIN / MAX</code> over the collapsed rows. <code>HAVING</code> filters those groups (a <code>WHERE</code> for aggregates).</li>
<li><strong>CTEs</strong> — <code>WITH name AS (SELECT ...)</code> names an intermediate result so the next query can read it. They turn a nested mess into a top-to-bottom pipeline of named steps — the SQL equivalent of extracting well-named local variables.</li>
<li><strong>Window functions</strong> — ranking and running totals that see neighbouring rows <em>without</em> collapsing them. The difference from <code>GROUP BY</code> is the whole point: a window keeps every input row and adds a computed column.</li>
</ul>` },
    { type: "code", lang: "sql", code: `-- Aggregation: revenue and order count per country, biggest first.
SELECT country,
       COUNT(*)            AS orders,
       ROUND(SUM(total_amount), 2) AS revenue
FROM orders
GROUP BY country
HAVING COUNT(*) >= 20         -- filter the GROUPS, not the rows
ORDER BY revenue DESC;

-- Window function: rank orders WITHIN each country by value, keeping every row.
-- A CTE computes the ranking; the outer query filters to the top 3 per country.
WITH ranked AS (
  SELECT order_id, country, total_amount,
         ROW_NUMBER() OVER (PARTITION BY country
                            ORDER BY total_amount DESC) AS rn
  FROM orders
)
SELECT country, order_id, total_amount
FROM ranked
WHERE rn <= 3                 -- top-N-per-group: impossible with GROUP BY alone
ORDER BY country, rn;`, caption: "GROUP BY collapses to one row per country; ROW_NUMBER keeps all rows and numbers them within each country partition." },
    { type: "svg", svg: `<svg viewBox="0 0 720 250" font-family="var(--mono)" font-size="12">
<text x="20" y="22" fill="var(--ink2)" font-size="11">GROUP BY — many rows collapse to one per group</text>
<rect x="20" y="32" width="150" height="86" rx="8" fill="var(--paper2)" stroke="var(--line)"/>
<text x="30" y="52" fill="var(--ink)">DE  120</text><text x="30" y="70" fill="var(--ink)">DE  300</text><text x="30" y="88" fill="var(--ink)">US  90</text><text x="30" y="106" fill="var(--ink)">US  410</text>
<line x1="176" y1="75" x2="228" y2="75" stroke="var(--ink2)"/><text x="188" y="68" fill="var(--ink2)" font-size="10">SUM</text>
<rect x="234" y="46" width="130" height="58" rx="8" fill="var(--paper2)" stroke="var(--accent)"/>
<text x="244" y="70" fill="var(--accent)">DE  420</text><text x="244" y="92" fill="var(--accent)">US  500</text>
<text x="234" y="126" fill="var(--ink2)" font-size="10">2 rows out — detail is gone</text>
<text x="400" y="22" fill="var(--ink2)" font-size="11">WINDOW — every row survives, gains a column</text>
<rect x="400" y="32" width="150" height="86" rx="8" fill="var(--paper2)" stroke="var(--line)"/>
<text x="410" y="52" fill="var(--ink)">DE  300</text><text x="410" y="70" fill="var(--ink)">DE  120</text><text x="410" y="88" fill="var(--ink)">US  410</text><text x="410" y="106" fill="var(--ink)">US  90</text>
<line x1="556" y1="75" x2="592" y2="75" stroke="var(--rust)"/>
<rect x="576" y="32" width="128" height="86" rx="8" fill="var(--paper2)" stroke="var(--rust)"/>
<text x="586" y="52" fill="var(--rust)">DE 300 · rn 1</text><text x="586" y="70" fill="var(--rust)">DE 120 · rn 2</text><text x="586" y="88" fill="var(--rust)">US 410 · rn 1</text><text x="586" y="106" fill="var(--rust)">US 90 · rn 2</text>
<text x="576" y="132" fill="var(--ink2)" font-size="10">4 rows out — ranked within country</text>
<text x="20" y="176" fill="var(--ink2)" font-size="11">THE FRAUD QUEUE — LEFT JOIN keeps all 240 orders, then WHERE keeps the 43 at/above threshold</text>
<rect x="20" y="188" width="684" height="48" rx="10" fill="var(--paper2)" stroke="var(--accent)"/>
<text x="355" y="210" text-anchor="middle" fill="var(--ink)">240 orders  ── LEFT JOIN fraud_scores ──▶  15 unscored land NULL  ──▶  WHERE score &gt;= FRAUD_REVIEW_THRESHOLD  ──▶  43 rows</text>
<text x="355" y="228" text-anchor="middle" fill="var(--ink2)" font-size="10">the 15 NULL-score orders fall out of a &gt;= filter — that is a data-quality flag, not a clean bill of health</text>
</svg>`, caption: "GROUP BY collapses; window functions annotate. The review queue is a LEFT JOIN then a threshold filter." },
    { type: "analogy", title: "Sub-assemblies on the line, and ranking without merging bins", html: `
<p>A CTE is a <strong>labeled sub-assembly station</strong> on the Freight Line. Instead of one monstrous workstation doing everything in a tangle, you build <code>ranked</code> at one station, hand it down the belt, and the next station reads from it by name. Anyone reading the line top-to-bottom sees the steps in order — that's what <code>WITH ranked AS (...)</code> buys you over a nested subquery.</p>
<p>The <code>GROUP BY</code>-vs-window distinction is the difference between <em>weighing a bin</em> and <em>ranking the parcels inside it</em>. <code>GROUP BY country</code> tips every parcel for a country into one bin and weighs it — you get a total, but the individual parcels are gone. A window function walks the same country's parcels and writes “#1, #2, #3” on each one, leaving them exactly where they sat. When you need “the top three orders <em>per</em> country,” you cannot weigh the bin — you must rank inside it and keep the parcels.</p>` },
    { type: "javaBridge", html: `
<p>You've written most of this in Java collections; SQL just does it declaratively and at table scale:</p>
<ul>
<li><code>GROUP BY country → SUM(total)</code> is <code>Collectors.groupingBy(Order::country, summingDouble(Order::total))</code> — same collapse, but the engine picks the algorithm (hash vs sort) instead of you.</li>
<li>A <strong>CTE</strong> is extracting a well-named intermediate: <code>var ranked = ...; use(ranked);</code>. It exists for readability and reuse within the query, not as a stored table.</li>
<li>A <strong>window function</strong> has no tidy Streams equivalent — you'd <code>groupingBy</code>, then sort each group, then hand-number the elements with an index, then flatten back. <code>ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)</code> is that whole dance in one clause.</li>
<li>The trap that has no Java analog: <strong><code>&gt;</code> vs <code>&gt;=</code> on a policy boundary.</strong> There's no compiler to tell you the fraud threshold is inclusive — only the spec, and the four orders sitting exactly on it.</li>
</ul>` },
  ],
  lab: {
    tier: "T1",
    understand: {
      engine: "sql",
      datasets: ["orders", "fraud_scores"],
      task: `<p><strong>Build the fraud-review queue.</strong> Risk wants every order whose fraud score is <em>at or above</em> <code>FRAUD_REVIEW_THRESHOLD</code> (0.80), joined back to the order so they have the customer and amount. Start from all orders with a <code>LEFT JOIN</code> to <code>fraud_scores</code>, then filter.</p><p>The starter uses <code>&gt;</code> — strictly greater than. Run it: you'll get <strong>39</strong> rows, and the expected-result check will tell you it wants <strong>43</strong>. The four missing orders are scored at <em>exactly</em> the <code>FRAUD_REVIEW_THRESHOLD</code> boundary (0.80). Change one operator so the boundary is inclusive.</p>`,
      starterQuery: `SELECT o.order_id, o.customer_id, o.total_amount, f.fraud_score
FROM orders o
LEFT JOIN fraud_scores f ON o.order_id = f.order_id
WHERE f.fraud_score > 0.80        -- exclusive: drops orders exactly at FRAUD_REVIEW_THRESHOLD
ORDER BY f.fraud_score DESC, o.order_id`,
      solutionQuery: `SELECT o.order_id, o.customer_id, o.total_amount, f.fraud_score
FROM orders o
LEFT JOIN fraud_scores f ON o.order_id = f.order_id
WHERE f.fraud_score >= 0.80       -- inclusive: 'at or above' FRAUD_REVIEW_THRESHOLD
ORDER BY f.fraud_score DESC, o.order_id`,
      hint: `The policy is “<em>at or above</em> the threshold,” so the boundary is inclusive. You have 39 rows; expected is 43 — <code>&gt;=</code> includes the 4 orders scored exactly at FRAUD_REVIEW_THRESHOLD (0.80) that <code>&gt;</code> silently drops. Change <code>&gt;</code> to <code>&gt;=</code>.`
    },
    buildWithAI: `I'm learning analytics SQL (CTEs, aggregations, window functions) for a fraud-review use case, coming from a Java background. Scaffold a real, runnable local project using DuckDB (an embedded SQL engine — no server to install).

1. Create a folder \`nimbusmart-sql\` with a Python venv and install \`duckdb\` and \`pytest\` (pin recent versions). Windows-friendly activation notes.

2. \`generate_data.py\` — deterministic (\`random.seed(42)\`) generator writing two CSVs into \`data/\`:
   - \`orders.csv\`: 240 rows — order_id (O-10001..O-10240), customer_id (C-0001..C-0060), total_amount (8..950, 2dp), country from [DE, US, IN, BR, JP, FR, AU]
   - \`fraud_scores.csv\`: scores for exactly 225 of those orders (15 unscored, chosen with the same seed) — order_id, fraud_score (0.01..0.99, 2dp). Define FRAUD_REVIEW_THRESHOLD = 0.80 as a named constant, then engineer the data so EXACTLY 43 orders end up with fraud_score >= FRAUD_REVIEW_THRESHOLD and EXACTLY 4 of those are exactly at it — assert both facts at the end of the script so the generator fails loudly if the invariant breaks.

3. \`queries.sql\` — write, with comments:
   - the review queue: orders LEFT JOIN fraud_scores WHERE fraud_score >= FRAUD_REVIEW_THRESHOLD (0.80), returning order_id, customer_id, total_amount, fraud_score
   - a top-3-orders-per-country query using ROW_NUMBER() OVER (PARTITION BY country ORDER BY total_amount DESC) inside a CTE

4. \`run.py\` — open an in-memory DuckDB, \`read_csv_auto\` both files into tables, execute each query, and print the row counts and first rows.

5. \`test_queue.py\` (pytest): assert the review-queue query returns exactly 43 rows; assert that swapping >= for > returns exactly 39 (proving the 4 boundary orders); assert the LEFT JOIN keeps all 240 orders before the WHERE (count orders LEFT JOINed = 240). Re-derive nothing by hand — compute expectations from the CSVs.

6. Run generate_data.py, then run.py, then \`pytest -q\`. Show me the review-queue result and explain, in one paragraph, why > and >= differ by exactly the 4 orders at the threshold. Windows-friendly paths throughout.`
  },
  check: [
    {
      type: "predict",
      q: "On the NimbusMart seed (43 orders scored ≥ FRAUD_REVIEW_THRESHOLD, 4 of them exactly 0.80), how many rows does this return?",
      code: `SELECT o.order_id, f.fraud_score
FROM orders o
LEFT JOIN fraud_scores f ON o.order_id = f.order_id
WHERE f.fraud_score > 0.80;   -- note: strictly greater than FRAUD_REVIEW_THRESHOLD`,
      options: ["39", "43", "225", "240"],
      answer: 0,
      explain: "`>` is exclusive, so it drops the 4 orders scored at exactly FRAUD_REVIEW_THRESHOLD (0.80): 43 − 4 = 39. The 15 unscored orders have a NULL fraud_score, and NULL never satisfies a comparison, so they're excluded too. This is the single-character gap that split the two analysts' queues in the cold open."
    },
    {
      type: "mcq",
      q: "The review policy reads “orders at or above FRAUD_REVIEW_THRESHOLD (0.80) go to human review.” Which predicate encodes it, and why does the choice matter here specifically?",
      options: [
        "<code>fraud_score &gt;= FRAUD_REVIEW_THRESHOLD</code> — the boundary is inclusive, and 4 orders sit exactly on it, so <code>&gt;</code> would silently hide them from reviewers",
        "<code>fraud_score &gt; FRAUD_REVIEW_THRESHOLD</code> — “at or above” always means strictly greater in risk contexts",
        "<code>fraud_score = FRAUD_REVIEW_THRESHOLD</code> — only orders exactly at the threshold need review",
        "Either works — with real-valued scores nothing ever lands exactly on the threshold"
      ],
      answer: 0,
      explain: "“At or above” is inclusive: `>=`. The distractor that bites teams is assuming scores never hit the boundary exactly — on this data 4 of them do, and on real scoring models rounded to 2 decimals, exact-boundary values are common, not rare. The operator is a policy decision wearing a syntax costume."
    },
    {
      type: "mcq",
      q: "You need the top 3 orders by <code>total_amount</code> <em>within each country</em>. Why can't a plain <code>GROUP BY country</code> do it?",
      options: [
        "<code>GROUP BY</code> collapses each country to one row, destroying the individual orders — you need a window function to rank rows while keeping them, then filter <code>rn &lt;= 3</code>",
        "<code>GROUP BY</code> can't be combined with <code>ORDER BY</code>, so ranking is impossible",
        "It can — <code>GROUP BY country LIMIT 3</code> returns the top 3 per country directly",
        "<code>GROUP BY</code> only works on numeric columns, and country is text"
      ],
      answer: 0,
      explain: "GROUP BY produces one row per country — the per-order detail you want to rank is gone. `LIMIT 3` would cap the whole result at 3 rows, not 3 per country. `ROW_NUMBER() OVER (PARTITION BY country ORDER BY total_amount DESC)` numbers orders inside each country while keeping every row, and the outer query filters `rn <= 3`."
    },
    {
      type: "predict",
      q: "How many rows does this top-1-per-country query return over the 240 orders (7 distinct countries)?",
      code: `WITH ranked AS (
  SELECT order_id, country,
         ROW_NUMBER() OVER (PARTITION BY country
                            ORDER BY total_amount DESC) AS rn
  FROM orders
)
SELECT country, order_id FROM ranked WHERE rn = 1;`,
      options: ["7", "240", "1", "43"],
      answer: 0,
      explain: "One partition per distinct country (7 of them), and `rn = 1` keeps the single highest-value order in each — so 7 rows. Change the filter to `rn <= 3` and you'd get up to 21. The window ranked all 240 rows first; the WHERE then sliced one per partition."
    }
  ],
  fieldNotes: `A fintech's transaction-review queue quietly ran on <code>score &gt; 0.90</code> for months after a threshold was migrated from a config file into a hand-written SQL view, where <code>&gt;=</code> became <code>&gt;</code> in the retype. The review threshold in the spec was inclusive; the view was exclusive. The blast radius was invisible precisely because it was small and boundary-shaped: only transactions scored at exactly 0.90 slipped past unreviewed, a few a day, each one a case the model had flagged as maximally suspicious-but-borderline. It surfaced in a quarterly audit when a chargeback traced back to an order that “should have been in the queue” and wasn't. The postmortem's one durable rule: any threshold that encodes a policy gets a test asserting the row count at and around the boundary — because <code>&gt;</code> vs <code>&gt;=</code> is not a style choice, it's the difference between reviewing a case and not.`
};
