// B2 — Slowly Changing Dimensions (T1 SQL)
// Verified facts (from data/nimbusmart/generate.py, seed 42):
//   customer_updates = 8 rows across 7 distinct customers.
//   C-0042 (base city Paris/FR) is the only customer with >1 update:
//     Munich/DE @ 2026-06-05T09:12:00, then Hamburg/DE @ 2026-06-22T14:03:00.
//   6 other customers have exactly 1 update each.
//   As-of 2026-06-10, C-0042's latest landed update is Munich; as-of 2026-06-25 it is Hamburg.
export default {
  id: "B2",
  track: "B",
  title: "Slowly Changing Dimensions",
  minutes: 26,
  coldOpen: "Finance reruns Q2 revenue-by-city and the numbers move — Munich is up, Hamburg is down — even though not one order changed. The culprit is customer C-0042, who moved from Munich to Hamburg on June 22nd. The nightly customer sync had simply overwritten her city to Hamburg, so every order she'd ever placed, including the April ones shipped to Munich, retroactively became a Hamburg order. The dimension told the truth about today and lied about every yesterday.",
  concept: [
    { type: "prose", html: `
<p>A dimension attribute you thought was static isn't. A customer moves city. A product changes category. A seller gets re-tiered. These change rarely and unpredictably — hence <strong>slowly changing dimensions</strong> — and the only real question is what your dimension does with the <em>old</em> value when a new one arrives. There is a taxonomy of answers (Type 0 through 6); in practice two of them carry almost all the weight, and choosing wrong is the cold open.</p>
<ul>
<li><strong>SCD Type 1 — overwrite.</strong> The new value replaces the old, in place. One row per customer, always current, no history. <code>UPDATE customers SET city = 'Hamburg' WHERE customer_id = 'C-0042'</code>. Simple, cheap, and it silently rewrites the past — which is exactly why Q2 moved.</li>
<li><strong>SCD Type 2 — version.</strong> The old row is closed and a new row is opened, so the customer now has <em>multiple</em> rows through time, each stamped with when it was valid. History is preserved; you can ask &ldquo;what city was C-0042 in on May 1st?&rdquo; and get Munich, not Hamburg.</li>
</ul>` },
    { type: "prose", html: `
<p>SCD2 buys history with three bookkeeping columns, and the whole pattern lives in them:</p>
<ul>
<li><code>valid_from</code> — the timestamp this version became true.</li>
<li><code>valid_to</code> — when it stopped being true (null, or a sentinel like <code>9999-12-31</code>, for the version still in force).</li>
<li><code>is_current</code> — a boolean flag on the one live row, so the common &ldquo;where are they <em>now</em>&rdquo; query doesn't have to reason about date ranges.</li>
</ul>
<p>So C-0042 becomes three rows, not one: <em>Paris</em> (valid_from her signup, valid_to June 5), <em>Munich</em> (June 5 → June 22), and <em>Hamburg</em> (June 22 → open, <code>is_current = Y</code>). A fact joins to the version whose <code>[valid_from, valid_to)</code> window contains the order's timestamp — so April orders find Munich-era rows and the past stops moving. The measures never changed; only the dimension learned to remember.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 260" font-family="var(--mono)" font-size="12">
<text x="20" y="26" fill="var(--ink2)" font-size="11">SCD1 — overwrite: one row, no memory</text>
<rect x="20" y="38" width="320" height="40" rx="8" fill="var(--paper2)" stroke="var(--rust)"/>
<text x="34" y="63" fill="var(--ink)">C-0042 · city = Hamburg</text>
<text x="250" y="63" fill="var(--rust)" font-size="10">(Munich erased)</text>
<text x="20" y="118" fill="var(--ink2)" font-size="11">SCD2 — version: a row per era, each stamped valid_from → valid_to</text>
<rect x="20" y="130" width="210" height="40" rx="8" fill="var(--paper2)" stroke="var(--line)"/>
<text x="34" y="149" fill="var(--ink)">C-0042 · Paris</text>
<text x="34" y="163" fill="var(--ink2)" font-size="9">signup → 06-05 · is_current N</text>
<rect x="242" y="130" width="210" height="40" rx="8" fill="var(--paper2)" stroke="var(--line)"/>
<text x="256" y="149" fill="var(--ink)">C-0042 · Munich</text>
<text x="256" y="163" fill="var(--ink2)" font-size="9">06-05 → 06-22 · is_current N</text>
<rect x="464" y="130" width="210" height="40" rx="8" fill="var(--paper2)" stroke="var(--green)" stroke-width="2"/>
<text x="478" y="149" fill="var(--ink)">C-0042 · Hamburg</text>
<text x="478" y="163" fill="var(--green)" font-size="9">06-22 → open · is_current Y</text>
<line x1="230" y1="150" x2="240" y2="150" stroke="var(--ink2)"/>
<line x1="452" y1="150" x2="462" y2="150" stroke="var(--ink2)"/>
<text x="20" y="210" fill="var(--ink2)" font-size="11">An April order (order_ts 04-18) joins the version whose window contains it:</text>
<rect x="20" y="220" width="654" height="30" rx="8" fill="var(--paper2)" stroke="var(--accent)"/>
<text x="34" y="240" fill="var(--accent)">order 04-18  ⋈  Munich? no — Paris row (signup → 06-05) contains 04-18  →  ships to Paris-era truth, not Hamburg</text>
</svg>`, caption: "SCD1 keeps only 'now'; SCD2 keeps an era per row, and facts join to the era that was true at event time." },
    { type: "code", lang: "sql", code: `-- Reconstruct C-0042's version history and flag the current row.
-- is_current = "this is the newest version": rank versions newest-first,
-- and the one ranked 1 is the live one.
WITH versions AS (
  SELECT customer_id,
         city,
         updated_at AS valid_from,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
  FROM customer_updates
)
SELECT customer_id, city, valid_from,
       CASE WHEN rn = 1 THEN 'Y' ELSE 'N' END AS is_current
FROM versions
WHERE customer_id = 'C-0042'
ORDER BY valid_from;
-- Munich  | 2026-06-05T09:12:00 | is_current N
-- Hamburg | 2026-06-22T14:03:00 | is_current Y`, caption: "The newest version wins the is_current flag — ranking direction is the whole trick." },
    { type: "analogy", title: "The bin card, not the whiteboard", html: `
<p>Every storage bin on NimbusMart's floor used to carry a <strong>whiteboard</strong>: current contents, wiped and rewritten each time stock changed. Ask &ldquo;what was in bin 44 last Tuesday?&rdquo; and the answer is a shrug — the whiteboard only ever knows <em>now</em>. That is SCD Type 1, and it's why the April audit couldn't be reconciled: the board had been wiped forty times since.</p>
<p>Warehouses that have been burned switch to a <strong>bin card</strong>: a card that hangs on the bin and is never erased. Each change adds a dated line — &ldquo;06-05 moved to Munich stock,&rdquo; &ldquo;06-22 moved to Hamburg stock&rdquo; — with the previous line struck through but still legible, and the live line marked with a tick. Now &ldquo;what was here on 04-18?&rdquo; is a finger running down dated rows until you find the one whose window covers that day. SCD2 is the bin card: append a dated version, strike the old one, tick the current — and the past holds still.</p>` },
    { type: "javaBridge", html: `
<p>In your JPA world, a customer address change is an <code>UPDATE</code>: <code>customer.setCity("Hamburg"); repository.save(customer);</code> — one row, mutated in place. That's SCD Type 1, and for the OLTP system it's <em>correct</em>: the shipping label needs today's address, not a history lesson.</p>
<ul>
<li>SCD2 is the <strong>envers / audit-table</strong> instinct you've reached for before — <code>@Audited</code>, or a hand-rolled <code>customer_history</code> with effective dates — but promoted to a first-class query shape, not a compliance afterthought. The history table <em>is</em> the dimension, and every analytical join uses it.</li>
<li>The bulk mechanism is <strong>Delta Lake's <code>MERGE</code></strong>, and it maps cleanly onto upsert semantics you already know — JPA's <code>save()</code> that inserts-or-updates. The twist: instead of updating the matched row, an SCD2 <code>MERGE</code> <em>closes</em> it (set <code>valid_to</code>, <code>is_current = false</code>) and <em>inserts</em> the new version in the same atomic statement. It's <code>save()</code> that keeps the old row instead of clobbering it — the same upsert reflex, told to remember.</li>
</ul>` },
  ],
  lab: {
    tier: "T1",
    understand: {
      engine: "sql",
      datasets: ["customers", "customer_updates"],
      task: `<p><strong>Rebuild C-0042's SCD2 version history and flag the live row.</strong> <code>customer_updates</code> is the change feed — each row is a city C-0042 moved to, with the timestamp it took effect. Rank the versions so the newest gets <code>is_current = 'Y'</code> and older versions get <code>'N'</code>.</p><p>The starter is one character wrong in the place that matters: its window orders <code>updated_at</code> <em>ascending</em>, so <code>rn = 1</code> lands on the <em>oldest</em> version and the flag marks Munich as current. But C-0042 lives in Hamburg now. Fix the window's sort direction so the newest version wins the flag.</p>`,
      starterQuery: `WITH versions AS (
  SELECT customer_id,
         city,
         updated_at AS valid_from,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at) AS rn
  FROM customer_updates
)
SELECT customer_id, city, valid_from,
       CASE WHEN rn = 1 THEN 'Y' ELSE 'N' END AS is_current
FROM versions
WHERE customer_id = 'C-0042'
ORDER BY valid_from;`,
      solutionQuery: `WITH versions AS (
  SELECT customer_id,
         city,
         updated_at AS valid_from,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
  FROM customer_updates
)
SELECT customer_id, city, valid_from,
       CASE WHEN rn = 1 THEN 'Y' ELSE 'N' END AS is_current
FROM versions
WHERE customer_id = 'C-0042'
ORDER BY valid_from;`,
      hint: `<p><code>is_current</code> means &ldquo;newest version&rdquo;, and <code>ROW_NUMBER()</code> assigns <code>1</code> to whichever row the window sorts first. Sorting <code>updated_at</code> ascending puts the <em>oldest</em> row first — so the flag decorates Munich (2026-06-05) and calls the past the present. Add <code>DESC</code> to the window's <code>ORDER BY updated_at</code> so <code>rn = 1</code> is the latest version. Expected diff: the <code>Y</code> jumps from the Munich row to the Hamburg row (2026-06-22) — the city C-0042 is actually in now.</p>`
    },
    buildWithAI: `I'm learning Slowly Changing Dimensions and I want to build a real SCD2 customer dimension locally with Delta Lake MERGE, not just the SQL. Assume a fresh machine, Python 3.10+, nothing else installed.

1. Create a project folder \`nimbusmart-scd2\` with a venv, and install pyspark (pin any recent 3.5.x), delta-spark (matching version), and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator writing two CSVs into \`data/\`:
   - \`customers.csv\`: 60 rows — customer_id (C-0001..C-0060), name, city, country, segment. Make C-0042's starting city Paris.
   - \`customer_updates.csv\`: a change feed of 8 rows across 7 customers where ONLY C-0042 appears twice: (C-0042, Munich, DE, 2026-06-05T09:12:00) then (C-0042, Hamburg, DE, 2026-06-22T14:03:00), plus 6 other customers with one move each. Columns: customer_id, city, country, updated_at.

3. Create \`build_scd2.py\` that:
   - starts a SparkSession configured for Delta (the delta-spark configure_spark_with_delta_pip helper), local[*]
   - seeds a Delta table \`customer_dim\` from customers.csv, adding SCD2 columns: valid_from (use created_at or a fixed seed date), valid_to = null, is_current = true
   - processes customer_updates IN updated_at ORDER, and for each change performs an SCD2 MERGE: when a matched customer's is_current row has a DIFFERENT city, set that row's valid_to = the update's updated_at and is_current = false, then INSERT a new version row (valid_from = updated_at, valid_to = null, is_current = true). Use the classic Delta two-step SCD2 MERGE (a MERGE that closes the old row + a follow-up insert of new versions), and comment each step.
   - prints C-0042's full version history ordered by valid_from

4. Create \`test_scd2.py\` (pytest) asserting, computed from the feed not hardcoded where possible:
   - after processing, C-0042 has exactly 3 versions (Paris, Munich, Hamburg) and exactly one has is_current = true
   - that current version's city is 'Hamburg'
   - a point-in-time query — the version whose [valid_from, valid_to) window contains 2026-05-01 — returns Paris, and for 2026-06-10 returns Munich
   - every customer has exactly one is_current = true row (no customer left with zero or two live versions)

5. Run generator, build_scd2.py, pytest. Then demonstrate Delta time travel: print the dimension \`VERSION AS OF\` an early commit vs the latest, so I can see history accrue. Windows-friendly paths please.`
  },
  check: [
    {
      type: "mcq",
      q: "Finance reran an unchanged Q2 and Munich's revenue moved. What did the nightly customer sync almost certainly do?",
      options: [
        "SCD Type 1 — it overwrote C-0042's city to Hamburg in place, so her older Munich-era orders retroactively re-attributed to Hamburg",
        "SCD Type 2 — it added a new versioned row, which is what caused the shift",
        "It deleted C-0042's orders, lowering Munich",
        "It changed the total_amount measures on the order fact"
      ],
      answer: 0,
      explain: "SCD1 overwrite keeps only the current value and has no memory of the old one. Once C-0042's single dimension row said 'Hamburg', every historical order joining to that row inherited Hamburg — including April orders that actually shipped to Munich. The measures never changed; the dimension rewrote the past. SCD2 versioning is the fix."
    },
    {
      type: "predict",
      q: "Across the whole 8-row <code>customer_updates</code> feed (7 distinct customers), how many customers will end up with more than one SCD2 version — i.e. more than one row in the change feed?",
      code: `SELECT COUNT(*) AS multi_version_customers
FROM (
  SELECT customer_id
  FROM customer_updates
  GROUP BY customer_id
  HAVING COUNT(*) > 1
) t;`,
      options: ["0", "1", "7", "8"],
      answer: 1,
      explain: "Only C-0042 appears twice in the feed (Munich, then Hamburg); the other 6 customers moved once each. So exactly 1 customer has more than one change and therefore multiple SCD2 versions. The feed has 8 rows across 7 customers: 6×1 + 1×2 = 8."
    },
    {
      type: "predict",
      q: "You want C-0042's city as it was known on 2026-06-10 — the latest version whose change had already landed by then. What does this return?",
      code: `SELECT city
FROM customer_updates
WHERE customer_id = 'C-0042'
  AND updated_at <= '2026-06-10T23:59:59'
ORDER BY updated_at DESC
LIMIT 1;`,
      options: ["Paris", "Munich", "Hamburg", "two rows: Munich and Hamburg"],
      answer: 1,
      explain: "The Munich change landed 2026-06-05 (<= 06-10, so it counts); the Hamburg change is 2026-06-22, which is in the future relative to the as-of date and is filtered out. Ordering the remaining rows newest-first and taking one gives Munich. This 'latest version whose valid_from <= as-of date' pattern IS point-in-time SCD2 reconstruction."
    },
    {
      type: "mcq",
      q: "Why does an SCD2 dimension carry an <code>is_current</code> boolean when <code>valid_to IS NULL</code> already identifies the live row?",
      options: [
        "It's a redundant convenience flag: the extremely common 'customers as they are now' query becomes a simple equality filter instead of a NULL/date-range check, and it's cheap to index",
        "is_current is required for the table to be valid; valid_to alone cannot mark a live row",
        "valid_to and is_current mean opposite things and must disagree",
        "is_current stores the customer's city, replacing the city column"
      ],
      answer: 0,
      explain: "You can derive 'current' from valid_to IS NULL (or valid_to = the sentinel), but the vast majority of queries just want today's dimension. A plain is_current = true filter is simpler to write, easy to index, and avoids every analyst re-deriving the same range logic. It's denormalized bookkeeping that pays for itself on read."
    }
  ],
  fieldNotes: `A logistics client ran SCD1 on their facility dimension for years and it never obviously hurt — until they relocated a regional hub and, three weeks later, the ops VP asked why on-time-delivery for the <em>previous</em> quarter had quietly dropped two points overnight. It hadn't: the hub's dimension row now carried the new location's zone, so every historical shipment re-joined to the new zone's SLA target, and a facility that had been hitting its old target was now &ldquo;missing&rdquo; a target that didn't exist when those shipments ran. Nothing in the fact table had changed; a single overwrite in a 400-row dimension had rewritten a quarter of history. Migrating that one dimension to SCD2 — valid_from, valid_to, is_current, and facts joining on event-time windows — took an afternoon and permanently ended the &ldquo;why did last quarter change?&rdquo; class of ticket. The rule they adopted: any dimension attribute a historical report groups or filters by is a candidate for versioning, and you decide that <em>before</em> the first relocation, not after.`
};
