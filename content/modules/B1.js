// B1 — From JPA Entities to Star Schemas (T1 SQL)
// Verified facts (from data/nimbusmart/generate.py, seed 42):
//   orders = 240; every order.customer_id exists in customers(60) → inner join = 240 rows (no fan-out; customer_id is unique)
//   revenue by segment: consumer = 194 orders / 89,808.90 ; business = 46 orders / 25,038.78 ; total 240 / 114,847.68
//   distinct order countries = 7 (DE,US,IN,BR,JP,FR,AU); FR has the most orders (58)
export default {
  id: "B1",
  track: "B",
  title: "From JPA Entities to Star Schemas",
  minutes: 24,
  coldOpen: "The analytics team asks NimbusMart's backend lead a one-line question: “revenue by customer segment, last quarter.” He knows the schema cold — so he writes the join across orders, order_lines, customers, addresses, segments, and segment_tiers, six tables deep, and it runs for ninety seconds and returns the wrong number because one of the joins fanned out. The schema that was perfect for placing an order is fighting him for trying to read one.",
  concept: [
    { type: "prose", html: `
<p>Your JPA entity graph is <strong>normalized</strong>, and it should be. Third normal form exists to protect <em>writes</em>: every fact lives in exactly one place, so an <code>UPDATE</code> touches one row and can't leave two copies disagreeing. A customer's segment sits in a <code>segments</code> table, referenced by id, because if you copied the segment name onto every order and the name changed, you'd have a consistency nightmare. That discipline is the whole reason the orders database can take ten thousand concurrent checkouts without corrupting itself.</p>
<p>Analytics has the opposite job. It never updates a historical order — it <em>reads</em> millions of them and asks aggregate questions: sum, count, group-by-something. And for reads, normalization is pure cost. Every dimension you normalized away is now a join you have to pay for on every query, and every extra join is another chance for a fan-out bug like the one in the cold open. The star schema is the deliberate inversion: <strong>normalize for writes, denormalize for reads.</strong></p>` },
    { type: "prose", html: `
<p>A star schema has exactly two kinds of table, and the entire model hangs on telling them apart:</p>
<ul>
<li><strong>Fact table</strong> — one row per <em>business event</em>, long and narrow. <code>orders</code> is a fact: one row per order, a foreign key to each dimension (<code>customer_id</code>, <code>seller_id</code>, <code>product_id</code>), and the <em>measures</em> you aggregate — <code>total_amount</code>, <code>item_count</code>. Facts grow forever; they are the millions.</li>
<li><strong>Dimension table</strong> — one row per <em>thing you slice by</em>, short and wide. <code>customers</code> is a dimension: one row per customer carrying every descriptive attribute you'd group or filter on — <code>segment</code>, <code>country</code>, <code>city</code>. Dimensions are the thousands, and they hold the adjectives.</li>
</ul>
<p>Drawn out, the facts sit in the middle and the dimensions surround them like points of a star — hence the name. A query is then always the same shape: scan the fact, join out to the dimensions you're slicing by, group, aggregate. No six-table chains, no fan-out roulette.</p>` },
    { type: "svg", svg: `<svg viewBox="0 0 720 300" font-family="var(--mono)" font-size="12">
<defs><marker id="b1arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0 L10 5 L0 10 z" fill="var(--ink2)"/></marker></defs>
<rect x="286" y="118" width="148" height="66" rx="8" fill="var(--paper2)" stroke="var(--rust)" stroke-width="2"/>
<text x="360" y="140" text-anchor="middle" fill="var(--rust)" font-weight="bold">orders (FACT)</text>
<text x="360" y="157" text-anchor="middle" fill="var(--ink2)" font-size="10">240 rows · one per order</text>
<text x="360" y="172" text-anchor="middle" fill="var(--ink2)" font-size="10">measures: total_amount, item_count</text>
<rect x="40" y="30" width="150" height="58" rx="8" fill="var(--paper2)" stroke="var(--accent)"/>
<text x="115" y="52" text-anchor="middle" fill="var(--accent)" font-weight="bold">customers (DIM)</text>
<text x="115" y="69" text-anchor="middle" fill="var(--ink2)" font-size="10">segment, country, city</text>
<rect x="530" y="30" width="150" height="58" rx="8" fill="var(--paper2)" stroke="var(--accent)"/>
<text x="605" y="52" text-anchor="middle" fill="var(--accent)" font-weight="bold">products (DIM)</text>
<text x="605" y="69" text-anchor="middle" fill="var(--ink2)" font-size="10">category, price, tags</text>
<rect x="40" y="214" width="150" height="58" rx="8" fill="var(--paper2)" stroke="var(--accent)"/>
<text x="115" y="236" text-anchor="middle" fill="var(--accent)" font-weight="bold">sellers (DIM)</text>
<text x="115" y="253" text-anchor="middle" fill="var(--ink2)" font-size="10">seller_id, home_zone</text>
<rect x="530" y="214" width="150" height="58" rx="8" fill="var(--paper2)" stroke="var(--accent)"/>
<text x="605" y="236" text-anchor="middle" fill="var(--accent)" font-weight="bold">calendar (DIM)</text>
<text x="605" y="253" text-anchor="middle" fill="var(--ink2)" font-size="10">day, week, quarter</text>
<line x1="190" y1="66" x2="292" y2="126" stroke="var(--ink2)" marker-end="url(#b1arr)"/>
<line x1="530" y1="66" x2="428" y2="126" stroke="var(--ink2)" marker-end="url(#b1arr)"/>
<line x1="190" y1="238" x2="292" y2="176" stroke="var(--ink2)" marker-end="url(#b1arr)"/>
<line x1="530" y1="238" x2="428" y2="176" stroke="var(--ink2)" marker-end="url(#b1arr)"/>
<text x="360" y="292" text-anchor="middle" fill="var(--ink2)" font-size="10">every query: scan the fact in the middle, join out to the dimensions you slice by</text>
</svg>`, caption: "One fact in the middle holding measures + foreign keys; dimensions around it holding the adjectives." },
    { type: "code", lang: "sql", code: `-- The star query is always the same shape: fact ⋈ dimension, group, aggregate.
-- 'segment' is an ADJECTIVE — it lives on the customer dimension, not the order fact.
SELECT c.segment,
       COUNT(*)                    AS order_count,
       ROUND(SUM(o.total_amount), 2) AS revenue
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id   -- fact ⋈ dimension
GROUP BY c.segment
ORDER BY revenue DESC;
-- consumer : 194 orders,  89,808.90
-- business :  46 orders,  25,038.78`, caption: "Slicing the order fact by a customer-dimension attribute — the canonical star read." },
    { type: "analogy", title: "The dispatch ledger vs. the supplier binder", html: `
<p>NimbusMart's warehouse keeps two very different books, and they are shaped differently on purpose.</p>
<p>The <strong>dispatch ledger</strong> is the fact table: one line per parcel that leaves the dock, all day, forever. Each line is terse — a timestamp, a destination code, a weight, a seller code. You never rewrite a ledger line; you only ever append and, later, add it up. Millions of near-identical rows.</p>
<p>The <strong>supplier binder</strong> is a dimension: one tabbed page per seller, thick with description — company name, home zone, contact, tier. A few hundred pages, each rich. When the finance team wants &ldquo;tonnage by seller tier&rdquo;, nobody re-copies the tier onto every ledger line. They read the terse ledger, and for each seller code flip to that tab in the binder to fetch the adjective. Terse events in one book, rich descriptions in another, joined by a code. That is a star schema — it's how the warehouse already keeps its paperwork.</p>` },
    { type: "javaBridge", html: `
<p>You built the normalized side of this in your sleep: <code>@Entity Order</code> with <code>@ManyToOne Customer</code>, <code>Customer</code> with <code>@ManyToOne Segment</code>, foreign keys and join tables all the way down. That graph is tuned for the transactional truth Hibernate protects: one authoritative row per fact, cascade rules, no update anomalies. Perfect for the write path.</p>
<ul>
<li>The star schema is not a &ldquo;better&rdquo; version of your entity graph — it's a <em>second</em> model of the same business, built for the read path, and fed <em>from</em> the OLTP one by a pipeline. You keep both. Don't try to serve dashboards off the entity graph or place orders against the star; each is wrong for the other's job.</li>
<li>The instinct that &ldquo;more normalized = more correct&rdquo; is a write-path instinct. On the read path, the six-table join you'd be proud of in JPA is the ninety-second query from the cold open. A dimension is a <em>flattened</em> join you paid for once, in the pipeline, instead of on every query — the same trade as a materialized view, made structural.</li>
</ul>` },
  ],
  lab: {
    tier: "T1",
    understand: {
      engine: "sql",
      datasets: ["orders", "customers"],
      task: `<p><strong>Answer the cold-open question: revenue and order count by customer <em>segment</em>.</strong> The catch that makes this a star query: <code>segment</code> is a dimension attribute — it lives on <code>customers</code>, not on <code>orders</code> — so you must join the order fact out to the customer dimension and group by the <em>dimension's</em> column.</p><p>The starter already joins <code>customers</code>, but it then groups by <code>o.country</code> — a column that lives on the fact, so the join is doing no work and the answer is the wrong shape entirely. Fix the <code>SELECT</code> and <code>GROUP BY</code> to use <code>c.segment</code>. Two rows should come back, not seven.</p>`,
      starterQuery: `SELECT o.country,
       COUNT(*)                      AS order_count,
       ROUND(SUM(o.total_amount), 2) AS revenue
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY o.country
ORDER BY revenue DESC;`,
      solutionQuery: `SELECT c.segment,
       COUNT(*)                      AS order_count,
       ROUND(SUM(o.total_amount), 2) AS revenue
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.segment
ORDER BY revenue DESC;`,
      hint: `<p>The query joins the customer dimension but never <em>uses</em> it — grouping by <code>o.country</code> is grouping by a fact column, which the fact could have answered alone. The whole point of the star is to slice by the dimension's attribute. Change both the <code>SELECT</code> list and the <code>GROUP BY</code> from <code>o.country</code> to <code>c.segment</code>. Expected diff: your 7 country rows collapse to exactly 2 segment rows — <code>consumer</code> (194 orders, 89,808.90) above <code>business</code> (46 orders, 25,038.78).</p>`
    },
    buildWithAI: `I'm learning dimensional modeling (star schemas: fact vs dimension tables) and I want to build the real thing locally in Spark SQL, not just read about it. Assume a fresh machine with Python 3.10+ and nothing else installed.

1. Create a project folder \`nimbusmart-star\` with a venv, and install pyspark (pin any recent 3.5.x) and pytest.

2. Create \`generate_data.py\`: deterministic (random.seed(42)) generator writing two CSVs into \`data/\` that mimic a normalized OLTP export:
   - \`orders.csv\`: 240 rows — order_id (O-10001..O-10240), customer_id (C-0001..C-0060), seller_id (one of S-101,S-204,S-355,S-410,S-777,S-812,S-903 with S-777 deliberately ~35% of rows), total_amount (8..950, 2dp), item_count (1..6), country (DE,US,IN,BR,JP,FR,AU)
   - \`customers.csv\`: 60 rows — customer_id, name, city, country, segment (about 20% 'business', rest 'consumer'). This is the dimension.

3. Create \`star_model.py\` that:
   - builds a SparkSession (local[*]) and reads both CSVs with EXPLICIT StructType schemas (no inferSchema)
   - registers them as temp views \`orders\` (the fact) and \`customers\` (the dimension)
   - runs and prints the star query: revenue and order_count by c.segment, joining the fact to the dimension on customer_id, grouped and ordered by revenue desc
   - ALSO prints revenue by seller_id to show a second slice of the same fact through a different dimension key
   - prints spark.sql("...").explain() for the segment query so I can see the join + hash aggregate in the physical plan

4. Create \`test_star_model.py\` (pytest) asserting, WITHOUT hardcoding the totals — recompute expected values by reading the CSVs with the plain csv module and summing in Python:
   - the fact⋈dimension inner join returns exactly 240 rows (every order has a matching customer; the join must not fan out)
   - SUM(total_amount) grouped by segment matches the Python-computed expected per-segment totals to within 0.01
   - there are exactly 2 distinct segments in the result

5. Run the generator, run star_model.py, run pytest. Then explain in comments WHY the customer segment could not have been answered from orders.csv alone (it's a dimension attribute), and why grouping by a fact column like country needs no join at all. Windows-friendly paths please.`
  },
  check: [
    {
      type: "mcq",
      q: "Why is <code>orders</code> the fact table and <code>customers</code> the dimension in NimbusMart's star, rather than the other way around?",
      options: [
        "orders has one row per business event (an order) and holds the measures you aggregate; customers has one row per thing you slice by and holds descriptive attributes",
        "orders has more columns than customers, and wide tables are always facts",
        "customers is the fact because customers are the most important business entity",
        "Whichever table is larger is always the dimension, to keep facts small"
      ],
      answer: 0,
      explain: "A fact is one row per event (an order), narrow, holding foreign keys plus measures (total_amount, item_count) — it's the millions. A dimension is one row per thing-you-group-by (a customer), wide with adjectives (segment, country) — the thousands. Row count and column width follow from that role, they don't define it."
    },
    {
      type: "predict",
      q: "Every one of the 240 orders was placed by a customer that exists in the 60-row <code>customers</code> table, and <code>customer_id</code> is unique in <code>customers</code>. How many rows does this fact⋈dimension inner join return?",
      code: `SELECT o.order_id, c.segment
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;`,
      options: ["60", "240", "300", "14400"],
      answer: 1,
      explain: "Joining a fact to a dimension on a unique dimension key never changes the fact's row count — each order matches exactly one customer. 240 orders in, 240 rows out. (If customer_id were NOT unique in the dimension, the fact would fan out and every downstream SUM would be silently inflated — the classic star-schema bug.)"
    },
    {
      type: "mcq",
      q: "A backend engineer wants to serve the executive revenue dashboard directly off the production OLTP database — the same normalized JPA entity graph that handles checkouts. What's the core problem?",
      options: [
        "The OLTP schema is normalized for safe writes, so every dashboard query pays a multi-table join and competes with live checkout traffic for the same rows",
        "OLTP databases can't compute SUM or GROUP BY at all",
        "There's no problem — the entity graph is already the ideal shape for analytics",
        "Dashboards require NoSQL, so a relational OLTP database can never feed one"
      ],
      answer: 0,
      explain: "Normalization optimizes the write path: one authoritative row per fact, updated safely. Analytics needs the opposite — wide, pre-joined, read-optimized shapes. Running heavy aggregate scans against the live transactional store means expensive multi-table joins AND lock/IO contention with the checkout path. The star schema is the separate read model, fed from OLTP by a pipeline."
    },
    {
      type: "mcq",
      q: "In the lab, the starter query joins <code>customers</code> but groups by <code>o.country</code>. Why does that make the join pointless?",
      options: [
        "country is a fact-table column, so the fact could produce that grouping with no join at all — the dimension is fetched and then never used",
        "You can never GROUP BY a column from the left table of a join",
        "country and segment are the same thing, so the query is already correct",
        "The join fails silently because country exists in both tables"
      ],
      answer: 0,
      explain: "You only pay for a dimension join to reach an attribute the fact doesn't have — like segment. country already lives on orders, so grouping by it uses nothing from customers; the join is dead weight and the answer is by-country, not the by-segment question asked. Group by c.segment and the join finally earns its keep."
    }
  ],
  fieldNotes: `A retail data team I worked with shipped a &ldquo;revenue by loyalty tier&rdquo; number to the exec dashboard that was 18% too high for a full quarter before anyone caught it. The cause was textbook: the order fact was joined to a customer dimension that was <em>not</em> unique on the join key — a botched load had left ~1 in 12 customers with two rows (an old address version and a new one both marked current). Every order for those customers matched twice, so their revenue counted twice, and because the inflation was concentrated in exactly the high-value tier the exec cared about, it looked plausible enough to survive three monthly reviews. The fix was one <code>ROW_NUMBER()</code> dedup in the dimension build; the lesson was that a fact⋈dimension join is only safe when the dimension key is genuinely unique, and &ldquo;genuinely&rdquo; means you tested it, not assumed it. We added a build-time assertion that the customer dimension's row count equals its distinct-customer_id count — the cheapest possible guard against the most expensive possible bug.`
};
