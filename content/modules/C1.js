// C1 — Python for Java Developers, Compressed (Track C, Tier 1 / Pyodide)
// Verified facts used by lab + checks (data/nimbusmart/generate.py, seed 42):
//   orders = 240 rows; channel ∈ {web, app}; app orders = 173
//   orders with total_amount >= 500 = 114; of those, channel == "app" = 77  (lab target)
//   [n*n for n in [1..6] if n%2==0] == [4, 16, 36]; '' or 'UNKNOWN' == 'UNKNOWN'
export default {
  id: "C1",
  track: "C",
  title: "Python for Java Developers, Compressed",
  minutes: 24,
  coldOpen: "A backend engineer three days into NimbusMart opens their first PySpark PR with a Java accent: every line ends in a semicolon, every variable has a declared type in a comment, and a helper loops with an index counter to build a list. It runs — Python forgives the semicolons — but the reviewer's note is one line: “this is Java wearing a Python hat; here's the same thing in a third of the code.” The diff that comes back is a single list comprehension.",
  concept: [
    { type: "prose", html: `
<p>You are not here to learn Python the language. You are here to learn the <em>slice</em> of Python that Spark, pandas, and SQL glue code actually demand — which is smaller and flatter than the Java you already own. Three habits to unlearn on day one:</p>
<ul>
<li><strong>No type ceremony.</strong> Python is dynamically typed and <em>duck typed</em>: a function works on any object that has the methods it calls, not on a declared class. There is no <code>&lt;T&gt;</code>, no <code>implements</code>, no cast. You lose compile-time safety; you gain code that reads like the problem.</li>
<li><strong>Indentation is syntax, semicolons are noise.</strong> A block is defined by its indentation, not by <code>{ }</code>. Statements end at the newline — a trailing <code>;</code> is legal but marks you as a tourist.</li>
<li><strong>Comprehensions replace loops.</strong> The <code>for (int i...)</code> that builds a list, the <code>stream().filter().map().collect()</code> — both collapse into one expression that reads left-to-right as “this, for each of those, where.”</li>
</ul>
<p>Two container types carry almost everything: the <strong>list</strong> (ordered, mutable — your <code>ArrayList</code>) and the <strong>dict</strong> (key→value — your <code>HashMap</code>, but also how every row and every JSON object arrives). Master those two and comprehensions, and you can read most PySpark glue.</p>` },
    { type: "code", lang: "python", code: `# Java habit                          # Python, the same intent
# List<String> ids = new ArrayList<>();
# for (Order o : orders)               ids = [o["order_id"]              # take this
#   if (o.getTotal() >= 500            for o in orders                    # for each of those
#       && o.getChannel().equals("app")) if o["total_amount"] >= 500     # where ...
#     ids.add(o.getId());                 and o["channel"] == "app"]      #     ... this holds

# A row is a dict, not a POJO. Access by key, not by getter:
order = {"order_id": "O-10007", "total_amount": 512.40, "channel": "app"}
order["channel"]          # "app"      — no getChannel(), no NullPointerException class
order.get("device", "?")  # "?"        — safe lookup with a default

# Truthiness: empty string / empty list / 0 / None are all "falsy"
city = ""
resolved = city or "UNKNOWN"   # "UNKNOWN"  — the Pythonic COALESCE
# 'and'/'or' return an operand, not a boolean — this is a feature, lean on it`, caption: "The comprehension is the filter+map+collect you already know, minus the ceremony." },
    { type: "svg", svg: `<svg viewBox="0 0 720 210" font-family="var(--mono)" font-size="12">
<text x="20" y="24" fill="var(--ink2)" font-size="11">ANATOMY OF A COMPREHENSION — read it left to right</text>
<rect x="20" y="40" width="680" height="46" rx="10" fill="var(--paper2)" stroke="var(--line)"/>
<text x="36" y="68" fill="var(--accent)" font-weight="bold">[ o["order_id"]</text>
<text x="210" y="68" fill="var(--ink)">for o in orders</text>
<text x="392" y="68" fill="var(--rust)">if o["total_amount"] &gt;= 500 ]</text>
<line x1="120" y1="96" x2="120" y2="120" stroke="var(--ink2)"/><text x="36" y="138" fill="var(--accent)" font-size="11">MAP — the value kept</text>
<line x1="270" y1="96" x2="270" y2="120" stroke="var(--ink2)"/><text x="210" y="138" fill="var(--ink)" font-size="11">SOURCE — each element</text>
<line x1="500" y1="96" x2="500" y2="120" stroke="var(--rust)"/><text x="392" y="138" fill="var(--rust)" font-size="11">FILTER — the predicate</text>
<text x="20" y="180" fill="var(--ink2)" font-size="11">Java: orders.stream().filter(o -&gt; o.getTotal() &gt;= 500).map(Order::getId).collect(toList())</text>
<text x="20" y="198" fill="var(--ink2)" font-size="11">Same three parts, same order of thought — the comprehension just drops the plumbing.</text>
</svg>`, caption: "map / source / filter — one expression, no accumulator variable, no builder." },
    { type: "analogy", title: "The dock that checks handles, not paperwork", html: `
<p>At the NimbusMart receiving dock, the loader doesn't demand each pallet present a stamped certificate proving it is <em>class</em> <code>Pallet</code>. The loader tries to grab the handles and lift. If the thing has handles in the right place, it ships — a crate, a drum, a bundled stack, doesn't matter. That's <strong>duck typing</strong>: “if it has the methods I call, it's the right shape.”</p>
<p>Java's dock is the opposite: nothing moves until the customs officer verifies the declared type on the manifest, at compile time, before a single box is lifted. Safer, slower, more paperwork. Python trusts the handles and finds out at run time if they tear off. For data glue — where every row is a loosely-typed dict off a wire — trusting the handles is usually the right trade.</p>` },
    { type: "javaBridge", html: `
<p>Map your existing muscle memory straight across — most of it transfers, three things change:</p>
<ul>
<li><code>Stream.filter().map().collect(toList())</code> → a <strong>list comprehension</strong> <code>[f(x) for x in xs if p(x)]</code>. Lazy Streams become an eager list; if you want laziness use a <em>generator</em> <code>(f(x) for x in xs)</code> — same syntax, round brackets.</li>
<li><code>POJO + getters</code> → a <strong>dict</strong> with <code>row["field"]</code>. No compile-time field checking; a typo is a <code>KeyError</code> at run time. <code>row.get(k, default)</code> is your null-safe accessor.</li>
<li><code>&lt;T&gt; generics</code> → <strong>nothing</strong>. Types are erased and unchecked. <code>Optional&lt;T&gt;</code> becomes <code>None</code> plus truthiness (<code>x or default</code>). You trade the compiler's guarantees for brevity — which is exactly why data teams add type hints and <code>pytest</code> back on top.</li>
</ul>` },
  ],
  lab: {
    tier: "T1",
    understand: {
      engine: "pyodide",
      datasets: ["orders"],
      task: `<p><strong>Wrangle the orders in plain Python — no pandas past line one.</strong> The <code>orders</code> DataFrame is handed to you; the starter drops it to a list of dicts with <code>.to_dict("records")</code> so you can practise comprehensions the way Spark glue code actually looks.</p><p>Goal: build <code>high_value_app</code> — the <code>order_id</code>s of orders that are <em>both</em> high-value (<code>total_amount &gt;= 500</code>) <em>and</em> placed via the <code>"app"</code> channel. The starter forgets the channel filter, so it returns every high-value order. Add the second condition to the comprehension, then Run.</p>`,
      starterCode: `# orders is a pandas DataFrame; drop to plain Python to practise comprehensions.
records = orders.to_dict("records")   # list[dict] — like List<Map<String,Object>>

# Goal: order_ids of HIGH-VALUE APP orders (total_amount >= 500 AND channel == "app").
# The starter is missing the channel filter — add it to the 'if' clause.
high_value_app = [o["order_id"]
                  for o in records
                  if o["total_amount"] >= 500]

print(f"{len(high_value_app)} orders matched")
print("first five:", high_value_app[:5])`,
      solutionCode: `records = orders.to_dict("records")

high_value_app = [o["order_id"]
                  for o in records
                  if o["total_amount"] >= 500 and o["channel"] == "app"]

print(f"{len(high_value_app)} orders matched")
print("first five:", high_value_app[:5])`,
      assertCode: `assert len(high_value_app) == 77, f"expected 77 high-value app orders, got {len(high_value_app)} (did you AND the channel filter?)"`
    },
    buildWithAI: `I'm a Java/backend developer learning Python idioms for data engineering — comprehensions, dicts, duck typing, truthiness. No Spark yet, just plain Python. Scaffold a real local project I can run on my own machine (assume only Python 3.10+ is installed).

1. Create a folder \`nimbusmart-python\` with a virtual environment (\`python -m venv .venv\`) and install pandas and pytest (pin recent versions). Give me the Windows activation line (\`.venv\\Scripts\\activate\`) and the macOS/Linux one.

2. \`generate_data.py\` — deterministic (\`random.seed(42)\`) generator writing \`data/orders.csv\` with 240 rows:
   - order_id: O-10001 .. O-10240
   - customer_id: C-0001 .. C-0060 (random)
   - total_amount: float 8..950, 2 decimals
   - status: from [placed, shipped, delivered, delivered, delivered, cancelled, returned]
   - country: from [DE, US, IN, BR, JP, FR, AU]
   - channel: from [web, app, app]
   Print the row count when done.

3. \`wrangle.py\` — read orders.csv with the standard-library \`csv\` module (NOT pandas — I want to practise plain Python), build a \`list[dict]\`, and coerce total_amount to float. Then implement these using comprehensions only — no manual for-loops with .append(), no semicolons, with type hints and one-line docstrings:
   - \`high_value_app(records) -> list[str]\`: order_ids where total_amount >= 500 AND channel == "app"
   - \`revenue_by_country(records) -> dict[str, float]\`: country -> summed total_amount
   - \`distinct_customers(records) -> set[str]\`: unique customer_ids

4. \`test_wrangle.py\` (pytest) — re-derive every expected value INDEPENDENTLY from the CSV with the csv module (do NOT hardcode numbers): assert len(high_value_app(...)) equals the independently counted matches; assert sum(revenue_by_country(...).values()) is within 0.01 of the total of all total_amounts; assert distinct_customers(...) is a subset of the 60 known ids.

5. Run \`python generate_data.py\`, then \`pytest -q\`. Finally, show me the \`high_value_app\` comprehension side by side with the equivalent Java \`stream().filter().map().collect(toList())\` and explain how the three clauses line up. Windows-friendly paths throughout.`
  },
  check: [
    {
      type: "predict",
      q: "What does this print? (No NimbusMart data — pure Python semantics.)",
      code: `nums = [1, 2, 3, 4, 5, 6]
evens_squared = [n * n for n in nums if n % 2 == 0]
print(evens_squared)`,
      options: ["[4, 16, 36]", "[1, 9, 25]", "[2, 4, 6]", "[1, 4, 9, 16, 25, 36]"],
      answer: 0,
      explain: "The comprehension reads: square n, for each n in nums, where n is even. Evens are 2, 4, 6 → 4, 16, 36. The filter (`if`) runs before the map (`n * n`), so odd numbers never get squared."
    },
    {
      type: "predict",
      q: "A customer row has an empty-string city. What prints?",
      code: `city = ""
resolved = city or "UNKNOWN"
print(resolved)`,
      options: ["UNKNOWN", "(an empty line)", "True", "None"],
      answer: 0,
      explain: "An empty string is falsy in Python, so `city or \"UNKNOWN\"` evaluates the right operand and returns \"UNKNOWN\". `or` returns the first truthy operand (or the last), not a boolean — this is the idiomatic COALESCE you'll use constantly on messy data."
    },
    {
      type: "mcq",
      q: "A function calls <code>row[\"total_amount\"]</code> and <code>row.get(\"device\")</code>. Under Python's duck typing, when does it fail if a row is missing <code>device</code>?",
      options: [
        "At run time only if you use <code>row[\"device\"]</code>; <code>row.get(\"device\")</code> returns <code>None</code> instead of raising",
        "At compile time — Python checks all keys exist before running",
        "Never — missing keys silently return an empty string",
        "At import time, when the function is defined"
      ],
      answer: 0,
      explain: "There is no compile-time key checking. `row[\"device\"]` on a missing key raises KeyError at run time; `row.get(\"device\")` returns None (or a supplied default). This is the exact trade you make leaving Java: brevity now, a run-time surprise if you're careless — which is why schema-drift-prone tables like order_events get `.get()` with defaults."
    },
    {
      type: "mcq",
      q: "Why does the reviewer call the index-counter loop “Java wearing a Python hat”?",
      options: [
        "Because a comprehension expresses the same filter-and-collect as one readable expression, with no accumulator variable to initialise, mutate, and get wrong",
        "Because loops are actually forbidden by the Python interpreter",
        "Because index counters run measurably slower than comprehensions on every input",
        "Because Python has no way to iterate with an index at all"
      ],
      answer: 0,
      explain: "Comprehensions aren't about raw speed — they're about removing the mutable accumulator and the off-by-one surface area. Loops are perfectly legal (and `enumerate` gives you an index when you truly need one); the point is that a filter+collect has a direct, declarative form, and reaching for a hand-rolled counter signals you're still thinking in the old idiom."
    }
  ],
  fieldNotes: `A payments-team engineer moving from Spring to PySpark spent an afternoon debugging a nightly job that silently produced too-few rows. The cause: they'd written <code>if row["channel"] is "app"</code>, copying <code>==</code>-vs-<code>equals</code> instincts sideways into Python's <code>is</code>. In Python <code>is</code> tests object identity, not value equality; short interned strings happened to pass in their unit test's tiny fixture and fail on the real data where the strings came from a different source and weren't interned. The fix was one character — <code>is</code> → <code>==</code> — but the lesson stuck: Python's forgiveness (no compiler to catch <code>is</code> vs <code>==</code>, no type to catch a KeyError) moves the whole safety burden onto tests and small, boring assertions. The engineers who survive the transition are the ones who add <code>pytest</code> back the day they lose <code>javac</code>.`
};
