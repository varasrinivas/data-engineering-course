// Headless DOM smoke test: mount every engine and assert it renders without throwing.
// Run: node scripts/smoke_dom.js   (requires jsdom in node_modules)
const fs = require("fs");
const path = require("path");
const { JSDOM } = require("jsdom");

const ROOT = path.resolve(__dirname, "..");
const dom = new JSDOM(`<!DOCTYPE html><body></body>`, { pretendToBeVisual: true });
global.window = dom.window;
global.document = dom.window.document;
global.navigator = dom.window.navigator;
global.getComputedStyle = dom.window.getComputedStyle;
global.HTMLElement = dom.window.HTMLElement;
// jsdom lacks these; engines may touch them
dom.window.requestAnimationFrame = cb => setTimeout(() => cb(Date.now ? 0 : 0), 0);
dom.window.matchMedia = dom.window.matchMedia || (() => ({ matches: false, addEventListener() {} }));

function load(file) { (0, eval)(fs.readFileSync(path.join(ROOT, file), "utf8")); }

// seed + engines
load("data/nimbusmart/seed.js");
const NIMBUS = window.NIMBUS;
window.FRAUD_REVIEW_THRESHOLD = window.FRAUD_REVIEW_THRESHOLD;
load("engine/sqlrunner.js");
load("engine/sparksim.js");
load("engine/traceplayer.js");
load("engine/pyrunner.js");
// traces
const TRACES = {};
for (const f of fs.readdirSync(path.join(ROOT, "engine/traces"))) {
  if (f.endsWith(".json") && f !== "schema.json")
    TRACES[f.replace(".json", "")] = JSON.parse(fs.readFileSync(path.join(ROOT, "engine/traces", f), "utf8"));
}
window.TRACES = TRACES;

let fails = 0;
function mountTest(name, engineKey, config) {
  const el = document.createElement("div");
  document.body.appendChild(el);
  try {
    const eng = window.Engines[engineKey];
    if (!eng || !eng.mount) throw new Error("no engine/mount for " + engineKey);
    eng.mount(el, config, { NIMBUS, TRACES, dark: false });
    const html = el.innerHTML;
    if (!html || html.length < 40) throw new Error("rendered almost nothing (" + html.length + " chars)");
    // simulate a Run click if there's a button
    const btns = el.querySelectorAll("button");
    let clicked = 0;
    btns.forEach(b => {
      if (/run/i.test(b.textContent) && clicked < 1) { try { b.click(); clicked++; } catch (e) {} }
    });
    console.log(`PASS  ${name}  (${html.length} chars, ${btns.length} buttons, ran=${clicked})`);
  } catch (e) {
    fails++;
    console.log(`FAIL  ${name}  :: ${e.message}`);
  }
}

// SQL lab (T1)
mountTest("sqlrunner / fraud-queue", "sql", {
  engine: "sql", datasets: ["orders", "fraud_scores"],
  task: "<p>Find the review queue.</p>",
  starterQuery: "SELECT COUNT(*) AS n FROM orders",
  solutionQuery: "SELECT COUNT(*) AS n FROM fraud_scores WHERE fraud_score >= 0.80",
  hint: "inclusive boundary",
});

// SparkSim lab (T2) — the D3 fraud-review chain
mountTest("sparksim / D3 lazy", "sparksim", {
  engine: "sparksim", datasets: ["orders", "fraud_scores"],
  task: "<p>Build the review queue.</p>",
  starterCode: `orders = spark.read.table("orders")
fraud = spark.read.table("fraud_scores")
review = (orders
    .join(fraud, "order_id")
    .filter(F.col("fraud_score") >= FRAUD_REVIEW_THRESHOLD)
    .select("order_id", "customer_id", "fraud_score", "total_amount"))
review.show()`,
  solutionCode: "review.show()",
  expect: { rows: 43, cols: ["order_id", "customer_id", "fraud_score", "total_amount"] },
});

// Trace players (T3) — all four
for (const t of Object.keys(TRACES)) {
  mountTest("trace / " + t, "trace", { engine: "trace", trace: t, task: "<p>Watch.</p>" });
}

// Pyodide (T1 Python) — should render UI without actually loading Pyodide
mountTest("pyrunner / pandas", "pyodide", {
  engine: "pyodide", datasets: ["orders"],
  task: "<p>Count the queue.</p>",
  starterCode: "import pandas as pd\nprint(len(orders))",
  solutionCode: "print((orders.fraud_score >= 0.80).sum())",
  assertCode: "assert True",
});

console.log(fails ? `\n${fails} engine(s) failed to render` : `\nAll engines render clean`);
process.exit(fails ? 1 : 0);
