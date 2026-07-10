// Full-course QA: load every fragment, mount its lab in headless DOM, and run
// cross-module consistency checks. Run: node scripts/qa.js
const fs = require("fs");
const path = require("path");
const { execFileSync } = require("child_process");
const { JSDOM } = require("jsdom");

const ROOT = path.resolve(__dirname, "..");
const MODDIR = path.join(ROOT, "content/modules");
const TRACEDIR = path.join(ROOT, "engine/traces");

// --- headless DOM ---
const dom = new JSDOM(`<!DOCTYPE html><body></body>`, { pretendToBeVisual: true });
for (const k of ["window", "document", "navigator", "getComputedStyle", "HTMLElement"])
  global[k] = k === "window" ? dom.window : dom.window[k];
dom.window.requestAnimationFrame = cb => setTimeout(() => cb(0), 0);
dom.window.matchMedia = dom.window.matchMedia || (() => ({ matches: false, addEventListener() {} }));

const load = f => (0, eval)(fs.readFileSync(path.join(ROOT, f), "utf8"));
load("data/nimbusmart/seed.js");
const NIMBUS = window.NIMBUS;
for (const e of ["sqlrunner", "sparksim", "traceplayer", "pyrunner"]) load("engine/" + e + ".js");
const TRACES = {};
for (const f of fs.readdirSync(TRACEDIR))
  if (f.endsWith(".json") && f !== "schema.json")
    TRACES[f.replace(".json", "")] = JSON.parse(fs.readFileSync(path.join(TRACEDIR, f), "utf8"));
window.TRACES = TRACES;

// --- load every fragment's object via node ESM eval ---
const ORDER = [..."A".repeat(0)]; // placeholder
const IDS = fs.readdirSync(MODDIR).filter(f => /^[A-H]\d\.js$/.test(f)).map(f => f.replace(".js", "")).sort();

function loadFragment(id) {
  const uri = "file:///" + path.join(MODDIR, id + ".js").replace(/\\/g, "/");
  const out = execFileSync("node", ["--input-type=module", "-e",
    `import('${uri}').then(m=>process.stdout.write(JSON.stringify(m.default)))`], { encoding: "utf8" });
  return JSON.parse(out);
}

let fails = 0, warns = 0;
const coldOpens = new Map(), fieldNotes = new Map();
const problems = [];

for (const id of IDS) {
  let mod;
  try { mod = loadFragment(id); }
  catch (e) { problems.push(`[${id}] fragment eval failed: ${e.message}`); fails++; continue; }

  // duplicate cold-open / field-notes detection (normalized prefix)
  const co = (mod.coldOpen || "").slice(0, 60).toLowerCase();
  if (coldOpens.has(co)) { problems.push(`[${id}] duplicate coldOpen prefix shared with ${coldOpens.get(co)}`); fails++; }
  else coldOpens.set(co, id);
  const fn = (mod.fieldNotes || "").slice(0, 60).toLowerCase();
  if (fieldNotes.has(fn)) { problems.push(`[${id}] duplicate fieldNotes prefix shared with ${fieldNotes.get(fn)}`); fails++; }
  else fieldNotes.set(fn, id);

  // trace reference resolves
  const u = mod.lab && mod.lab.understand || {};
  if (u.engine === "trace") {
    if (!TRACES[u.trace]) { problems.push(`[${id}] references missing trace '${u.trace}'`); fails++; }
  }

  // mount the lab in DOM
  const el = document.createElement("div");
  document.body.appendChild(el);
  try {
    const eng = window.Engines[u.engine];
    if (!eng) throw new Error("no engine " + u.engine);
    eng.mount(el, u, { NIMBUS, TRACES, dark: false });
    if ((el.innerHTML || "").length < 40) throw new Error("rendered <40 chars");
    // click Run if present
    [...el.querySelectorAll("button")].find(b => /^run|run$/i.test(b.textContent.trim()))?.click();
  } catch (e) { problems.push(`[${id}] lab mount (${u.engine}) failed: ${e.message}`); fails++; }

  el.remove();
}

// orphan traces (authored but unreferenced)
const referenced = new Set(IDS.map(id => { try { return loadFragment(id).lab.understand.trace; } catch { return null; } }).filter(Boolean));
for (const t of Object.keys(TRACES)) if (!referenced.has(t)) { console.log(`note: trace '${t}' authored but not referenced by any module`); warns++; }

console.log(`\nmodules checked: ${IDS.length}`);
console.log(`traces: ${Object.keys(TRACES).length}, referenced: ${referenced.size}`);
if (problems.length) { console.log("\nPROBLEMS:"); problems.forEach(p => console.log("  " + p)); }
console.log(`\n${fails ? "FAIL — " + fails + " problem(s)" : "PASS — all labs mount, no dup cold-opens, all trace refs resolve"}`);
process.exit(fails ? 1 : 0);
