"""validate.py <path-or-ID> [...] [--final] | --all [--final]

Structural validation of module fragments against the CLAUDE.md MODS schema.
Uses node to evaluate the fragment (fragments are ESM object literals), then
validates the resulting JSON in Python.

--final: TODO markers become errors instead of warnings.
Exit code 0 = all pass, 1 = any failure.
"""
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MODULES = ROOT / "content" / "modules"
TRACES = ROOT / "engine" / "traces"

TABLES = {"customers", "customer_updates", "products", "orders", "fraud_scores",
          "payments", "order_events", "couriers", "courier_pings"}
SECTION_TYPES = {"prose", "svg", "code", "analogy", "javaBridge"}
CHECK_TYPES = {"mcq", "predict"}
TIERS = {"T1", "T2", "T3"}
# Ops sparksim v1 implements (keep in sync with engine/sparksim.js SUPPORTED_OPS)
SPARKSIM_OPS = {
    "read", "table", "select", "filter", "where", "withColumn", "withColumnRenamed",
    "groupBy", "agg", "orderBy", "sort", "join", "limit", "distinct", "drop",
    "dropDuplicates", "count", "show", "collect", "write", "alias", "over",
    "partitionBy", "cache", "union", "explain", "printSchema",
    # F.* aggregate/window functions and column methods in the documented subset
    "sum", "avg", "min", "max", "countDistinct", "row_number", "rank", "dense_rank",
    "broadcast", "isNull", "isNotNull",
}


def load_fragment(path: Path):
    uri = path.resolve().as_uri()
    code = (f"import('{uri}').then(m => "
            f"process.stdout.write(JSON.stringify(m.default)))"
            f".catch(e => {{ console.error(String(e)); process.exit(2); }})")
    r = subprocess.run(["node", "--input-type=module", "-e", code],
                       capture_output=True, text=True, encoding="utf-8")
    if r.returncode != 0:
        return None, f"fragment does not parse/evaluate: {r.stderr.strip()}"
    try:
        return json.loads(r.stdout), None
    except json.JSONDecodeError as e:
        return None, f"fragment JSON round-trip failed: {e}"


def validate(path: Path, final: bool):
    errors, warnings = [], []
    src = path.read_text(encoding="utf-8")
    mod, err = load_fragment(path)
    if err:
        return [err], []

    mid = path.stem
    # -- required keys
    for k in ("id", "track", "title", "minutes", "coldOpen", "concept", "lab",
              "check", "fieldNotes"):
        if k not in mod:
            errors.append(f"missing key: {k}")
    if errors:
        return errors, warnings

    if mod["id"] != mid:
        errors.append(f"id '{mod['id']}' != filename '{mid}'")
    if mod["track"] != mid[0]:
        errors.append(f"track '{mod['track']}' != '{mid[0]}'")
    if not isinstance(mod["minutes"], (int, float)) or not (5 <= mod["minutes"] <= 90):
        errors.append("minutes must be a number in [5, 90]")
    if not isinstance(mod["coldOpen"], str) or len(mod["coldOpen"]) < 80:
        errors.append("coldOpen missing or too short (< 80 chars)")

    # -- concept sections
    concept = mod["concept"]
    if not isinstance(concept, list) or len(concept) < 3:
        errors.append("concept must be a list of >= 3 sections")
    else:
        types = [s.get("type") for s in concept]
        bad = [t for t in types if t not in SECTION_TYPES]
        if bad:
            errors.append(f"unknown concept section types: {bad}")
        if types.count("analogy") != 1:
            errors.append(f"need exactly one analogy section, found {types.count('analogy')}")
        if types.count("javaBridge") != 1:
            errors.append(f"need exactly one javaBridge section, found {types.count('javaBridge')}")
        for i, s in enumerate(concept):
            body_key = "svg" if s.get("type") == "svg" else ("code" if s.get("type") == "code" else "html")
            if not s.get(body_key):
                errors.append(f"concept[{i}] ({s.get('type')}) has empty '{body_key}'")

    # -- lab
    lab = mod["lab"]
    tier = lab.get("tier")
    if tier not in TIERS:
        errors.append(f"lab.tier must be one of {TIERS}, got {tier!r}")
    und = lab.get("understand")
    if not isinstance(und, dict) or not und:
        errors.append("lab.understand missing/empty")
    else:
        eng = und.get("engine")
        if tier == "T1" and eng not in ("sql", "pyodide"):
            errors.append(f"T1 lab engine must be sql|pyodide, got {eng!r}")
        if tier == "T2" and eng != "sparksim":
            errors.append(f"T2 lab engine must be sparksim, got {eng!r}")
        if tier == "T3" and eng != "trace":
            errors.append(f"T3 lab engine must be trace, got {eng!r}")
        for t in und.get("datasets", []):
            if t not in TABLES:
                errors.append(f"lab dataset '{t}' is not a NIMBUS table")
        if eng == "trace":
            tname = und.get("trace", "")
            tpath = TRACES / f"{tname}.json"
            if not tname or not tpath.exists():
                errors.append(f"trace '{tname}' not found in engine/traces/")
            else:
                try:
                    tr = json.loads(tpath.read_text(encoding="utf-8"))
                    steps = tr.get("steps")
                    assert isinstance(steps, list) and len(steps) >= 3
                    assert all("narration" in s for s in steps)
                except Exception as e:
                    errors.append(f"trace '{tname}' malformed: {e}")
        if eng == "sql" and not und.get("solutionQuery"):
            errors.append("T1-SQL lab needs solutionQuery (expected-result diff)")
        if eng == "sparksim":
            code = und.get("starterCode", "") + und.get("solutionCode", "")
            if not code:
                errors.append("sparksim lab needs starterCode")
            for call in set(re.findall(r"\.([A-Za-z_]\w*)\s*\(", code)):
                if call not in SPARKSIM_OPS and not call.startswith(("col", "lit")) \
                        and call not in ("desc", "asc", "csv", "json", "parquet",
                                         "option", "format", "mode", "saveAsTable"):
                    warnings.append(f"sparksim starterCode uses op not in SPARKSIM_OPS: .{call}(")
    bwai = lab.get("buildWithAI", "")
    if not isinstance(bwai, str) or len(bwai) < 300:
        errors.append("lab.buildWithAI missing or too short to be a real, self-contained prompt")

    # -- checks
    checks = mod["check"]
    if not isinstance(checks, list) or not (3 <= len(checks) <= 5):
        errors.append(f"check must have 3-5 questions, found {len(checks) if isinstance(checks, list) else 'n/a'}")
    else:
        for i, c in enumerate(checks):
            if c.get("type") not in CHECK_TYPES:
                errors.append(f"check[{i}].type must be mcq|predict")
            if not c.get("q"):
                errors.append(f"check[{i}] missing q")
            opts = c.get("options")
            if not isinstance(opts, list) or len(opts) < 3:
                errors.append(f"check[{i}] needs >= 3 options")
            elif not isinstance(c.get("answer"), int) or not (0 <= c["answer"] < len(opts)):
                errors.append(f"check[{i}].answer must index into options")
            if not c.get("explain"):
                errors.append(f"check[{i}] missing explain")
            if c.get("type") == "predict" and not c.get("code"):
                errors.append(f"check[{i}] predict question missing code")

    # -- fieldNotes
    if not isinstance(mod["fieldNotes"], str) or len(mod["fieldNotes"]) < 120:
        errors.append("fieldNotes missing or too thin to be a real war story")

    # -- threshold discipline: bare 0.80 / 0.8 must ride with the named constant
    for m in re.finditer(r"(?<![\d.\w])0\.80?(?![\d])", src):
        ctx = src[max(0, m.start() - 90):m.end() + 90]
        if "FRAUD_REVIEW_THRESHOLD" not in ctx:
            line = src.count("\n", 0, m.start()) + 1
            errors.append(f"bare fraud-threshold literal at line {line} without FRAUD_REVIEW_THRESHOLD nearby")

    # -- TODO markers
    todos = len(re.findall(r"\bTODO\b", src))
    if todos:
        (errors if final else warnings).append(f"{todos} TODO marker(s) remain")

    return errors, warnings


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    final = "--final" in sys.argv
    if "--all" in sys.argv:
        paths = sorted(MODULES.glob("[A-H]*.js"))
    else:
        paths = []
        for a in args:
            p = Path(a)
            paths.append(p if p.exists() else MODULES / f"{a.upper()}.js")
    if not paths:
        raise SystemExit("usage: validate.py <path-or-ID> [...] [--final] | --all")

    failed = 0
    for p in paths:
        if not p.exists():
            print(f"[FAIL] {p.name}: file not found")
            failed += 1
            continue
        errors, warnings = validate(p, final)
        status = "FAIL" if errors else "ok"
        if errors:
            failed += 1
        print(f"[{status:4s}] {p.stem}"
              + (f" — {len(errors)} error(s), {len(warnings)} warning(s)" if errors or warnings else ""))
        for e in errors:
            print(f"        E: {e}")
        for w in warnings:
            print(f"        W: {w}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
