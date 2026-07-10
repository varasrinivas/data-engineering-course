/*
 * engine/sqlrunner.js — Tier-1 SQL lab engine (engine: "sql").
 *
 * Hand-rolled tokenizer + recursive-descent parser + evaluator for exactly
 * the dialect subset in docs/04-ENGINE-CONTRACT.md. No libraries, no DOM
 * work at load time (all rendering happens inside mount), no import/export.
 *
 * NULL semantics (deliberately simple, taught in the course): any comparison
 * involving NULL is false; COUNT(x) skips NULLs; SUM/AVG/MIN/MAX skip NULLs
 * (empty -> NULL); IS [NOT] NULL is the only way to test for NULL.
 * ISO timestamp strings compare lexicographically — that is by design.
 */
(function () {
  "use strict";
  var W = typeof window !== "undefined" ? window : globalThis;
  W.Engines = W.Engines || {};

  /* ================================ errors ================================ */
  class SqlError extends Error {
    constructor(msg) { super(msg); this.name = "SqlError"; }
  }

  /* =============================== tokenizer ============================== */
  const KEYWORDS = new Set((
    "WITH AS SELECT DISTINCT FROM JOIN INNER LEFT OUTER ON WHERE AND OR NOT " +
    "IN BETWEEN LIKE IS NULL GROUP BY HAVING ORDER ASC DESC LIMIT " +
    "CASE WHEN THEN ELSE END OVER PARTITION " +
    "UNION EXCEPT INTERSECT QUALIFY RIGHT FULL CROSS EXISTS OFFSET"
  ).split(" "));

  function tokenize(sql) {
    const toks = [];
    let i = 0;
    const n = sql.length;
    while (i < n) {
      const c = sql[i];
      if (/\s/.test(c)) { i++; continue; }
      if (c === "-" && sql[i + 1] === "-") { while (i < n && sql[i] !== "\n") i++; continue; }
      if (c === "/" && sql[i + 1] === "*") {
        const e = sql.indexOf("*/", i + 2);
        if (e < 0) throw new SqlError("Unterminated /* ... */ comment.");
        i = e + 2; continue;
      }
      if (c === "'") {
        let s = "", j = i + 1;
        for (;;) {
          if (j >= n) throw new SqlError("Unterminated string literal — strings are single-quoted, e.g. 'delivered'.");
          if (sql[j] === "'") {
            if (sql[j + 1] === "'") { s += "'"; j += 2; continue; } // '' escapes a quote
            j++; break;
          }
          s += sql[j++];
        }
        toks.push({ t: "str", v: s });
        i = j; continue;
      }
      if (/[0-9]/.test(c) || (c === "." && /[0-9]/.test(sql[i + 1] || ""))) {
        let j = i;
        while (j < n && /[0-9.]/.test(sql[j])) j++;
        const raw = sql.slice(i, j);
        const v = parseFloat(raw);
        if (!Number.isFinite(v)) throw new SqlError(`Bad number literal '${raw}'.`);
        toks.push({ t: "num", v, raw });
        i = j; continue;
      }
      if (/[A-Za-z_]/.test(c)) {
        let j = i;
        while (j < n && /[A-Za-z0-9_]/.test(sql[j])) j++;
        const raw = sql.slice(i, j), up = raw.toUpperCase();
        if (KEYWORDS.has(up)) toks.push({ t: "kw", v: up, raw });
        else toks.push({ t: "id", v: raw.toLowerCase(), raw });
        i = j; continue;
      }
      const two = sql.slice(i, i + 2);
      if (two === "<=" || two === ">=" || two === "!=" || two === "<>") {
        toks.push({ t: "op", v: two === "<>" ? "!=" : two }); i += 2; continue;
      }
      if ("()*,.=<>+-/;".indexOf(c) >= 0) { toks.push({ t: "op", v: c }); i++; continue; }
      throw new SqlError(`Unexpected character '${c}' in query.`);
    }
    toks.push({ t: "eof" });
    return toks;
  }

  /* ================================ parser ================================ */
  const SUBQ_MSG = "Subqueries aren't in the course engine — use a WITH ... AS CTE instead.";

  function parseSql(text) {
    const toks = tokenize(String(text == null ? "" : text));
    let i = 0;
    const peek = (k) => toks[i + (k || 0)];
    const isOp = (op, k) => { const t = peek(k); return t.t === "op" && t.v === op; };
    const isKw = (w, k) => { const t = peek(k); return t.t === "kw" && t.v === w; };
    const tryOp = (op) => (isOp(op) ? (i++, true) : false);
    const tryKw = (w) => (isKw(w) ? (i++, true) : false);
    const tokStr = (t) => (t.t === "eof" ? "end of query" : "'" + (t.raw !== undefined ? t.raw : t.v) + "'");
    function expectOp(op, what) {
      if (!tryOp(op)) throw new SqlError(`Expected '${op}'${what ? " " + what : ""}, got ${tokStr(peek())}.`);
    }
    function expectKw(w, what) {
      if (!tryKw(w)) throw new SqlError(`Expected ${w}${what ? " " + what : ""}, got ${tokStr(peek())}.`);
    }
    function expectIdent(what) {
      const t = peek();
      if (t.t !== "id") throw new SqlError(`Expected ${what}, got ${tokStr(t)}.`);
      i++; return t;
    }
    function noSubquery(where) {
      if (isKw("SELECT") || isKw("WITH")) throw new SqlError(SUBQ_MSG + (where ? ` (found a SELECT ${where})` : ""));
    }

    function parseQuery() {
      const ctes = [];
      if (tryKw("WITH")) {
        do {
          const name = expectIdent("a CTE name").v;
          expectKw("AS", "after the CTE name");
          expectOp("(", "before the CTE body");
          const s = parseSelect();
          expectOp(")", "to close the CTE body");
          ctes.push({ name, select: s });
        } while (tryOp(","));
      }
      const select = parseSelect();
      tryOp(";");
      if (peek().t !== "eof") {
        const t = peek();
        const FRIENDLY = {
          UNION: "UNION isn't in the course engine — run the queries separately, or restructure with a CTE.",
          EXCEPT: "EXCEPT isn't in the course engine — use a LEFT JOIN ... IS NULL pattern instead.",
          INTERSECT: "INTERSECT isn't in the course engine — use an INNER JOIN instead.",
          QUALIFY: "QUALIFY isn't in the course engine — compute the window in a WITH ... AS CTE and filter in the outer SELECT.",
          OFFSET: "OFFSET isn't in the course engine — only LIMIT n is supported.",
        };
        if (t.t === "kw" && FRIENDLY[t.v]) throw new SqlError(FRIENDLY[t.v]);
        throw new SqlError(`Unexpected ${tokStr(t)} after the end of the query.`);
      }
      return { ctes, select };
    }

    function parseSelect() {
      if (!isKw("SELECT")) {
        throw new SqlError(`Expected SELECT, got ${tokStr(peek())} — only SELECT queries run in the course engine.`);
      }
      i++;
      const distinct = tryKw("DISTINCT");
      const items = [];
      do { items.push(parseSelectItem()); } while (tryOp(","));

      let from = null; const joins = [];
      let where = null, groupBy = [], having = null; const orderBy = [];
      let limit = null;

      if (tryKw("FROM")) {
        from = parseTableRef();
        for (;;) {
          let jt = null;
          if (isKw("JOIN")) { i++; jt = "inner"; }
          else if (isKw("INNER")) { i++; expectKw("JOIN", "after INNER"); jt = "inner"; }
          else if (isKw("LEFT")) { i++; tryKw("OUTER"); expectKw("JOIN", "after LEFT"); jt = "left"; }
          else if (isKw("RIGHT") || isKw("FULL") || isKw("CROSS")) {
            throw new SqlError(`${peek().v} JOIN isn't in the course engine — use INNER JOIN or LEFT JOIN (swap the table order if you need the other side kept).`);
          }
          else break;
          const ref = parseTableRef();
          expectKw("ON", "after the joined table (equi-join required)");
          const on = parseExpr();
          joins.push({ type: jt, ref, on });
        }
      }
      if (tryKw("WHERE")) where = parseExpr();
      if (tryKw("GROUP")) {
        expectKw("BY", "after GROUP");
        do { groupBy.push(parseExpr()); } while (tryOp(","));
      }
      if (tryKw("HAVING")) having = parseExpr();
      if (tryKw("ORDER")) {
        expectKw("BY", "after ORDER");
        do {
          const e = parseExpr();
          let desc = false;
          if (tryKw("DESC")) desc = true; else tryKw("ASC");
          orderBy.push({ e, desc });
        } while (tryOp(","));
      }
      if (tryKw("LIMIT")) {
        const t = peek();
        if (t.t !== "num") throw new SqlError("LIMIT expects a number, e.g. LIMIT 10.");
        i++; limit = Math.max(0, Math.floor(t.v));
      }
      return { distinct, items, from, joins, where, groupBy, having, orderBy, limit };
    }

    function parseSelectItem() {
      if (isOp("*")) { i++; return { star: true }; }
      if (peek().t === "id" && isOp(".", 1) && isOp("*", 2)) {
        const t = peek(); i += 3;
        return { starOf: t.v };
      }
      const expr = parseExpr();
      let alias = null, label = null;
      if (tryKw("AS")) { const a = expectIdent("an alias after AS"); alias = a.v; label = a.raw; }
      else if (peek().t === "id") { const a = peek(); i++; alias = a.v; label = a.raw; }
      return { expr, alias, label };
    }

    function parseTableRef() {
      if (isOp("(")) { i++; noSubquery("in FROM"); throw new SqlError("Expected a table name in FROM."); }
      const t = expectIdent("a table name");
      let alias = t.v;
      if (peek().t === "id") alias = toks[i++].v;
      return { name: t.v, alias };
    }

    /* ---- expressions: OR < AND < NOT < predicate < add < mul < unary ---- */
    function parseExpr() { return parseOr(); }
    function parseOr() {
      let l = parseAnd();
      while (tryKw("OR")) l = { t: "or", l, r: parseAnd() };
      return l;
    }
    function parseAnd() {
      let l = parseNot();
      while (tryKw("AND")) l = { t: "and", l, r: parseNot() };
      return l;
    }
    function parseNot() {
      if (tryKw("NOT")) return { t: "not", e: parseNot() };
      return parsePredicate();
    }
    function parsePredicate() {
      const e = parseAdd();
      if (tryKw("IS")) {
        const neg = tryKw("NOT");
        expectKw("NULL", "— only IS [NOT] NULL is supported after IS");
        return { t: "isnull", e, neg };
      }
      let neg = false;
      if (isKw("NOT") && (isKw("IN", 1) || isKw("LIKE", 1) || isKw("BETWEEN", 1))) { i++; neg = true; }
      if (tryKw("IN")) {
        expectOp("(", "after IN");
        noSubquery("inside IN (...)");
        const list = [parseExpr()];
        while (tryOp(",")) list.push(parseExpr());
        expectOp(")", "to close the IN list");
        return { t: "in", e, list, neg };
      }
      if (tryKw("LIKE")) return { t: "like", e, p: parseAdd(), neg };
      if (tryKw("BETWEEN")) {
        const lo = parseAdd();
        expectKw("AND", "in BETWEEN lo AND hi");
        return { t: "between", e, lo, hi: parseAdd(), neg };
      }
      if (neg) throw new SqlError("NOT here must be followed by IN, LIKE or BETWEEN.");
      const t = peek();
      if (t.t === "op" && ["=", "!=", "<", "<=", ">", ">="].indexOf(t.v) >= 0) {
        i++;
        return { t: "cmp", op: t.v, l: e, r: parseAdd() };
      }
      return e;
    }
    function parseAdd() {
      let l = parseMul();
      while (isOp("+") || isOp("-")) { const op = toks[i++].v; l = { t: "ar", op, l, r: parseMul() }; }
      return l;
    }
    function parseMul() {
      let l = parseUnary();
      while (isOp("*") || isOp("/")) { const op = toks[i++].v; l = { t: "ar", op, l, r: parseUnary() }; }
      return l;
    }
    function parseUnary() {
      if (tryOp("-")) return { t: "neg", e: parseUnary() };
      if (tryOp("+")) return parseUnary();
      return parsePrimary();
    }
    function parsePrimary() {
      const t = peek();
      if (t.t === "num") { i++; return { t: "num", v: t.v }; }
      if (t.t === "str") { i++; return { t: "str", v: t.v }; }
      if (isKw("NULL")) { i++; return { t: "null" }; }
      if (isKw("CASE")) { i++; return parseCase(); }
      if (isKw("EXISTS")) throw new SqlError("EXISTS " + SUBQ_MSG);
      if (isKw("SELECT") || isKw("WITH")) throw new SqlError(SUBQ_MSG);
      if (isOp("(")) {
        i++;
        noSubquery("inside parentheses");
        const e = parseExpr();
        expectOp(")", "to close the parenthesized expression");
        return e;
      }
      if (t.t === "id") {
        if (isOp("(", 1)) { i++; return parseCall(t); }
        if (isOp(".", 1)) {
          i += 2;
          if (isOp("*")) throw new SqlError(`'${t.raw}.*' is only allowed as a whole item in the SELECT list.`);
          const c = expectIdent(`a column name after '${t.raw}.'`);
          return { t: "col", q: t.v, name: c.v };
        }
        i++;
        return { t: "col", q: null, name: t.v };
      }
      throw new SqlError(`Unexpected ${tokStr(t)} where a value or column was expected.`);
    }

    function parseCase() {
      if (!isKw("WHEN")) {
        throw new SqlError("Only searched CASE is supported — write CASE WHEN <condition> THEN <value> ... [ELSE <value>] END.");
      }
      const whens = [];
      while (tryKw("WHEN")) {
        const c = parseExpr();
        expectKw("THEN", "after the WHEN condition");
        whens.push({ c, v: parseExpr() });
      }
      const els = tryKw("ELSE") ? parseExpr() : null;
      expectKw("END", "to close the CASE expression");
      return { t: "case", whens, els };
    }

    const SCALARS = { round: [1, 2], coalesce: [1, 99], upper: [1, 1], lower: [1, 1], trim: [1, 1], length: [1, 1], substr: [2, 3] };

    function parseCall(t) { // t = ident token; the '(' is at toks[i]
      const name = t.v;
      i++; // consume '('
      const isWinOnly = name === "row_number" || name === "rank" || name === "dense_rank";
      const isAgg = ["count", "sum", "avg", "min", "max"].indexOf(name) >= 0;
      if (!isWinOnly && !isAgg && !(name in SCALARS)) {
        throw new SqlError(`Unknown function ${t.raw.toUpperCase()}() — supported: ROUND, COALESCE, UPPER, LOWER, TRIM, LENGTH, SUBSTR; aggregates COUNT/SUM/AVG/MIN/MAX; windows ROW_NUMBER/RANK/DENSE_RANK.`);
      }
      let star = false, distinct = false; const args = [];
      if (isOp("*")) { i++; star = true; }
      else if (!isOp(")")) {
        noSubquery("as a function argument");
        distinct = tryKw("DISTINCT");
        args.push(parseExpr());
        while (tryOp(",")) args.push(parseExpr());
      }
      expectOp(")", `to close ${name.toUpperCase()}(...)`);
      if (isKw("OVER")) { i++; return parseOver(name, star, distinct, args); }
      if (isWinOnly) throw new SqlError(`${name.toUpperCase()}() is a window function — add OVER (PARTITION BY ... ORDER BY ...).`);
      if (isAgg) {
        if (star && name !== "count") throw new SqlError(`${name.toUpperCase()}(*) isn't valid — only COUNT(*) takes '*'.`);
        if (distinct && name !== "count") throw new SqlError("DISTINCT inside an aggregate is only supported as COUNT(DISTINCT x).");
        if (!star && args.length !== 1) throw new SqlError(`${name.toUpperCase()}() takes exactly one argument.`);
        return { t: "agg", name, star, distinct, arg: args[0] || null };
      }
      const lo = SCALARS[name][0], hi = SCALARS[name][1];
      if (star || distinct || args.length < lo || args.length > hi) {
        throw new SqlError(`${name.toUpperCase()}() expects ${lo === hi ? lo : lo + " to " + hi} argument(s).`);
      }
      return { t: "fn", name, args };
    }

    function parseOver(name, star, distinct, args) {
      const ok = ["row_number", "rank", "dense_rank", "sum", "count"].indexOf(name) >= 0;
      if (!ok) throw new SqlError(`${name.toUpperCase()}(...) OVER isn't in the course engine — supported window functions: ROW_NUMBER(), RANK(), DENSE_RANK(), SUM(x), COUNT(*).`);
      if (distinct) throw new SqlError("DISTINCT isn't supported inside window functions.");
      if (name === "sum" && (star || args.length !== 1)) throw new SqlError("SUM(x) OVER needs exactly one argument.");
      if ((name === "row_number" || name === "rank" || name === "dense_rank") && (star || args.length)) {
        throw new SqlError(`${name.toUpperCase()}() takes no arguments.`);
      }
      expectOp("(", "after OVER");
      const partitionBy = []; const orderBy = [];
      if (tryKw("PARTITION")) {
        expectKw("BY", "after PARTITION");
        do { partitionBy.push(parseExpr()); } while (tryOp(","));
      }
      if (tryKw("ORDER")) {
        expectKw("BY", "after ORDER (inside OVER)");
        do {
          const e = parseExpr();
          let desc = false;
          if (tryKw("DESC")) desc = true; else tryKw("ASC");
          orderBy.push({ e, desc });
        } while (tryOp(","));
      }
      expectOp(")", "to close OVER (...)");
      return { t: "win", fn: name, star, arg: args[0] || null, partitionBy, orderBy };
    }

    return parseQuery();
  }

  /* =========================== value semantics ============================ */
  function num(v) {
    if (typeof v === "number") return v;
    if (v === null || v === undefined) return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  function normVal(v) {
    if (typeof v === "number") { const r = Math.round(v * 1e6) / 1e6; return r === 0 ? 0 : r; }
    if (v === undefined) return null;
    return v;
  }
  function normKey(arr) { return JSON.stringify(arr.map(normVal)); }
  function truthy(v) { return !!v; }

  /** Total order for ORDER BY / MIN / MAX: NULL first, numbers numeric, else string. */
  function cmp(a, b) {
    if (a === null && b === null) return 0;
    if (a === null) return -1;
    if (b === null) return 1;
    if (typeof a === "boolean") a = a ? 1 : 0;
    if (typeof b === "boolean") b = b ? 1 : 0;
    if (typeof a === "number" && typeof b === "number") return a < b ? -1 : a > b ? 1 : 0;
    const as = typeof a === "object" ? JSON.stringify(a) : String(a);
    const bs = typeof b === "object" ? JSON.stringify(b) : String(b);
    return as < bs ? -1 : as > bs ? 1 : 0;
  }
  function eqv(a, b) {
    if (a === null || b === null) return false;
    if (typeof a === "number" && typeof b === "number") return Math.abs(a - b) < 1e-9;
    if (typeof a === "object" || typeof b === "object") return normKey([a]) === normKey([b]);
    return a === b;
  }
  function compareOp(op, a, b) {
    if (a === null || b === null || a === undefined || b === undefined) return false; // NULL never compares
    if (op === "=") return eqv(a, b);
    if (op === "!=") return !eqv(a, b);
    const c = cmp(a, b);
    if (op === "<") return c < 0;
    if (op === "<=") return c <= 0;
    if (op === ">") return c > 0;
    return c >= 0; // >=
  }
  const likeCache = {};
  function likeRe(p) {
    if (!likeCache[p]) {
      const re = p.replace(/[.*+?^${}()|[\]\\]/g, "\\$&").replace(/%/g, "[\\s\\S]*").replace(/_/g, ".");
      likeCache[p] = new RegExp("^" + re + "$");
    }
    return likeCache[p];
  }

  /* ============================ expression walk =========================== */
  function walkExpr(e, f) {
    if (!e || typeof e !== "object" || !e.t) return;
    f(e);
    const sub = (x) => walkExpr(x, f);
    if (e.l) sub(e.l); if (e.r) sub(e.r); if (e.e) sub(e.e);
    if (e.p) sub(e.p); if (e.lo) sub(e.lo); if (e.hi) sub(e.hi);
    if (e.arg) sub(e.arg); if (e.els) sub(e.els);
    if (e.list) e.list.forEach(sub);
    if (e.args) e.args.forEach(sub);
    if (e.whens) e.whens.forEach((w) => { sub(w.c); sub(w.v); });
    if (e.partitionBy) e.partitionBy.forEach(sub);
    if (e.orderBy) e.orderBy.forEach((o) => sub(o.e));
  }
  function hasNode(e, type) {
    let found = false;
    walkExpr(e, (n) => { if (n.t === type) found = true; });
    return found;
  }

  /* ================================ frames ================================ */
  /** union of row-object keys, first-seen order (order_events drifts on purpose) */
  function colsOf(rows) {
    const s = new Set();
    for (const r of rows) for (const k in r) s.add(k.toLowerCase());
    return Array.from(s);
  }
  function tableRows(env, name) {
    const rows = env[name];
    if (!rows || !Array.isArray(rows)) {
      throw new SqlError(`Unknown table '${name}'. Available: ${Object.keys(env).join(", ")}.`);
    }
    return rows;
  }
  function baseFrame(env, ref) {
    const rows = tableRows(env, ref.name);
    const frame = { order: [ref.alias], cols: {}, rows: [] };
    frame.cols[ref.alias] = colsOf(rows);
    frame.rows = rows.map((r) => { const o = {}; o[ref.alias] = r; return o; });
    return frame;
  }
  function colVal(frame, frow, q, name) {
    if (q) {
      const list = frame.cols[q];
      if (!list) throw new SqlError(`Unknown table alias '${q}' — FROM/JOIN define: ${frame.order.join(", ")}.`);
      if (list.indexOf(name) < 0) throw new SqlError(`Column '${name}' isn't on '${q}' (columns: ${list.join(", ")}).`);
      const r = frow[q];
      if (!r) return null; // LEFT JOIN miss
      const v = r[name];
      return v === undefined ? null : v;
    }
    let owner = null;
    for (const al of frame.order) {
      if (frame.cols[al].indexOf(name) >= 0) {
        if (owner) throw new SqlError(`Ambiguous column '${name}' — qualify it (${owner}.${name} or ${al}.${name}).`);
        owner = al;
      }
    }
    if (!owner) {
      const all = frame.order.map((al) => frame.cols[al].join(", ")).join("; ");
      throw new SqlError(`Unknown column '${name}'. Available: ${all || "(no FROM table)"}.`);
    }
    const r = frow[owner];
    if (!r) return null;
    const v = r[name];
    return v === undefined ? null : v;
  }

  function applyJoin(frame, env, j) {
    const alias = j.ref.alias;
    if (frame.cols[alias]) throw new SqlError(`Duplicate table alias '${alias}' — give the joined table a different alias.`);
    const rrows = tableRows(env, j.ref.name);
    const rcols = colsOf(rrows);
    // Flatten ON into ANDed column equalities (the only join form we support).
    const eqs = [];
    (function flat(e) {
      if (e.t === "and") { flat(e.l); flat(e.r); return; }
      if (e.t === "cmp" && e.op === "=" && e.l.t === "col" && e.r.t === "col") { eqs.push([e.l, e.r]); return; }
      throw new SqlError("JOIN ... ON supports only column equalities (a.col = b.col), optionally combined with AND. Filter other conditions in WHERE.");
    })(j.on);
    const sideOf = (c) => {
      if (c.q) {
        if (c.q === alias) return "R";
        if (frame.cols[c.q]) return "L";
        throw new SqlError(`Unknown table alias '${c.q}' in the JOIN condition.`);
      }
      const inR = rcols.indexOf(c.name) >= 0;
      const inL = frame.order.some((al) => frame.cols[al].indexOf(c.name) >= 0);
      if (inR && inL) throw new SqlError(`Ambiguous column '${c.name}' in JOIN — qualify it, e.g. ${alias}.${c.name}.`);
      if (inR) return "R";
      if (inL) return "L";
      throw new SqlError(`Unknown column '${c.name}' in the JOIN condition.`);
    };
    const pairs = eqs.map(([a, b]) => {
      const sa = sideOf(a), sb = sideOf(b);
      if (sa === sb) throw new SqlError("Each JOIN equality must compare a column from the tables already in FROM with a column of the newly joined table.");
      return sa === "L" ? { l: a, r: b } : { l: b, r: a };
    });
    // hash the right side (NULL keys never match, as in SQL)
    const hash = new Map();
    for (const rr of rrows) {
      const vals = pairs.map((p) => { const v = rr[p.r.name]; return v === undefined ? null : v; });
      if (vals.some((v) => v === null)) continue;
      const k = normKey(vals);
      const a = hash.get(k);
      if (a) a.push(rr); else hash.set(k, [rr]);
    }
    const out = [];
    for (const fr of frame.rows) {
      const vals = pairs.map((p) => colVal(frame, fr, p.l.q, p.l.name));
      const matches = vals.some((v) => v === null) ? null : hash.get(normKey(vals));
      if (matches && matches.length) {
        for (const rr of matches) { const o = Object.assign({}, fr); o[alias] = rr; out.push(o); }
      } else if (j.type === "left") {
        const o = Object.assign({}, fr); o[alias] = null; out.push(o);
      }
    }
    frame.order.push(alias);
    frame.cols[alias] = rcols;
    frame.rows = out;
  }

  /* =============================== evaluator ============================== */
  function evalExpr(e, ctx) {
    switch (e.t) {
      case "num": return e.v;
      case "str": return e.v;
      case "null": return null;
      case "col": return colVal(ctx.frame, ctx.row, e.q, e.name);
      case "neg": { const v = num(evalExpr(e.e, ctx)); return v === null ? null : -v; }
      case "ar": {
        const a = num(evalExpr(e.l, ctx)), b = num(evalExpr(e.r, ctx));
        if (a === null || b === null) return null;
        if (e.op === "+") return a + b;
        if (e.op === "-") return a - b;
        if (e.op === "*") return a * b;
        return b === 0 ? null : a / b;
      }
      case "cmp": return compareOp(e.op, evalExpr(e.l, ctx), evalExpr(e.r, ctx));
      case "and": return truthy(evalExpr(e.l, ctx)) && truthy(evalExpr(e.r, ctx));
      case "or": return truthy(evalExpr(e.l, ctx)) || truthy(evalExpr(e.r, ctx));
      case "not": return !truthy(evalExpr(e.e, ctx));
      case "in": {
        const v = evalExpr(e.e, ctx);
        const res = e.list.some((x) => eqv(v, evalExpr(x, ctx)));
        return e.neg ? !res : res;
      }
      case "between": {
        const v = evalExpr(e.e, ctx), lo = evalExpr(e.lo, ctx), hi = evalExpr(e.hi, ctx);
        const res = v !== null && lo !== null && hi !== null && cmp(v, lo) >= 0 && cmp(v, hi) <= 0;
        return e.neg ? !res : res;
      }
      case "like": {
        const s = evalExpr(e.e, ctx), p = evalExpr(e.p, ctx);
        const res = typeof s === "string" && typeof p === "string" && likeRe(p).test(s);
        return e.neg ? !res : res;
      }
      case "isnull": {
        const r = evalExpr(e.e, ctx) === null;
        return e.neg ? !r : r;
      }
      case "case": {
        for (const w of e.whens) if (truthy(evalExpr(w.c, ctx))) return evalExpr(w.v, ctx);
        return e.els ? evalExpr(e.els, ctx) : null;
      }
      case "fn": return evalScalar(e, ctx);
      case "agg": return evalAgg(e, ctx);
      case "win": {
        const m = ctx.winvals && ctx.winvals.get(e);
        if (!m) throw new SqlError("Window functions are only allowed in the SELECT list — alias the window there and (if you need to filter on it) move it into a WITH ... AS CTE.");
        return m.get(ctx.unit);
      }
      default: throw new SqlError("Internal: unknown expression node '" + e.t + "'.");
    }
  }

  function evalScalar(e, ctx) {
    const name = e.name;
    if (name === "coalesce") {
      for (const a of e.args) { const v = evalExpr(a, ctx); if (v !== null) return v; }
      return null;
    }
    const v0 = evalExpr(e.args[0], ctx);
    if (name === "round") {
      const v = num(v0);
      if (v === null) return null;
      const n = e.args[1] ? (num(evalExpr(e.args[1], ctx)) || 0) : 0;
      const f = Math.pow(10, n);
      return (v < 0 ? -1 : 1) * Math.round((Math.abs(v) + 1e-12) * f) / f;
    }
    if (v0 === null) return null;
    if (name === "upper") return String(v0).toUpperCase();
    if (name === "lower") return String(v0).toLowerCase();
    if (name === "trim") return String(v0).trim();
    if (name === "length") return String(v0).length;
    // substr(s, start[, len]) — 1-based, like SQL
    const s = String(v0);
    const start = num(evalExpr(e.args[1], ctx));
    if (start === null) return null;
    const from = start > 0 ? start - 1 : 0;
    if (e.args[2]) {
      const len = num(evalExpr(e.args[2], ctx));
      if (len === null) return null;
      return s.slice(from, from + Math.max(0, len));
    }
    return s.slice(from);
  }

  function evalAgg(e, ctx) {
    if (ctx.inAgg) throw new SqlError("Aggregates can't be nested (e.g. SUM(COUNT(*)) isn't valid).");
    const rows = ctx.rows;
    if (!rows) throw new SqlError(`${e.name.toUpperCase()}(...) is an aggregate — it needs GROUP BY (or an aggregate-only SELECT) and can't be used in WHERE; filter aggregates with HAVING.`);
    const per = (r) => evalExpr(e.arg, { frame: ctx.frame, row: r, rows: null, inAgg: true, winvals: null });
    if (e.name === "count") {
      if (e.star) return rows.length;
      if (e.distinct) {
        const seen = new Set();
        for (const r of rows) { const v = per(r); if (v !== null) seen.add(normKey([v])); }
        return seen.size;
      }
      let c = 0;
      for (const r of rows) if (per(r) !== null) c++;
      return c;
    }
    if (e.name === "sum" || e.name === "avg") {
      let sum = 0, n = 0;
      for (const r of rows) { const v = num(per(r)); if (v !== null) { sum += v; n++; } }
      if (!n) return null;
      return e.name === "sum" ? sum : sum / n;
    }
    // min / max
    let best = null;
    for (const r of rows) {
      const v = per(r);
      if (v === null) continue;
      if (best === null || (e.name === "min" ? cmp(v, best) < 0 : cmp(v, best) > 0)) best = v;
    }
    return best;
  }

  /* ============================ window functions ========================== */
  function cmpKeys(ka, kb, obs) {
    for (let i = 0; i < obs.length; i++) {
      const c = cmp(ka[i], kb[i]);
      if (c) return obs[i].desc ? -c : c;
    }
    return 0;
  }

  function computeWindows(winNodes, units, ctxOf) {
    const winvals = new Map();
    for (const w of winNodes) {
      if (winvals.has(w)) continue;
      const vals = new Map();
      const parts = new Map();
      for (const u of units) {
        const k = normKey(w.partitionBy.map((e) => evalExpr(e, ctxOf(u))));
        const a = parts.get(k);
        if (a) a.push(u); else parts.set(k, [u]);
      }
      for (const part of parts.values()) {
        let arr = part, keys = null;
        if (w.orderBy.length) {
          keys = new Map(part.map((u) => [u, w.orderBy.map((ob) => evalExpr(ob.e, ctxOf(u)))]));
          arr = part.slice().sort((a, b) => cmpKeys(keys.get(a), keys.get(b), w.orderBy)); // stable
        }
        if (w.fn === "row_number") {
          if (!w.orderBy.length) throw new SqlError("ROW_NUMBER() needs ORDER BY inside OVER (...).");
          arr.forEach((u, i) => vals.set(u, i + 1));
        } else if (w.fn === "rank" || w.fn === "dense_rank") {
          if (!w.orderBy.length) throw new SqlError(w.fn.toUpperCase() + "() needs ORDER BY inside OVER (...).");
          let rank = 0, dense = 0, prev = null;
          arr.forEach((u, i) => {
            const k = keys.get(u);
            if (prev === null || cmpKeys(prev, k, w.orderBy) !== 0) { dense++; rank = i + 1; prev = k; }
            vals.set(u, w.fn === "rank" ? rank : dense);
          });
        } else { // sum / count over the partition (cumulative when ORDER BY present)
          const contrib = (u) => (w.fn === "count"
            ? (w.star ? 1 : (evalExpr(w.arg, ctxOf(u)) === null ? 0 : 1))
            : num(evalExpr(w.arg, ctxOf(u))));
          if (!w.orderBy.length) {
            let tot = null, cnt = 0;
            for (const u of arr) {
              const c = contrib(u);
              if (w.fn === "count") cnt += c;
              else if (c !== null) tot = (tot === null ? 0 : tot) + c;
            }
            for (const u of arr) vals.set(u, w.fn === "count" ? cnt : tot);
          } else {
            // standard frame: unbounded preceding .. current row, peers included
            let i = 0, runSum = null, runCnt = 0;
            while (i < arr.length) {
              let j = i;
              while (j < arr.length && cmpKeys(keys.get(arr[i]), keys.get(arr[j]), w.orderBy) === 0) j++;
              for (let k2 = i; k2 < j; k2++) {
                const c = contrib(arr[k2]);
                if (w.fn === "count") runCnt += c;
                else if (c !== null) runSum = (runSum === null ? 0 : runSum) + c;
              }
              for (let k2 = i; k2 < j; k2++) vals.set(arr[k2], w.fn === "count" ? runCnt : runSum);
              i = j;
            }
          }
        }
      }
      winvals.set(w, vals);
    }
    return winvals;
  }

  /* ============================ SELECT pipeline =========================== */
  function autoName(e) {
    if (e.t === "col") return e.name;
    if (e.t === "agg" || e.t === "fn") return e.name;
    if (e.t === "win") return e.fn;
    if (e.t === "case") return "case";
    return "expr";
  }

  function execSelect(sel, env) {
    let frame;
    if (sel.from) {
      frame = baseFrame(env, sel.from);
      for (const j of sel.joins) applyJoin(frame, env, j);
    } else {
      frame = { order: [], cols: {}, rows: [{}] };
    }

    let rows = frame.rows;
    if (sel.where) {
      if (hasNode(sel.where, "agg")) throw new SqlError("Aggregates aren't allowed in WHERE — filter aggregate results with HAVING.");
      if (hasNode(sel.where, "win")) throw new SqlError("Window functions aren't allowed in WHERE — compute them in a WITH ... AS CTE and filter in the outer SELECT.");
      rows = rows.filter((r) => truthy(evalExpr(sel.where, { frame, row: r, rows: null, winvals: null })));
    }

    // expand * / t.* into concrete select items
    const items = [];
    for (const it of sel.items) {
      if (it.star) {
        if (!frame.order.length) throw new SqlError("SELECT * needs a FROM table.");
        for (const al of frame.order) for (const c of frame.cols[al]) items.push({ label: c, alias: null, expr: { t: "col", q: al, name: c } });
      } else if (it.starOf) {
        if (!frame.cols[it.starOf]) throw new SqlError(`Unknown table alias '${it.starOf}' in '${it.starOf}.*'.`);
        for (const c of frame.cols[it.starOf]) items.push({ label: c, alias: null, expr: { t: "col", q: it.starOf, name: c } });
      } else {
        items.push({ label: it.label || autoName(it.expr), alias: it.alias, expr: it.expr });
      }
    }

    // GROUP BY may reference a SELECT alias (common convenience) — substitute it
    const groupBy = (sel.groupBy || []).map((e) => {
      if (e.t === "col" && !e.q) {
        const hit = items.find((it) => it.alias === e.name);
        if (hit && !hasNode(hit.expr, "agg") && !hasNode(hit.expr, "win")) return hit.expr;
      }
      return e;
    });
    for (const e of groupBy) {
      if (hasNode(e, "agg") || hasNode(e, "win")) throw new SqlError("GROUP BY can't contain aggregates or window functions.");
    }

    const grouped = groupBy.length > 0 || !!sel.having ||
      items.some((it) => hasNode(it.expr, "agg")) ||
      sel.orderBy.some((ob) => hasNode(ob.e, "agg"));

    let units;
    if (grouped) {
      if (groupBy.length) {
        const m = new Map();
        for (const r of rows) {
          const k = normKey(groupBy.map((e) => evalExpr(e, { frame, row: r, rows: null, winvals: null })));
          const g = m.get(k);
          if (g) g.push(r); else m.set(k, [r]);
        }
        units = Array.from(m.values()).map((g) => ({ row: g[0], rows: g }));
      } else {
        units = [{ row: rows[0] || {}, rows }];
      }
    } else {
      units = rows.map((r) => ({ row: r, rows: null }));
    }

    // HAVING filters groups (before windows — no filtering on window results)
    if (sel.having) {
      if (hasNode(sel.having, "win")) throw new SqlError("Window functions aren't allowed in HAVING — compute them in a WITH ... AS CTE and filter in the outer SELECT.");
      units = units.filter((u) => truthy(evalExpr(sel.having, { frame, row: u.row, rows: u.rows, winvals: null, unit: u })));
    }

    // window functions over the post-WHERE / post-GROUP BY units
    const winNodes = [];
    const collect = (e) => walkExpr(e, (n) => { if (n.t === "win") winNodes.push(n); });
    items.forEach((it) => collect(it.expr));
    sel.orderBy.forEach((ob) => collect(ob.e));
    const bareCtx = (u) => ({ frame, row: u.row, rows: u.rows, winvals: null, unit: u });
    const winvals = winNodes.length ? computeWindows(winNodes, units, bareCtx) : null;
    const ctxOf = (u) => ({ frame, row: u.row, rows: u.rows, winvals, unit: u });

    let outs = units.map((u) => ({ u, vals: items.map((it) => evalExpr(it.expr, ctxOf(u))) }));

    if (sel.distinct) {
      const seen = new Set();
      outs = outs.filter((o) => { const k = normKey(o.vals); if (seen.has(k)) return false; seen.add(k); return true; });
    }

    if (sel.orderBy.length) {
      const keyFns = sel.orderBy.map((ob) => {
        if (ob.e.t === "col" && !ob.e.q) {
          const idx = items.findIndex((it) => (it.alias || it.label || "").toLowerCase() === ob.e.name);
          if (idx >= 0) return (o) => o.vals[idx]; // SELECT alias wins, incl. window aliases
        }
        return (o) => evalExpr(ob.e, ctxOf(o.u));
      });
      outs.sort((a, b) => {
        for (let i = 0; i < keyFns.length; i++) {
          const c = cmp(keyFns[i](a), keyFns[i](b));
          if (c) return sel.orderBy[i].desc ? -c : c;
        }
        return 0; // stable
      });
    }

    if (sel.limit != null) outs = outs.slice(0, sel.limit);
    return { columns: items.map((it) => it.label), rows: outs.map((o) => o.vals) };
  }

  function resultToRows(res) {
    const cols = res.columns.map((c) => String(c).toLowerCase());
    return res.rows.map((vals) => {
      const o = {};
      cols.forEach((c, i) => { o[c] = vals[i] === undefined ? null : vals[i]; });
      return o;
    });
  }

  /**
   * Run a SQL query over plain-object tables.
   * @param {string} sqlText
   * @param {Object<string, Array<Object>>} tables — e.g. window.NIMBUS
   * @returns {{columns: string[], rows: Array<Array<*>>}}
   * @throws {SqlError} with a friendly message on bad SQL / unsupported constructs
   */
  function run(sqlText, tables) {
    if (sqlText == null || !String(sqlText).trim()) throw new SqlError("Write a SELECT query first.");
    const q = parseSql(sqlText);
    const env = {};
    for (const k in (tables || {})) env[k.toLowerCase()] = tables[k];
    for (const cte of q.ctes) env[cte.name] = resultToRows(execSelect(cte.select, env));
    return execSelect(q.select, env);
  }

  /* ============================ expected diff ============================= */
  function fmtNum(v) {
    if (Number.isInteger(v)) return String(v);
    return String(Math.round(v * 1e6) / 1e6);
  }
  function fmtValInline(v) {
    if (v === null || v === undefined) return "NULL";
    if (typeof v === "number") return fmtNum(v);
    if (typeof v === "object") return JSON.stringify(v);
    return "'" + String(v) + "'";
  }
  function fmtRowInline(vals) {
    let s = "(" + vals.map(fmtValInline).join(", ") + ")";
    if (s.length > 110) s = s.slice(0, 107) + "…)";
    return s;
  }

  /** Compare user result to expected; returns {pass, msg} with a mechanical diff hint. */
  function compareResults(user, sol, orderSensitive) {
    const uc = user.columns.map((c) => String(c).toLowerCase());
    const sc = sol.columns.map((c) => String(c).toLowerCase());
    if (uc.length !== sc.length || uc.some((c, i) => c !== sc[i])) {
      return {
        pass: false,
        msg: `Column mismatch — yours: (${user.columns.join(", ") || "none"}); expected: (${sol.columns.join(", ")}). Check names, aliases, and column order.`,
      };
    }
    const key = (r) => normKey(r);
    if (user.rows.length !== sol.rows.length) {
      let msg = `Not there yet — your result has ${user.rows.length} row${user.rows.length === 1 ? "" : "s"}; expected ${sol.rows.length}.`;
      const ucnt = new Map();
      for (const r of user.rows) ucnt.set(key(r), (ucnt.get(key(r)) || 0) + 1);
      const miss = sol.rows.find((r) => { const k = key(r); const c = ucnt.get(k) || 0; if (c > 0) { ucnt.set(k, c - 1); return false; } return true; });
      if (miss) msg += ` e.g. this expected row is missing from yours: ${fmtRowInline(miss)}.`;
      else {
        const scnt = new Map();
        for (const r of sol.rows) scnt.set(key(r), (scnt.get(key(r)) || 0) + 1);
        const extra = user.rows.find((r) => { const k = key(r); const c = scnt.get(k) || 0; if (c > 0) { scnt.set(k, c - 1); return false; } return true; });
        if (extra) msg += ` e.g. this row of yours shouldn't be there: ${fmtRowInline(extra)}.`;
      }
      return { pass: false, msg };
    }
    if (orderSensitive) {
      for (let i = 0; i < user.rows.length; i++) {
        if (key(user.rows[i]) !== key(sol.rows[i])) {
          return {
            pass: false,
            msg: `Row ${i + 1} differs — yours: ${fmtRowInline(user.rows[i])}; expected: ${fmtRowInline(sol.rows[i])}. (Row order counts because your query has ORDER BY.)`,
          };
        }
      }
      return { pass: true, msg: `Matches expected — ${user.rows.length} rows.` };
    }
    const scnt = new Map();
    for (const r of sol.rows) scnt.set(key(r), (scnt.get(key(r)) || 0) + 1);
    let extra = null;
    for (const r of user.rows) {
      const k = key(r), c = scnt.get(k) || 0;
      if (c <= 0) { extra = r; break; }
      scnt.set(k, c - 1);
    }
    if (extra) {
      let missing = null;
      for (const [k, c] of scnt) { if (c > 0) { missing = sol.rows.find((r) => key(r) === k); break; } }
      let msg = `Same row count (${user.rows.length}) but the rows differ — e.g. yours has ${fmtRowInline(extra)}`;
      if (missing) msg += ` where expected has ${fmtRowInline(missing)}`;
      return { pass: false, msg: msg + "." };
    }
    return { pass: true, msg: `Matches expected — ${user.rows.length} rows (compared ignoring row order; add ORDER BY to control it).` };
  }

  function userHasOrderBy(sqlText) {
    try { return parseSql(sqlText).select.orderBy.length > 0; } catch (e) { return false; }
  }

  /* ================================== UI ================================== */
  function escHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  }
  function h(doc, tag, cls, html) {
    const n = doc.createElement(tag);
    if (cls) n.className = cls;
    if (html != null) n.innerHTML = html;
    return n;
  }
  function fmtCell(v) {
    if (v === null || v === undefined) return '<span style="opacity:.45;font-style:italic">NULL</span>';
    if (typeof v === "number") return escHtml(fmtNum(v));
    if (typeof v === "object") return escHtml(JSON.stringify(v));
    return escHtml(String(v));
  }
  function gridEl(doc, res, capRows) {
    const cap = capRows || 50;
    const wrap = h(doc, "div", "eng-grid");
    const shown = res.rows.slice(0, cap);
    let html = "<table><thead><tr>" + res.columns.map((c) => "<th>" + escHtml(c) + "</th>").join("") + "</tr></thead><tbody>";
    for (const r of shown) html += "<tr>" + r.map((v) => "<td>" + fmtCell(v) + "</td>").join("") + "</tr>";
    html += "</tbody></table>";
    wrap.innerHTML = html;
    return wrap;
  }
  function firstNonNull(rows, col) {
    for (const r of rows) { const v = r[col]; if (v !== null && v !== undefined) return v; }
    return null;
  }
  function schemaEl(doc, datasets, tables) {
    const det = doc.createElement("details");
    det.style.cssText = "border:1px solid var(--line);border-radius:10px;background:var(--paper2);padding:.45rem .9rem;margin-bottom:.8rem;font-family:var(--mono);font-size:.74rem;color:var(--ink)";
    const sum = doc.createElement("summary");
    sum.textContent = "Schema — " + datasets.length + " table" + (datasets.length === 1 ? "" : "s");
    sum.style.cssText = "cursor:pointer;color:var(--ink2)";
    det.appendChild(sum);
    for (const name of datasets) {
      const rows = tables[name] || tables[String(name).toLowerCase()] || [];
      const cols = colsOf(rows).map((c) => {
        const v = firstNonNull(rows, c);
        if (Array.isArray(v)) return c + " []";
        if (v && typeof v === "object") return c + " {" + Object.keys(v).join(",") + "}";
        return c;
      });
      const d = h(doc, "div", null,
        '<span style="color:var(--accent);font-weight:700">' + escHtml(name) + "</span> " +
        '<span style="opacity:.6">(' + rows.length + " rows)</span><br>&nbsp;&nbsp;" + escHtml(cols.join(", ")));
      d.style.margin = ".45rem 0";
      det.appendChild(d);
    }
    return det;
  }

  /**
   * Mount the SQL lab into `el`. config: { datasets, starterQuery, solutionQuery, hint }.
   * All DOM work lives here — loading this file never touches `document`.
   */
  function mount(el, config, ctx) {
    const doc = el.ownerDocument;
    config = config || {};
    const tables = (ctx && ctx.NIMBUS) || W.NIMBUS || {};

    const root = h(doc, "div");

    const head = h(doc, "div");
    head.style.cssText = "display:flex;align-items:center;gap:.7rem;flex-wrap:wrap;margin-bottom:.7rem";
    head.appendChild(h(doc, "span", "eng-badge real", "real execution"));
    const note = h(doc, "span", "eng-note", "Course SQL engine — a strict, honest subset. Not a full database.");
    note.style.marginTop = "0";
    head.appendChild(note);
    root.appendChild(head);

    if (config.datasets && config.datasets.length) root.appendChild(schemaEl(doc, config.datasets, tables));

    const area = h(doc, "div", "eng-area");
    const ta = doc.createElement("textarea");
    ta.value = config.starterQuery || "";
    ta.spellcheck = false;
    area.appendChild(ta);
    root.appendChild(area);

    const btns = h(doc, "div");
    btns.style.cssText = "display:flex;gap:.6rem;margin-top:.7rem;align-items:center;flex-wrap:wrap";
    const runBtn = h(doc, "button", "eng-btn", "▶ Run");
    runBtn.type = "button";
    btns.appendChild(runBtn);
    let expBtn = null;
    if (config.solutionQuery) {
      expBtn = h(doc, "button", "eng-btn ghost", "show expected");
      expBtn.type = "button";
      btns.appendChild(expBtn);
    }
    const kbd = h(doc, "span", "eng-note", "Ctrl+Enter runs");
    kbd.style.marginTop = "0";
    btns.appendChild(kbd);
    root.appendChild(btns);

    const out = h(doc, "div");
    const expOut = h(doc, "div");
    root.append(out, expOut);

    let failedOnce = false;
    let solCache;
    function solution() {
      if (solCache === undefined) {
        try { solCache = run(config.solutionQuery, tables); }
        catch (e) { solCache = null; solCache = { err: e }; }
      }
      return solCache;
    }
    function appendHint(container) {
      if (config.hint && failedOnce) {
        const hd = h(doc, "div", "eng-note", "Hint: " + config.hint);
        container.appendChild(hd);
      }
    }

    function doRun() {
      out.innerHTML = "";
      let res;
      try { res = run(ta.value, tables); }
      catch (err) {
        failedOnce = true;
        const msg = err && err.name === "SqlError" ? err.message : "Engine hiccup: " + (err && err.message ? err.message : String(err));
        out.appendChild(h(doc, "div", "eng-err", "<strong>SQL error.</strong> " + escHtml(msg)));
        appendHint(out);
        return;
      }
      out.appendChild(gridEl(doc, res, 50));
      const noteTxt = res.rows.length > 50
        ? "showing first 50 of " + res.rows.length + " rows"
        : res.rows.length + " row" + (res.rows.length === 1 ? "" : "s");
      out.appendChild(h(doc, "div", "eng-note", noteTxt));
      if (!config.solutionQuery) return;
      const sol = solution();
      if (sol && sol.err) {
        out.appendChild(h(doc, "div", "eng-note", "expected-result check unavailable (solution query failed to run)"));
        return;
      }
      const verdict = compareResults(res, sol, userHasOrderBy(ta.value));
      if (verdict.pass) {
        out.appendChild(h(doc, "div", "eng-pass", "<strong>✓</strong> " + escHtml(verdict.msg)));
      } else {
        failedOnce = true;
        out.appendChild(h(doc, "div", "eng-err", "<strong>Not matching yet.</strong> " + escHtml(verdict.msg)));
        appendHint(out);
      }
    }

    let expShown = false;
    if (expBtn) {
      expBtn.onclick = () => {
        expShown = !expShown;
        expOut.innerHTML = "";
        expBtn.textContent = expShown ? "hide expected" : "show expected";
        if (!expShown) return;
        const sol = solution();
        if (sol && sol.err) {
          expOut.appendChild(h(doc, "div", "eng-err", "Solution query failed: " + escHtml(sol.err.message)));
          return;
        }
        expOut.appendChild(h(doc, "div", "eng-note", "Expected result — " + sol.rows.length + " row" + (sol.rows.length === 1 ? "" : "s") + ":"));
        expOut.appendChild(gridEl(doc, sol, 50));
      };
    }
    runBtn.onclick = doRun;
    const onKey = (e) => {
      if ((e.ctrlKey || e.metaKey) && e.key === "Enter") { e.preventDefault(); doRun(); }
    };
    ta.addEventListener("keydown", onKey);

    el.appendChild(root);
    return {
      destroy() {
        ta.removeEventListener("keydown", onKey);
        if (root.parentNode) root.parentNode.removeChild(root);
      },
    };
  }

  W.Engines.sql = { mount, run, SqlError };
})();
