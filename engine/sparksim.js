/* ============================================================================
   engine/sparksim.js — SparkSim v1
   A teaching-subset DataFrame engine over the NimbusMart seed data.
   Results are REAL (computed in-browser); partition counts / task
   parallelism / timings are MODELED and badged `simulated`.

   Plain browser script: no import/export, no top-level await.
   Node-safe: loading with `global.window = { Engines: {} }` never
   touches `document` — all DOM work lives inside mount().

   Exposes: window.Engines.sparksim = { mount, parse, execute, SimError }
   ============================================================================ */
(function () {
  "use strict";

  var win = (typeof window !== "undefined") ? window
          : (typeof globalThis !== "undefined" ? globalThis : {});
  win.Engines = win.Engines || {};

  /* ==================== SimError — every user-facing failure ============== */
  function SimError(message) {
    var e = Error.call(this, message);
    this.name = "SimError";
    this.message = message;
    this.stack = e.stack;
  }
  SimError.prototype = Object.create(Error.prototype);
  SimError.prototype.constructor = SimError;

  /* Honest one-liners for real-Spark things students will inevitably try. */
  var HONEST_NOTES = {
    selectExpr: "it parses SQL fragments into the same column expressions — use .select(F.col(\"x\"), ...) here and you lose nothing but the string parsing.",
    sql: "spark.sql() feeds SQL text through the exact same Catalyst optimizer your DataFrame chain uses — the plans come out identical.",
    repartition: "it forces a full shuffle (an Exchange) to redistribute rows into N partitions; SparkSim only models partitions, so there is nothing real to redistribute.",
    coalesce: "it merges partitions down WITHOUT a shuffle (a narrow dependency) — cheaper than repartition, but it can leave data skewed.",
    when: "F.when(...).otherwise(...) builds a CASE WHEN expression evaluated per-row inside the JVM — no shuffle, just a projection.",
    otherwise: "F.when(...).otherwise(...) builds a CASE WHEN expression evaluated per-row inside the JVM — no shuffle, just a projection.",
    pivot: "pivot turns distinct row values into new columns — under the hood it is a groupBy aggregate that must first discover the distinct values (an extra job).",
    explode: "explode is a generator: one output row per array element, so it changes row counts — not a scalar expression.",
    udf: "a Python UDF ships every row across the JVM↔Python boundary (the 'UDF tax', module E5) — built-in F.* functions stay in the JVM and vectorize.",
    cast: "casting changes a column's type in the plan; SparkSim's seed data already carries its final types.",
    persist: "persist/cache mark a plan for reuse so later actions skip recomputation — only .cache() is modeled here.",
    unpersist: "persist/cache mark a plan for reuse so later actions skip recomputation — only .cache() is modeled here.",
    sort: "it is an alias for .orderBy — call .orderBy(...) here.",
    na: "null-handling helpers rewrite to filters and projections — express them with .filter(F.col(\"x\").isNotNull()) here.",
    fillna: "fillna rewrites to a projection with coalesce(col, default) per column — express the intent with .withColumn.",
    dropna: "dropna rewrites to a filter on isNotNull across columns — write the filter explicitly here.",
    toPandas: "it collects the WHOLE distributed result into one pandas DataFrame on the driver — a classic out-of-memory trap.",
    rdd: "dropping to the RDD API leaves Catalyst blind — the optimizer can no longer rewrite your plan.",
    crossJoin: "a cross join pairs every row with every row (|L|×|R|) — Spark makes you ask for it explicitly because it is almost always a mistake.",
    sample: "sample filters rows with a per-partition random coin flip — no shuffle, but results vary run to run unless you fix the seed.",
    checkpoint: "checkpoint truncates the lineage by materializing to storage — used when plans grow too deep to replan cheaply."
  };
  var GENERIC_NOTE = "most 'missing' methods compile down to the same handful of plan nodes you can see here — project, filter, aggregate, join, exchange.";

  function unsupported(what, key) {
    var note = HONEST_NOTES[key !== undefined ? key : what] || GENERIC_NOTE;
    throw new SimError("SparkSim v1 doesn't implement " + what + " — here's the concept anyway: " + note);
  }

  /* ==================== Tokenizer ========================================= */
  function tokenize(src) {
    var toks = [], i = 0, n = src.length, depth = 0;
    function isDigit(c) { return c >= "0" && c <= "9"; }
    function isIdStart(c) { return /[A-Za-z_]/.test(c); }
    function isId(c) { return /[A-Za-z0-9_]/.test(c); }
    while (i < n) {
      var c = src[i];
      if (c === "#") { while (i < n && src[i] !== "\n") i++; continue; }
      if (c === "\n") { if (depth === 0) toks.push({ t: "nl" }); i++; continue; }
      if (c === " " || c === "\t" || c === "\r" || c === ";") { i++; continue; }
      if (c === "\\" && src[i + 1] === "\n") { i += 2; continue; }
      if (c === '"' || c === "'") {
        var q = c, s = "", j = i + 1;
        while (j < n && src[j] !== q) {
          if (src[j] === "\\" && j + 1 < n) {
            var esc = src[j + 1];
            s += (esc === "n" ? "\n" : esc === "t" ? "\t" : esc);
            j += 2;
          } else { s += src[j]; j++; }
        }
        if (j >= n) throw new SimError("Unclosed string starting at: " + src.slice(i, i + 20) + "…");
        toks.push({ t: "str", v: s }); i = j + 1; continue;
      }
      if (isDigit(c) || (c === "." && isDigit(src[i + 1] || ""))) {
        var j2 = i, sawDot = false;
        while (j2 < n && (isDigit(src[j2]) || (src[j2] === "." && !sawDot && isDigit(src[j2 + 1] || "")))) {
          if (src[j2] === ".") sawDot = true;
          j2++;
        }
        toks.push({ t: "num", v: parseFloat(src.slice(i, j2)) }); i = j2; continue;
      }
      if (isIdStart(c)) {
        var j3 = i;
        while (j3 < n && isId(src[j3])) j3++;
        toks.push({ t: "name", v: src.slice(i, j3) }); i = j3; continue;
      }
      var two = src.slice(i, i + 2);
      if (two === "==" || two === "!=" || two === ">=" || two === "<=") {
        toks.push({ t: "op", v: two }); i += 2; continue;
      }
      if ("=<>+-*/&|.,()[]".indexOf(c) >= 0) {
        if (c === "(" || c === "[") depth++;
        if (c === ")" || c === "]") depth = Math.max(0, depth - 1);
        toks.push({ t: "op", v: c }); i++; continue;
      }
      if (c === "~") throw new SimError("SparkSim v1 doesn't implement ~ (negation) — here's the concept anyway: it flips a boolean column; invert the comparison instead (== becomes !=, isNull becomes isNotNull).");
      if (c === "!") throw new SimError("Unexpected '!' — Python comparisons use != (and Spark conditions combine with & and |).");
      throw new SimError("SparkSim couldn't read the character '" + c + "' near: " + src.slice(Math.max(0, i - 12), i + 12).trim());
    }
    toks.push({ t: "nl" });
    return toks;
  }

  /* Split token stream into statements at depth-0 newlines. */
  function splitStatements(toks) {
    var stmts = [], cur = [];
    for (var i = 0; i < toks.length; i++) {
      if (toks[i].t === "nl") { if (cur.length) stmts.push(cur); cur = []; }
      else cur.push(toks[i]);
    }
    if (cur.length) stmts.push(cur);
    return stmts;
  }

  /* ==================== Expression parser (Python subset) ================= */
  /* AST kinds: name, num, str, const, list, attr{obj,name},
     mcall{obj,method,args}, bin{op,l,r}, neg{e}.
     Call args: { kw: string|null, e: ast }                                   */
  function ExprParser(toks) { this.toks = toks; this.i = 0; }
  ExprParser.prototype = {
    peek: function () { return this.toks[this.i] || null; },
    next: function () { return this.toks[this.i++] || null; },
    isOp: function (v) { var t = this.peek(); return !!t && t.t === "op" && t.v === v; },
    eatOp: function (v) { if (this.isOp(v)) { this.i++; return true; } return false; },
    expectOp: function (v, ctx) {
      if (!this.eatOp(v)) {
        var t = this.peek();
        throw new SimError("Expected '" + v + "' " + (ctx || "") + " but found " +
          (t ? "'" + (t.v !== undefined ? t.v : t.t) + "'" : "end of line") + ".");
      }
    },
    parse: function () {
      var e = this.parseOr();
      var t = this.peek();
      if (t) throw new SimError("SparkSim couldn't parse past '" + (t.v !== undefined ? t.v : t.t) + "' — check for a missing '.' or ','.");
      return e;
    },
    parseOr: function () {
      var e = this.parseAnd();
      while (this.isOp("|")) { this.next(); e = { k: "bin", op: "|", l: e, r: this.parseAnd() }; }
      return e;
    },
    parseAnd: function () {
      var e = this.parseCmp();
      while (this.isOp("&")) { this.next(); e = { k: "bin", op: "&", l: e, r: this.parseCmp() }; }
      return e;
    },
    parseCmp: function () {
      var e = this.parseAdd(), t = this.peek();
      if (t && t.t === "op" && ["==", "!=", ">=", "<=", ">", "<"].indexOf(t.v) >= 0) {
        this.next();
        e = { k: "bin", op: t.v, l: e, r: this.parseAdd() };
      }
      return e;
    },
    parseAdd: function () {
      var e = this.parseMul();
      while (this.isOp("+") || this.isOp("-")) {
        var op = this.next().v;
        e = { k: "bin", op: op, l: e, r: this.parseMul() };
      }
      return e;
    },
    parseMul: function () {
      var e = this.parseUnary();
      while (this.isOp("*") || this.isOp("/")) {
        var op = this.next().v;
        e = { k: "bin", op: op, l: e, r: this.parseUnary() };
      }
      return e;
    },
    parseUnary: function () {
      if (this.isOp("-")) { this.next(); return { k: "neg", e: this.parseUnary() }; }
      return this.parsePostfix();
    },
    parsePostfix: function () {
      var e = this.parsePrimary();
      for (;;) {
        if (this.isOp(".")) {
          this.next();
          var t = this.next();
          if (!t || t.t !== "name") throw new SimError("Expected a method or attribute name after '.'.");
          if (this.isOp("(")) e = { k: "mcall", obj: e, method: t.v, args: this.parseArgs() };
          else e = { k: "attr", obj: e, name: t.v };
        } else if (this.isOp("(")) {
          e = { k: "mcall", obj: null, method: null, fn: e, args: this.parseArgs() };
        } else break;
      }
      return e;
    },
    parseArgs: function () {
      this.expectOp("(", "to open the call");
      var args = [];
      if (this.eatOp(")")) return args;
      for (;;) {
        var t = this.peek(), t2 = this.toks[this.i + 1];
        if (t && t.t === "name" && t2 && t2.t === "op" && t2.v === "=") {
          this.i += 2;
          args.push({ kw: t.v, e: this.parseOr() });
        } else {
          args.push({ kw: null, e: this.parseOr() });
        }
        if (this.eatOp(",")) continue;
        this.expectOp(")", "to close the call");
        break;
      }
      return args;
    },
    parsePrimary: function () {
      var t = this.next();
      if (!t) throw new SimError("Unexpected end of expression.");
      if (t.t === "num") return { k: "num", v: t.v };
      if (t.t === "str") return { k: "str", v: t.v };
      if (t.t === "name") {
        if (t.v === "True") return { k: "const", v: true };
        if (t.v === "False") return { k: "const", v: false };
        if (t.v === "None") return { k: "const", v: null };
        if (t.v === "lambda") unsupported("lambda expressions", "udf");
        return { k: "name", v: t.v };
      }
      if (t.t === "op" && t.v === "(") {
        var e = this.parseOr();
        this.expectOp(")", "to close the parenthesis");
        return e;
      }
      if (t.t === "op" && t.v === "[") {
        var items = [];
        if (!this.eatOp("]")) {
          for (;;) {
            items.push(this.parseOr());
            if (this.eatOp(",")) continue;
            this.expectOp("]", "to close the list");
            break;
          }
        }
        return { k: "list", items: items };
      }
      throw new SimError("SparkSim couldn't parse the token '" + (t.v !== undefined ? t.v : t.t) + "'.");
    }
  };

  /* ==================== Column-expression helpers ========================= */
  /* Col expr AST: {t:"col",name} {t:"lit",v} {t:"bin",op,l,r}
     {t:"nullchk",e,not} {t:"alias",e,name} {t:"sort",e,dir}                  */
  function litOf(v) { return { t: "lit", v: v }; }

  function exprStr(e) {
    switch (e.t) {
      case "col": return e.name;
      case "lit": return typeof e.v === "string" ? JSON.stringify(e.v) : String(e.v);
      case "bin":
        if (e.op === "&" || e.op === "|")
          return "(" + exprStr(e.l) + (e.op === "&" ? " AND " : " OR ") + exprStr(e.r) + ")";
        return exprStr(e.l) + " " + e.op + " " + exprStr(e.r);
      case "nullchk": return exprStr(e.e) + " IS " + (e.not ? "NOT " : "") + "NULL";
      case "alias": return exprStr(e.e) + " AS " + e.name;
      case "sort": return exprStr(e.e) + (e.dir < 0 ? " DESC" : " ASC");
      default: return "?";
    }
  }

  function colRefsOf(e, set) {
    set = set || {};
    if (!e || typeof e !== "object") return set;
    if (e.t === "col") { set[e.name] = true; return set; }
    if (e.l) colRefsOf(e.l, set);
    if (e.r) colRefsOf(e.r, set);
    if (e.e) colRefsOf(e.e, set);
    return set;
  }
  function refList(e) { return Object.keys(colRefsOf(e)); }

  /* ==================== Interpreter (builds the logical plan) ============ */
  /* Sim values are tagged: {__sim:"df"|"col"|"win"|"winfn"|"agg"|"ns"|
     "writer"|"grouped"|"method"|"none", ...}                                 */
  function isSim(v, tag) { return v && typeof v === "object" && v.__sim === tag; }
  function mkDf(plan, broadcast) { return { __sim: "df", plan: plan, broadcast: !!broadcast }; }
  function mkCol(e) { return { __sim: "col", e: e }; }

  function toColExpr(v, ctx) {
    if (isSim(v, "col")) return v.e;
    if (v === null || typeof v === "number" || typeof v === "string" || typeof v === "boolean") return litOf(v);
    throw new SimError("Can't use " + describeVal(v) + " inside a column expression" + (ctx ? " (" + ctx + ")" : "") + ".");
  }
  function describeVal(v) {
    if (isSim(v, "df")) return "a DataFrame";
    if (isSim(v, "win")) return "a window spec";
    if (isSim(v, "agg")) return "an aggregate expression";
    if (isSim(v, "winfn")) return "a window function";
    if (Array.isArray(v)) return "a list";
    return "the value " + JSON.stringify(v);
  }
  function needStr(v, what) {
    if (typeof v !== "string") throw new SimError(what + " needs a quoted string, got " + describeVal(v) + ".");
    return v;
  }

  function normSortKey(v, ctx) {
    if (typeof v === "string") return { e: { t: "col", name: v }, dir: 1 };
    if (isSim(v, "col")) {
      if (v.e.t === "sort") return { e: v.e.e, dir: v.e.dir };
      return { e: v.e, dir: 1 };
    }
    throw new SimError(ctx + " takes column names or F.col(...).asc()/.desc(), got " + describeVal(v) + ".");
  }

  function aggArgName(v, fn) {
    if (v === undefined) throw new SimError("F." + fn + "() needs a column argument.");
    if (typeof v === "string") return v;
    if (isSim(v, "col") && v.e.t === "col") return v.e.name;
    throw new SimError("F." + fn + "(...) takes a column name (or F.col(\"x\")), got " + describeVal(v) + ".");
  }

  /* --- interpreter state per parse -------------------------------------- */
  function InterpState(tables) {
    this.env = {};
    this.tables = tables || null;
    this.lazyLog = [];
    this.action = null;
    this.step = 0;
  }
  InterpState.prototype.log = function (kind, desc) {
    this.step += 1;
    this.lazyLog.push({ step: this.step, kind: kind, desc: desc });
  };
  InterpState.prototype.threshold = function () {
    return (typeof win.FRAUD_REVIEW_THRESHOLD === "number") ? win.FRAUD_REVIEW_THRESHOLD : 0.80;
  };

  function evalAst(ast, st) {
    switch (ast.k) {
      case "num": return ast.v;
      case "str": return ast.v;
      case "const": return ast.v;
      case "list": return ast.items.map(function (it) { return evalAst(it, st); });
      case "name": return lookupName(ast.v, st);
      case "attr": return resolveAttr(evalAst(ast.obj, st), ast.name, st);
      case "mcall": {
        if (ast.fn) {
          var f = evalAst(ast.fn, st);
          if (isSim(f, "method")) return applyMethod(f.self, f.name, evalArgs(ast.args, st), st);
          throw new SimError(describeVal(f) + " isn't callable — check the method name.");
        }
        var obj = evalAst(ast.obj, st);
        var m = resolveAttr(obj, ast.method, st);
        if (isSim(m, "method")) return applyMethod(m.self, m.name, evalArgs(ast.args, st), st);
        /* attr that resolved to a value but got called anyway, e.g. df.write() */
        throw new SimError("." + ast.method + " is a property, not a method — drop the parentheses.");
      }
      case "bin": return evalBin(ast, st);
      case "neg": {
        var v = evalAst(ast.e, st);
        if (typeof v === "number") return -v;
        if (isSim(v, "col")) return mkCol({ t: "bin", op: "-", l: litOf(0), r: v.e });
        throw new SimError("Can't negate " + describeVal(v) + ".");
      }
    }
    throw new SimError("SparkSim couldn't interpret that expression.");
  }

  function evalArgs(args, st) {
    var pos = [], kw = {};
    args.forEach(function (a) {
      var v = evalAst(a.e, st);
      if (a.kw) kw[a.kw] = v; else pos.push(v);
    });
    return { pos: pos, kw: kw };
  }

  function lookupName(name, st) {
    if (name === "spark") return { __sim: "ns", name: "spark" };
    if (name === "F") return { __sim: "ns", name: "F" };
    if (name === "Window") return { __sim: "ns", name: "Window" };
    if (name === "FRAUD_REVIEW_THRESHOLD") return st.threshold();
    if (Object.prototype.hasOwnProperty.call(st.env, name)) return st.env[name];
    throw new SimError("The name '" + name + "' isn't defined. SparkSim knows: spark, F, Window, FRAUD_REVIEW_THRESHOLD, and the variables you've assigned (" +
      (Object.keys(st.env).join(", ") || "none yet") + ").");
  }

  function evalBin(ast, st) {
    var l = evalAst(ast.l, st), r = evalAst(ast.r, st), op = ast.op;
    var lCol = isSim(l, "col"), rCol = isSim(r, "col");
    if (op === "&" || op === "|") {
      if (!lCol || !rCol)
        throw new SimError("Combine conditions with & / | between PARENTHESIZED column conditions, e.g. (F.col(\"a\") > 1) & (F.col(\"b\") == 2).");
      return mkCol({ t: "bin", op: op, l: l.e, r: r.e });
    }
    if (lCol || rCol) {
      return mkCol({ t: "bin", op: op, l: toColExpr(l), r: toColExpr(r) });
    }
    if (typeof l === "number" && typeof r === "number") {
      switch (op) {
        case "+": return l + r; case "-": return l - r;
        case "*": return l * r; case "/": return l / r;
        case "==": return l === r; case "!=": return l !== r;
        case ">": return l > r; case ">=": return l >= r;
        case "<": return l < r; case "<=": return l <= r;
      }
    }
    throw new SimError("Can't apply '" + op + "' between " + describeVal(l) + " and " + describeVal(r) + ".");
  }

  /* --- attribute resolution ---------------------------------------------- */
  var DF_METHODS = ["select", "filter", "where", "withColumn", "withColumnRenamed",
    "drop", "distinct", "dropDuplicates", "groupBy", "agg", "orderBy", "limit",
    "union", "join", "cache", "explain", "printSchema", "show", "count", "collect"];
  var F_FNS = ["col", "lit", "broadcast", "count", "sum", "avg", "min", "max",
    "countDistinct", "row_number", "rank"];
  var COL_METHODS = ["alias", "desc", "asc", "isNull", "isNotNull"];

  function method(self, name) { return { __sim: "method", self: self, name: name }; }

  function resolveAttr(obj, name, st) {
    if (isSim(obj, "ns")) {
      if (obj.name === "spark") {
        if (name === "read") return { __sim: "ns", name: "spark.read" };
        if (name === "sql") unsupported("spark.sql(...)", "sql");
        unsupported("spark." + name + "(...)", name);
      }
      if (obj.name === "spark.read") {
        if (name === "table") return method(obj, "read.table");
        unsupported("spark.read." + name + "(...) — only the embedded NimbusMart tables exist here; use spark.read.table(\"name\")", "__none__");
      }
      if (obj.name === "F") {
        if (F_FNS.indexOf(name) >= 0) return method(obj, "F." + name);
        unsupported("F." + name + "(...)", name);
      }
      if (obj.name === "Window") {
        if (name === "partitionBy" || name === "orderBy") return method({ __sim: "win", parts: [], orders: [] }, "win." + name);
        unsupported("Window." + name + "(...)", name);
      }
    }
    if (isSim(obj, "df")) {
      if (name === "write") return { __sim: "writer", df: obj, fmt: null, mode: null };
      if (DF_METHODS.indexOf(name) >= 0) {
        if (name === "agg") throw new SimError(".agg(...) only works right after .groupBy(...) — group first, then aggregate.");
        return method(obj, name);
      }
      unsupported("." + name + "()", name);
    }
    if (isSim(obj, "grouped")) {
      if (name === "agg") return method(obj, "grouped.agg");
      if (name === "count") throw new SimError("SparkSim v1 doesn't implement .groupBy(...).count() — use .agg(F.count(\"*\").alias(\"n\")) for the same aggregate with a named column.");
      unsupported(".groupBy(...)." + name + "()", name);
    }
    if (isSim(obj, "col")) {
      if (COL_METHODS.indexOf(name) >= 0) return method(obj, "col." + name);
      if (name === "over") throw new SimError(".over(w) applies to window functions like F.row_number(), F.rank(), F.sum(\"x\") — not to a plain column.");
      unsupported("." + name + "() on a column", name);
    }
    if (isSim(obj, "agg")) {
      if (name === "alias") return method(obj, "agg.alias");
      if (name === "over") return method(obj, "agg.over");
      unsupported("." + name + "() on an aggregate", name);
    }
    if (isSim(obj, "winfn")) {
      if (name === "over") return method(obj, "winfn.over");
      if (name === "alias") return method(obj, "winfn.alias");
      unsupported("." + name + "() on a window function", name);
    }
    if (isSim(obj, "win")) {
      if (name === "partitionBy" || name === "orderBy") return method(obj, "win." + name);
      unsupported("Window..." + name + "(...)", name);
    }
    if (isSim(obj, "writer")) {
      if (["format", "mode", "option", "saveAsTable"].indexOf(name) >= 0) return method(obj, "writer." + name);
      unsupported(".write." + name + "(...)", name);
    }
    throw new SimError("Can't read '." + name + "' from " + describeVal(obj) + ".");
  }

  /* --- schema inference --------------------------------------------------- */
  function tableCols(tables, t) {
    var rows = tables && tables[t];
    if (!rows || !rows.length) return [];
    return Object.keys(rows[0]);
  }
  function schemaOf(node, tables) {
    if (!tables) return null;
    switch (node.op) {
      case "scan": return tableCols(tables, node.table);
      case "filter": case "limit": case "sort": case "distinct": case "dropDuplicates":
        return schemaOf(node.child, tables);
      case "project": return node.items.map(function (i) { return i.name; });
      case "withColumn": case "window": {
        var s = schemaOf(node.child, tables);
        return s.indexOf(node.name) >= 0 ? s : s.concat([node.name]);
      }
      case "rename":
        return schemaOf(node.child, tables).map(function (c) { return c === node.from ? node.to : c; });
      case "drop":
        return schemaOf(node.child, tables).filter(function (c) { return node.cols.indexOf(c) < 0; });
      case "aggregate":
        return node.keys.concat(node.aggs.map(function (a) { return a.name; }));
      case "union": return schemaOf(node.left, tables);
      case "join": {
        var L = schemaOf(node.left, tables), R = schemaOf(node.right, tables);
        return L.concat(R.filter(function (c) { return node.on.indexOf(c) < 0; }));
      }
    }
    return [];
  }

  /* --- DataFrame method application --------------------------------------- */
  function applyMethod(self, name, args, st) {
    var pos = args.pos, kw = args.kw;

    /* ---- sources ---- */
    if (name === "read.table") {
      var t = needStr(pos[0], "spark.read.table(...)");
      if (st.tables && !st.tables[t])
        throw new SimError("No NimbusMart table called \"" + t + "\". Available: " + Object.keys(st.tables).join(", ") + ".");
      st.log("transformation", "read.table(\"" + t + "\")");
      return mkDf({ op: "scan", table: t });
    }

    /* ---- F namespace ---- */
    if (name.slice(0, 2) === "F.") {
      var fn = name.slice(2);
      if (fn === "col") return mkCol({ t: "col", name: needStr(pos[0], "F.col(...)") });
      if (fn === "lit") {
        var lv = pos[0];
        if (lv !== null && typeof lv !== "number" && typeof lv !== "string" && typeof lv !== "boolean")
          throw new SimError("F.lit(...) takes a number, string, boolean or None.");
        return mkCol(litOf(lv));
      }
      if (fn === "broadcast") {
        if (!isSim(pos[0], "df")) throw new SimError("F.broadcast(...) takes a DataFrame.");
        return mkDf(pos[0].plan, true);
      }
      if (fn === "row_number" || fn === "rank") {
        if (pos.length) throw new SimError("F." + fn + "() takes no arguments — the window supplies partition and order.");
        return { __sim: "winfn", fn: fn, arg: null, win: null };
      }
      if (fn === "count") {
        var ca = (pos[0] === "*" || pos[0] === undefined) ? "*" : aggArgName(pos[0], "count");
        return { __sim: "agg", fn: "count", arg: ca, alias: null };
      }
      /* sum / avg / min / max / countDistinct */
      return { __sim: "agg", fn: fn, arg: aggArgName(pos[0], fn), alias: null };
    }

    /* ---- column methods ---- */
    if (name.slice(0, 4) === "col.") {
      var ce = self.e, cm = name.slice(4);
      if (cm === "alias") return mkCol({ t: "alias", e: ce, name: needStr(pos[0], ".alias(...)") });
      if (cm === "desc") return mkCol({ t: "sort", e: ce, dir: -1 });
      if (cm === "asc") return mkCol({ t: "sort", e: ce, dir: 1 });
      if (cm === "isNull") return mkCol({ t: "nullchk", e: ce, not: false });
      if (cm === "isNotNull") return mkCol({ t: "nullchk", e: ce, not: true });
    }

    /* ---- aggregate / window-fn methods ---- */
    if (name === "agg.alias") { return { __sim: "agg", fn: self.fn, arg: self.arg, alias: needStr(pos[0], ".alias(...)") }; }
    if (name === "agg.over") {
      if (self.fn !== "sum")
        unsupported("F." + self.fn + "(...).over(...) — SparkSim v1 windows support row_number, rank and sum; the others behave analogously", "__none__");
      if (!isSim(pos[0], "win")) throw new SimError(".over(...) needs a Window spec (w = Window.partitionBy(...).orderBy(...)).");
      return { __sim: "winfn", fn: "sum", arg: self.arg, win: pos[0] };
    }
    if (name === "winfn.over") {
      if (!isSim(pos[0], "win")) throw new SimError(".over(...) needs a Window spec (w = Window.partitionBy(...).orderBy(...)).");
      return { __sim: "winfn", fn: self.fn, arg: self.arg, win: pos[0] };
    }
    if (name === "winfn.alias") {
      throw new SimError("Name a window column via .withColumn(\"name\", F." + self.fn + "().over(w)) — the withColumn provides the name.");
    }

    /* ---- Window spec ---- */
    if (name === "win.partitionBy" || name === "win.orderBy") {
      var w2 = { __sim: "win", parts: self.parts.slice(), orders: self.orders.slice() };
      if (name === "win.partitionBy") {
        pos.forEach(function (p) {
          if (typeof p === "string") w2.parts.push(p);
          else if (isSim(p, "col") && p.e.t === "col") w2.parts.push(p.e.name);
          else throw new SimError("Window.partitionBy(...) takes column names.");
        });
      } else {
        pos.forEach(function (p) { w2.orders.push(normSortKey(p, "Window.orderBy(...)")); });
      }
      return w2;
    }

    /* ---- writer chain ---- */
    if (name.slice(0, 7) === "writer.") {
      var wm = name.slice(7);
      if (wm === "format") return { __sim: "writer", df: self.df, fmt: needStr(pos[0], ".format(...)"), mode: self.mode };
      if (wm === "mode") return { __sim: "writer", df: self.df, fmt: self.fmt, mode: needStr(pos[0], ".mode(...)") };
      if (wm === "option") return self; /* tolerated, ignored */
      if (wm === "saveAsTable") {
        var tbl = needStr(pos[0], ".saveAsTable(...)");
        var desc = "write" + (self.fmt ? ".format(\"" + self.fmt + "\")" : "") +
          (self.mode ? ".mode(\"" + self.mode + "\")" : "") + ".saveAsTable(\"" + tbl + "\")";
        recordAction(st, "write", self.df.plan, { table: tbl, desc: desc });
        return { __sim: "none" };
      }
    }

    /* ---- grouped ---- */
    if (name === "grouped.agg") {
      var aggs = pos.map(function (a) {
        if (!isSim(a, "agg")) throw new SimError(".agg(...) takes aggregate expressions like F.count(\"*\").alias(\"n\") or F.sum(\"x\"), got " + describeVal(a) + ".");
        return { fn: a.fn, arg: a.arg, name: a.alias || (a.fn + "(" + a.arg + ")") };
      });
      if (!aggs.length) throw new SimError(".agg(...) needs at least one aggregate expression.");
      var gplan = { op: "aggregate", child: self.df.plan, keys: self.keys, aggs: aggs };
      st.log("transformation", "groupBy(" + self.keys.join(", ") + ").agg(" +
        aggs.map(function (a) { return a.fn + "(" + a.arg + ") AS " + a.name; }).join(", ") + ")");
      return mkDf(gplan);
    }

    /* ---- DataFrame methods ---- */
    if (!isSim(self, "df")) throw new SimError("." + name + "(...) isn't available on " + describeVal(self) + ".");
    var df = self, plan = df.plan;

    switch (name) {
      case "select": {
        if (!pos.length) throw new SimError(".select(...) needs at least one column.");
        var items = pos.map(function (a) {
          if (typeof a === "string") {
            if (a === "*") throw new SimError("SparkSim v1 doesn't implement select(\"*\") — here's the concept anyway: '*' expands to every input column at analysis time; name the columns you actually need (projection pruning will thank you).");
            return { expr: { t: "col", name: a }, name: a };
          }
          if (isSim(a, "col")) {
            var e = a.e;
            if (e.t === "alias") return { expr: e.e, name: e.name };
            if (e.t === "col") return { expr: e, name: e.name };
            if (e.t === "sort") throw new SimError(".desc()/.asc() belong in orderBy, not select.");
            return { expr: e, name: exprStr(e) };
          }
          if (isSim(a, "agg")) throw new SimError("Aggregates go inside .groupBy(...).agg(...), not .select(...).");
          throw new SimError(".select(...) takes column names or F.col(...) expressions, got " + describeVal(a) + ".");
        });
        st.log("transformation", "select(" + items.map(function (i) { return i.name; }).join(", ") + ")");
        return mkDf({ op: "project", child: plan, items: items });
      }
      case "filter": case "where": {
        var cond = pos[0];
        if (!isSim(cond, "col")) throw new SimError(".filter(...) needs a column condition, e.g. F.col(\"status\") == \"delivered\", got " + describeVal(cond) + ".");
        if (cond.e.t === "sort") throw new SimError(".desc()/.asc() are sort markers — they don't belong in a filter condition.");
        st.log("transformation", "filter(" + exprStr(cond.e) + ")");
        return mkDf({ op: "filter", child: plan, cond: cond.e });
      }
      case "withColumn": {
        var wname = needStr(pos[0], ".withColumn(name, ...)");
        var wval = pos[1];
        if (isSim(wval, "winfn")) {
          if (!wval.win) throw new SimError("F." + wval.fn + "() needs .over(w) — define w = Window.partitionBy(...).orderBy(...).");
          if ((wval.fn === "row_number" || wval.fn === "rank") && !wval.win.orders.length)
            throw new SimError("F." + wval.fn + "() needs an ORDERED window — add .orderBy(...) to the Window spec.");
          st.log("transformation", "withColumn(" + wname + ", " + wval.fn + "() OVER (partitionBy " +
            wval.win.parts.join(", ") + (wval.win.orders.length ? " orderBy " + wval.win.orders.map(function (o) { return exprStr(o.e) + (o.dir < 0 ? " DESC" : ""); }).join(", ") : "") + "))");
          return mkDf({ op: "window", child: plan, name: wname, fn: wval.fn, arg: wval.arg, win: { parts: wval.win.parts, orders: wval.win.orders } });
        }
        if (isSim(wval, "agg")) throw new SimError("Plain aggregates go in .groupBy(...).agg(...); to aggregate over a window use F.sum(\"x\").over(w).");
        var wexpr = toColExpr(wval, ".withColumn value");
        st.log("transformation", "withColumn(" + wname + ", " + exprStr(wexpr) + ")");
        return mkDf({ op: "withColumn", child: plan, name: wname, expr: wexpr });
      }
      case "withColumnRenamed": {
        var from = needStr(pos[0], ".withColumnRenamed(from, to)"), to = needStr(pos[1], ".withColumnRenamed(from, to)");
        st.log("transformation", "withColumnRenamed(" + from + " → " + to + ")");
        return mkDf({ op: "rename", child: plan, from: from, to: to });
      }
      case "drop": {
        var dcols = pos.map(function (p) {
          if (typeof p === "string") return p;
          if (isSim(p, "col") && p.e.t === "col") return p.e.name;
          throw new SimError(".drop(...) takes column names.");
        });
        st.log("transformation", "drop(" + dcols.join(", ") + ")");
        return mkDf({ op: "drop", child: plan, cols: dcols });
      }
      case "distinct":
        st.log("transformation", "distinct()");
        return mkDf({ op: "distinct", child: plan });
      case "dropDuplicates": {
        var subset = null;
        if (pos.length) {
          var lst = Array.isArray(pos[0]) ? pos[0] : pos;
          subset = lst.map(function (p) { return needStr(p, ".dropDuplicates([...])"); });
        }
        st.log("transformation", "dropDuplicates(" + (subset ? "[" + subset.join(", ") + "]" : "") + ")");
        return mkDf({ op: "dropDuplicates", child: plan, cols: subset });
      }
      case "groupBy": {
        var keys = pos.map(function (p) {
          if (typeof p === "string") return p;
          if (isSim(p, "col") && p.e.t === "col") return p.e.name;
          throw new SimError(".groupBy(...) takes column names.");
        });
        if (!keys.length) throw new SimError(".groupBy(...) needs at least one key column (global aggregates aren't in SparkSim v1 — .count() covers the common case).");
        return { __sim: "grouped", df: df, keys: keys };
      }
      case "orderBy": {
        var okeys = pos.map(function (p) { return normSortKey(p, ".orderBy(...)"); });
        if (!okeys.length) throw new SimError(".orderBy(...) needs at least one sort key.");
        st.log("transformation", "orderBy(" + okeys.map(function (o) { return exprStr(o.e) + (o.dir < 0 ? " DESC" : ""); }).join(", ") + ")");
        return mkDf({ op: "sort", child: plan, keys: okeys });
      }
      case "limit": {
        var ln = pos[0];
        if (typeof ln !== "number") throw new SimError(".limit(n) takes a number.");
        st.log("transformation", "limit(" + ln + ")");
        return mkDf({ op: "limit", child: plan, n: Math.max(0, Math.floor(ln)) });
      }
      case "union": {
        if (!isSim(pos[0], "df")) throw new SimError(".union(...) takes another DataFrame.");
        if (st.tables) {
          var lu = schemaOf(plan, st.tables), ru = schemaOf(pos[0].plan, st.tables);
          var lset = lu.slice().sort().join("|"), rset = ru.slice().sort().join("|");
          if (lset !== rset)
            throw new SimError("union needs matching columns. Left has [" + lu.join(", ") + "], right has [" + ru.join(", ") + "] — select/rename them into the same shape first. (Real Spark unions by POSITION, an infamous silent-corruption trap.)");
        }
        st.log("transformation", "union(...)");
        return mkDf({ op: "union", left: plan, right: pos[0].plan });
      }
      case "join": {
        var other = pos[0];
        if (!isSim(other, "df")) throw new SimError(".join(...) needs a DataFrame as its first argument.");
        var on = kw.on !== undefined ? kw.on : pos[1];
        var how = kw.how !== undefined ? kw.how : (pos[2] !== undefined ? pos[2] : "inner");
        if (typeof on === "string") on = [on];
        if (!Array.isArray(on) || !on.length || !on.every(function (c) { return typeof c === "string"; })) {
          if (isSim(on, "col")) throw new SimError("SparkSim v1 joins on column-name equality only — use .join(df2, \"col\") or on=[\"col\"], not a column expression.");
          throw new SimError(".join(...) needs join keys: a column name or on=[\"col\", ...].");
        }
        how = needStr(how, "how=");
        if (how !== "inner" && how !== "left")
          unsupported("how=\"" + how + "\" joins — SparkSim v1 supports \"inner\" and \"left\"; right/outer/semi/anti reshape which side survives, same shuffle mechanics", "__none__");
        if (st.tables) {
          var Lj = schemaOf(plan, st.tables), Rj = schemaOf(other.plan, st.tables);
          on.forEach(function (c) {
            if (Lj.indexOf(c) < 0) throw new SimError("Join key '" + c + "' isn't in the left side (columns: " + Lj.join(", ") + ").");
            if (Rj.indexOf(c) < 0) throw new SimError("Join key '" + c + "' isn't in the right side (columns: " + Rj.join(", ") + ").");
          });
          var dup = Rj.filter(function (c) { return on.indexOf(c) < 0 && Lj.indexOf(c) >= 0; });
          if (dup.length)
            throw new SimError("Both sides carry non-key column(s) [" + dup.join(", ") + "] — drop or rename one side before joining. (Real Spark keeps both and errors later on the ambiguous reference; SparkSim v1 makes you resolve it up front.)");
        }
        st.log("transformation", "join(" + (other.broadcast ? "broadcast " : "") + "on [" + on.join(", ") + "], how=" + how + ")");
        return mkDf({ op: "join", left: plan, right: other.plan, on: on, how: how, broadcast: !!other.broadcast });
      }
      case "cache":
        st.log("transformation", "cache() — marks this plan for reuse; no job runs");
        return df;
      case "explain":
        st.log("transformation", "explain() — prints the plan; no job runs");
        return { __sim: "none" };
      case "printSchema":
        st.log("transformation", "printSchema() — schema comes from the plan; no job runs");
        return { __sim: "none" };
      case "show": {
        var sn = (typeof pos[0] === "number") ? Math.floor(pos[0]) : null;
        recordAction(st, "show", plan, { n: sn, desc: "show(" + (sn !== null ? sn : "") + ")" });
        return { __sim: "none" };
      }
      case "count":
        recordAction(st, "count", plan, { desc: "count()" });
        return { __sim: "none" };
      case "collect":
        recordAction(st, "collect", plan, { desc: "collect()" });
        return { __sim: "none" };
    }
    unsupported("." + name + "()", name);
  }

  function recordAction(st, name, plan, params) {
    if (st.action)
      throw new SimError("SparkSim runs exactly one action per cell — " + st.action.desc + " already fired; remove ." + name + "(...) or the earlier action.");
    st.action = { name: name, plan: plan, params: params, desc: params.desc };
    st.log("action", params.desc);
  }

  /* ==================== Program parse ===================================== */
  function parseProgram(codeText, tables) {
    if (typeof codeText !== "string" || !codeText.trim())
      throw new SimError("Nothing to run — write a DataFrame chain ending in an action like .show() or .count().");
    var st = new InterpState(tables);
    var stmts = splitStatements(tokenize(codeText));
    var infos = [], lastDf = null;
    stmts.forEach(function (toks) {
      var target = null, exprToks = toks;
      if (toks.length >= 2 && toks[0].t === "name" && toks[1].t === "op" && toks[1].v === "=") {
        target = toks[0].v;
        exprToks = toks.slice(2);
        if (!exprToks.length) throw new SimError("The assignment to '" + target + "' has no right-hand side.");
      }
      var ast = new ExprParser(exprToks).parse();
      var val = evalAst(ast, st);
      if (target) st.env[target] = val;
      if (isSim(val, "df")) lastDf = val;
      infos.push({ kind: target ? "assign" : "expr", target: target });
    });
    return {
      statements: infos,
      plan: st.action ? st.action.plan : (lastDf ? lastDf.plan : null),
      action: st.action,
      lazyLog: st.lazyLog
    };
  }

  /* ==================== Optimizer ========================================= */
  function cloneTree(n) {
    if (!n) return n;
    var c = {};
    for (var k in n) if (Object.prototype.hasOwnProperty.call(n, k)) c[k] = n[k];
    if (c.child) c.child = cloneTree(c.child);
    if (c.left) c.left = cloneTree(c.left);
    if (c.right) c.right = cloneTree(c.right);
    return c;
  }

  function pushdown(node, tables) {
    if (!node) return node;
    if (node.child) node.child = pushdown(node.child, tables);
    if (node.left) node.left = pushdown(node.left, tables);
    if (node.right) node.right = pushdown(node.right, tables);
    if (node.op !== "filter") return node;

    var c = node.child, refs = refList(node.cond);
    function within(schema) { return refs.every(function (r) { return schema.indexOf(r) >= 0; }); }

    if (c.op === "join" && tables) {
      var L = schemaOf(c.left, tables), R = schemaOf(c.right, tables);
      if (within(L)) {
        var fL = { op: "filter", cond: node.cond, child: c.left, pushed: true };
        c.left = pushdown(fL, tables);
        return c;
      }
      if (within(R) && c.how === "inner") {
        var fR = { op: "filter", cond: node.cond, child: c.right, pushed: true };
        c.right = pushdown(fR, tables);
        return c;
      }
    }
    if (c.op === "sort" || c.op === "limit" && false) { /* limit: never */ }
    if (c.op === "sort") {
      var fS = { op: "filter", cond: node.cond, child: c.child, pushed: true };
      c.child = pushdown(fS, tables);
      return c;
    }
    if (c.op === "aggregate" && refs.every(function (r) { return c.keys.indexOf(r) >= 0; })) {
      var fA = { op: "filter", cond: node.cond, child: c.child, pushed: true };
      c.child = pushdown(fA, tables);
      return c;
    }
    if (c.op === "withColumn" && refs.indexOf(c.name) < 0) {
      var fW = { op: "filter", cond: node.cond, child: c.child, pushed: node.pushed || false };
      /* only mark pushed if it actually crosses something interesting below */
      c.child = pushdown(fW, tables);
      return c;
    }
    /* NOTE: never pushed through op:"window" — filtering before a window
       function changes row_number/rank/sum results. */
    return node;
  }

  function pruneProjections(node, demand, tables) {
    /* demand: object-set of column names needed above this node */
    if (!tables) return;
    function set(list) { var s = {}; list.forEach(function (x) { s[x] = true; }); return s; }
    function keys(s) { return Object.keys(s); }
    switch (node.op) {
      case "scan": {
        var all = tableCols(tables, node.table);
        var kept = all.filter(function (c) { return demand[c]; });
        if (kept.length && kept.length < all.length) {
          node.prunedTo = kept;
          node.prunedCount = all.length - kept.length;
        }
        return;
      }
      case "filter": {
        var d = set(keys(demand)); refList(node.cond).forEach(function (r) { d[r] = true; });
        pruneProjections(node.child, d, tables); return;
      }
      case "project": {
        var dp = {};
        node.items.forEach(function (i) { refList(i.expr).forEach(function (r) { dp[r] = true; }); });
        pruneProjections(node.child, dp, tables); return;
      }
      case "withColumn": {
        var dw = set(keys(demand).filter(function (c) { return c !== node.name; }));
        refList(node.expr).forEach(function (r) { dw[r] = true; });
        pruneProjections(node.child, dw, tables); return;
      }
      case "window": {
        var dn = set(keys(demand).filter(function (c) { return c !== node.name; }));
        node.win.parts.forEach(function (p) { dn[p] = true; });
        node.win.orders.forEach(function (o) { refList(o.e).forEach(function (r) { dn[r] = true; }); });
        if (node.arg) dn[node.arg] = true;
        pruneProjections(node.child, dn, tables); return;
      }
      case "rename": {
        var dr = {};
        keys(demand).forEach(function (c) { dr[c === node.to ? node.from : c] = true; });
        pruneProjections(node.child, dr, tables); return;
      }
      case "drop": pruneProjections(node.child, demand, tables); return;
      case "distinct": {
        pruneProjections(node.child, set(schemaOf(node.child, tables)), tables); return;
      }
      case "dropDuplicates": {
        var dd = set(keys(demand));
        (node.cols || schemaOf(node.child, tables)).forEach(function (c) { dd[c] = true; });
        pruneProjections(node.child, dd, tables); return;
      }
      case "aggregate": {
        var da = {};
        node.keys.forEach(function (k) { da[k] = true; });
        node.aggs.forEach(function (a) { if (a.arg !== "*") da[a.arg] = true; });
        pruneProjections(node.child, da, tables); return;
      }
      case "sort": {
        var ds = set(keys(demand));
        node.keys.forEach(function (o) { refList(o.e).forEach(function (r) { ds[r] = true; }); });
        pruneProjections(node.child, ds, tables); return;
      }
      case "limit": pruneProjections(node.child, demand, tables); return;
      case "union": {
        pruneProjections(node.left, demand, tables);
        pruneProjections(node.right, demand, tables); return;
      }
      case "join": {
        var L2 = schemaOf(node.left, tables), R2 = schemaOf(node.right, tables);
        var dl = {}, drr = {};
        keys(demand).forEach(function (c) {
          if (L2.indexOf(c) >= 0) dl[c] = true;
          if (R2.indexOf(c) >= 0 && node.on.indexOf(c) < 0) drr[c] = true;
        });
        node.on.forEach(function (c) { dl[c] = true; drr[c] = true; });
        pruneProjections(node.left, dl, tables);
        pruneProjections(node.right, drr, tables); return;
      }
    }
  }

  function optimize(logical, tables) {
    var opt = pushdown(cloneTree(logical), tables);
    if (tables) {
      var rootDemand = {};
      (schemaOf(opt, tables) || []).forEach(function (c) { rootDemand[c] = true; });
      pruneProjections(opt, rootDemand, tables);
    }
    return opt;
  }

  /* ==================== Executor ========================================== */
  function truthy(v) { return v === true; }

  function cmpVals(a, b, dir) {
    var an = (a === null || a === undefined), bn = (b === null || b === undefined);
    if (an && bn) return 0;
    if (an) return dir < 0 ? 1 : -1;   /* nulls first asc, last desc */
    if (bn) return dir < 0 ? -1 : 1;
    var c;
    if (typeof a === "number" && typeof b === "number") c = a < b ? -1 : a > b ? 1 : 0;
    else { var as = String(a), bs = String(b); c = as < bs ? -1 : as > bs ? 1 : 0; }
    return c * dir;
  }

  function evalCol(e, row) {
    switch (e.t) {
      case "col": {
        if (!(e.name in row)) {
          throw new SimError("Column '" + e.name + "' isn't in this DataFrame. Columns here: " + Object.keys(row).join(", ") + ".");
        }
        var v = row[e.name];
        return v === undefined ? null : v;
      }
      case "lit": return e.v;
      case "alias": return evalCol(e.e, row);
      case "sort": throw new SimError(".desc()/.asc() are sort markers — they can't be evaluated as values.");
      case "nullchk": {
        var nv = evalCol(e.e, row);
        var isN = (nv === null || nv === undefined);
        return e.not ? !isN : isN;
      }
      case "bin": {
        var op = e.op;
        if (op === "&" || op === "|") {
          var lb = truthy(evalCol(e.l, row)), rb = truthy(evalCol(e.r, row));
          return op === "&" ? (lb && rb) : (lb || rb);
        }
        var a = evalCol(e.l, row), b = evalCol(e.r, row);
        var anull = (a === null || a === undefined), bnull = (b === null || b === undefined);
        if (["==", "!=", ">", ">=", "<", "<="].indexOf(op) >= 0) {
          if (anull || bnull) return false;           /* Spark: null comparisons are not true */
          if (typeof a === "number" && typeof b === "number") {
            switch (op) {
              case "==": return a === b; case "!=": return a !== b;
              case ">": return a > b; case ">=": return a >= b;
              case "<": return a < b; case "<=": return a <= b;
            }
          }
          var as2 = String(a), bs2 = String(b);
          switch (op) {
            case "==": return as2 === bs2; case "!=": return as2 !== bs2;
            case ">": return as2 > bs2; case ">=": return as2 >= bs2;
            case "<": return as2 < bs2; case "<=": return as2 <= bs2;
          }
        }
        if (anull || bnull) return null;              /* arithmetic with null → null */
        if (typeof a === "number" && typeof b === "number") {
          switch (op) {
            case "+": return a + b; case "-": return a - b;
            case "*": return a * b; case "/": return b === 0 ? null : a / b;
          }
        }
        if (op === "+" && (typeof a === "string" || typeof b === "string")) return String(a) + String(b);
        return null;
      }
    }
    throw new SimError("SparkSim couldn't evaluate that expression.");
  }

  function computeAgg(a, rows) {
    if (a.fn === "count" && a.arg === "*") return rows.length;
    var vals = [];
    rows.forEach(function (r) {
      var v = r[a.arg];
      if (v !== null && v !== undefined) vals.push(v);
    });
    switch (a.fn) {
      case "count": return vals.length;
      case "countDistinct": { var s = {}; vals.forEach(function (v) { s[String(v)] = true; }); return Object.keys(s).length; }
      case "sum": return vals.length ? vals.reduce(function (x, y) { return x + y; }, 0) : null;
      case "avg": return vals.length ? vals.reduce(function (x, y) { return x + y; }, 0) / vals.length : null;
      case "min": { if (!vals.length) return null; return vals.reduce(function (m, v) { return cmpVals(v, m, 1) < 0 ? v : m; }); }
      case "max": { if (!vals.length) return null; return vals.reduce(function (m, v) { return cmpVals(v, m, 1) > 0 ? v : m; }); }
    }
    throw new SimError("Unknown aggregate " + a.fn + ".");
  }

  function sortRows(rows, keys) {
    var idx = rows.map(function (r, i) { return { r: r, i: i } });
    idx.sort(function (x, y) {
      for (var k = 0; k < keys.length; k++) {
        var kk = keys[k];
        var c = cmpVals(evalCol(kk.e, x.r), evalCol(kk.e, y.r), kk.dir);
        if (c !== 0) return c;
      }
      return x.i - y.i;                                /* stable */
    });
    return idx.map(function (x) { return x.r; });
  }

  function execPlan(node, tables) {
    var out;
    switch (node.op) {
      case "scan": {
        var data = tables[node.table];
        if (!data) throw new SimError("No NimbusMart table called \"" + node.table + "\".");
        out = data.slice();
        break;
      }
      case "filter": {
        var rows = execPlan(node.child, tables);
        out = rows.filter(function (r) { return truthy(evalCol(node.cond, r)); });
        break;
      }
      case "project": {
        out = execPlan(node.child, tables).map(function (r) {
          var o = {};
          node.items.forEach(function (i) { o[i.name] = evalCol(i.expr, r); });
          return o;
        });
        break;
      }
      case "withColumn": {
        out = execPlan(node.child, tables).map(function (r) {
          var o = {};
          for (var k in r) o[k] = r[k];
          o[node.name] = evalCol(node.expr, r);
          return o;
        });
        break;
      }
      case "window": out = execWindow(node, tables); break;
      case "rename": {
        out = execPlan(node.child, tables).map(function (r) {
          var o = {};
          for (var k in r) o[k === node.from ? node.to : k] = r[k];
          return o;
        });
        break;
      }
      case "drop": {
        out = execPlan(node.child, tables).map(function (r) {
          var o = {};
          for (var k in r) if (node.cols.indexOf(k) < 0) o[k] = r[k];
          return o;
        });
        break;
      }
      case "distinct": case "dropDuplicates": {
        var drows = execPlan(node.child, tables);
        var keyCols = (node.op === "dropDuplicates" && node.cols) ? node.cols
          : (drows.length ? Object.keys(drows[0]) : []);
        var seen = {}, kept = [];
        drows.forEach(function (r) {
          var key = JSON.stringify(keyCols.map(function (c) { return r[c] === undefined ? null : r[c]; }));
          if (!seen[key]) { seen[key] = true; kept.push(r); }   /* keeps first */
        });
        out = kept;
        break;
      }
      case "aggregate": {
        var arows = execPlan(node.child, tables);
        var groups = {}, order = [];
        arows.forEach(function (r) {
          var key = JSON.stringify(node.keys.map(function (k) { return r[k] === undefined ? null : r[k]; }));
          if (!groups[key]) { groups[key] = []; order.push(key); }
          groups[key].push(r);
        });
        out = order.map(function (key) {
          var grp = groups[key], o = {};
          node.keys.forEach(function (k) { o[k] = grp[0][k] === undefined ? null : grp[0][k]; });
          node.aggs.forEach(function (a) { o[a.name] = computeAgg(a, grp); });
          return o;
        });
        break;
      }
      case "sort": out = sortRows(execPlan(node.child, tables), node.keys); break;
      case "limit": out = execPlan(node.child, tables).slice(0, node.n); break;
      case "union": {
        var lrows = execPlan(node.left, tables), rrows = execPlan(node.right, tables);
        var cols = lrows.length ? Object.keys(lrows[0]) : schemaOf(node.left, tables);
        out = lrows.concat(rrows.map(function (r) {
          var o = {};
          cols.forEach(function (c) { o[c] = r[c] === undefined ? null : r[c]; });
          return o;
        }));
        break;
      }
      case "join": {
        var L = execPlan(node.left, tables), R = execPlan(node.right, tables);
        var on = node.on;
        var rCols = (R.length ? Object.keys(R[0]) : schemaOf(node.right, tables) || [])
          .filter(function (c) { return on.indexOf(c) < 0; });
        var map = {};
        R.forEach(function (r) {
          var vals = on.map(function (c) { return r[c]; });
          if (vals.some(function (v) { return v === null || v === undefined; })) return;  /* null keys never match */
          var key = JSON.stringify(vals);
          (map[key] = map[key] || []).push(r);
        });
        var joined = [];
        L.forEach(function (l) {
          var vals = on.map(function (c) { return l[c]; });
          var nullKey = vals.some(function (v) { return v === null || v === undefined; });
          var matches = nullKey ? null : map[JSON.stringify(vals)];
          if (matches && matches.length) {
            matches.forEach(function (rr) {
              var o = {};
              for (var k in l) o[k] = l[k];
              rCols.forEach(function (c) { o[c] = rr[c] === undefined ? null : rr[c]; });
              joined.push(o);
            });
          } else if (node.how === "left") {
            var o2 = {};
            for (var k2 in l) o2[k2] = l[k2];
            rCols.forEach(function (c) { o2[c] = null; });
            joined.push(o2);
          }
        });
        out = joined;
        break;
      }
      default: throw new SimError("SparkSim can't execute plan node '" + node.op + "'.");
    }
    node.outRows = out.length;
    return out;
  }

  function execWindow(node, tables) {
    var rows = execPlan(node.child, tables);
    var parts = {}, order = [];
    rows.forEach(function (r) {
      var key = JSON.stringify(node.win.parts.map(function (p) { return r[p] === undefined ? null : r[p]; }));
      if (!parts[key]) { parts[key] = []; order.push(key); }
      parts[key].push(r);
    });
    var out = [];
    order.forEach(function (key) {
      var grp = node.win.orders.length ? sortRows(parts[key], node.win.orders) : parts[key];
      function tie(i, j) {
        for (var k = 0; k < node.win.orders.length; k++) {
          var o = node.win.orders[k];
          if (cmpVals(evalCol(o.e, grp[i]), evalCol(o.e, grp[j]), 1) !== 0) return false;
        }
        return true;
      }
      var vals = new Array(grp.length);
      if (node.fn === "row_number") {
        for (var i = 0; i < grp.length; i++) vals[i] = i + 1;
      } else if (node.fn === "rank") {
        for (var i2 = 0; i2 < grp.length; i2++) {
          vals[i2] = (i2 > 0 && tie(i2, i2 - 1)) ? vals[i2 - 1] : i2 + 1;
        }
      } else if (node.fn === "sum") {
        if (!node.win.orders.length) {
          var tot = null;
          grp.forEach(function (r) {
            var v = r[node.arg];
            if (v !== null && v !== undefined) tot = (tot === null ? 0 : tot) + v;
          });
          for (var i3 = 0; i3 < grp.length; i3++) vals[i3] = tot;
        } else {
          /* Spark default frame with ORDER BY: RANGE unbounded preceding →
             current row — tied rows share the running total. */
          var running = null, i4 = 0;
          while (i4 < grp.length) {
            var j = i4;
            while (j + 1 < grp.length && tie(j + 1, i4)) j++;
            for (var g = i4; g <= j; g++) {
              var v2 = grp[g][node.arg];
              if (v2 !== null && v2 !== undefined) running = (running === null ? 0 : running) + v2;
            }
            for (var g2 = i4; g2 <= j; g2++) vals[g2] = running;
            i4 = j + 1;
          }
        }
      }
      grp.forEach(function (r, i5) {
        var o = {};
        for (var k in r) o[k] = r[k];
        o[node.name] = vals[i5];
        out.push(o);
      });
    });
    return out;
  }

  /* ==================== Physical plan ===================================== */
  function px(name, detail, children, opts) {
    var p = { name: name, detail: detail || "", children: children || [] };
    if (opts) for (var k in opts) p[k] = opts[k];
    return p;
  }
  function sortKeyStr(keys) {
    return keys.map(function (o) { return exprStr(o.e) + (o.dir < 0 ? " DESC" : " ASC"); }).join(", ");
  }

  function toPhysical(n) {
    switch (n.op) {
      case "scan": {
        var d = n.table + (n.prunedTo
          ? " [" + n.prunedTo.join(", ") + "] (pruned " + n.prunedCount + " cols)"
          : "");
        return px("Scan", d, [], { rows: n.outRows });
      }
      case "filter":
        return px("Filter", exprStr(n.cond), [toPhysical(n.child)], { rows: n.outRows, pushed: !!n.pushed });
      case "project":
        return px("Project", "[" + n.items.map(function (i) { return i.name; }).join(", ") + "]",
          [toPhysical(n.child)], { rows: n.outRows });
      case "withColumn":
        return px("Project", "[*, " + n.name + " := " + exprStr(n.expr) + "]", [toPhysical(n.child)], { rows: n.outRows });
      case "rename":
        return px("Project", "[* except " + n.from + " → " + n.to + "]", [toPhysical(n.child)], { rows: n.outRows });
      case "drop":
        return px("Project", "[* except " + n.cols.join(", ") + "]", [toPhysical(n.child)], { rows: n.outRows });
      case "window": {
        var wdet = n.fn + "(" + (n.arg || "") + ") over (partition by " + n.win.parts.join(", ") +
          (n.win.orders.length ? " order by " + sortKeyStr(n.win.orders) : "") + ")";
        return px("Window", wdet, [
          px("Sort", "[" + n.win.parts.join(", ") + (n.win.orders.length ? ", " + sortKeyStr(n.win.orders) : "") + "]", [
            px("Exchange", "hashpartitioning(" + n.win.parts.join(", ") + ")", [toPhysical(n.child)], { exchange: true })
          ])
        ], { rows: n.outRows });
      }
      case "aggregate": {
        var aggStr = n.aggs.map(function (a) { return a.fn + "(" + a.arg + ") AS " + a.name; }).join(", ");
        return px("HashAggregate", "(final) keys=[" + n.keys.join(", ") + "], [" + aggStr + "]", [
          px("Exchange", "hashpartitioning(" + n.keys.join(", ") + ")", [
            px("HashAggregate", "(partial) keys=[" + n.keys.join(", ") + "]", [toPhysical(n.child)])
          ], { exchange: true })
        ], { rows: n.outRows });
      }
      case "sort":
        return px("Sort", "[" + sortKeyStr(n.keys) + "]", [
          px("Exchange", "rangepartitioning(" + sortKeyStr(n.keys) + ")", [toPhysical(n.child)], { exchange: true })
        ], { rows: n.outRows });
      case "distinct": case "dropDuplicates": {
        var dk = n.op === "dropDuplicates" && n.cols ? n.cols.join(", ") : "all columns";
        return px("HashAggregate", "(final) dedup keys=[" + dk + "]", [
          px("Exchange", "hashpartitioning(" + dk + ")", [
            px("HashAggregate", "(partial) dedup keys=[" + dk + "]", [toPhysical(n.child)])
          ], { exchange: true })
        ], { rows: n.outRows });
      }
      case "limit": return px("Limit", String(n.n), [toPhysical(n.child)], { rows: n.outRows });
      case "union": return px("Union", "", [toPhysical(n.left), toPhysical(n.right)], { rows: n.outRows });
      case "join": {
        if (n.broadcast) {
          return px("BroadcastHashJoin", "[" + n.on.join(", ") + "], " + n.how, [
            toPhysical(n.left),
            px("BroadcastExchange", "", [toPhysical(n.right)], { broadcast: true })
          ], { rows: n.outRows });
        }
        return px("SortMergeJoin", "[" + n.on.join(", ") + "], " + n.how, [
          px("Sort", "[" + n.on.join(", ") + "]", [
            px("Exchange", "hashpartitioning(" + n.on.join(", ") + ")", [toPhysical(n.left)], { exchange: true })
          ]),
          px("Sort", "[" + n.on.join(", ") + "]", [
            px("Exchange", "hashpartitioning(" + n.on.join(", ") + ")", [toPhysical(n.right)], { exchange: true })
          ])
        ], { rows: n.outRows });
      }
    }
    throw new SimError("No physical mapping for '" + n.op + "'.");
  }

  /* ==================== Stage derivation ================================== */
  function buildStages(proot) {
    var stages = [], edges = [];
    function mk() {
      var s = { id: 0, ops: [], shuffleOut: false, broadcastOut: false, maxRows: 0, depsObjs: [] };
      stages.push(s); return s;
    }
    function walk(p, stage) {
      if (p.exchange) {
        var cs = mk();
        cs.shuffleOut = true;
        stage.depsObjs.push(cs);
        edges.push({ fromObj: cs, toObj: stage, type: "shuffle", label: p.detail });
        p.children.forEach(function (c) { walk(c, cs); });
        return;
      }
      if (p.broadcast) {
        var bs = mk();
        bs.broadcastOut = true;
        stage.depsObjs.push(bs);
        edges.push({ fromObj: bs, toObj: stage, type: "broadcast", label: "broadcast" });
        p.children.forEach(function (c) { walk(c, bs); });
        return;
      }
      stage.ops.push(p.name + (p.detail ? " " + p.detail : ""));
      if (typeof p.rows === "number") stage.maxRows = Math.max(stage.maxRows, p.rows);
      p.children.forEach(function (c) { walk(c, stage); });
    }
    var root = mk();
    walk(proot, root);
    /* number in execution order: dependencies first */
    var ordered = [];
    (function num(s) {
      if (s._done) return;
      s.depsObjs.forEach(num);
      s._done = true;
      ordered.push(s);
    })(root);
    ordered.forEach(function (s, i) { s.id = i + 1; });
    var result = ordered.map(function (s) {
      return {
        id: s.id,
        ops: s.ops.slice().reverse(),                        /* scan first */
        shuffleOut: s.shuffleOut,
        broadcastOut: s.broadcastOut,
        taskCount: Math.max(1, Math.min(4, Math.ceil((s.maxRows || 1) / 64))),
        deps: s.depsObjs.map(function (d) { return d.id; })
      };
    });
    var edgeList = edges.map(function (e) {
      return { from: e.fromObj.id, to: e.toObj.id, type: e.type, label: e.label };
    });
    return { stages: result, edges: edgeList };
  }

  /* ==================== Plan text rendering =============================== */
  function logicalLabel(n, optimized) {
    switch (n.op) {
      case "scan":
        return "Scan " + n.table + (optimized && n.prunedTo
          ? " [" + n.prunedTo.join(", ") + "] (pruned " + n.prunedCount + " cols)" : "");
      case "filter": return "Filter " + exprStr(n.cond);
      case "project": return "Project [" + n.items.map(function (i) { return i.name; }).join(", ") + "]";
      case "withColumn": return "Project [*, " + n.name + " := " + exprStr(n.expr) + "]";
      case "window": return "Window " + n.fn + "(" + (n.arg || "") + ") partitionBy [" + n.win.parts.join(", ") + "]" +
        (n.win.orders.length ? " orderBy [" + sortKeyStr(n.win.orders) + "]" : "");
      case "rename": return "Project [* except " + n.from + " → " + n.to + "]";
      case "drop": return "Project [* except " + n.cols.join(", ") + "]";
      case "distinct": return "Deduplicate [all columns]";
      case "dropDuplicates": return "Deduplicate [" + (n.cols ? n.cols.join(", ") : "all columns") + "]";
      case "aggregate": return "Aggregate keys=[" + n.keys.join(", ") + "], [" +
        n.aggs.map(function (a) { return a.fn + "(" + a.arg + ") AS " + a.name; }).join(", ") + "]";
      case "sort": return "Sort [" + sortKeyStr(n.keys) + "]";
      case "limit": return "Limit " + n.n;
      case "union": return "Union";
      case "join": return "Join " + n.how + " on [" + n.on.join(", ") + "]" + (n.broadcast ? " (broadcast hint)" : "");
    }
    return n.op;
  }
  function logicalLines(node, optimized) {
    var out = [];
    (function rec(n, depth) {
      var tag = null;
      if (optimized && n.op === "filter" && n.pushed) tag = "pushed";
      if (optimized && n.op === "scan" && n.prunedTo) tag = "pruned";
      out.push({ depth: depth, text: logicalLabel(n, optimized), tag: tag });
      if (n.child) rec(n.child, depth + 1);
      if (n.left) { rec(n.left, depth + 1); rec(n.right, depth + 1); }
    })(node, 0);
    return out;
  }
  function physicalLines(node) {
    var out = [];
    (function rec(p, depth) {
      var tag = p.exchange ? "exchange" : (p.broadcast ? "broadcast" : null);
      out.push({ depth: depth, text: p.name + (p.detail ? " " + p.detail : ""), tag: tag });
      p.children.forEach(function (c) { rec(c, depth + 1); });
    })(node, 0);
    return out;
  }
  function linesToText(lines) {
    return lines.map(function (l) {
      var pad = "";
      for (var i = 0; i < l.depth; i++) pad += "  ";
      return pad + (l.depth ? "+- " : "") + l.text;
    }).join("\n");
  }

  /* ==================== Public API: parse / execute ======================= */
  function apiParse(codeText, tables) {
    var p = parseProgram(codeText, tables || win.NIMBUS || null);
    return {
      statements: p.statements,
      plan: p.plan,
      action: p.action ? p.action.name : null,
      lazyLog: p.lazyLog
    };
  }

  function apiExecute(codeText, tables) {
    tables = tables || win.NIMBUS;
    if (!tables) throw new SimError("No seed data loaded — window.NIMBUS is missing.");
    var p = parseProgram(codeText, tables);
    if (!p.action)
      throw new SimError("Nothing executed! Every method you called is a lazy TRANSFORMATION — Spark only computes when an ACTION fires. Add .show(), .count() or .collect() to the chain.");
    var logical = p.action.plan;
    var optimized = optimize(logical, tables);
    var rows = execPlan(optimized, tables);
    var physical = toPhysical(optimized);
    var sd = buildStages(physical);
    var columns, gridRows;
    if (p.action.name === "count") {
      columns = ["count"];
      gridRows = [[rows.length]];
    } else {
      columns = schemaOf(optimized, tables) || (rows.length ? Object.keys(rows[0]) : []);
      gridRows = rows.map(function (r) {
        return columns.map(function (c) { return r[c] === undefined ? null : r[c]; });
      });
    }
    return {
      result: { columns: columns, rows: gridRows },
      action: p.action.name,
      actionDesc: p.action.desc,
      actionParams: p.action.params,
      lazyLog: p.lazyLog,
      stages: sd.stages,
      edges: sd.edges,
      plans: { logical: logical, optimized: optimized, physical: physical },
      planText: {
        logical: linesToText(logicalLines(logical, false)),
        optimized: linesToText(logicalLines(optimized, true)),
        physical: linesToText(physicalLines(physical))
      }
    };
  }

  /* ==================== mount(el, config, ctx) — all DOM below =========== */
  var STYLE_ID = "sparksim-style";
  var CSS = [
    ".ss-panel{border:1px solid var(--line);border-radius:10px;background:var(--card);padding:.9rem 1rem;margin-top:.9rem;}",
    ".ss-h{font-family:var(--mono);font-size:.66rem;letter-spacing:.1em;text-transform:uppercase;color:var(--ink2);display:flex;gap:.6rem;align-items:center;margin-bottom:.6rem;}",
    ".ss-ribbon{display:flex;flex-wrap:wrap;gap:.45rem;align-items:center;min-height:2rem;}",
    ".ss-chip{font-family:var(--mono);font-size:.68rem;padding:.28rem .6rem;border-radius:99px;border:1px solid var(--line);background:var(--paper2);color:var(--ink);opacity:0;transform:translateX(-10px);transition:opacity .28s ease,transform .28s ease;}",
    ".ss-chip.on{opacity:1;transform:none;}",
    ".ss-chip.action{background:var(--accent);border-color:var(--accent);color:var(--paper);font-weight:600;}",
    ".ss-arrow{color:var(--ink2);font-family:var(--mono);font-size:.7rem;opacity:0;transition:opacity .28s ease;}",
    ".ss-arrow.on{opacity:.7;}",
    ".ss-cols{display:flex;gap:.9rem;flex-wrap:wrap;align-items:flex-start;}",
    ".ss-plan{flex:1 1 230px;min-width:210px;}",
    ".ss-plan h5{font-family:var(--mono);font-size:.62rem;letter-spacing:.08em;text-transform:uppercase;color:var(--ink2);margin:0 0 .35rem;}",
    ".ss-plan pre{font-family:var(--mono);font-size:.68rem;line-height:1.55;background:var(--paper2);color:var(--ink);padding:.6rem .7rem;border-radius:8px;overflow-x:auto;margin:0;}",
    ".ss-push{color:var(--green);font-weight:700;animation:ss-pulse 1.6s ease 2;}",
    ".ss-prune{color:var(--green);font-weight:600;}",
    ".ss-exch{color:var(--rust);font-weight:600;}",
    ".ss-bcast{color:var(--gold);}",
    "@keyframes ss-pulse{0%{opacity:.25}50%{opacity:1}100%{opacity:1}}",
    ".ss-dagwrap{overflow-x:auto;}",
    ".ss-btnrow{display:flex;gap:.6rem;margin-top:.6rem;align-items:center;flex-wrap:wrap;}"
  ].join("\n");

  function h(tag, cls, html) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (html !== undefined) e.innerHTML = html;
    return e;
  }
  function escHtml(s) {
    return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }
  function svgEl(tag, attrs) {
    var e = document.createElementNS("http://www.w3.org/2000/svg", tag);
    for (var k in attrs) e.setAttribute(k, attrs[k]);
    return e;
  }
  function fmtCell(v) {
    if (v === null || v === undefined) return "null";
    if (typeof v === "number" && !Number.isInteger(v)) {
      var r = Math.round(v * 100) / 100;
      return String(r);
    }
    if (typeof v === "object") return JSON.stringify(v);
    return String(v);
  }

  function mount(el, config, ctx) {
    config = config || {};
    ctx = ctx || {};
    var tables = ctx.NIMBUS || win.NIMBUS;
    var timers = [];
    function later(fn, ms) { timers.push(setTimeout(fn, ms)); }
    function clearTimers() { timers.forEach(clearTimeout); timers = []; }

    if (!document.getElementById(STYLE_ID)) {
      var styleEl = document.createElement("style");
      styleEl.id = STYLE_ID;
      styleEl.textContent = CSS;
      document.head.appendChild(styleEl);
    }

    var root = h("div", "sparksim");
    el.appendChild(root);

    function panel(title, badgeHtml) {
      var p = h("div", "ss-panel");
      p.appendChild(h("div", "ss-h", escHtml(title) + (badgeHtml || "")));
      var body = h("div");
      p.appendChild(body);
      root.appendChild(p);
      return { panel: p, body: body };
    }

    /* ---- 1 · CODE ---- */
    var code = panel("1 · code — pyspark (sparksim subset)");
    var area = h("div", "eng-area");
    var ta = document.createElement("textarea");
    ta.value = config.starterCode || "";
    ta.spellcheck = false;
    var lineCount = (ta.value.match(/\n/g) || []).length + 1;
    ta.rows = Math.max(4, Math.min(18, lineCount + 1));
    area.appendChild(ta);
    code.body.appendChild(area);
    var btnRow = h("div", "ss-btnrow");
    var runBtn = h("button", "eng-btn", "Run ▸");
    btnRow.appendChild(runBtn);
    if (config.solutionCode) {
      var solBtn = h("button", "eng-btn ghost", "reveal solution");
      solBtn.onclick = function () {
        var ok = (typeof win.confirm === "function") ? win.confirm("Replace your code with the solution?") : true;
        if (ok) { ta.value = config.solutionCode; ta.rows = Math.max(4, Math.min(18, (config.solutionCode.match(/\n/g) || []).length + 2)); }
      };
      btnRow.appendChild(solBtn);
    }
    code.body.appendChild(btnRow);
    var errBox = h("div");
    code.body.appendChild(errBox);

    /* ---- 2 · LAZY RIBBON ---- */
    var lazy = panel("2 · lazy evaluation");
    var ribbon = h("div", "ss-ribbon");
    ribbon.innerHTML = "<span class='eng-note'>Run the code to watch transformations queue up…</span>";
    lazy.body.appendChild(ribbon);
    var caption = h("div", "eng-note");
    caption.style.display = "none";
    caption.textContent = "Transformations queued. Nothing computed until the action.";
    lazy.body.appendChild(caption);

    /* ---- 3 · DAG ---- */
    var dag = panel("3 · dag — job → stages → tasks",
      " <span class='eng-badge sim'>simulated</span>");
    dag.panel.style.display = "none";
    var dagWrap = h("div", "ss-dagwrap");
    dag.body.appendChild(dagWrap);
    if (config.dagNotes) dag.body.appendChild(h("div", "eng-note", config.dagNotes));

    /* ---- 4 · PLAN ---- */
    var planP = panel("4 · query plans — logical → optimized → physical");
    planP.panel.style.display = "none";
    var planCols = h("div", "ss-cols");
    planP.body.appendChild(planCols);

    /* ---- 5 · RESULT ---- */
    var res = panel("5 · result", " <span class='eng-badge real'>real execution</span>");
    res.panel.style.display = "none";
    var resBody = h("div");
    res.body.appendChild(resBody);

    /* honesty footer — always visible */
    root.appendChild(h("div", "eng-note",
      "Results are real (computed on the seed data in your browser). Partition counts, task parallelism and timings are modeled — <span class='eng-badge sim'>simulated</span>."));

    function hideOutputs() {
      dag.panel.style.display = "none";
      planP.panel.style.display = "none";
      res.panel.style.display = "none";
    }

    function showError(e) {
      var msg = (e && e.name === "SimError") ? e.message
        : "SparkSim hit an internal snag: " + (e && e.message ? e.message : String(e));
      errBox.innerHTML = "";
      errBox.appendChild(h("div", "eng-err", escHtml(msg)));
    }

    /* ------- DAG SVG ------- */
    function renderDag(out) {
      dagWrap.innerHTML = "";
      var stages = out.stages, edges = out.edges;
      var byId = {};
      stages.forEach(function (s) { byId[s.id] = s; });
      /* level = longest dependency chain below */
      var levelOf = {};
      function level(s) {
        if (levelOf[s.id] !== undefined) return levelOf[s.id];
        var l = s.deps.length ? 1 + Math.max.apply(null, s.deps.map(function (d) { return level(byId[d]); })) : 0;
        levelOf[s.id] = l;
        return l;
      }
      stages.forEach(level);
      var cols = {};
      stages.forEach(function (s) { (cols[levelOf[s.id]] = cols[levelOf[s.id]] || []).push(s); });
      var nLevels = Object.keys(cols).length;
      var W = 178, GAP = 96, PADX = 18, PADY = 46, VGAP = 26;
      var boxH = {}, pos = {};
      var maxColH = 0;
      Object.keys(cols).forEach(function (lv) {
        var y = PADY;
        cols[lv].forEach(function (s) {
          var opsShown = Math.min(s.ops.length, 4);
          var hgt = 24 + opsShown * 12 + (s.ops.length > 4 ? 12 : 0) + 22;
          boxH[s.id] = hgt;
          pos[s.id] = { x: PADX + (+lv) * (W + GAP), y: y };
          y += hgt + VGAP;
        });
        maxColH = Math.max(maxColH, y);
      });
      var svgW = PADX * 2 + nLevels * W + (nLevels - 1) * GAP;
      var svgH = maxColH + 14;
      var svg = svgEl("svg", { viewBox: "0 0 " + svgW + " " + svgH, width: "100%", style: "min-width:" + Math.min(svgW, 760) + "px;max-width:" + svgW + "px;display:block;" });
      var defs = svgEl("defs", {});
      var marker = svgEl("marker", { id: "ss-arr", viewBox: "0 0 8 8", refX: "7", refY: "4", markerWidth: "7", markerHeight: "7", orient: "auto" });
      var mpath = svgEl("path", { d: "M0,0 L8,4 L0,8 z", fill: "var(--gold)" });
      marker.appendChild(mpath);
      defs.appendChild(marker);
      svg.appendChild(defs);

      /* job frame */
      svg.appendChild(svgEl("rect", {
        x: 6, y: 22, width: svgW - 12, height: svgH - 30, rx: 12,
        fill: "none", stroke: "var(--line)", "stroke-dasharray": "3 4"
      }));
      var jobLabel = svgEl("text", { x: 14, y: 15, "font-family": "var(--mono)", "font-size": "10", fill: "var(--ink2)" });
      jobLabel.textContent = "Job 1 · " + out.actionDesc + " · " + stages.length + " stage" + (stages.length > 1 ? "s" : "");
      svg.appendChild(jobLabel);

      /* edges first (under boxes) */
      edges.forEach(function (e) {
        var a = pos[e.from], b = pos[e.to];
        if (!a || !b) return;
        var x1 = a.x + W, y1 = a.y + boxH[e.from] / 2;
        var x2 = b.x, y2 = b.y + boxH[e.to] / 2;
        var line = svgEl("line", { x1: x1, y1: y1, x2: x2, y2: y2 });
        var lbl = svgEl("text", {
          x: (x1 + x2) / 2, y: Math.min(y1, y2) - 7, "text-anchor": "middle",
          "font-family": "var(--mono)", "font-size": "9"
        });
        if (e.type === "shuffle") {
          line.setAttribute("stroke", "var(--rust)");
          line.setAttribute("stroke-width", "3.5");
          line.setAttribute("stroke-dasharray", "8 6");
          lbl.setAttribute("fill", "var(--rust)");
          lbl.textContent = "shuffle (cross-dock)";
        } else {
          line.setAttribute("stroke", "var(--gold)");
          line.setAttribute("stroke-width", "1.2");
          line.setAttribute("stroke-dasharray", "2 4");
          line.setAttribute("marker-end", "url(#ss-arr)");
          lbl.setAttribute("fill", "var(--gold)");
          lbl.textContent = "broadcast";
        }
        svg.appendChild(line);
        svg.appendChild(lbl);
      });

      /* stage boxes */
      stages.forEach(function (s) {
        var p = pos[s.id], hgt = boxH[s.id];
        var g = svgEl("g", {});
        g.appendChild(svgEl("rect", {
          x: p.x, y: p.y, width: W, height: hgt, rx: 9,
          fill: "var(--paper2)", stroke: "var(--line)"
        }));
        var title = svgEl("text", { x: p.x + 10, y: p.y + 15, "font-family": "var(--mono)", "font-size": "10", "font-weight": "700", fill: "var(--ink)" });
        title.textContent = "Stage " + s.id;
        g.appendChild(title);
        var oy = p.y + 28;
        s.ops.slice(0, 4).forEach(function (op) {
          var t = svgEl("text", { x: p.x + 10, y: oy, "font-family": "var(--mono)", "font-size": "8.5", fill: "var(--ink2)" });
          t.textContent = op.length > 30 ? op.slice(0, 29) + "…" : op;
          g.appendChild(t);
          oy += 12;
        });
        if (s.ops.length > 4) {
          var more = svgEl("text", { x: p.x + 10, y: oy, "font-family": "var(--mono)", "font-size": "8.5", fill: "var(--ink2)" });
          more.textContent = "+" + (s.ops.length - 4) + " more";
          g.appendChild(more);
          oy += 12;
        }
        /* task bars */
        for (var tI = 0; tI < s.taskCount; tI++) {
          g.appendChild(svgEl("rect", {
            x: p.x + 10 + tI * 21, y: p.y + hgt - 15, width: 16, height: 6, rx: 2,
            fill: "var(--accent)", opacity: "0.75"
          }));
        }
        var tl = svgEl("text", { x: p.x + 10 + s.taskCount * 21 + 4, y: p.y + hgt - 9, "font-family": "var(--mono)", "font-size": "8", fill: "var(--gold)" });
        tl.textContent = s.taskCount + " task" + (s.taskCount > 1 ? "s" : "") + " · simulated";
        g.appendChild(tl);
        svg.appendChild(g);
      });
      dagWrap.appendChild(svg);
    }

    /* ------- PLAN panel ------- */
    function renderPlans(out) {
      planCols.innerHTML = "";
      function planCol(title, lines, kind) {
        var c = h("div", "ss-plan");
        c.appendChild(h("h5", "", escHtml(title)));
        var pre = document.createElement("pre");
        pre.innerHTML = lines.map(function (l) {
          var pad = "";
          for (var i = 0; i < l.depth; i++) pad += "  ";
          var txt = escHtml(pad + (l.depth ? "+- " : "") + l.text);
          if (l.tag === "pushed") return "<span class='ss-push'>" + txt + "  ← pushed down</span>";
          if (l.tag === "pruned") return "<span class='ss-prune'>" + txt + "</span>";
          if (l.tag === "exchange") return "<span class='ss-exch'>" + txt + "</span>";
          if (l.tag === "broadcast") return "<span class='ss-bcast'>" + txt + "</span>";
          return txt;
        }).join("\n");
        c.appendChild(pre);
        planCols.appendChild(c);
      }
      planCol("logical", logicalLines(out.plans.logical, false));
      planCol("optimized (catalyst)", logicalLines(out.plans.optimized, true));
      planCol("physical", physicalLines(out.plans.physical));
    }

    /* ------- RESULT panel ------- */
    function renderResult(out) {
      resBody.innerHTML = "";
      var total = out.result.rows.length;
      var cap = 50;
      if (out.action === "show" && out.actionParams && typeof out.actionParams.n === "number")
        cap = Math.min(cap, out.actionParams.n);
      var shown = out.result.rows.slice(0, cap);
      var summary = out.actionDesc + " — " + total + " row" + (total === 1 ? "" : "s") +
        (out.action !== "count" ? " × " + out.result.columns.length + " col" + (out.result.columns.length === 1 ? "" : "s") : "");
      if (out.action === "write") summary += " (displayed only — nothing persisted)";
      resBody.appendChild(h("div", "eng-note", escHtml(summary)));

      var grid = h("div", "eng-grid");
      var table = document.createElement("table");
      var thead = document.createElement("thead"), trh = document.createElement("tr");
      out.result.columns.forEach(function (c) {
        var th = document.createElement("th"); th.textContent = c; trh.appendChild(th);
      });
      thead.appendChild(trh); table.appendChild(thead);
      var tbody = document.createElement("tbody");
      shown.forEach(function (r) {
        var tr = document.createElement("tr");
        r.forEach(function (v) {
          var td = document.createElement("td");
          td.textContent = fmtCell(v);
          if (v === null || v === undefined) td.style.opacity = "0.45";
          tr.appendChild(td);
        });
        tbody.appendChild(tr);
      });
      table.appendChild(tbody);
      grid.appendChild(table);
      resBody.appendChild(grid);
      if (total > shown.length)
        resBody.appendChild(h("div", "eng-note", "showing " + shown.length + " of " + total + " rows"));

      /* expect check */
      if (config.expect) {
        var exp = config.expect, diffs = [];
        if (typeof exp.rows === "number" && total !== exp.rows)
          diffs.push("expected " + exp.rows + " rows, got " + total);
        if (Array.isArray(exp.cols)) {
          var got = out.result.columns;
          if (got.length !== exp.cols.length || exp.cols.some(function (c, i) { return got[i] !== c; }))
            diffs.push("expected columns [" + exp.cols.join(", ") + "], got [" + got.join(", ") + "]");
        }
        if (diffs.length)
          resBody.appendChild(h("div", "eng-err", "Not quite: " + escHtml(diffs.join("; ")) + "."));
        else
          resBody.appendChild(h("div", "eng-pass", "✓ Matches the expected shape" +
            (typeof exp.rows === "number" ? " — " + exp.rows + " rows" : "") +
            (Array.isArray(exp.cols) ? ", columns [" + escHtml(exp.cols.join(", ")) + "]" : "") + "."));
      }
    }

    /* ------- Run ------- */
    function run() {
      clearTimers();
      errBox.innerHTML = "";
      hideOutputs();
      caption.style.display = "none";
      ribbon.innerHTML = "";
      var out;
      try {
        out = apiExecute(ta.value, tables);
      } catch (e) {
        ribbon.innerHTML = "<span class='eng-note'>—</span>";
        showError(e);
        return;
      }
      /* animate lazy ribbon */
      var delay = 60;
      out.lazyLog.forEach(function (entry, i) {
        var chip = h("span", "ss-chip" + (entry.kind === "action" ? " action" : ""),
          escHtml(entry.desc));
        chip.title = entry.kind;
        if (i > 0) {
          var arrow = h("span", "ss-arrow", "→");
          ribbon.appendChild(arrow);
          later(function () { arrow.classList.add("on"); }, delay);
        }
        ribbon.appendChild(chip);
        var fireAt = delay;
        later(function () { chip.classList.add("on"); }, fireAt);
        delay += entry.kind === "action" ? 320 : 150;
        if (entry.kind === "action") {
          later(function () {
            caption.style.display = "";
            renderDag(out);
            renderPlans(out);
            renderResult(out);
            dag.panel.style.display = "";
            planP.panel.style.display = "";
            res.panel.style.display = "";
          }, fireAt + 300);
        }
      });
    }
    runBtn.onclick = run;

    return {
      destroy: function () { clearTimers(); if (root.parentNode) root.parentNode.removeChild(root); }
    };
  }

  /* ==================== export ============================================ */
  win.Engines.sparksim = {
    mount: mount,
    parse: apiParse,
    execute: apiExecute,
    SimError: SimError
  };
})();
