/* engine/pyrunner.js — Tier 1 Python (Pyodide) runner (docs/04-ENGINE-CONTRACT.md §2/§3).
 * Plain browser script: no imports, no top-level await, no DOM access at load time.
 * Exposes window.Engines.pyodide = { mount(el, config, ctx) }.
 * Real in-browser execution ("real execution" badge). Pyodide is the single permitted
 * lazy CDN dependency; when offline, the lab degrades to a solution walkthrough.
 */
(function () {
  "use strict";

  var CDN_URL = "https://cdn.jsdelivr.net/pyodide/v0.26.4/full/pyodide.js";
  var STYLE_ID = "pyr-style";
  var STYLE = [
    ".pyr-head{display:flex;align-items:center;gap:.6rem;margin-bottom:.6rem;flex-wrap:wrap}",
    ".pyr-kind{font-family:var(--mono);font-size:.7rem;color:var(--ink2)}",
    ".pyr-row{display:flex;gap:.5rem;margin-top:.6rem;flex-wrap:wrap;align-items:center}",
    ".pyr-out{font-family:var(--mono);font-size:.78rem;background:var(--code-bg);color:var(--code-ink);border-radius:8px;padding:.7rem .9rem;margin-top:.8rem;white-space:pre-wrap;overflow:auto;max-height:300px}",
    ".pyr-tb{font-family:var(--mono);font-size:.72rem;white-space:pre-wrap;margin:.4rem 0 0;overflow-x:auto}"
  ].join("\n");

  function h(doc, tag, cls, text) {
    var n = doc.createElement(tag);
    if (cls) n.className = cls;
    if (text != null) n.textContent = text;
    return n;
  }

  function ensureStyle(doc) {
    if (!doc.getElementById(STYLE_ID)) {
      var st = doc.createElement("style");
      st.id = STYLE_ID;
      st.textContent = STYLE;
      (doc.head || doc.documentElement).appendChild(st);
    }
  }

  function esc(s) {
    return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  function isPyIdent(name) {
    return typeof name === "string" && /^[A-Za-z_][A-Za-z0-9_]*$/.test(name);
  }

  function tracebackTail(msg, n) {
    var lines = String(msg == null ? "" : msg).replace(/\s+$/, "").split("\n");
    return lines.slice(-(n || 3)).join("\n");
  }

  // Inject the Pyodide loader <script> exactly once, lazily (first Run click).
  function loadPyodideScript(doc) {
    if (typeof window.loadPyodide === "function") return Promise.resolve();
    if (window.__NM_PYODIDE_SCRIPT__) return window.__NM_PYODIDE_SCRIPT__;
    window.__NM_PYODIDE_SCRIPT__ = new Promise(function (resolve, reject) {
      var s = doc.createElement("script");
      s.src = CDN_URL;
      s.onload = function () { resolve(); };
      s.onerror = function () {
        window.__NM_PYODIDE_SCRIPT__ = null; // allow a retry once back online
        reject(new Error("pyodide-cdn-unreachable"));
      };
      (doc.head || doc.documentElement).appendChild(s);
    });
    return window.__NM_PYODIDE_SCRIPT__;
  }

  // One shared runtime for all Python labs on the page (cached promise on window).
  function getRuntime(doc, onNote) {
    if (window.__NM_PYODIDE__) return window.__NM_PYODIDE__;
    window.__NM_PYODIDE__ = loadPyodideScript(doc)
      .then(function () {
        onNote("starting the Python interpreter…");
        return window.loadPyodide();
      })
      .then(function (py) {
        onNote("loading pandas — one-time download…");
        return py.loadPackage("pandas").then(function () { return py; });
      });
    window.__NM_PYODIDE__.catch(function () { window.__NM_PYODIDE__ = null; });
    return window.__NM_PYODIDE__;
  }

  // Datasets cross the JS→Python boundary as a JSON string set via globals
  // (never interpolated into source — no quoting/escaping bugs possible).
  function injectDatasets(py, names, nimbus) {
    py.runPython("import pandas as pd\nimport json");
    for (var i = 0; i < (names || []).length; i++) {
      var t = names[i];
      if (!isPyIdent(t) || !nimbus || !Array.isArray(nimbus[t])) continue;
      py.globals.set("__nm_rows__", JSON.stringify(nimbus[t]));
      py.runPython(t + " = pd.DataFrame(json.loads(__nm_rows__))");
    }
    py.runPython("globals().pop('__nm_rows__', None)");
  }

  function mount(el, config, ctx) {
    if (!el || !el.ownerDocument) return {};
    var doc = el.ownerDocument;
    config = config || {};
    ensureStyle(doc);
    el.innerHTML = "";
    // NOTE: config.task is rendered by the player above this container — not duplicated here.

    var head = h(doc, "div", "pyr-head");
    head.appendChild(h(doc, "span", "eng-badge real", "real execution"));
    head.appendChild(h(doc, "span", "pyr-kind", "python · pandas — runs in your browser via Pyodide"));
    el.appendChild(head);

    var area = h(doc, "div", "eng-area");
    var ta = doc.createElement("textarea");
    ta.value = config.starterCode || "";
    ta.spellcheck = false;
    ta.rows = Math.min(18, Math.max(6, String(config.starterCode || "").split("\n").length + 2));
    area.appendChild(ta);
    el.appendChild(area);

    var row = h(doc, "div", "pyr-row");
    var runBtn = h(doc, "button", "eng-btn", "Run");
    row.appendChild(runBtn);

    var solPre = null;
    if (config.solutionCode) {
      var revBtn = h(doc, "button", "eng-btn ghost", "reveal solution");
      row.appendChild(revBtn);
      revBtn.addEventListener("click", function () {
        if (!solPre) {
          solPre = h(doc, "pre", "pyr-out");
          solPre.textContent = config.solutionCode;
          solPre.style.display = "none";
          el.appendChild(solPre);
        }
        var hidden = solPre.style.display === "none";
        solPre.style.display = hidden ? "" : "none";
        revBtn.textContent = hidden ? "hide solution" : "reveal solution";
      });
    }
    el.appendChild(row);

    var note = h(doc, "div", "eng-note");
    note.style.display = "none";
    el.appendChild(note);
    var out = h(doc, "pre", "pyr-out");
    out.style.display = "none";
    el.appendChild(out);
    var status = h(doc, "div");   // PASS / error banners
    el.appendChild(status);
    var fallback = h(doc, "div"); // offline walkthrough
    el.appendChild(fallback);

    var injected = false;
    var running = false;

    function setNote(txt) { note.style.display = ""; note.textContent = txt; }
    function clearNote() { note.style.display = "none"; }
    function setStatus(cls, html) {
      status.innerHTML = "";
      var d = h(doc, "div", cls);
      d.innerHTML = html;
      status.appendChild(d);
    }

    function renderOffline() {
      fallback.innerHTML = "";
      var n = h(doc, "div", "eng-note");
      n.textContent = "offline mode — the Python runtime loads once from a CDN and it couldn't be reached. " +
        "Read the completed solution below as a walkthrough; the Run button will retry the download.";
      fallback.appendChild(n);
      if (config.solutionCode) {
        var p = h(doc, "pre", "pyr-out");
        p.textContent = config.solutionCode;
        fallback.appendChild(p);
        fallback.appendChild(h(doc, "div", "eng-note", "run it when you're back online"));
      }
    }

    function onRun() {
      if (running) return;
      running = true;
      runBtn.disabled = true;
      status.innerHTML = "";
      fallback.innerHTML = "";
      setNote("fetching Python runtime — ~10 MB, first run only…");

      getRuntime(doc, setNote).then(function (py) {
        return runUserCode(py);
      }, function () {
        clearNote();
        renderOffline();
      }).catch(function (e) {
        setStatus("eng-err", "<strong>engine error</strong> — " + esc(e && e.message ? e.message : String(e)));
      }).then(function () {
        clearNote();
        running = false;
        runBtn.disabled = false;
      });
    }

    function runUserCode(py) {
      if (!injected) {
        setNote("loading NimbusMart tables as pandas DataFrames…");
        injectDatasets(py, config.datasets, (ctx && ctx.NIMBUS) || window.NIMBUS || {});
        injected = true;
      }
      setNote("running…");
      var buf = [];
      py.setStdout({ batched: function (s) { buf.push(s); } });
      py.setStderr({ batched: function (s) { buf.push(s); } });

      var finish = function (failed) {
        try { py.setStdout(); py.setStderr(); } catch (e) { /* ignore */ }
        out.style.display = "";
        out.textContent = buf.length ? buf.join("\n") : "(no output — print() something, or end with an expression)";
        if (!failed && config.assertCode) runAssert(py);
      };

      return py.runPythonAsync(ta.value).then(function (res) {
        if (res !== undefined && res !== null) {
          try { buf.push(String(res)); } catch (e) { /* ignore repr failures */ }
          if (res && typeof res.destroy === "function") { try { res.destroy(); } catch (e) { /* ignore */ } }
        }
        finish(false);
      }, function (err) {
        finish(true);
        setStatus("eng-err", "<strong>Python error</strong><pre class=\"pyr-tb\">" +
          esc(tracebackTail(err && err.message)) + "</pre>");
      });
    }

    function runAssert(py) {
      try {
        py.runPython(config.assertCode);
        setStatus("eng-pass", "<strong>PASS</strong> — assertion holds");
      } catch (err) {
        var msg = String((err && err.message) || "");
        var m = msg.match(/AssertionError:?[^\n]*/);
        if ((err && err.type === "AssertionError") || m) {
          setStatus("eng-err", "<strong>FAIL</strong> — " +
            esc(m && m[0].replace(/^AssertionError:?\s*/, "") ? m[0] : "assertion failed"));
        } else {
          setStatus("eng-err", "<strong>check failed</strong><pre class=\"pyr-tb\">" +
            esc(tracebackTail(msg)) + "</pre>");
        }
      }
    }

    runBtn.addEventListener("click", onRun);
    return { destroy: function () { /* runtime is shared; nothing to tear down */ } };
  }

  if (typeof window !== "undefined") {
    window.Engines = window.Engines || {};
    window.Engines.pyodide = { mount: mount };
  }
})();
