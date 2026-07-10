/* engine/traceplayer.js — Tier 3 scripted-trace player (docs/04-ENGINE-CONTRACT.md §2/§3/§4).
 * Plain browser script: no imports, no top-level await, no DOM access at load time.
 * Exposes window.Engines.trace = { mount(el, config, ctx) }.
 * Traces are authored keyframes — always badged "simulation" (honesty rule, 03-LAB-ENGINE-SPEC Tier 3).
 */
(function () {
  "use strict";

  var STYLE_ID = "tp-style";
  var STYLE = [
    ".tp-head{display:flex;align-items:center;gap:.6rem;flex-wrap:wrap}",
    ".tp-title{font-family:var(--display);font-size:1.05rem;font-weight:600}",
    ".tp-controls{display:flex;align-items:center;gap:.5rem;margin-top:.7rem;flex-wrap:wrap}",
    ".tp-scrub{flex:1;min-width:140px;accent-color:var(--accent);cursor:pointer}",
    ".tp-count{font-family:var(--mono);font-size:.72rem;color:var(--ink2);min-width:3.4rem;text-align:right}",
    ".tp-narr{margin-top:.9rem;padding:.7rem 1rem;border-left:3px solid var(--accent);background:var(--paper2);border-radius:0 8px 8px 0;font-size:.92rem;min-height:2.6rem;transition:opacity .3s}",
    ".tp-visual{margin-top:.9rem}",
    ".tp-cols{display:grid;grid-template-columns:1fr 1fr;gap:1.1rem}",
    "@media (max-width:640px){.tp-cols{grid-template-columns:1fr}}",
    ".tp-colhead{font-family:var(--mono);font-size:.64rem;letter-spacing:.12em;color:var(--ink2);border-bottom:1px solid var(--line);padding-bottom:.25rem;margin-bottom:.5rem}",
    ".tp-exec-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(120px,1fr));gap:.55rem;margin-top:.6rem}",
    ".tp-exec{border:1px solid var(--line);border-radius:8px;padding:.45rem .55rem;background:var(--card)}",
    ".tp-exec-id{font-family:var(--mono);font-size:.6rem;color:var(--ink2);margin-bottom:.3rem}",
    ".tp-task-wrap{margin:.28rem 0}",
    ".tp-task{height:9px;border-radius:4px;background:var(--green);min-width:2px;transition:width .9s ease,background-color .5s ease}",
    ".tp-task.hot{background:var(--rust)}",
    ".tp-task-label{font-family:var(--mono);font-size:.56rem;color:var(--ink2);margin-top:.1rem}",
    ".tp-parts{display:flex;gap:2px;height:36px;margin-top:.6rem;align-items:stretch}",
    ".tp-part{background:var(--accent);opacity:.5;border-radius:3px;min-width:3px;display:flex;align-items:center;justify-content:center;overflow:hidden;transition:flex-grow .9s ease,opacity .5s ease,background-color .5s ease}",
    ".tp-part.hot{background:var(--rust);opacity:.95}",
    ".tp-part span{font-family:var(--mono);font-size:.56rem;color:var(--paper);white-space:nowrap}",
    ".tp-bars{margin-top:.6rem;display:flex;flex-direction:column;gap:.4rem}",
    ".tp-bar-row{display:grid;grid-template-columns:minmax(80px,160px) 1fr 64px;gap:.55rem;align-items:center}",
    ".tp-bar-label{font-family:var(--mono);font-size:.64rem;color:var(--ink2);text-align:right;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}",
    ".tp-bar-track{background:var(--paper2);border-radius:4px;height:14px;overflow:hidden}",
    ".tp-bar-fill{height:100%;border-radius:4px;background:var(--accent);min-width:2px;transition:width .9s ease,background-color .5s ease}",
    ".tp-bar-fill.hot{background:var(--rust)}",
    ".tp-bar-val{font-family:var(--mono);font-size:.6rem;color:var(--ink2);white-space:nowrap}",
    ".tp-files{display:flex;flex-wrap:wrap;gap:2px;margin-top:.6rem;max-height:150px;overflow:hidden}",
    ".tp-file{width:5px;height:5px;background:var(--gold);border-radius:1px;opacity:.85;transition:opacity .4s}",
    ".tp-files-cap{font-family:var(--mono);font-size:.68rem;color:var(--ink2);margin-top:.35rem}",
    ".tp-note{margin-top:.7rem;padding:.5rem .8rem;border:1px dashed var(--line);border-radius:8px;font-size:.82rem;color:var(--ink2);background:var(--paper2)}",
    ".tp-chips{display:flex;flex-wrap:wrap;gap:.4rem;margin-top:.8rem}",
    ".tp-chip{font-family:var(--mono);font-size:.68rem;padding:.18rem .55rem;border:1px solid var(--line);border-radius:6px;background:var(--paper2);color:var(--ink)}"
  ].join("\n");

  var PLAY_MS = 1600; // one keyframe per 1.6s during auto-play

  // ---------- small helpers (no DOM access until called from mount) ----------

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

  function num(v, fallback) {
    return (typeof v === "number" && isFinite(v)) ? v : fallback;
  }

  function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }

  function escHtml(s) {
    return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  // Keep exactly n children in parent, creating via make() — lets CSS
  // transitions animate updates instead of rebuilding the subtree.
  function syncChildren(parent, n, make) {
    while (parent.children.length > n) parent.removeChild(parent.lastChild);
    while (parent.children.length < n) parent.appendChild(make());
  }

  function humanKb(kb) {
    kb = num(kb, 0);
    if (kb >= 1024) return (Math.round((kb / 1024) * 10) / 10).toLocaleString("en-US") + " MB";
    return kb.toLocaleString("en-US") + " KB";
  }

  // ---------- visual state machine: one updater per state key ----------

  function updateExecutors(doc, slot, list) {
    if (!Array.isArray(list) || !list.length) { slot.innerHTML = ""; return; }
    var grid = slot.firstChild;
    if (!grid || grid.className !== "tp-exec-grid") {
      slot.innerHTML = "";
      grid = h(doc, "div", "tp-exec-grid");
      slot.appendChild(grid);
    }
    syncChildren(grid, list.length, function () {
      var card = h(doc, "div", "tp-exec");
      card.appendChild(h(doc, "div", "tp-exec-id", ""));
      card.appendChild(h(doc, "div"));
      return card;
    });
    for (var i = 0; i < list.length; i++) {
      var ex = (list[i] && typeof list[i] === "object") ? list[i] : {};
      var card = grid.children[i];
      card.children[0].textContent = ex.id != null ? String(ex.id) : "exec-" + (i + 1);
      var tasksEl = card.children[1];
      var tasks = Array.isArray(ex.tasks) ? ex.tasks : [];
      syncChildren(tasksEl, tasks.length, function () {
        var w = h(doc, "div", "tp-task-wrap");
        w.appendChild(h(doc, "div", "tp-task"));
        w.appendChild(h(doc, "div", "tp-task-label", ""));
        return w;
      });
      for (var j = 0; j < tasks.length; j++) {
        var t = (tasks[j] && typeof tasks[j] === "object") ? tasks[j] : {};
        var wrap = tasksEl.children[j];
        var bar = wrap.children[0];
        bar.style.width = clamp(num(t.pct, 0), 0, 100) + "%";
        bar.className = "tp-task" + (t.hot ? " hot" : "");
        var lab = wrap.children[1];
        lab.textContent = t.label != null ? String(t.label) : "";
        lab.style.display = t.label != null ? "" : "none";
      }
    }
  }

  function updatePartitions(doc, slot, list) {
    if (!Array.isArray(list) || !list.length) { slot.innerHTML = ""; return; }
    var strip = slot.firstChild;
    if (!strip || strip.className !== "tp-parts") {
      slot.innerHTML = "";
      strip = h(doc, "div", "tp-parts");
      slot.appendChild(strip);
    }
    syncChildren(strip, list.length, function () {
      var p = h(doc, "div", "tp-part");
      p.appendChild(h(doc, "span", null, ""));
      return p;
    });
    for (var i = 0; i < list.length; i++) {
      var d = (list[i] && typeof list[i] === "object") ? list[i] : {};
      var p = strip.children[i];
      p.style.flexGrow = String(Math.max(num(d.size, 1), 0.1));
      p.className = "tp-part" + (d.hot ? " hot" : "");
      p.firstChild.textContent = (d.hot && d.label != null) ? String(d.label) : "";
      p.title = (d.label != null ? d.label + " · " : "") + num(d.size, 0);
    }
  }

  function updateBars(doc, slot, list) {
    if (!Array.isArray(list) || !list.length) { slot.innerHTML = ""; return; }
    var box = slot.firstChild;
    if (!box || box.className !== "tp-bars") {
      slot.innerHTML = "";
      box = h(doc, "div", "tp-bars");
      slot.appendChild(box);
    }
    var biggest = 1;
    for (var k = 0; k < list.length; k++) {
      var b0 = list[k] || {};
      biggest = Math.max(biggest, num(b0.max, 0), num(b0.value, 0));
    }
    syncChildren(box, list.length, function () {
      var row = h(doc, "div", "tp-bar-row");
      row.appendChild(h(doc, "div", "tp-bar-label", ""));
      var track = h(doc, "div", "tp-bar-track");
      track.appendChild(h(doc, "div", "tp-bar-fill"));
      row.appendChild(track);
      row.appendChild(h(doc, "div", "tp-bar-val", ""));
      return row;
    });
    for (var i = 0; i < list.length; i++) {
      var b = (list[i] && typeof list[i] === "object") ? list[i] : {};
      var row = box.children[i];
      row.children[0].textContent = b.label != null ? String(b.label) : "";
      var denom = num(b.max, 0) > 0 ? num(b.max, 1) : biggest;
      var fill = row.children[1].firstChild;
      fill.style.width = clamp((num(b.value, 0) / denom) * 100, 0, 100) + "%";
      fill.className = "tp-bar-fill" + (b.hot ? " hot" : "");
      var v = num(b.value, 0);
      row.children[2].textContent = v.toLocaleString("en-US") + (b.unit != null ? String(b.unit) : "");
    }
  }

  function updateFiles(doc, slot, data) {
    if (!data || typeof data !== "object") { slot.innerHTML = ""; return; }
    var wrap = slot.firstChild;
    if (!wrap || wrap.className !== "tp-files-wrap") {
      slot.innerHTML = "";
      wrap = h(doc, "div", "tp-files-wrap");
      wrap.appendChild(h(doc, "div", "tp-files"));
      wrap.appendChild(h(doc, "div", "tp-files-cap", ""));
      slot.appendChild(wrap);
    }
    var count = Math.max(0, Math.floor(num(data.count, 0)));
    var field = wrap.children[0];
    syncChildren(field, Math.min(count, 400), function () { return h(doc, "div", "tp-file"); });
    var cap = count.toLocaleString("en-US") + " files";
    if (num(data.avgKb, 0) > 0) cap += " · avg " + humanKb(data.avgKb);
    wrap.children[1].textContent = cap;
  }

  function updateNote(doc, slot, note) {
    if (typeof note !== "string" || !note) { slot.innerHTML = ""; return; }
    var card = slot.firstChild;
    if (!card || card.className !== "tp-note") {
      slot.innerHTML = "";
      card = h(doc, "div", "tp-note");
      slot.appendChild(card);
    }
    card.innerHTML = note;
  }

  // A "column" owns one slot div per state key, updated in place each keyframe.
  function makeColumn(doc, parent, label) {
    var root = h(doc, "div", "tp-col");
    if (label) root.appendChild(h(doc, "div", "tp-colhead", label));
    var slots = {
      executors: h(doc, "div"),
      partitions: h(doc, "div"),
      bars: h(doc, "div"),
      files: h(doc, "div"),
      note: h(doc, "div")
    };
    root.appendChild(slots.executors);
    root.appendChild(slots.partitions);
    root.appendChild(slots.bars);
    root.appendChild(slots.files);
    root.appendChild(slots.note);
    parent.appendChild(root);
    return slots;
  }

  function updateColumn(doc, slots, state) {
    state = (state && typeof state === "object") ? state : {};
    updateExecutors(doc, slots.executors, state.executors);
    updatePartitions(doc, slots.partitions, state.partitions);
    updateBars(doc, slots.bars, state.bars);
    updateFiles(doc, slots.files, state.files);
    updateNote(doc, slots.note, state.note);
  }

  // ---------- mount ----------

  function mount(el, config, ctx) {
    if (!el || !el.ownerDocument) return {};
    var doc = el.ownerDocument;
    config = config || {};
    ensureStyle(doc);
    el.innerHTML = "";

    var traces = (ctx && ctx.TRACES) || (typeof window !== "undefined" ? window.TRACES : null) || {};
    var trace = traces[config.trace];
    if (!trace || !Array.isArray(trace.steps) || !trace.steps.length) {
      var err = h(doc, "div", "eng-err");
      err.innerHTML = "Trace <code>" + escHtml(config.trace || "(unnamed)") +
        "</code> is not loaded. Expected <code>engine/traces/" + escHtml(config.trace || "?") +
        ".json</code> in the TRACES bundle.";
      el.appendChild(err);
      return {};
    }
    var steps = trace.steps;
    var isCompare = trace.compare === true;

    // header: title + honesty badge (always "simulation" for traces)
    var head = h(doc, "div", "tp-head");
    head.appendChild(h(doc, "div", "tp-title", trace.title || trace.id || "trace"));
    head.appendChild(h(doc, "span", "eng-badge sim", trace.badge || "simulation"));
    el.appendChild(head);

    // transport controls + scrubber (one notch per step)
    var controls = h(doc, "div", "tp-controls");
    var playBtn = h(doc, "button", "eng-btn", "play");
    var backBtn = h(doc, "button", "eng-btn ghost", "‹ step");
    var fwdBtn = h(doc, "button", "eng-btn ghost", "step ›");
    var scrub = doc.createElement("input");
    scrub.type = "range";
    scrub.min = "0";
    scrub.max = String(steps.length - 1);
    scrub.step = "1";
    scrub.value = "0";
    scrub.className = "tp-scrub";
    scrub.setAttribute("aria-label", "trace step");
    var count = h(doc, "div", "tp-count", "");
    controls.appendChild(playBtn);
    controls.appendChild(backBtn);
    controls.appendChild(fwdBtn);
    controls.appendChild(scrub);
    controls.appendChild(count);
    el.appendChild(controls);

    var narr = h(doc, "div", "tp-narr");
    el.appendChild(narr);
    var visual = h(doc, "div", "tp-visual");
    el.appendChild(visual);
    var chips = h(doc, "div", "tp-chips");
    el.appendChild(chips);

    var columns = [];
    if (isCompare) {
      var two = h(doc, "div", "tp-cols");
      columns.push(makeColumn(doc, two, "BEFORE"));
      columns.push(makeColumn(doc, two, "AFTER"));
      visual.appendChild(two);
    } else {
      columns.push(makeColumn(doc, visual, null));
    }

    var idx = 0;
    var timer = null;

    function renderStep(i) {
      idx = clamp(Math.round(num(i, 0)), 0, steps.length - 1);
      var step = (steps[idx] && typeof steps[idx] === "object") ? steps[idx] : {};
      try {
        narr.innerHTML = typeof step.narration === "string" ? step.narration : "";
        var state = (step.state && typeof step.state === "object") ? step.state : {};
        if (isCompare) {
          updateColumn(doc, columns[0], state.before);
          updateColumn(doc, columns[1], state.after);
        } else {
          updateColumn(doc, columns[0], state);
        }
        chips.innerHTML = "";
        if (step.metrics && typeof step.metrics === "object") {
          Object.keys(step.metrics).forEach(function (k) {
            var v = step.metrics[k];
            chips.appendChild(h(doc, "span", "tp-chip",
              k + " " + (typeof v === "number" ? v.toLocaleString("en-US") : String(v))));
          });
        }
      } catch (e) { /* malformed keyframe: keep the previous frame, never throw */ }
      scrub.value = String(idx);
      count.textContent = (idx + 1) + " / " + steps.length;
    }

    function stop() {
      if (timer) { clearInterval(timer); timer = null; }
      playBtn.textContent = "play";
    }

    function playPause() {
      if (timer) { stop(); return; }
      if (idx >= steps.length - 1) renderStep(0); // replay from the top
      playBtn.textContent = "pause";
      timer = setInterval(function () {
        if (idx >= steps.length - 1) { stop(); return; }
        renderStep(idx + 1);
      }, PLAY_MS);
    }

    playBtn.addEventListener("click", playPause);
    backBtn.addEventListener("click", function () { stop(); renderStep(idx - 1); });
    fwdBtn.addEventListener("click", function () { stop(); renderStep(idx + 1); });
    scrub.addEventListener("input", function () { stop(); renderStep(parseInt(scrub.value, 10) || 0); });

    renderStep(0);
    return { destroy: function () { stop(); } };
  }

  if (typeof window !== "undefined") {
    window.Engines = window.Engines || {};
    window.Engines.trace = { mount: mount };
  }
})();
