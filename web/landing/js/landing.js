/* Moirai landing page motion. Vanilla, no dependencies, CSP-safe (external file, no eval).
   One requestAnimationFrame loop drives every continuous animation; each animation runs only while its
   element is near the viewport, the tab is visible, motion is allowed and the visitor has not paused it.
   With prefers-reduced-motion every animation renders one static frame instead. */
(function () {
  "use strict";

  var root = document.documentElement;
  root.classList.remove("no-js");
  root.classList.add("js");

  var reduceQuery = window.matchMedia("(prefers-reduced-motion: reduce)");
  var fineQuery = window.matchMedia("(hover: hover) and (pointer: fine)");
  var reduced = reduceQuery.matches;
  var paused = false;
  var refreshScroll = function () {};

  function clamp(v, lo, hi) { return v < lo ? lo : v > hi ? hi : v; }
  function lerp(a, b, t) { return a + (b - a) * t; }
  function easeOutExpo(t) { return t >= 1 ? 1 : 1 - Math.pow(2, -10 * t); }
  function easeOutQuart(t) { return 1 - Math.pow(1 - t, 4); }
  function easeInOut(t) { return t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2; }
  function smooth(e0, e1, x) { var t = clamp((x - e0) / (e1 - e0), 0, 1); return t * t * (3 - 2 * t); }

  /* ---------------------------------------------------------------- scheduler */
  var tickers = [];
  var rafId = 0;
  var lastNow = 0;

  function running() { return !reduced && !paused && !document.hidden; }

  function frame(now) {
    rafId = 0;
    var dt = lastNow ? Math.min(64, now - lastNow) : 16;
    lastNow = now;
    var any = false;
    for (var i = 0; i < tickers.length; i++) {
      var t = tickers[i];
      if (!t.visible) continue;
      t.time += dt;
      t.fn(t.time, dt);
      any = true;
    }
    if (any && running()) rafId = window.requestAnimationFrame(frame);
    else lastNow = 0;
  }

  function wake() {
    if (!rafId && running()) {
      lastNow = 0;
      rafId = window.requestAnimationFrame(frame);
    }
  }

  function addTicker(el, fn, onStatic) {
    var t = { fn: fn, onStatic: onStatic, visible: !el, time: 0 };
    tickers.push(t);
    if (el && "IntersectionObserver" in window) {
      new IntersectionObserver(function (entries) {
        t.visible = entries[entries.length - 1].isIntersecting;
        if (t.visible) wake();
      }, { rootMargin: "120px 0px" }).observe(el);
    } else {
      t.visible = true;
    }
    return t;
  }

  function renderStaticAll() {
    for (var i = 0; i < tickers.length; i++) if (tickers[i].onStatic) tickers[i].onStatic();
  }

  document.addEventListener("visibilitychange", wake);
  reduceQuery.addEventListener("change", function (e) {
    reduced = e.matches;
    updateToggle();
    refreshScroll();
    if (reduced) {
      renderStaticAll();
      revealAll();
    } else {
      wake();
    }
  });

  /* ---------------------------------------------------------------- pause toggle */
  var toggle = document.querySelector("[data-motion-toggle]");
  var toggleLabel = document.querySelector("[data-motion-label]");
  function updateToggle() {
    if (!toggle) return;
    toggle.hidden = reduced;
    toggle.setAttribute("aria-pressed", paused ? "true" : "false");
    toggleLabel.textContent = paused ? "Play animations" : "Pause animations";
    toggle.title = toggleLabel.textContent;
  }
  if (toggle) {
    toggle.addEventListener("click", function () {
      paused = !paused;
      root.classList.toggle("is-paused", paused);
      updateToggle();
      refreshScroll();
      wake();
    });
    updateToggle();
  }

  /* ---------------------------------------------------------------- reveals */
  var revealEls = Array.prototype.slice.call(document.querySelectorAll("[data-reveal]"));
  var revealHooks = [];

  function reveal(el) {
    if (el.classList.contains("is-in")) return;
    el.classList.add("is-in");
    for (var i = 0; i < revealHooks.length; i++) revealHooks[i](el);
  }
  function revealAll() { revealEls.forEach(reveal); }

  function setupReveals() {
    if (reduced || !("IntersectionObserver" in window)) {
      revealEls.forEach(function (el) { el.classList.add("is-in"); });
      return;
    }
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) {
        if (e.isIntersecting) {
          reveal(e.target);
          io.unobserve(e.target);
        }
      });
    }, { rootMargin: "0px 0px -12% 0px", threshold: 0 });
    var fold = window.innerHeight * 0.92;
    revealEls.forEach(function (el) {
      if (el.getBoundingClientRect().top > fold) {
        el.classList.add("is-armed");
        io.observe(el);
      } else {
        el.classList.add("is-in");
      }
    });
    window.addEventListener("beforeprint", revealAll);
  }

  /* ---------------------------------------------------------------- counters (real design facts only) */
  function setupCounters() {
    var nodes = Array.prototype.slice.call(document.querySelectorAll("[data-count]"));
    revealHooks.push(function (el) {
      if (reduced) return;
      nodes.forEach(function (n) {
        if (!el.contains(n) || !el.classList.contains("is-armed")) return;
        var target = parseInt(n.getAttribute("data-count"), 10);
        var start = 0;
        var dur = 1100;
        n.textContent = "0";
        function step(now) {
          if (!start) start = now;
          var p = clamp((now - start) / dur, 0, 1);
          n.textContent = String(Math.round(target * easeOutQuart(p)));
          if (p < 1) window.requestAnimationFrame(step);
        }
        window.requestAnimationFrame(step);
      });
    });
  }

  /* ---------------------------------------------------------------- nav */
  function setupNav() {
    var nav = document.querySelector("[data-nav]");
    if (nav && "IntersectionObserver" in window) {
      var sentinel = document.createElement("div");
      sentinel.setAttribute("aria-hidden", "true");
      sentinel.style.cssText = "position:absolute;left:0;top:0;width:1px;height:24px;pointer-events:none";
      document.body.appendChild(sentinel);
      new IntersectionObserver(function (entries) {
        nav.classList.toggle("is-scrolled", !entries[0].isIntersecting);
      }).observe(sentinel);
    }
    var links = Array.prototype.slice.call(document.querySelectorAll(".nav__links a"));
    var byId = {};
    links.forEach(function (a) { byId[a.getAttribute("href").slice(1)] = a; });
    if (!("IntersectionObserver" in window)) return;
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) {
        var a = byId[e.target.id];
        if (!a) return;
        if (e.isIntersecting) {
          links.forEach(function (l) { l.removeAttribute("aria-current"); });
          a.setAttribute("aria-current", "true");
        } else if (a.getAttribute("aria-current")) {
          a.removeAttribute("aria-current");
        }
      });
    }, { rootMargin: "-45% 0px -50% 0px" });
    Object.keys(byId).forEach(function (id) {
      var s = document.getElementById(id);
      if (s) io.observe(s);
    });
  }

  /* ---------------------------------------------------------------- scroll: progress thread, aurora and card parallax */
  var aurora = document.querySelector(".aurora");
  var deck = document.querySelector(".hero__deck");

  function setupScroll() {
    var fill = document.querySelector(".progress__fill");
    var cssDriven = window.CSS && CSS.supports && CSS.supports("animation-timeline: scroll()") && !reduced;
    var pending = false;
    function update() {
      pending = false;
      var max = document.documentElement.scrollHeight - window.innerHeight;
      var p = max > 0 ? clamp(window.scrollY / max, 0, 1) : 0;
      if (fill && !cssDriven) fill.style.setProperty("--progress", String(p));
      /* Parallax inputs live on the layers that use them, so a scroll never restyles the whole page.
         Reduced motion keeps the layers at rest; the pause toggle freezes them where they are. */
      if (paused) return;
      if (aurora) aurora.style.setProperty("--sp", reduced ? "0" : p.toFixed(4));
      if (deck) deck.style.setProperty("--hs", reduced ? "0" : String(Math.round(Math.min(window.scrollY, window.innerHeight))));
    }
    window.addEventListener("scroll", function () {
      if (!pending) { pending = true; window.requestAnimationFrame(update); }
    }, { passive: true });
    window.addEventListener("resize", update);
    update();
    return update;
  }

  /* ---------------------------------------------------------------- cursor glow + magnetic buttons */
  function setupPointer() {
    if (!fineQuery.matches) return;
    var glow = document.querySelector(".glow");
    var tx = -1000, ty = -1000, gx = -1000, gy = -1000;
    var glowTicker = null;
    if (glow) {
      glowTicker = addTicker(null, function () {
        gx = lerp(gx, tx, 0.14);
        gy = lerp(gy, ty, 0.14);
        glow.style.setProperty("--gx", gx.toFixed(1) + "px");
        glow.style.setProperty("--gy", gy.toFixed(1) + "px");
        if (Math.abs(gx - tx) < 0.5 && Math.abs(gy - ty) < 0.5) glowTicker.visible = false;
      });
      window.addEventListener("pointermove", function (e) {
        if (reduced) return;
        tx = e.clientX; ty = e.clientY;
        if (gx < -900) { gx = tx; gy = ty; }
        glow.classList.add("is-on");
        glowTicker.visible = true;
        if (deck && !paused) {
          deck.style.setProperty("--px", (tx / window.innerWidth * 2 - 1).toFixed(3));
          deck.style.setProperty("--py", (ty / window.innerHeight * 2 - 1).toFixed(3));
        }
        wake();
      }, { passive: true });
      document.addEventListener("pointerleave", function () {
        glow.classList.remove("is-on");
        if (deck) { deck.style.setProperty("--px", "0"); deck.style.setProperty("--py", "0"); }
      });
    }

    Array.prototype.slice.call(document.querySelectorAll("[data-magnetic]")).forEach(function (btn) {
      btn.addEventListener("pointermove", function (e) {
        if (reduced || paused) return;
        var r = btn.getBoundingClientRect();
        var dx = (e.clientX - (r.left + r.width / 2)) / (r.width / 2);
        var dy = (e.clientY - (r.top + r.height / 2)) / (r.height / 2);
        btn.style.setProperty("--mx", (clamp(dx, -1, 1) * 6).toFixed(1) + "px");
        btn.style.setProperty("--my", (clamp(dy, -1, 1) * 4).toFixed(1) + "px");
      });
      btn.addEventListener("pointerleave", function () {
        btn.style.setProperty("--mx", "0px");
        btn.style.setProperty("--my", "0px");
      });
    });
    return glowTicker;
  }

  /* ---------------------------------------------------------------- hero: three strands into the M, then the cut */
  var GILT = "184,144,63";
  var GILT_HI = "217,184,114";
  var MARK = [0, 1, 2].map(function (i) {
    var a = 18 + 8 * i;
    var top = [9.96, 30, 50.04][i];
    var mid = [58.38, 70, 81.62][i];
    var legX = 128 - a;
    return { a: a, top: top, mid: mid, legX: legX, cutY: 85.7 - 0.38 * (legX - 87), cornerY: 106 + 8 * i };
  });
  var INTRO_DRAW = 2100;
  var STRAND_DELAY = 140;
  var CUT_AT = 2450;
  var CUT_DUR = 200;

  function setupHero() {
    var canvas = document.querySelector("[data-hero-canvas]");
    if (!canvas || !canvas.getContext) return;
    var ctx = canvas.getContext("2d");
    var hero = canvas.parentElement;
    var W = 0, H = 0, dpr = 1, S = 0, k = 1, x0 = 0, y0 = 0, amp = 0, lw = 2, textRight = 0;
    var copy = hero.querySelector(".hero__copy");
    var pts = [[], [], []];

    function layout() {
      var rect = hero.getBoundingClientRect();
      W = Math.max(1, Math.round(rect.width));
      H = Math.max(1, Math.round(rect.height));
      dpr = Math.min(window.devicePixelRatio || 1, 2);
      canvas.width = Math.round(W * dpr);
      canvas.height = Math.round(H * dpr);
      var cr = copy ? copy.getBoundingClientRect() : null;
      textRight = W >= 900 && cr ? cr.right - rect.left : 0;
      if (W >= 1200) {
        /* Wide: the mark sits right of the copy with room for the floating glass cards (CSS reads --mk-*). */
        var navEl = document.querySelector(".nav__inner");
        var navBottom = navEl ? navEl.getBoundingClientRect().bottom : 76;
        var right = Math.min(W - 48, W / 2 + 640);
        S = Math.min(H * 0.46, W * 0.28, 440);
        x0 = right - S * 0.66 - 288; /* 288 = .dk--ballot width in landing.css */
        y0 = Math.max(navBottom + 24, (navBottom + H) / 2 - 58 * S / 128);
        amp = H * 0.06;
      } else if (W >= 900) {
        S = Math.min(H * 0.6, W * 0.34, 560);
        x0 = W * 0.93 - S;
        /* The strands run in low, under the call to action, then climb into the mark. */
        y0 = Math.max(88, H * 0.86 - 114 * S / 128);
        amp = H * 0.07;
      } else {
        S = Math.min(W * 0.5, H * 0.28, 300);
        x0 = W - S - Math.max(16, W * 0.06);
        y0 = H - S * (130 / 128) - Math.max(24, H * 0.04);
        amp = H * 0.05;
      }
      k = S / 128;
      lw = Math.max(1.75, 3.2 * k * 0.72);
      hero.style.setProperty("--mk-x", x0.toFixed(1) + "px");
      hero.style.setProperty("--mk-y", y0.toFixed(1) + "px");
      hero.style.setProperty("--mk-s", S.toFixed(1) + "px");
    }

    function price(x, t, i) {
      var q = x * 0.0042 - t * 0.00032;
      var shared = 0.52 * Math.sin(q) + 0.3 * Math.sin(q * 2.3 + 1.7) + 0.14 * Math.sin(q * 5.9 + 0.4) + 0.07 * Math.sin(q * 13.7 + 2.2);
      var qi = x * 0.011 - t * (0.0005 + i * 0.00007) + i * 2.1;
      var own = Math.sin(qi) * 0.6 + Math.sin(qi * 3.1 + i) * 0.4;
      return shared + own * 0.16;
    }

    function build(t) {
      for (var i = 0; i < 3; i++) {
        var m = MARK[i];
        var cx = x0 + m.a * k;
        var cy = y0 + m.cornerY * k;
        var p = pts[i];
        p.length = 0;
        var step = W < 900 ? 5 : 6;
        for (var x = -12; x < cx; x += step) {
          var u = (cx - x) / Math.max(1, cx);
          var env = smooth(0.02, 0.4, u);
          var spread = (i - 1) * 10 * k * 0.25 * env * (1 + u);
          p.push(x, cy + env * amp * price(x, t, i) + spread);
        }
        p.push(cx, cy);
      }
    }

    function polyLen(arr) {
      var len = 0;
      for (var j = 2; j < arr.length; j += 2) len += Math.hypot(arr[j] - arr[j - 2], arr[j + 1] - arr[j - 1]);
      return len;
    }

    function strandPath(i, gap) {
      var m = MARK[i];
      var p = pts[i];
      var X = function (v) { return x0 + v * k; };
      var Y = function (v) { return y0 + v * k; };
      ctx.beginPath();
      ctx.moveTo(p[0], p[1]);
      for (var j = 2; j < p.length; j += 2) ctx.lineTo(p[j], p[j + 1]);
      ctx.lineTo(X(m.a), Y(m.top));
      ctx.lineTo(X(64), Y(m.mid));
      ctx.lineTo(X(m.legX), Y(m.top));
      if (gap > 0.01) {
        ctx.lineTo(X(m.legX), Y(m.cutY - gap));
        ctx.moveTo(X(m.legX), Y(m.cutY + gap));
      }
      ctx.lineTo(X(m.legX), Y(106));
    }

    function markLen(i) {
      var m = MARK[i];
      return (m.cornerY - m.top) + Math.hypot(64 - m.a, m.mid - m.top) * 2 + (106 - m.top);
    }

    function draw(t, isStatic) {
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, W, H);
      /* A collapsed hero (mid-resize, print, full-page capture) has no room for the mark; draw nothing. */
      if (W < 64 || H < 64) return;
      build(isStatic ? 0 : t);
      ctx.lineJoin = "miter";
      ctx.miterLimit = 12;
      ctx.lineCap = "butt";

      var cutP = isStatic ? 1 : clamp((t - CUT_AT) / CUT_DUR, 0, 1);
      var gapP = isStatic ? 1 : easeOutExpo(clamp((t - CUT_AT - CUT_DUR * 0.6) / 500, 0, 1));
      var gap = 4.5 * gapP;

      for (var i = 0; i < 3; i++) {
        var total = polyLen(pts[i]) + markLen(i) * k;
        var cx0 = x0 + MARK[i].a * k;
        var xEnd = x0 + 128 * k;
        var f = cx0 / xEnd;
        var grad = ctx.createLinearGradient(0, 0, xEnd, 0);
        var dim = textRight > 0 ? clamp(textRight / cx0, 0.3, 0.8) : 0.45;
        /* The tail fades in from the left; inside the mark the gilt warms from #B8903F to #D9B872 and back. */
        grad.addColorStop(0, "rgba(" + GILT + ",0)");
        grad.addColorStop(f * dim * 0.6, "rgba(" + GILT + ",0.16)");
        grad.addColorStop(f * dim, "rgba(" + GILT + ",0.34)");
        grad.addColorStop(f, "rgba(" + GILT + ",1)");
        grad.addColorStop(f + (1 - f) * 0.45, "rgba(" + GILT_HI + ",1)");
        grad.addColorStop(1, "rgba(" + GILT + ",1)");
        var p = isStatic ? 1 : easeInOut(clamp((t - i * STRAND_DELAY) / INTRO_DRAW, 0, 1));
        if (p <= 0) continue;
        if (p < 1) ctx.setLineDash([total * p, total]);
        else ctx.setLineDash([]);
        ctx.lineDashOffset = 0;
        ctx.strokeStyle = grad;
        ctx.lineWidth = lw;
        strandPath(i, gap);
        ctx.stroke();

        /* A glint rides each strand into the mark every few seconds once the cut has happened. */
        if (!isStatic && t > CUT_AT + 800) {
          var period = 6200;
          var local = (t - CUT_AT - 800 + i * 1700) % period;
          var run = 2600;
          if (local < run) {
            var head = easeInOut(local / run) * total;
            var seg = Math.max(40, total * 0.06);
            ctx.setLineDash([seg, total * 2]);
            ctx.lineDashOffset = -(head - seg);
            ctx.strokeStyle = "rgba(255,255,255," + (0.9 * Math.sin(Math.PI * local / run)).toFixed(3) + ")";
            ctx.lineWidth = lw;
            strandPath(i, gap);
            ctx.stroke();
          }
        }
      }
      ctx.setLineDash([]);

      /* The blade: Atropos cuts the last leg. */
      if (cutP > 0) {
        var bx0 = x0 + 117 * k, by0 = y0 + 74.3 * k, bx1 = x0 + 87 * k, by1 = y0 + 85.7 * k;
        var e = easeOutQuart(cutP);
        ctx.strokeStyle = "#8E2424";
        ctx.lineWidth = Math.max(1.25, lw * 0.75);
        ctx.beginPath();
        ctx.moveTo(bx0, by0);
        ctx.lineTo(lerp(bx0, bx1, e), lerp(by0, by1, e));
        ctx.stroke();
        var flash = isStatic ? 0 : 1 - clamp((t - CUT_AT - CUT_DUR) / 600, 0, 1);
        if (flash > 0 && cutP >= 1) {
          ctx.strokeStyle = "rgba(142,36,36," + (0.4 * flash).toFixed(3) + ")";
          ctx.lineWidth = Math.max(1, lw * 0.35);
          ctx.beginPath();
          ctx.moveTo(lerp(bx1, bx0, 1.25), lerp(by1, by0, 1.25));
          ctx.lineTo(lerp(bx0, bx1, 1.25), lerp(by0, by1, 1.25));
          ctx.stroke();
        }
      }
    }

    var ticker;
    function drawStatic() { draw(0, true); }
    layout();
    if ("ResizeObserver" in window) {
      new ResizeObserver(function () {
        layout();
        if (reduced || !ticker.visible || !running()) draw(ticker.time, reduced);
      }).observe(hero);
    }
    ticker = addTicker(hero, function (t) { draw(t, false); }, drawStatic);
    if (reduced) drawStatic();
  }

  /* ---------------------------------------------------------------- pipeline: scroll-driven path + packets */
  function setupPipeline() {
    var section = document.querySelector("[data-pipeline]");
    if (!section) return;
    var steps = Array.prototype.slice.call(section.querySelectorAll("[data-step]"));
    var nodes = Array.prototype.slice.call(section.querySelectorAll("[data-pl-node]"));
    var line = section.querySelector("[data-pl-line]");
    var loop = section.querySelector("[data-pl-loop]");
    var packetsG = section.querySelector("[data-pl-packets]");
    var list = section.querySelector("[data-steps]");
    var NODE_Y = function (i) { return 40 + i * 86; };
    var active = -1;

    function setActive(i) {
      if (i === active) return;
      active = i;
      steps.forEach(function (s, j) {
        s.classList.toggle("is-active", j === i);
        s.classList.toggle("is-past", j < i);
      });
      nodes.forEach(function (n, j) {
        n.classList.toggle("is-active", j === i);
        n.classList.toggle("is-past", j < i);
      });
      if (line) line.style.setProperty("--pl-offset", String(602 - (NODE_Y(Math.max(0, i)) - 40)));
      section.classList.toggle("is-looped", i === steps.length - 1);
      list.style.setProperty("--steps-progress", String((i + 1) / steps.length));
    }

    list.style.setProperty("--steps-progress", "0");
    if ("IntersectionObserver" in window) {
      var io = new IntersectionObserver(function (entries) {
        entries.forEach(function (e) {
          if (e.isIntersecting) setActive(parseInt(e.target.getAttribute("data-step"), 10));
        });
      }, { rootMargin: "-45% 0px -45% 0px" });
      steps.forEach(function (s) { io.observe(s); });
    } else {
      setActive(steps.length - 1);
    }

    if (!packetsG) return;
    var NS = "http://www.w3.org/2000/svg";
    var pool = [];
    for (var n = 0; n < 8; n++) {
      var c = document.createElementNS(NS, "circle");
      c.setAttribute("class", "pl-packet");
      c.setAttribute("r", "4");
      c.setAttribute("cx", "80");
      c.setAttribute("opacity", "0");
      packetsG.appendChild(c);
      pool.push(c);
    }
    var loopLen = loop && loop.getTotalLength ? loop.getTotalLength() : 0;
    var SPEED = 0.22; /* px of the 690-unit diagram per ms */
    var GAP = 520;

    addTicker(section, function (t) {
      var endY = NODE_Y(Math.max(0, active));
      var span = endY - 40;
      var dur = Math.max(1, span / SPEED);
      for (var j = 0; j < 5; j++) {
        var c2 = pool[j];
        var local = (t - j * GAP) % (GAP * 5);
        if (active < 1 || local < 0 || local > dur) { c2.setAttribute("opacity", "0"); continue; }
        var y = 40 + local * SPEED;
        c2.setAttribute("cx", "80");
        c2.setAttribute("cy", y.toFixed(1));
        c2.setAttribute("opacity", (1 - smooth(0.85, 1, local / dur)).toFixed(3));
      }
      for (var m = 5; m < 8; m++) {
        var c3 = pool[m];
        if (active !== steps.length - 1 || !loopLen) { c3.setAttribute("opacity", "0"); continue; }
        var ldur = 2600;
        var lt = (t + (m - 5) * 870) % ldur;
        var pt = loop.getPointAtLength(easeInOut(lt / ldur) * loopLen);
        c3.setAttribute("cx", pt.x.toFixed(1));
        c3.setAttribute("cy", pt.y.toFixed(1));
        c3.setAttribute("opacity", (Math.sin(Math.PI * lt / ldur) * 0.9).toFixed(3));
      }
    }, function () {
      pool.forEach(function (c4) { c4.setAttribute("opacity", "0"); });
    });
  }

  /* ---------------------------------------------------------------- council: blind round, debate, decision */
  var AGENTS = [[320, 80], [493.2, 180], [493.2, 380], [320, 480], [146.8, 380], [146.8, 180]];
  var CENTER = [320, 280];
  /* Claim exchanges per debate round; opposite agents are skipped so no chord hides behind the Manager. */
  var ROUNDS = [[[0, 2], [1, 5], [3, 4]], [[0, 1], [2, 4], [3, 5]], [[0, 5], [1, 3], [2, 4]]];
  var T_BLIND = 3000, T_ROUND = 2400, T_DECIDE = 2600, T_HOLD = 1400;
  var T_DEBATE_END = T_BLIND + T_ROUND * ROUNDS.length;
  var T_LOOP = T_DEBATE_END + T_DECIDE + T_HOLD;
  var CAPTIONS = [
    "Blind round: each agent answers alone.",
    "Debate round 1 of up to 3: anonymous claims, checked by code.",
    "Debate round 2 of up to 3: anonymous claims, checked by code.",
    "Debate round 3 of up to 3: anonymous claims, checked by code.",
    "The fixed-rule Manager decides and hands the order to Risk."
  ];

  function setupCouncil() {
    var section = document.querySelector("[data-council]");
    if (!section) return;
    var NS = "http://www.w3.org/2000/svg";
    var chordsG = section.querySelector("[data-c-chords]");
    var packetsG = section.querySelector("[data-c-packets]");
    var spokes = section.querySelector("[data-c-spokes]");
    var out = section.querySelector("[data-c-out]");
    var caption = section.querySelector("[data-c-caption]");
    var agents = Array.prototype.slice.call(section.querySelectorAll("[data-c-agent]"));
    var phases = Array.prototype.slice.call(section.querySelectorAll("[data-c-phase]"));
    var fills = phases.map(function (p) { return p.querySelector(".timeline__fill"); });

    /* Packet schedule: blind answers to the Manager, claims along chords, final collection, decision. */
    var schedule = [];
    AGENTS.forEach(function (a, i) {
      schedule.push({ from: a, to: CENTER, t0: 400 + i * 90, dur: 1700, claim: false, speaker: i });
    });
    var chordEls = [];
    ROUNDS.forEach(function (pairs, r) {
      var rs = T_BLIND + r * T_ROUND;
      pairs.forEach(function (pair, j) {
        var ln = document.createElementNS(NS, "path");
        ln.setAttribute("d", "M" + AGENTS[pair[0]][0] + " " + AGENTS[pair[0]][1] + "L" + AGENTS[pair[1]][0] + " " + AGENTS[pair[1]][1]);
        ln.setAttribute("pathLength", "1");
        ln.setAttribute("stroke-dasharray", "1");
        ln.setAttribute("opacity", "0");
        chordsG.appendChild(ln);
        chordEls.push({ el: ln, t0: rs + j * 120, t1: rs + T_ROUND });
        schedule.push({ from: AGENTS[pair[0]], to: AGENTS[pair[1]], t0: rs + 350 + j * 120, dur: 1100, claim: true, speaker: pair[0] });
        schedule.push({ from: AGENTS[pair[1]], to: AGENTS[pair[0]], t0: rs + 700 + j * 120, dur: 1100, claim: true, speaker: pair[1] });
      });
    });
    AGENTS.forEach(function (a, i) {
      schedule.push({ from: a, to: CENTER, t0: T_DEBATE_END + 100 + i * 50, dur: 900, claim: false, speaker: -1 });
    });
    schedule.push({ from: [364, 280], to: [636, 280], t0: T_DEBATE_END + 1500, dur: 900, claim: false, speaker: -1 });
    schedule.forEach(function (s) {
      var c = document.createElementNS(NS, "circle");
      c.setAttribute("class", "c-packet");
      c.setAttribute("r", s.claim ? "6" : "5");
      c.setAttribute("opacity", "0");
      packetsG.appendChild(c);
      s.el = c;
      s.verified = null;
    });

    var lastCaption = -1;
    var lastPhase = -1;

    function render(tau) {
      var phase = tau < T_BLIND ? 0 : tau < T_DEBATE_END ? 1 : 2;
      var round = phase === 1 ? Math.floor((tau - T_BLIND) / T_ROUND) : -1;
      var capIdx = phase === 0 ? 0 : phase === 1 ? 1 + round : 4;
      if (capIdx !== lastCaption) { caption.textContent = CAPTIONS[capIdx]; lastCaption = capIdx; }
      if (phase !== lastPhase) {
        phases.forEach(function (p, j) { p.classList.toggle("is-current", j === phase); });
        section.classList.toggle("is-blind", phase === 0);
        section.classList.toggle("is-deciding", phase === 2 && tau < T_DEBATE_END + T_DECIDE);
        spokes.classList.toggle("is-lit", phase !== 1);
        lastPhase = phase;
      }
      if (phase === 2 && tau >= T_DEBATE_END + T_DECIDE) section.classList.remove("is-deciding");
      fills[0].style.setProperty("--fill", clamp(tau / T_BLIND, 0, 1).toFixed(3));
      fills[1].style.setProperty("--fill", clamp((tau - T_BLIND) / (T_DEBATE_END - T_BLIND), 0, 1).toFixed(3));
      fills[2].style.setProperty("--fill", clamp((tau - T_DEBATE_END) / T_DECIDE, 0, 1).toFixed(3));

      chordEls.forEach(function (c) {
        var inP = clamp((tau - c.t0) / 500, 0, 1);
        var outP = clamp((tau - c.t1 + 300) / 300, 0, 1);
        c.el.setAttribute("stroke-dashoffset", (1 - easeOutQuart(inP)).toFixed(3));
        c.el.setAttribute("opacity", (inP > 0 ? 0.55 * (1 - outP) : 0).toFixed(3));
      });

      var speaking = [false, false, false, false, false, false];
      schedule.forEach(function (s) {
        var p = (tau - s.t0) / s.dur;
        if (p < 0 || p > 1.35) { s.el.setAttribute("opacity", "0"); return; }
        var e = easeInOut(clamp(p, 0, 1));
        s.el.setAttribute("cx", lerp(s.from[0], s.to[0], e).toFixed(1));
        s.el.setAttribute("cy", lerp(s.from[1], s.to[1], e).toFixed(1));
        s.el.setAttribute("opacity", (p <= 1 ? 1 : 1 - (p - 1) / 0.35).toFixed(3));
        var verified = s.claim && p >= 1;
        if (verified !== s.verified) { s.el.classList.toggle("is-verified", verified); s.verified = verified; }
        if (s.speaker >= 0 && p < 0.25) speaking[s.speaker] = true;
      });
      agents.forEach(function (a, i) { a.classList.toggle("is-speaking", speaking[i]); });

      var outP2 = clamp((tau - T_DEBATE_END - 1400) / 900, 0, 1);
      var fade = clamp((tau - (T_LOOP - 500)) / 500, 0, 1);
      out.style.setProperty("--c-out", (1 - easeOutQuart(outP2) * (1 - fade)).toFixed(3));
    }

    function renderStatic() {
      section.classList.remove("is-blind", "is-deciding");
      spokes.classList.add("is-lit");
      phases.forEach(function (p) { p.classList.remove("is-current"); });
      fills.forEach(function (f) { f.style.setProperty("--fill", "1"); });
      chordEls.forEach(function (c) {
        c.el.setAttribute("stroke-dashoffset", "0");
        c.el.setAttribute("opacity", "0.35");
      });
      schedule.forEach(function (s) { s.el.setAttribute("opacity", "0"); });
      agents.forEach(function (a) { a.classList.remove("is-speaking"); });
      out.style.setProperty("--c-out", "0");
      caption.textContent = "Blind round, up to three debate rounds, then the Manager decides.";
    }

    addTicker(section, function (t) { render(t % T_LOOP); }, renderStatic);
    if (reduced) renderStatic();
    else render(0);
  }

  /* ---------------------------------------------------------------- boot */
  setupReveals();
  setupCounters();
  setupNav();
  refreshScroll = setupScroll();
  setupPointer();
  setupHero();
  setupPipeline();
  setupCouncil();
  wake();
})();
