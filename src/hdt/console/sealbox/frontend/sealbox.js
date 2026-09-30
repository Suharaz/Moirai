/*
 * Sealbox: Streamlit custom component (component protocol v1, plain JS, no build step).
 *
 * Renders write-only inputs for one secret, seals the plaintext in this browser frame with the scope
 * PUBLIC key (sealcore.js + vendored libsodium) and returns exactly {sealed_blob, last4, fingerprint}
 * to the console. Plaintext never leaves this frame: inputs are cleared right after sealing, nothing is
 * logged, there is no reveal control, and the frame's CSP forbids network connections.
 */
(function () {
  "use strict";

  var core = window.HdtSealCore;
  var root = document.getElementById("root");
  var ORIGIN = window.location.origin;
  var TOKEN_VALUE = /^(#[0-9a-fA-F]{3,8}|rgba?\([0-9., ]+\))$/;
  var TOKEN_NAME = /^[a-z0-9-]{1,24}$/;
  var FONTS = [
    ["Fira Sans", "fira-sans-latin-400-normal.woff2", "400"],
    ["Fira Sans", "fira-sans-latin-600-normal.woff2", "600"],
    ["Fira Code", "fira-code-latin-400-normal.woff2", "400"],
  ];

  var view = null; // {signature, inputs: [{spec, el}], button, error, status}
  var ready = false;
  var fontsLoaded = false;

  function post(type, fields) {
    var message = { isStreamlitMessage: true, type: type };
    Object.keys(fields || {}).forEach(function (k) {
      message[k] = fields[k];
    });
    window.parent.postMessage(message, ORIGIN);
  }

  function reportHeight() {
    post("streamlit:setFrameHeight", { height: Math.ceil(document.documentElement.scrollHeight) + 2 });
  }

  function loadFonts() {
    if (fontsLoaded || typeof FontFace !== "function" || !document.fonts) {
      return;
    }
    fontsLoaded = true;
    FONTS.forEach(function (f) {
      // Component frames live at <base>/component/<name>/index.html; fonts are served by the console at
      // <base>/app/static/fonts/ (same origin, allowed by font-src 'self').
      var url = new URL("../../app/static/fonts/" + f[1], window.location.href).href;
      var face = new FontFace(f[0], "url(" + url + ")", { weight: f[2], style: "normal" });
      face.load().then(
        function (loaded) {
          document.fonts.add(loaded);
        },
        function () {
          /* the system fallback font stays in use */
        },
      );
    });
  }

  function applyTokens(tokens) {
    if (!tokens || typeof tokens !== "object") {
      return;
    }
    Object.keys(tokens).forEach(function (name) {
      var value = String(tokens[name]);
      if (TOKEN_NAME.test(name) && TOKEN_VALUE.test(value)) {
        document.documentElement.style.setProperty("--" + name, value);
      }
    });
  }

  function el(tag, cls, text) {
    var node = document.createElement(tag);
    if (cls) {
      node.className = cls;
    }
    if (text !== undefined && text !== null) {
      node.textContent = text;
    }
    return node;
  }

  function signatureOf(args) {
    var fields = (args.fields || []).map(function (f) {
      return f.name + ":" + f.kind;
    });
    return [args.scope, args.name, args.public_key, args.nonce, fields.join(",")].join("|");
  }

  function clearInputs() {
    if (!view) {
      return;
    }
    view.inputs.forEach(function (item) {
      item.el.value = "";
      item.el.removeAttribute("aria-invalid");
    });
  }

  function setError(message, input) {
    view.error.textContent = message || "";
    if (input) {
      input.setAttribute("aria-invalid", "true");
      input.focus();
    }
    reportHeight();
  }

  function collectPayload() {
    var payload = {};
    for (var i = 0; i < view.inputs.length; i += 1) {
      var item = view.inputs[i];
      var raw = item.el.value;
      item.el.removeAttribute("aria-invalid");
      try {
        if (item.spec.kind === "user_ids") {
          payload[item.spec.name] = core.parseUserIds(raw);
        } else {
          core.validateSecretString(item.spec.label, raw);
          payload[item.spec.name] = raw;
        }
      } catch (e) {
        setError(e.message, item.el);
        return null;
      }
    }
    return payload;
  }

  function onSeal(args) {
    if (!ready || view.button.disabled) {
      return;
    }
    setError("");
    var payload = collectPayload();
    if (!payload) {
      return;
    }
    view.button.disabled = true;
    view.button.classList.add("is-loading");
    var result;
    try {
      result = core.sealSecret(window.sodium, args.public_key, payload, args.primary);
    } catch (e) {
      payload = null;
      view.button.disabled = false;
      view.button.classList.remove("is-loading");
      setError(e.message);
      return;
    }
    payload = null;
    clearInputs();
    view.button.classList.remove("is-loading");
    view.button.disabled = false;
    view.status.textContent =
      "Sealed in this browser. Key ..." + result.last4 + ", fingerprint " + result.fingerprint +
      ". The inputs were cleared; confirm with a TOTP code below to save.";
    post("streamlit:setComponentValue", {
      value: { sealed_blob: result.sealed_blob, last4: result.last4, fingerprint: result.fingerprint },
      dataType: "json",
    });
    reportHeight();
  }

  function build(args) {
    root.textContent = "";
    var note = el("div", "callout");
    note.setAttribute("role", "note");
    note.appendChild(document.createTextNode("The key is sealed in your browser with the public key of scope "));
    note.appendChild(el("b", "mono", String(args.scope || "")));
    note.appendChild(
      document.createTextNode(
        ". Only the owning service can decrypt it. It cannot be viewed again after saving.",
      ),
    );
    root.appendChild(note);

    var inputs = [];
    (args.fields || []).forEach(function (spec, idx) {
      var field = el("div", "field");
      var id = "sb-" + idx;
      var label = el("label", null, spec.label);
      label.setAttribute("for", id);
      var input = el("input", "input mono");
      input.id = id;
      input.type = spec.kind === "user_ids" ? "text" : "password";
      input.autocomplete = "off";
      input.spellcheck = false;
      input.setAttribute("autocapitalize", "off");
      input.setAttribute("autocorrect", "off");
      input.setAttribute("data-lpignore", "true");
      field.appendChild(label);
      field.appendChild(input);
      if (spec.help) {
        var help = el("span", "help", spec.help);
        help.id = id + "-h";
        input.setAttribute("aria-describedby", help.id);
        field.appendChild(help);
      }
      root.appendChild(field);
      inputs.push({ spec: spec, el: input });
    });

    var error = el("div", "err");
    error.setAttribute("role", "alert");
    root.appendChild(error);

    var row = el("div", "row");
    var button = el("button", "btn", "Seal in browser");
    button.type = "button";
    button.disabled = !ready;
    row.appendChild(button);
    root.appendChild(row);

    var status = el("div", "status");
    status.setAttribute("aria-live", "polite");
    status.textContent = ready ? "" : "Loading the sealing library...";
    root.appendChild(status);

    view = { signature: signatureOf(args), inputs: inputs, button: button, error: error, status: status };
    button.addEventListener("click", function () {
      onSeal(args);
    });
    inputs.forEach(function (item) {
      item.el.addEventListener("keydown", function (event) {
        if (event.key === "Enter") {
          event.preventDefault();
          onSeal(args);
        }
      });
    });
  }

  function render(args) {
    applyTokens(args.tokens);
    loadFonts();
    if (!view || view.signature !== signatureOf(args)) {
      build(args);
    }
    view.button.disabled = !ready || args.locked === true;
    reportHeight();
  }

  window.addEventListener("message", function (event) {
    if (event.source !== window.parent || event.origin !== ORIGIN) {
      return;
    }
    var data = event.data;
    if (!data || data.type !== "streamlit:render") {
      return;
    }
    render(data.args || {});
  });

  window.sodium.ready.then(
    function () {
      ready = true;
      if (view) {
        view.button.disabled = false;
        view.status.textContent = "";
        reportHeight();
      }
    },
    function () {
      if (view) {
        view.status.textContent = "The sealing library failed to load. Reload the page.";
        reportHeight();
      }
    },
  );

  post("streamlit:componentReady", { apiVersion: 1 });
})();
