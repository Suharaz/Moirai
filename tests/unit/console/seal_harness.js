// Test harness: runs the console sealbox frontend scripts exactly as a browser loads them (plain
// <script> globals, no module system, no node APIs visible to the scripts) inside an isolated vm
// context, then seals one payload. Only the Web Crypto random source is provided, as in a browser.
// Usage: node seal_harness.js <frontend_dir> <public_key_b64> <payload_json> <primary_field>
"use strict";
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const [dir, publicKey, payloadJson, primaryField] = process.argv.slice(2);
const context = { crypto: globalThis.crypto, setTimeout: globalThis.setTimeout };
context.self = context;
context.window = context;
vm.createContext(context);
for (const file of ["vendor/libsodium-sumo.js", "vendor/libsodium-wrappers-sumo.js", "sealcore.js"]) {
  vm.runInContext(fs.readFileSync(path.join(dir, file), "utf8"), context, { filename: file });
}
context.payloadJson = payloadJson;
context.publicKey = publicKey;
context.primaryField = primaryField;
vm.runInContext(
  "sodium.ready.then(function () {" +
    "  var payload = JSON.parse(payloadJson);" +
    "  var result = HdtSealCore.sealSecret(sodium, publicKey, payload, primaryField);" +
    "  return JSON.stringify({ result: result, canonical: HdtSealCore.canonicalJson(payload) });" +
    "})",
  context,
)
  .then((text) => process.stdout.write(text))
  .catch((err) => {
    process.stdout.write(JSON.stringify({ error: String(err && err.message ? err.message : err) }));
    process.exitCode = 2;
  });
