/*
 * Browser-side sealing core for the console API key form (no DOM access, no network access).
 *
 * The plaintext secret is serialized as canonical JSON (keys sorted, compact separators, UTF-8; the
 * same rules as hdt.core.ids.canonical_json), sealed with libsodium crypto_box_seal (X25519 +
 * XSalsa20-Poly1305, compatible with PyNaCl SealedBox) under the PUBLIC key of the owning scope, and
 * only three values leave this function: the sealed blob, the last 4 characters of the primary key
 * string and the first 16 hex characters of sha256(plaintext JSON bytes).
 */
(function (root, factory) {
  "use strict";
  var api = factory();
  if (typeof module === "object" && module && module.exports) {
    module.exports = api;
  }
  root.HdtSealCore = api;
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  var MIN_SECRET_LENGTH = 12;
  var RESULT_KEYS = ["fingerprint", "last4", "sealed_blob"];

  function canonicalize(value) {
    if (Array.isArray(value)) {
      return value.map(canonicalize);
    }
    if (value !== null && typeof value === "object") {
      var out = {};
      Object.keys(value)
        .sort()
        .forEach(function (key) {
          out[key] = canonicalize(value[key]);
        });
      return out;
    }
    if (typeof value === "number" && !isFinite(value)) {
      throw new Error("NaN and Infinity are not allowed");
    }
    return value;
  }

  function canonicalJson(value) {
    return JSON.stringify(canonicalize(value));
  }

  function parseUserIds(text) {
    var parts = String(text || "")
      .split(/[\s,;]+/)
      .filter(function (p) {
        return p.length > 0;
      });
    if (parts.length === 0) {
      throw new Error("Enter at least one Telegram user id.");
    }
    var seen = {};
    var ids = [];
    parts.forEach(function (p) {
      if (!/^[1-9][0-9]{0,15}$/.test(p)) {
        throw new Error("Telegram user ids are positive whole numbers.");
      }
      var n = Number(p);
      if (!Number.isSafeInteger(n)) {
        throw new Error("Telegram user id is too large.");
      }
      if (!seen[n]) {
        seen[n] = true;
        ids.push(n);
      }
    });
    return ids;
  }

  function validateSecretString(label, value) {
    if (typeof value !== "string" || value.length === 0) {
      throw new Error(label + " is required.");
    }
    if (value !== value.trim()) {
      throw new Error(label + " must not start or end with spaces.");
    }
    if (value.length < MIN_SECRET_LENGTH) {
      throw new Error(label + " looks too short (at least " + MIN_SECRET_LENGTH + " characters).");
    }
  }

  /*
   * sodium: the initialized libsodium-wrappers object.
   * publicKeyB64: scope X25519 public key, standard base64 (32 bytes).
   * payload: plain object with the secret fields (strings, or an integer list for allowed_user_ids).
   * primaryField: field whose last 4 characters are shown on the API keys page.
   */
  function sealSecret(sodium, publicKeyB64, payload, primaryField) {
    var publicKey;
    try {
      publicKey = sodium.from_base64(String(publicKeyB64 || ""), sodium.base64_variants.ORIGINAL);
    } catch (e) {
      throw new Error("The scope public key is not valid base64.");
    }
    if (publicKey.length !== sodium.crypto_box_PUBLICKEYBYTES) {
      throw new Error("The scope public key must be 32 bytes.");
    }
    var primary = payload[primaryField];
    validateSecretString("The key", primary);
    var plaintext = sodium.from_string(canonicalJson(payload));
    var sealed = sodium.crypto_box_seal(plaintext, publicKey);
    var digest = sodium.crypto_hash_sha256(plaintext);
    var result = {
      sealed_blob: sodium.to_base64(sealed, sodium.base64_variants.ORIGINAL),
      last4: primary.slice(-4),
      fingerprint: sodium.to_hex(digest).slice(0, 16),
    };
    sodium.memzero(plaintext);
    sodium.memzero(digest);
    return result;
  }

  return {
    MIN_SECRET_LENGTH: MIN_SECRET_LENGTH,
    RESULT_KEYS: RESULT_KEYS,
    canonicalJson: canonicalJson,
    parseUserIds: parseUserIds,
    validateSecretString: validateSecretString,
    sealSecret: sealSecret,
  };
});
