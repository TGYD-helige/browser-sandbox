import assert from "node:assert/strict";
import test from "node:test";
import { validateAuthRequest } from "../dist/browser-auth.js";

const spec = {
  targetId: "page",
  origin: "https://example.com",
  fields: [{ id: "username", label: "Email", type: "email", selector: "#email" }],
};
test("authentication validates exact origins, bounded fields and method choices", () => {
  assert.equal(validateAuthRequest(spec).timeoutMs, 300_000);
  for (const origin of [
    "http://example.com",
    "https://example.com/path",
    "https://user:pass@example.com",
    "file:///tmp",
    "bad",
  ])
    assert.throws(
      () => validateAuthRequest({ ...spec, origin }),
      /browser_auth_origin_invalid/,
    );
  assert.throws(
    () => validateAuthRequest({ ...spec, origin: "http://127.0.0.1:8080" }),
    /origin_invalid/,
  );
  assert.equal(
    validateAuthRequest({ ...spec, origin: "http://127.0.0.1:8080" }, true).origin,
    "http://127.0.0.1:8080",
  );
  for (const extra of [
    { fields: [] },
    { fields: [{ ...spec.fields[0], id: ["username"] }] },
    { fields: Array(9).fill(spec.fields[0]) },
    { timeoutMs: 600_001 },
    { fields: [{ ...spec.fields[0], value: "must-not-accept-credentials" }] },
    { options: [{ id: "method", label: "Email", selector: "button" }] },
  ])
    assert.throws(() => validateAuthRequest({ ...spec, ...extra }), /request_invalid/);
  const choices = validateAuthRequest({
    ...spec,
    fields: [],
    options: [{ id: "email", label: "Use email", selector: "#method" }],
  });
  assert.equal(choices.options[0].id, "email");
});
