#!/usr/bin/env -S deno run --allow-all
import $ from "jsr:@david/dax@0.44.2";
import { parseArgs } from "jsr:@std/cli@1.0.27/parse-args";
import { encodeBase64 } from "jsr:@std/encoding@1/base64";

const flags = parseArgs(Deno.args, {
  boolean: ["fresh"],
  default: { fresh: false },
});

const ROOT = new URL(".", import.meta.url).pathname;
const APP_HTML = `${ROOT}src/trace_diff_app.html`;
const OUTPUT = `${ROOT}trace-diff.html`;
const TARGET = "wasm32-unknown-unknown";

async function main() {
  const tmpDir = await Deno.makeTempDir({ prefix: "trace-diff-wasm-" });
  try {
    await buildWasm(tmpDir);
    await assembleHtml(tmpDir);
  } finally {
    await Deno.remove(tmpDir, { recursive: true });
  }
}

// Step 1+2: Compile WASM and run wasm-bindgen
async function buildWasm(outDir: string) {
  $.logStep("Building WASM (release)...");
  await $`cargo build --target ${TARGET} --release`;

  $.logStep("Running wasm-bindgen...");
  const wasmPath = `${ROOT}target/${TARGET}/release/trace_diff.wasm`;
  await $`wasm-bindgen --target web --out-dir ${outDir} ${wasmPath}`;

  const wasmSize = (await Deno.stat(`${outDir}/trace_diff_bg.wasm`)).size;
  $.logLight(`  WASM binary: ${(wasmSize / 1024).toFixed(0)} KB`);
}

// Step 3+4: Base64-encode WASM, de-ESM-ify glue, inject into HTML
async function assembleHtml(outDir: string) {
  $.logStep("Assembling single HTML file...");

  // Read inputs
  const [wasmBytes, glueJs, templateHtml] = await Promise.all([
    Deno.readFile(`${outDir}/trace_diff_bg.wasm`),
    Deno.readTextFile(`${outDir}/trace_diff.js`),
    Deno.readTextFile(APP_HTML),
  ]);

  // Base64-encode WASM
  const wasmBase64 = encodeBase64(wasmBytes);
  $.logLight(`  Base64 WASM: ${(wasmBase64.length / 1024).toFixed(0)} KB`);

  // De-ESM-ify the glue JS
  const deEsm = deEsmify(glueJs);

  // Inject WASM base64
  let html = templateHtml.replace(
    /\/\*__WASM_BASE64__\*\/""\/\*__END__\*\//,
    `/*__WASM_BASE64__*/"${wasmBase64}"/*__END__*/`,
  );

  // Inject WASM glue (replace everything between markers)
  const gluePattern =
    /\/\*__WASM_GLUE_START__\*\/[\s\S]*?\/\*__WASM_GLUE_END__\*\//;
  if (!gluePattern.test(html)) {
    throw new Error(
      "Could not find /*__WASM_GLUE_START__*/.../*__WASM_GLUE_END__*/ markers in template",
    );
  }
  html = html.replace(
    gluePattern,
    `/*__WASM_GLUE_START__*/\n${deEsm}\n/*__WASM_GLUE_END__*/`,
  );

  await Deno.writeTextFile(OUTPUT, html);
  const outSize = (await Deno.stat(OUTPUT)).size;
  $.logStep(
    `Done! Output: trace-diff.html (${(outSize / 1024).toFixed(0)} KB)`,
  );
}

/**
 * Transform the wasm-bindgen ES module output into an IIFE that returns
 * `{ initSync, diff_traces }`.
 *
 * The generated JS has this structure:
 *   - JSDoc + `export function diff_traces(...)` — the public API
 *   - `export function init()` — called by __wbindgen_start, not needed directly
 *   - `function __wbg_get_imports()` — builds the import object
 *   - helper functions (getDataViewMemory0, passStringToWasm0, etc.)
 *   - `function initSync(module)` — synchronous init from ArrayBuffer
 *   - `async function __wbg_init(...)` — async init (not needed for inline WASM)
 *   - `export { initSync, __wbg_init as default }` — ES module exports
 *
 * We need to:
 *   1. Strip all `export` keywords
 *   2. Remove the async `__wbg_init` function (unused — we use initSync)
 *   3. Remove the final `export { ... }` line
 *   4. Remove the `__wbg_load` helper (only used by __wbg_init)
 *   5. Wrap in IIFE returning { initSync, diff_traces }
 *   6. Downgrade `let`/`const` to `var` for maximum compat
 */
function deEsmify(js: string): string {
  let code = js;

  // Remove the @ts-self-types comment
  code = code.replace(/\/\* @ts-self-types=.*?\*\/\n?/, "");

  // Remove JSDoc comment blocks (/** ... */)
  code = code.replace(/\/\*\*[\s\S]*?\*\/\n?/g, "");

  // Strip `export` from `export function ...`
  code = code.replace(/^export function /gm, "function ");

  // Remove the final `export { ... }` line
  code = code.replace(/^export \{[^}]*\};?\s*$/gm, "");

  // Remove the async __wbg_init function entirely
  code = code.replace(
    /^async function __wbg_init\([\s\S]*?^}\s*$/m,
    "",
  );

  // Remove the __wbg_load function entirely (only used by __wbg_init)
  code = code.replace(
    /^async function __wbg_load\([\s\S]*?^}\s*$/m,
    "",
  );

  // Remove the `init()` function (called internally by __wbindgen_start)
  code = code.replace(/^function init\(\) \{[\s\S]*?^}\s*$/m, "");

  // Downgrade let/const to var for broad compat
  code = code.replace(/\b(let|const) /g, "var ");

  // Collapse multiple blank lines
  code = code.replace(/\n{3,}/g, "\n\n");

  // Trim
  code = code.trim();

  // Wrap in IIFE
  return `var wasmGlue = (function() {
  'use strict';

  var wasm = null;

${indent(code, 2)}

  // Simplified initSync: accepts an ArrayBuffer directly.
  function initSyncFromBuffer(bytes) {
    if (wasm !== null) return;
    var imports = __wbg_get_imports();
    var mod = new WebAssembly.Module(bytes);
    var instance = new WebAssembly.Instance(mod, imports);
    wasm = instance.exports;
    cachedDataViewMemory0 = null;
    cachedUint8ArrayMemory0 = null;
    wasm.__wbindgen_start();
  }

  return { initSync: initSyncFromBuffer, diff_traces: diff_traces };
})();`;
}

function indent(text: string, spaces: number): string {
  var pad = " ".repeat(spaces);
  return text
    .split("\n")
    .map((line) => (line.trim() === "" ? "" : pad + line))
    .join("\n");
}

if (import.meta.main) {
  await main();
}
