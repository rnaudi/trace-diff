//! WASM entry points for browser-based trace diffing.
//!
//! Thin wrappers around the core pipeline that accept/return strings via
//! `wasm_bindgen`. The browser JS calls `diff_traces` with two JSON blobs
//! and gets back a `TraceDiffPayload` serialized as JSON.

use wasm_bindgen::prelude::*;

use crate::diff::compute_diff;
use crate::match_trees::{MatchConfig, match_trees};
use crate::model::{TraceError, parse_trace_file};
use crate::web::build_trace_diff_payload;

/// Maximum combined input size (50 MiB). Prevents the WASM module from
/// spending unbounded memory/time on oversized payloads.
const MAX_INPUT_BYTES: usize = 50 * 1024 * 1024;

/// One-time WASM initialization: install a panic hook so Rust panics surface
/// as readable JS console errors instead of "unreachable executed".
#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}

/// Run the full trace-diff pipeline and return the `TraceDiffPayload` as a JSON string.
///
/// # Errors
///
/// Returns a `JsValue` error string if parsing fails or the diff cannot be serialized.
#[wasm_bindgen]
pub fn diff_traces(
    baseline: &str,
    candidate: &str,
    baseline_label: &str,
    candidate_label: &str,
    include_all: bool,
) -> Result<String, JsValue> {
    let total_bytes = baseline.len() + candidate.len();
    if total_bytes > MAX_INPUT_BYTES {
        return Err(JsValue::from_str(&format!(
            "{}",
            TraceError::InputTooLarge {
                bytes: total_bytes,
                max_bytes: MAX_INPUT_BYTES,
            }
        )));
    }

    let src = parse_trace_file(baseline).map_err(|e| JsValue::from_str(&format!("{e}")))?;
    let dst = parse_trace_file(candidate).map_err(|e| JsValue::from_str(&format!("{e}")))?;

    let config = MatchConfig::default();
    let mappings = match_trees(&src, &dst, &config);
    let diff = compute_diff(&src, &dst, &mappings, include_all);

    let payload = build_trace_diff_payload(diff, &src, &dst, baseline_label, candidate_label);

    serde_json::to_string(&payload).map_err(|e| JsValue::from_str(&format!("{e}")))
}
