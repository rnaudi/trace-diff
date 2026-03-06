//! Integration tests for the trace-diff pipeline.
//!
//! These tests serve as the primary entry point for understanding how the
//! library is used. They mirror exactly what the WASM entry point
//! [`trace_diff::wasm::diff_traces`] does — the same five steps, in the
//! same order, with the same types. Read these tests first when onboarding.
//!
//! # Pipeline (same as `wasm::diff_traces`)
//!
//! 1. **Parse** — `parse_trace_file(json)` auto-detects the format (OTLP or
//!    Datadog UI export) and builds a `SpanTree`.
//! 2. **Match** — `match_trees(&src, &dst, &config)` runs the `GumTree`
//!    algorithm to find structural correspondence between the two trees.
//! 3. **Diff** — `compute_diff(&src, &dst, &mappings, include_all)` walks
//!    both trees in aligned pre-order and produces a `TraceDiff` with
//!    per-node classifications (matched / inserted / deleted) and generic
//!    JSON-level field diffs.
//! 4. **Payload** — `build_trace_diff_payload(diff, ...)` assembles the
//!    rendering payload that the browser app consumes.
//! 5. **Serialize** — `serde_json::to_string(&payload)` produces the JSON
//!    string returned to JS across the WASM boundary.

use trace_diff::diff::{DiffKind, compute_diff};
use trace_diff::match_trees::{MatchConfig, match_trees};
use trace_diff::model::{DetectedFormat, detect_format, parse_trace_file};
use trace_diff::web::{TraceDiffPayload, build_trace_diff_payload};

// Fixtures loaded at compile time — same as shipping real trace files.
const OTLP_BASELINE: &str = include_str!("fixtures/otlp_baseline.json");
const OTLP_CANDIDATE: &str = include_str!("fixtures/otlp_candidate.json");
const DD_BASELINE: &str = include_str!("fixtures/datadog_baseline.json");
const DD_CANDIDATE: &str = include_str!("fixtures/datadog_candidate.json");

/// Run the full five-step pipeline and return the deserialized payload.
///
/// This is the exact sequence that `wasm::diff_traces` executes, minus the
/// `wasm_bindgen` wrapper and the 50 MiB input-size guard.
fn run_pipeline(
    baseline: &str,
    candidate: &str,
    baseline_label: &str,
    candidate_label: &str,
    include_all: bool,
) -> TraceDiffPayload {
    // Step 1: Parse — auto-detects OTLP vs Datadog format.
    let src = parse_trace_file(baseline).expect("baseline should parse");
    let dst = parse_trace_file(candidate).expect("candidate should parse");

    // Step 2: Match — GumTree algorithm finds structural correspondence.
    let config = MatchConfig::default();
    let mappings = match_trees(&src, &dst, &config);

    // Step 3: Diff — aligned walk produces per-node diff entries.
    let diff = compute_diff(&src, &dst, &mappings, include_all);

    // Step 4: Payload — assemble the rendering payload.
    let payload = build_trace_diff_payload(diff, &src, &dst, baseline_label, candidate_label);

    // Step 5: Serialize + deserialize round-trip (proves the JSON contract).
    let json = serde_json::to_string(&payload).expect("payload should serialize");
    serde_json::from_str(&json).expect("payload should deserialize")
}

/// Full pipeline with OTLP-format traces.
///
/// Baseline has 6 spans: `agent.execute` (root) → `llm.call`, `tool.call`,
/// `llm.call`, `tool.call`, `agent.complete`.
///
/// Candidate has 6 spans: same root → `llm.call`, `tool.call`, `llm.call`
/// (with changed model attribute), `tool.call`, `agent.review` (new).
///
/// Differences:
/// - `agent.complete` is deleted (only in baseline)
/// - `agent.review` is inserted (only in candidate)
/// - The second `llm.call` has a changed `llm.model` attribute
#[test]
fn full_pipeline_otlp() {
    let payload = run_pipeline(
        OTLP_BASELINE,
        OTLP_CANDIDATE,
        "baseline",
        "candidate",
        false,
    );

    let summary = &payload.summary;

    // Both traces have 6 spans.
    assert_eq!(summary.src_span_count, 6, "baseline should have 6 spans");
    assert_eq!(summary.dst_span_count, 6, "candidate should have 6 spans");

    // Structural expectations: some matched, one inserted, one deleted.
    assert!(
        summary.matched_count >= 4,
        "at least root + 3 children should match"
    );
    assert!(
        summary.inserted_count >= 1,
        "agent.review should be inserted"
    );
    assert!(
        summary.deleted_count >= 1,
        "agent.complete should be deleted"
    );

    // Verify we can find specific entries by name and kind.
    let deleted: Vec<_> = payload
        .entries
        .iter()
        .filter(|e| e.kind == DiffKind::Deleted)
        .collect();
    assert!(
        deleted.iter().any(|e| e.name == "agent.complete"),
        "agent.complete should appear as deleted, got: {:?}",
        deleted.iter().map(|e| &e.name).collect::<Vec<_>>()
    );

    let inserted: Vec<_> = payload
        .entries
        .iter()
        .filter(|e| e.kind == DiffKind::Inserted)
        .collect();
    assert!(
        inserted.iter().any(|e| e.name == "agent.review"),
        "agent.review should appear as inserted, got: {:?}",
        inserted.iter().map(|e| &e.name).collect::<Vec<_>>()
    );

    // The second llm.call has a changed attribute → should have json_diffs.
    let modified_llm: Vec<_> = payload
        .entries
        .iter()
        .filter(|e| e.kind == DiffKind::Matched && !e.json_diffs.is_empty())
        .collect();
    assert!(
        !modified_llm.is_empty(),
        "at least one matched span should have field-level diffs"
    );

    // Payload should carry both tree renderings.
    assert!(!payload.baseline_tree.is_empty());
    assert!(!payload.candidate_tree.is_empty());
    assert_eq!(payload.baseline_label, "baseline");
    assert_eq!(payload.candidate_label, "candidate");
}

/// Full pipeline with Datadog UI export-format traces.
///
/// Baseline has 3 spans: `spring.handler` (root) → `http.request`,
/// `redis.command`.
///
/// Candidate has 3 spans: same root → `http.request` (changed resource),
/// `pg.query` (new).
///
/// Differences:
/// - `redis.command` is deleted (only in baseline)
/// - `pg.query` is inserted (only in candidate)
/// - `http.request` resource changed from `"GET /api/data"` to `"GET /api/users"`
#[test]
fn full_pipeline_datadog() {
    let payload = run_pipeline(DD_BASELINE, DD_CANDIDATE, "v1.2.3", "v1.3.0", false);

    let summary = &payload.summary;

    // Both traces have 3 spans.
    assert_eq!(summary.src_span_count, 3, "baseline should have 3 spans");
    assert_eq!(summary.dst_span_count, 3, "candidate should have 3 spans");

    // Structural expectations.
    assert!(summary.matched_count >= 1, "at least root should match");
    assert!(summary.inserted_count >= 1, "pg.query should be inserted");
    assert!(
        summary.deleted_count >= 1,
        "redis.command should be deleted"
    );

    let deleted_names: Vec<_> = payload
        .entries
        .iter()
        .filter(|e| e.kind == DiffKind::Deleted)
        .map(|e| e.name.as_str())
        .collect();
    assert!(
        deleted_names.contains(&"redis.command"),
        "redis.command should be deleted, got: {deleted_names:?}"
    );

    let inserted_names: Vec<_> = payload
        .entries
        .iter()
        .filter(|e| e.kind == DiffKind::Inserted)
        .map(|e| e.name.as_str())
        .collect();
    assert!(
        inserted_names.contains(&"pg.query"),
        "pg.query should be inserted, got: {inserted_names:?}"
    );

    assert_eq!(payload.baseline_label, "v1.2.3");
    assert_eq!(payload.candidate_label, "v1.3.0");
}

/// Identical traces should produce all-matched entries with no field diffs.
///
/// This is the "no change" baseline: when you diff a trace against itself,
/// every span is matched and no JSON-level differences exist.
#[test]
fn identical_traces_produce_no_diffs() {
    let payload = run_pipeline(OTLP_BASELINE, OTLP_BASELINE, "same", "same", false);

    let summary = &payload.summary;

    assert_eq!(summary.src_span_count, summary.dst_span_count);
    assert_eq!(summary.matched_count, summary.src_span_count);
    assert_eq!(summary.inserted_count, 0, "no spans should be inserted");
    assert_eq!(summary.deleted_count, 0, "no spans should be deleted");

    // No entry should have field-level diffs (all noise fields are excluded,
    // and non-noise fields are identical).
    let entries_with_diffs: Vec<_> = payload
        .entries
        .iter()
        .filter(|e| !e.json_diffs.is_empty())
        .collect();
    assert!(
        entries_with_diffs.is_empty(),
        "identical traces should have zero field diffs, but found {} entries with diffs",
        entries_with_diffs.len()
    );

    assert!(payload.divergence_index.is_none(), "no divergence point");
    assert_eq!(payload.total_field_diffs, 0);
}

/// Format auto-detection correctly identifies OTLP and Datadog inputs.
///
/// The `detect_format` function inspects top-level JSON keys to determine
/// the format without the caller having to specify it. This is the first
/// step inside `parse_trace_file`.
#[test]
fn format_auto_detection() {
    assert_eq!(
        detect_format(OTLP_BASELINE).expect("OTLP should be detected"),
        DetectedFormat::Otlp,
    );
    assert_eq!(
        detect_format(DD_BASELINE).expect("Datadog should be detected"),
        DetectedFormat::DatadogUi,
    );

    // Both formats parse into valid SpanTrees.
    let otlp_tree = parse_trace_file(OTLP_BASELINE).expect("OTLP should parse");
    let dd_tree = parse_trace_file(DD_BASELINE).expect("Datadog should parse");

    assert_eq!(otlp_tree.len(), 6, "OTLP trace has 6 spans");
    assert_eq!(dd_tree.len(), 3, "Datadog trace has 3 spans");
}

/// The `include_all` flag controls noise filtering in the diff output.
///
/// When `include_all` is `false` (default), per-request fields like span IDs,
/// timestamps, and hostnames are excluded from JSON diffs. When `true`, every
/// field difference is reported. The filtered run should therefore have fewer
/// or equal total field diffs compared to the unfiltered run.
#[test]
fn include_all_vs_filtered() {
    let filtered = run_pipeline(DD_BASELINE, DD_CANDIDATE, "a", "b", false);
    let unfiltered = run_pipeline(DD_BASELINE, DD_CANDIDATE, "a", "b", true);

    // Structural summary should be identical — include_all only affects
    // JSON-level field diffs within matched spans, not tree matching.
    assert_eq!(
        filtered.summary.matched_count,
        unfiltered.summary.matched_count
    );
    assert_eq!(
        filtered.summary.inserted_count,
        unfiltered.summary.inserted_count
    );
    assert_eq!(
        filtered.summary.deleted_count,
        unfiltered.summary.deleted_count
    );

    // Unfiltered should surface at least as many field diffs as filtered.
    assert!(
        unfiltered.total_field_diffs >= filtered.total_field_diffs,
        "unfiltered ({}) should have >= field diffs than filtered ({})",
        unfiltered.total_field_diffs,
        filtered.total_field_diffs,
    );
}
