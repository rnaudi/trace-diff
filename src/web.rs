//! Trace diff HTML rendering.
//!
//! Builds a `TraceDiffPayload` from the diff computation output and serializes
//! it as JSON embedded in a single self-contained HTML file. The template is
//! loaded at compile time via `include_str!("trace_template.html")`.
//!
//! # Data / visualization split
//!
//! This module handles data assembly and JSON serialization only. All rendering
//! (HTML, CSS, JS) lives in `trace_template.html`. No `anstyle` imports here.

use serde::{Deserialize, Serialize};

use crate::diff::{DiffEntry, DiffSummary, TraceDiff};
use crate::model::{SpanTree, TraceError};

/// Top-level payload embedded as JSON in the trace diff HTML template.
#[derive(Debug, Serialize, Deserialize)]
pub struct TraceDiffPayload {
    /// Label for the baseline trace (filename or user-supplied name).
    pub baseline_label: String,
    /// Label for the candidate trace.
    pub candidate_label: String,
    /// Flat list of diff entries in aligned pre-order.
    pub entries: Vec<DiffEntry>,
    /// Summary statistics.
    pub summary: DiffSummary,
    /// Index into `entries` of the first divergence point, if any.
    pub divergence_index: Option<usize>,
    /// Baseline trace tree in a renderable format.
    pub baseline_tree: Vec<TreeNode>,
    /// Candidate trace tree in a renderable format.
    pub candidate_tree: Vec<TreeNode>,
    /// Maximum duration (nanoseconds) across all entry spans — for heatmap normalization.
    pub max_duration_ns: u64,
    /// Total number of field-level diffs across all entries.
    pub total_field_diffs: usize,
}

/// A flattened tree node for rendering individual tree panels.
#[derive(Debug, Serialize, Deserialize)]
pub struct TreeNode {
    /// Span name.
    pub name: String,
    /// Depth in tree (for indentation).
    pub depth: usize,
    /// Duration in milliseconds (for display).
    pub duration_ms: f64,
    /// Status string.
    pub status: String,
    /// Key attributes (flattened to string pairs).
    pub attributes: Vec<(String, String)>,
    /// Number of direct children.
    pub child_count: usize,
}

/// Build the payload from a `TraceDiff` and the source trees.
///
/// Takes `diff` by value to avoid deep-cloning `entries` (which contain raw
/// `serde_json::Value` fields). The caller typically doesn't need `diff`
/// after building the payload.
#[must_use]
pub fn build_trace_diff_payload(
    diff: TraceDiff,
    src: &SpanTree,
    dst: &SpanTree,
    baseline_label: &str,
    candidate_label: &str,
) -> TraceDiffPayload {
    let max_duration_ns = diff
        .entries
        .iter()
        .flat_map(|e| [e.src_duration_ns, e.dst_duration_ns])
        .flatten()
        .max()
        .unwrap_or(0);

    let total_field_diffs: usize = diff.entries.iter().map(|e| e.json_diffs.len()).sum();

    TraceDiffPayload {
        baseline_label: baseline_label.to_owned(),
        candidate_label: candidate_label.to_owned(),
        entries: diff.entries,
        summary: diff.summary,
        divergence_index: diff.divergence_index,
        baseline_tree: flatten_tree(src),
        candidate_tree: flatten_tree(dst),
        max_duration_ns,
        total_field_diffs,
    }
}

/// Flatten a `SpanTree` into a pre-order list of `TreeNode`s for rendering.
///
/// Computes depth on the fly via a stack-based walk since `SpanNode` doesn't
/// store depth.
fn flatten_tree(tree: &SpanTree) -> Vec<TreeNode> {
    let mut result = Vec::with_capacity(tree.len());
    // Stack of (node_id, depth).
    let mut stack = vec![(tree.root, 0usize)];
    while let Some((nid, depth)) = stack.pop() {
        let node = &tree.nodes[nid];
        let span = &node.span;
        result.push(TreeNode {
            name: span.name.clone(),
            depth,
            #[expect(
                clippy::cast_precision_loss,
                reason = "trace durations fit within f64 mantissa"
            )]
            duration_ms: span.duration_nanos() as f64 / 1_000_000.0,
            status: span.status_str().to_owned(),
            attributes: span
                .attributes
                .iter()
                .map(|kv| (kv.key.clone(), kv.value.as_display_string()))
                .collect(),
            child_count: node.children.len(),
        });
        // Push children in reverse so leftmost is popped first.
        for &cid in node.children.iter().rev() {
            stack.push((cid, depth + 1));
        }
    }
    result
}

/// Generate the self-contained HTML file by injecting JSON into the template.
///
/// The template contains a `/*__DATA__*/null/*__END__*/` placeholder inside a
/// `<script>` tag that gets replaced with the serialized payload JSON.
///
/// # Errors
///
/// Returns `Err` if the payload cannot be serialized to JSON.
pub fn render_trace_html(payload: &TraceDiffPayload) -> Result<String, TraceError> {
    let template = include_str!("trace_template.html");
    let json = serde_json::to_string(payload).map_err(TraceError::Serialization)?;
    let html = template.replace("/*__DATA__*/null/*__END__*/", &json);
    Ok(html)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::compute_diff;
    use crate::match_trees::{MatchConfig, match_trees};
    use crate::model::{Span, SpanKind, SpanTree, Status, StatusCode};

    fn span(id: &str, parent: Option<&str>, name: &str, start: u64) -> Span {
        Span {
            trace_id: "t1".to_owned(),
            span_id: id.to_owned(),
            trace_state: None,
            parent_span_id: parent.map(str::to_owned),
            flags: None,
            name: name.to_owned(),
            kind: SpanKind::Internal,
            start_time_unix_nano: Some(start.to_string()),
            end_time_unix_nano: Some((start + 1_000_000).to_string()),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            events: Vec::new(),
            dropped_events_count: 0,
            links: Vec::new(),
            dropped_links_count: 0,
            status: Some(Status {
                message: None,
                code: StatusCode::Ok,
            }),
        }
    }

    #[test]
    fn payload_serializes_round_trip() {
        let spans = vec![
            span("1", None, "root", 0),
            span("2", Some("1"), "child", 100),
        ];
        let src = SpanTree::from_spans(spans.clone()).unwrap();
        let dst = SpanTree::from_spans(spans).unwrap();
        let mappings = match_trees(&src, &dst, &MatchConfig::default());
        let diff = compute_diff(&src, &dst, &mappings, true);
        let payload = build_trace_diff_payload(diff, &src, &dst, "base.json", "cand.json");

        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("base.json"));
        assert!(json.contains("cand.json"));
        assert!(json.contains("\"matched\""));
    }

    #[test]
    fn render_html_replaces_placeholder() {
        let spans = vec![span("1", None, "root", 0)];
        let src = SpanTree::from_spans(spans.clone()).unwrap();
        let dst = SpanTree::from_spans(spans).unwrap();
        let mappings = match_trees(&src, &dst, &MatchConfig::default());
        let diff = compute_diff(&src, &dst, &mappings, true);
        let payload = build_trace_diff_payload(diff, &src, &dst, "a.json", "b.json");

        let html = render_trace_html(&payload).unwrap();
        assert!(html.contains("a.json"));
        assert!(!html.contains("/*__DATA__*/null/*__END__*/"));
    }

    #[test]
    fn flatten_tree_preserves_pre_order() {
        let spans = vec![
            span("1", None, "root", 0),
            span("2", Some("1"), "alpha", 100),
            span("3", Some("1"), "beta", 200),
            span("4", Some("2"), "gamma", 150),
        ];
        let tree = SpanTree::from_spans(spans).unwrap();
        let flat = flatten_tree(&tree);

        assert_eq!(flat.len(), 4);
        assert_eq!(flat[0].name, "root");
        assert_eq!(flat[0].depth, 0);
        // alpha comes before beta (children sorted by start time).
        assert_eq!(flat[1].name, "alpha");
        // gamma is child of alpha.
        assert_eq!(flat[2].name, "gamma");
        assert_eq!(flat[2].depth, 2);
        assert_eq!(flat[3].name, "beta");
    }

    #[test]
    fn tree_node_duration_ms_conversion() {
        let spans = vec![span("1", None, "root", 0)];
        let tree = SpanTree::from_spans(spans).unwrap();
        let flat = flatten_tree(&tree);
        // 1_000_000 nanos = 1.0 ms
        assert!((flat[0].duration_ms - 1.0).abs() < 0.001);
    }
}
