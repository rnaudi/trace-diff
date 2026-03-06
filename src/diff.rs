//! Diff computation from a tree matching.
//!
//! Given two `SpanTree`s and a `MappingStore`, classifies every node as
//! matched (with optional JSON-level diffs), inserted, or deleted. Uses
//! generic recursive JSON comparison so that *every* field change is
//! surfaced — not just a hardcoded subset.
//!
//! **Noise filtering**: per-request fields (span IDs, timestamps, hostnames,
//! DD agent internals) are excluded by default via `DEFAULT_EXCLUDES`. Pass
//! `include_all: true` to `compute_diff()` to disable filtering.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::match_trees::MappingStore;
use crate::model::{NodeId, SpanTree};

/// Paths excluded from JSON diffs by default. These are per-request identity
/// and infrastructure fields that always differ between two runs of the same
/// code path and carry no diagnostic value.
///
/// Covers both Datadog UI export and OTLP field names (no collisions).
/// Prefix matching: `meta._dd.p.` excludes `meta._dd.p.tid`, `meta._dd.p.ftid`, etc.
const DEFAULT_EXCLUDES: &[&str] = &[
    // -- DD top-level per-request identity --
    "trace_id",
    "span_id",
    "parent_id",
    "start",
    "end",
    "host_id",
    "hostname",
    "children_ids",  // exact: when whole array is added/removed
    "children_ids[", // prefix: individual array elements
    "resource_hash",
    "ingestion_reason",
    "host_groups",
    "metadata",
    // -- DD meta: agent/infra internals --
    "meta._dd.agent_hostname",
    "meta._dd.tracer_host",
    "meta._dd.p.", // prefix: _dd.p.tid, _dd.p.ftid, etc.
    "meta.thread.name",
    "meta.runtime-id",
    // -- DD metrics: sampling/agent internals --
    "metrics.thread.id",
    "metrics._dd.agent_priority_sampler.", // prefix
    "metrics._dd.agent_errors_sampler.",   // prefix
    "metrics._sampling_priority_v1",
    "metrics._top_level",
    // -- OTLP per-request identity --
    "traceId",
    "spanId",
    "parentSpanId",
    "startTimeUnixNano",
    "endTimeUnixNano",
];

/// Returns `true` if `path` matches any entry in the exclude list.
/// Supports both exact match and prefix match (excludes ending with `.` or `[`).
fn is_excluded(path: &str) -> bool {
    DEFAULT_EXCLUDES.iter().any(|&pattern| {
        if pattern.ends_with('.') || pattern.ends_with('[') {
            path.starts_with(pattern)
        } else {
            path == pattern
        }
    })
}

/// Classify a JSON diff path into a semantic category for grouping in the UI.
fn categorize_diff_path(path: &str) -> &'static str {
    if path.starts_with("status")
        || path.contains("error")
        || path.contains("exception")
        || path.contains("fault")
    {
        "errors"
    } else if path.contains("duration") || path.contains("Time") || path.contains("latency") {
        "performance"
    } else if path.contains("version")
        || path.contains("commit")
        || path.contains("deploy")
        || path.contains("build")
    {
        "deployment"
    } else if path.contains("http") || path.contains("url") || path.contains("grpc") {
        "http"
    } else {
        "other"
    }
}

/// Complete diff between two span trees.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceDiff {
    /// Per-node classifications, in pre-order of a merged/aligned traversal.
    pub entries: Vec<DiffEntry>,
    /// Summary statistics.
    pub summary: DiffSummary,
    /// Index of the first entry where children differ (the divergence point).
    /// `None` if the trees are identical.
    pub divergence_index: Option<usize>,
}

/// A single node in the diff output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffEntry {
    /// What happened to this node.
    pub kind: DiffKind,
    /// Span name (e.g., "tool.call", "llm.call").
    pub name: String,
    /// Depth in the aligned tree (for indentation).
    pub depth: usize,
    /// Which side this node comes from. Both for matched, Left for deleted, Right for inserted.
    pub side: Side,
    /// Generic JSON-level diffs between the two raw span objects.
    /// Empty for inserted/deleted nodes (the full JSON is in `src_json`/`dst_json`).
    /// For matched nodes: every field-level change as a JSON path.
    pub json_diffs: Vec<JsonDiff>,
    /// Raw JSON of the baseline (src) span. Present for matched and deleted nodes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub src_json: Option<Value>,
    /// Raw JSON of the candidate (dst) span. Present for matched and inserted nodes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dst_json: Option<Value>,

    // -- Pre-computed fields for the rendering layer --
    /// Baseline span duration in nanoseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub src_duration_ns: Option<u64>,
    /// Candidate span duration in nanoseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dst_duration_ns: Option<u64>,
    /// Baseline span normalized status (`"ok"`, `"error"`, `"unset"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub src_status: Option<String>,
    /// Candidate span normalized status.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dst_status: Option<String>,
    /// Baseline service name (from `service.name` attribute).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub src_service: Option<String>,
    /// Candidate service name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dst_service: Option<String>,
    /// Baseline resource identifier (DD `resource`, OTLP `http.route`/`url.path`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub src_resource: Option<String>,
    /// Candidate resource identifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dst_resource: Option<String>,
    /// Duration change percentage `((dst - src) / src * 100)`. `None` when
    /// either side is missing or the baseline duration is zero.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_delta_pct: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DiffKind {
    /// Node exists in both trees, JSON fields may differ.
    Matched,
    /// Node only exists in the candidate (dst) tree.
    Inserted,
    /// Node only exists in the baseline (src) tree.
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Both,
    Left,
    Right,
}

/// A single JSON-level difference between two matched span objects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonDiff {
    /// JSON path to the changed value (e.g. "status.code", "attributes[0].value.intValue").
    pub path: String,
    /// What changed.
    pub change: JsonChange,
    /// Semantic category inferred from the path: `"errors"`, `"performance"`,
    /// `"deployment"`, `"http"`, or `"other"`.
    pub category: String,
}

/// The type of change at a given JSON path.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum JsonChange {
    /// Value exists only in the candidate.
    Added { value: Value },
    /// Value exists only in the baseline.
    Removed { value: Value },
    /// Value differs between baseline and candidate.
    Modified { old: Value, new: Value },
}

/// Summary statistics for the diff.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffSummary {
    pub src_span_count: usize,
    pub dst_span_count: usize,
    pub matched_count: usize,
    pub inserted_count: usize,
    pub deleted_count: usize,
    pub modified_count: usize,
}

/// Recursively diff two JSON values, producing a list of path-based changes.
///
/// Objects are compared key-by-key. Arrays of keyed objects (where every element
/// has a `"key"` string field, as in OTLP attributes/events) are matched by key
/// to avoid false diffs from reordering. Other arrays fall back to positional
/// (index-by-index) comparison. Scalars are compared for equality.
fn diff_json(prefix: &str, old: &Value, new: &Value) -> Vec<JsonDiff> {
    // Same value — no diff.
    if old == new {
        return Vec::new();
    }

    match (old, new) {
        (Value::Object(old_map), Value::Object(new_map)) => {
            let mut diffs = Vec::new();

            // Keys in old.
            for (key, old_val) in old_map {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                match new_map.get(key) {
                    Some(new_val) => {
                        diffs.extend(diff_json(&path, old_val, new_val));
                    }
                    None => {
                        diffs.push(json_diff(
                            path,
                            JsonChange::Removed {
                                value: old_val.clone(),
                            },
                        ));
                    }
                }
            }

            // Keys only in new.
            for (key, new_val) in new_map {
                if !old_map.contains_key(key) {
                    let path = if prefix.is_empty() {
                        key.clone()
                    } else {
                        format!("{prefix}.{key}")
                    };
                    diffs.push(json_diff(
                        path,
                        JsonChange::Added {
                            value: new_val.clone(),
                        },
                    ));
                }
            }

            diffs
        }
        (Value::Array(old_arr), Value::Array(new_arr)) => {
            // If both arrays are keyed (every element is an object with a
            // "key" string field, like OTLP attributes), match by key to
            // avoid false diffs from reordering.
            if is_keyed_array(old_arr) && is_keyed_array(new_arr) {
                return diff_keyed_array(prefix, old_arr, new_arr);
            }

            // Positional (index-by-index) fallback.
            let mut diffs = Vec::new();
            let max_len = old_arr.len().max(new_arr.len());

            for i in 0..max_len {
                let path = format!("{prefix}[{i}]");
                match (old_arr.get(i), new_arr.get(i)) {
                    (Some(ov), Some(nv)) => {
                        diffs.extend(diff_json(&path, ov, nv));
                    }
                    (Some(ov), None) => {
                        diffs.push(json_diff(path, JsonChange::Removed { value: ov.clone() }));
                    }
                    (None, Some(nv)) => {
                        diffs.push(json_diff(path, JsonChange::Added { value: nv.clone() }));
                    }
                    (None, None) => unreachable!(),
                }
            }

            diffs
        }
        // Type mismatch or scalar difference.
        _ => {
            vec![json_diff(
                prefix.to_owned(),
                JsonChange::Modified {
                    old: old.clone(),
                    new: new.clone(),
                },
            )]
        }
    }
}

/// Returns `true` if every element in the array is an object containing a
/// `"key"` field with a string value. This is the shape used by OTLP for
/// attributes, resource attributes, and event attributes.
fn is_keyed_array(arr: &[Value]) -> bool {
    !arr.is_empty()
        && arr.iter().all(|v| {
            v.as_object()
                .and_then(|obj| obj.get("key"))
                .and_then(Value::as_str)
                .is_some()
        })
}

/// Diff two keyed arrays by matching on the `"key"` field instead of position.
/// Produces paths like `attributes.service.name` rather than `attributes[0]`.
fn diff_keyed_array(prefix: &str, old: &[Value], new: &[Value]) -> Vec<JsonDiff> {
    // Build a lookup from key → element for the new array.
    let new_map: HashMap<&str, &Value> = new
        .iter()
        .filter_map(|v| {
            let key = v.as_object()?.get("key")?.as_str()?;
            Some((key, v))
        })
        .collect();

    let old_keys: HashSet<&str> = old
        .iter()
        .filter_map(|v| v.as_object()?.get("key")?.as_str())
        .collect();

    let mut diffs = Vec::new();

    // Walk old elements in their original order.
    for old_val in old {
        let Some(key) = old_val
            .as_object()
            .and_then(|o| o.get("key"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        let path = if prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{prefix}.{key}")
        };
        match new_map.get(key) {
            Some(&new_val) => {
                diffs.extend(diff_json(&path, old_val, new_val));
            }
            None => {
                diffs.push(json_diff(
                    path,
                    JsonChange::Removed {
                        value: old_val.clone(),
                    },
                ));
            }
        }
    }

    // Walk new elements in their original order — emit only additions.
    for new_val in new {
        let Some(key) = new_val
            .as_object()
            .and_then(|o| o.get("key"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        if !old_keys.contains(key) {
            let path = if prefix.is_empty() {
                key.to_owned()
            } else {
                format!("{prefix}.{key}")
            };
            diffs.push(json_diff(
                path,
                JsonChange::Added {
                    value: new_val.clone(),
                },
            ));
        }
    }

    diffs
}

/// Construct a `JsonDiff` with auto-categorized path.
fn json_diff(path: String, change: JsonChange) -> JsonDiff {
    let category = categorize_diff_path(&path).to_owned();
    JsonDiff {
        path,
        change,
        category,
    }
}

/// Compute the full diff between two matched span trees.
///
/// When `include_all` is false (the default), per-request noise fields
/// (timestamps, span IDs, hostnames, agent internals) are excluded from
/// the JSON diffs. The raw JSON in `src_json`/`dst_json` still contains
/// everything — filtering only affects `json_diffs`.
#[must_use]
pub fn compute_diff(
    src: &SpanTree,
    dst: &SpanTree,
    mappings: &MappingStore,
    include_all: bool,
) -> TraceDiff {
    let mut entries = Vec::new();
    let mut matched_count = 0usize;
    let mut inserted_count = 0usize;
    let mut deleted_count = 0usize;
    let mut modified_count = 0usize;
    let mut divergence_index: Option<usize> = None;

    // Walk both trees in aligned pre-order.
    aligned_walk(
        src,
        dst,
        mappings,
        src.root,
        dst.root,
        0,
        &mut entries,
        &mut divergence_index,
    );

    // Filter out noise unless --include-all was passed.
    if !include_all {
        for entry in &mut entries {
            entry.json_diffs.retain(|d| !is_excluded(&d.path));
        }
    }

    // Count categories.
    for entry in &entries {
        match entry.kind {
            DiffKind::Matched => {
                matched_count += 1;
                if !entry.json_diffs.is_empty() {
                    modified_count += 1;
                }
            }
            DiffKind::Inserted => inserted_count += 1,
            DiffKind::Deleted => deleted_count += 1,
        }
    }

    let summary = DiffSummary {
        src_span_count: src.len(),
        dst_span_count: dst.len(),
        matched_count,
        inserted_count,
        deleted_count,
        modified_count,
    };

    TraceDiff {
        entries,
        summary,
        divergence_index,
    }
}

/// Compute the percentage change between two durations.
///
/// Returns `None` if the baseline is zero (avoiding division by zero) or if
/// the absolute change is less than 1% (noise suppression).
fn duration_delta_pct(src_ns: u64, dst_ns: u64) -> Option<f64> {
    if src_ns == 0 {
        return None;
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "nanosecond durations fit within f64 mantissa for reasonable trace spans"
    )]
    let pct = (dst_ns as f64 - src_ns as f64) / src_ns as f64 * 100.0;
    if pct.abs() < 1.0 { None } else { Some(pct) }
}

/// A frame for the iterative `aligned_walk` stack.
enum WalkFrame {
    /// Process a matched pair of nodes (both src and dst exist).
    Matched {
        src_node: NodeId,
        dst_node: NodeId,
        depth: usize,
    },
    /// Process an unmatched subtree (inserted or deleted).
    Subtree {
        node: NodeId,
        depth: usize,
        kind: DiffKind,
        side: Side,
        /// If true, `node` refers to the src tree; otherwise the dst tree.
        tree_is_src: bool,
    },
}

/// Walk two trees in aligned pre-order, producing `DiffEntry`s.
///
/// For matched nodes we descend into both children sets, aligning by mapping.
/// Unmatched src children are emitted as deleted; unmatched dst children as inserted.
///
/// Uses an explicit stack instead of recursion to avoid stack overflow on
/// deeply nested traces (especially relevant in WASM's 1 MB default stack).
#[expect(
    clippy::too_many_arguments,
    reason = "private helper called from one site; args are inherent to the dual-tree walk"
)]
fn aligned_walk(
    src: &SpanTree,
    dst: &SpanTree,
    mappings: &MappingStore,
    src_root: NodeId,
    dst_root: NodeId,
    initial_depth: usize,
    entries: &mut Vec<DiffEntry>,
    divergence_index: &mut Option<usize>,
) {
    let mut stack = vec![WalkFrame::Matched {
        src_node: src_root,
        dst_node: dst_root,
        depth: initial_depth,
    }];

    while let Some(frame) = stack.pop() {
        match frame {
            WalkFrame::Matched {
                src_node,
                dst_node,
                depth,
            } => {
                // Compute generic JSON diff between the two raw span values.
                let json_diffs = diff_json("", &src.nodes[src_node].raw, &dst.nodes[dst_node].raw);

                let src_span = &src.nodes[src_node].span;
                let dst_span = &dst.nodes[dst_node].span;
                let src_dur = src_span.duration_nanos();
                let dst_dur = dst_span.duration_nanos();

                entries.push(DiffEntry {
                    kind: DiffKind::Matched,
                    name: dst_span.name.clone(),
                    depth,
                    side: Side::Both,
                    json_diffs,
                    src_json: some_if_present(&src.nodes[src_node].raw),
                    dst_json: some_if_present(&dst.nodes[dst_node].raw),
                    src_duration_ns: Some(src_dur),
                    dst_duration_ns: Some(dst_dur),
                    src_status: Some(src_span.status_str().to_owned()),
                    dst_status: Some(dst_span.status_str().to_owned()),
                    src_service: src_span.service_name(),
                    dst_service: dst_span.service_name(),
                    src_resource: src_span.resource_name(),
                    dst_resource: dst_span.resource_name(),
                    duration_delta_pct: duration_delta_pct(src_dur, dst_dur),
                });

                // Align children: build a merged sequence of (Option<src_child>, Option<dst_child>).
                let aligned = align_children(src, dst, mappings, src_node, dst_node);

                // Detect divergence: if any child is inserted or deleted, this is a divergence point.
                if divergence_index.is_none() {
                    let has_structural_change =
                        aligned.iter().any(|(s, d)| s.is_none() || d.is_none());
                    if has_structural_change {
                        *divergence_index = Some(entries.len() - 1);
                    }
                }

                // Push children in reverse so the first child is processed first (pre-order).
                for (s_child, d_child) in aligned.into_iter().rev() {
                    match (s_child, d_child) {
                        (Some(sc), Some(dc)) => {
                            stack.push(WalkFrame::Matched {
                                src_node: sc,
                                dst_node: dc,
                                depth: depth + 1,
                            });
                        }
                        (Some(sc), None) => {
                            stack.push(WalkFrame::Subtree {
                                node: sc,
                                depth: depth + 1,
                                kind: DiffKind::Deleted,
                                side: Side::Left,
                                tree_is_src: true,
                            });
                        }
                        (None, Some(dc)) => {
                            stack.push(WalkFrame::Subtree {
                                node: dc,
                                depth: depth + 1,
                                kind: DiffKind::Inserted,
                                side: Side::Right,
                                tree_is_src: false,
                            });
                        }
                        (None, None) => unreachable!(),
                    }
                }
            }
            WalkFrame::Subtree {
                node,
                depth,
                kind,
                side,
                tree_is_src,
            } => {
                let tree = if tree_is_src { src } else { dst };
                emit_subtree(tree, node, depth, kind, side, entries);
            }
        }
    }
}

/// Wrap a raw JSON value in `Some`, or `None` if it's `Value::Null`.
fn some_if_present(v: &Value) -> Option<Value> {
    if v.is_null() { None } else { Some(v.clone()) }
}

/// Align children of two matched parent nodes into a merged sequence.
///
/// Uses the mapping to pair up matched children, and interleaves
/// unmatched children (insertions/deletions) at their natural positions.
fn align_children(
    src: &SpanTree,
    dst: &SpanTree,
    mappings: &MappingStore,
    src_parent: NodeId,
    dst_parent: NodeId,
) -> Vec<(Option<NodeId>, Option<NodeId>)> {
    let src_children = &src.nodes[src_parent].children;
    let dst_children = &dst.nodes[dst_parent].children;

    // O(1) lookup sets/maps instead of repeated linear scans.
    let src_child_set: HashSet<NodeId> = src_children.iter().copied().collect();
    let dst_child_set: HashSet<NodeId> = dst_children.iter().copied().collect();
    let src_child_pos: HashMap<NodeId, usize> = src_children
        .iter()
        .enumerate()
        .map(|(i, &id)| (id, i))
        .collect();

    // Build a map: for each dst child, its matched src child (if any).
    let dst_to_src_child: Vec<Option<NodeId>> = dst_children
        .iter()
        .map(|&dc| mappings.get_src(dc).filter(|sc| src_child_set.contains(sc)))
        .collect();

    // Track which src children are accounted for.
    let mut src_used: Vec<bool> = vec![false; src_children.len()];

    let mut result = Vec::new();

    // Walk through dst children in order.
    // Before each dst child, emit any unmatched src children that appear before
    // the matched src child's position.
    let mut src_cursor = 0;

    for (di, &dc) in dst_children.iter().enumerate() {
        if let Some(sc) = dst_to_src_child[di] {
            // Find the position of this src child.
            let sc_pos = src_child_pos.get(&sc).copied().unwrap_or(0);

            // Emit any unmatched src children before this position.
            while src_cursor <= sc_pos {
                if src_cursor < src_children.len() && src_cursor < sc_pos && !src_used[src_cursor] {
                    // Check if this src child has a mapping to some dst child
                    // that we'll encounter later. If not, it's deleted.
                    let sc_at_cursor = src_children[src_cursor];
                    if !mappings.has_src(sc_at_cursor)
                        || mappings
                            .get_dst(sc_at_cursor)
                            .is_none_or(|d| !dst_child_set.contains(&d))
                    {
                        result.push((Some(sc_at_cursor), None));
                    }
                    src_used[src_cursor] = true;
                }
                src_cursor += 1;
            }

            // Emit the matched pair.
            if let Some(&pos) = src_child_pos.get(&sc) {
                src_used[pos] = true;
            }
            result.push((Some(sc), Some(dc)));
        } else {
            // dst child has no matching src child — inserted.
            result.push((None, Some(dc)));
        }
    }

    // Emit any remaining unmatched src children (deleted).
    for (i, &sc) in src_children.iter().enumerate() {
        if !src_used[i]
            && (!mappings.has_src(sc)
                || mappings
                    .get_dst(sc)
                    .is_none_or(|d| !dst_child_set.contains(&d)))
        {
            result.push((Some(sc), None));
        }
    }

    result
}

/// Emit an entire subtree as inserted or deleted entries.
///
/// Uses an explicit stack instead of recursion to avoid stack overflow on
/// deeply nested traces (especially relevant in WASM's 1 MB default stack).
fn emit_subtree(
    tree: &SpanTree,
    node: NodeId,
    depth: usize,
    kind: DiffKind,
    side: Side,
    entries: &mut Vec<DiffEntry>,
) {
    // Stack holds (node_id, depth).
    let mut stack = vec![(node, depth)];

    while let Some((current, current_depth)) = stack.pop() {
        let span_node = &tree.nodes[current];
        let span = &span_node.span;
        let (src_json, dst_json) = match side {
            Side::Left => (some_if_present(&span_node.raw), None),
            Side::Right => (None, some_if_present(&span_node.raw)),
            Side::Both => (
                some_if_present(&span_node.raw),
                some_if_present(&span_node.raw),
            ),
        };
        let dur = span.duration_nanos();
        let status = Some(span.status_str().to_owned());
        let service = span.service_name();
        let resource = span.resource_name();
        let (src_duration_ns, dst_duration_ns, src_status, dst_status) = match side {
            Side::Left => (Some(dur), None, status, None),
            Side::Right => (None, Some(dur), None, status),
            Side::Both => (Some(dur), Some(dur), status.clone(), status),
        };
        let (src_service, dst_service) = match side {
            Side::Left => (service, None),
            Side::Right => (None, service),
            Side::Both => (service.clone(), service),
        };
        let (src_resource, dst_resource) = match side {
            Side::Left => (resource, None),
            Side::Right => (None, resource),
            Side::Both => (resource.clone(), resource),
        };
        entries.push(DiffEntry {
            kind,
            name: span.name.clone(),
            depth: current_depth,
            side,
            json_diffs: Vec::new(),
            src_json,
            dst_json,
            src_duration_ns,
            dst_duration_ns,
            src_status,
            dst_status,
            src_service,
            dst_service,
            src_resource,
            dst_resource,
            duration_delta_pct: None, // single-side entry, no delta
        });

        // Push children in reverse so the first child is processed first (pre-order).
        for &child in tree.nodes[current].children.iter().rev() {
            stack.push((child, current_depth + 1));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::match_trees::{MatchConfig, match_trees};
    use crate::model::{AnyValue, KeyValue, Span, SpanKind, SpanTree, Status, StatusCode};

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
            end_time_unix_nano: Some((start + 1000).to_string()),
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

    fn span_with_attr(
        id: &str,
        parent: Option<&str>,
        name: &str,
        start: u64,
        attrs: Vec<(&str, &str)>,
    ) -> Span {
        let mut s = span(id, parent, name, start);
        s.attributes = attrs
            .into_iter()
            .map(|(k, v)| KeyValue {
                key: k.to_owned(),
                value: AnyValue::String(v.to_owned()),
            })
            .collect();
        s
    }

    /// Build a `SpanTree` with raw JSON values derived by re-serializing the typed spans.
    /// This gives us realistic raw JSON for diffing in tests.
    fn tree_with_raw(spans: Vec<Span>) -> SpanTree {
        let raws: Vec<Value> = spans
            .iter()
            .map(|s| serde_json::to_value(s).unwrap_or(Value::Null))
            .collect();
        SpanTree::from_spans_with_raw(spans, raws).unwrap()
    }

    #[test]
    fn identical_trees_no_changes() {
        let spans = vec![
            span("1", None, "root", 0),
            span("2", Some("1"), "child", 100),
        ];
        let src = tree_with_raw(spans.clone());
        let dst = tree_with_raw(spans);
        let mappings = match_trees(&src, &dst, &MatchConfig::default());
        let diff = compute_diff(&src, &dst, &mappings, true);

        assert_eq!(diff.summary.matched_count, 2);
        assert_eq!(diff.summary.inserted_count, 0);
        assert_eq!(diff.summary.deleted_count, 0);
        assert_eq!(diff.summary.modified_count, 0);
        assert!(diff.divergence_index.is_none());
    }

    #[test]
    fn inserted_node_shows_in_diff() {
        let src_spans = vec![
            span("1", None, "root", 0),
            span("2", Some("1"), "child_a", 100),
        ];
        let dst_spans = vec![
            span("1", None, "root", 0),
            span("2", Some("1"), "child_a", 100),
            span("3", Some("1"), "child_b", 200),
        ];
        let src = tree_with_raw(src_spans);
        let dst = tree_with_raw(dst_spans);
        let mappings = match_trees(&src, &dst, &MatchConfig::default());
        let diff = compute_diff(&src, &dst, &mappings, true);

        assert_eq!(diff.summary.inserted_count, 1);
        assert!(diff.divergence_index.is_some());

        let inserted = diff
            .entries
            .iter()
            .find(|e| e.kind == DiffKind::Inserted)
            .unwrap();
        assert_eq!(inserted.name, "child_b");
        // Inserted node should have dst_json but not src_json.
        assert!(inserted.dst_json.is_some());
        assert!(inserted.src_json.is_none());
    }

    #[test]
    fn deleted_node_shows_in_diff() {
        let src_spans = vec![
            span("1", None, "root", 0),
            span("2", Some("1"), "child_a", 100),
            span("3", Some("1"), "child_b", 200),
        ];
        let dst_spans = vec![
            span("1", None, "root", 0),
            span("2", Some("1"), "child_a", 100),
        ];
        let src = tree_with_raw(src_spans);
        let dst = tree_with_raw(dst_spans);
        let mappings = match_trees(&src, &dst, &MatchConfig::default());
        let diff = compute_diff(&src, &dst, &mappings, true);

        assert_eq!(diff.summary.deleted_count, 1);

        let deleted = diff
            .entries
            .iter()
            .find(|e| e.kind == DiffKind::Deleted)
            .unwrap();
        assert_eq!(deleted.name, "child_b");
        // Deleted node should have src_json but not dst_json.
        assert!(deleted.src_json.is_some());
        assert!(deleted.dst_json.is_none());
    }

    #[test]
    fn modified_attributes_detected_via_json_diff() {
        let src_spans = vec![
            span("1", None, "root", 0),
            span_with_attr(
                "2",
                Some("1"),
                "tool.call",
                100,
                vec![("tool.name", "read_file"), ("tool.path", "old.py")],
            ),
        ];
        let dst_spans = vec![
            span("1", None, "root", 0),
            span_with_attr(
                "2",
                Some("1"),
                "tool.call",
                100,
                vec![("tool.name", "read_file"), ("tool.path", "new.py")],
            ),
        ];
        let src = tree_with_raw(src_spans);
        let dst = tree_with_raw(dst_spans);
        let mappings = match_trees(&src, &dst, &MatchConfig::default());
        let diff = compute_diff(&src, &dst, &mappings, true);

        assert_eq!(diff.summary.modified_count, 1);

        // Find the tool.call entry — it should have json_diffs.
        let modified_entry = diff
            .entries
            .iter()
            .find(|e| !e.json_diffs.is_empty())
            .unwrap();
        assert_eq!(modified_entry.name, "tool.call");

        // Should detect the attribute value change.
        // Note: re-serialized AnyValue uses Rust enum variant names (e.g. "String")
        // rather than OTLP JSON keys (e.g. "stringValue"). Both sides are serialized
        // the same way so the diff still works correctly.
        let paths: Vec<&str> = modified_entry
            .json_diffs
            .iter()
            .map(|d| d.path.as_str())
            .collect();
        assert!(
            paths.iter().any(|p| p.contains("attributes")),
            "expected an attributes path change, got: {paths:?}"
        );
    }

    #[test]
    fn summary_counts_are_consistent() {
        let src_spans = vec![
            span("1", None, "root", 0),
            span("2", Some("1"), "a", 100),
            span("3", Some("1"), "b", 200),
            span("4", Some("1"), "c", 300),
        ];
        let dst_spans = vec![
            span("1", None, "root", 0),
            span("2", Some("1"), "a", 100),
            span("5", Some("1"), "d", 250),
            span("4", Some("1"), "c", 300),
        ];
        let src = tree_with_raw(src_spans);
        let dst = tree_with_raw(dst_spans);
        let mappings = match_trees(&src, &dst, &MatchConfig::default());
        let diff = compute_diff(&src, &dst, &mappings, true);

        // Matched + Deleted should account for all src nodes.
        assert_eq!(
            diff.summary.matched_count + diff.summary.deleted_count,
            diff.summary.src_span_count,
        );
        // Matched + Inserted should account for all dst nodes.
        assert_eq!(
            diff.summary.matched_count + diff.summary.inserted_count,
            diff.summary.dst_span_count,
        );
    }

    #[test]
    fn matched_entries_carry_both_json_values() {
        let src_spans = vec![
            span("1", None, "root", 0),
            span_with_attr(
                "2",
                Some("1"),
                "tool.call",
                100,
                vec![("tool.name", "read_file"), ("tool.path", "old.py")],
            ),
        ];
        // Candidate has different end time (start+5000 instead of start+1000).
        let mut dst_root = span("1", None, "root", 0);
        dst_root.end_time_unix_nano = Some("5000".to_owned());
        let dst_spans = vec![
            dst_root,
            span_with_attr(
                "2",
                Some("1"),
                "tool.call",
                100,
                vec![("tool.name", "read_file"), ("tool.path", "new.py")],
            ),
        ];
        let src = tree_with_raw(src_spans);
        let dst = tree_with_raw(dst_spans);
        let mappings = match_trees(&src, &dst, &MatchConfig::default());
        let diff = compute_diff(&src, &dst, &mappings, true);

        // All matched entries should have both src_json and dst_json.
        for entry in &diff.entries {
            if entry.kind == DiffKind::Matched {
                assert!(
                    entry.src_json.is_some(),
                    "matched entry '{}' missing src_json",
                    entry.name
                );
                assert!(
                    entry.dst_json.is_some(),
                    "matched entry '{}' missing dst_json",
                    entry.name
                );
            }
        }

        // Root should have json_diffs for the endTimeUnixNano change.
        let root = &diff.entries[0];
        assert!(
            root.json_diffs
                .iter()
                .any(|d| d.path.contains("endTimeUnixNano")),
            "expected endTimeUnixNano diff on root, got: {:?}",
            root.json_diffs.iter().map(|d| &d.path).collect::<Vec<_>>()
        );
    }

    #[test]
    fn json_diff_detects_type_changes() {
        let old = serde_json::json!({"a": 1, "b": "hello"});
        let new = serde_json::json!({"a": "one", "b": "hello"});
        let diffs = diff_json("", &old, &new);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].path, "a");
        assert!(matches!(diffs[0].change, JsonChange::Modified { .. }));
    }

    #[test]
    fn json_diff_detects_array_length_changes() {
        let old = serde_json::json!({"items": [1, 2, 3]});
        let new = serde_json::json!({"items": [1, 2, 3, 4]});
        let diffs = diff_json("", &old, &new);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].path, "items[3]");
        assert!(matches!(diffs[0].change, JsonChange::Added { .. }));
    }

    #[test]
    fn json_diff_detects_nested_object_changes() {
        let old = serde_json::json!({"status": {"code": 1, "message": "ok"}});
        let new = serde_json::json!({"status": {"code": 2, "message": "error"}});
        let diffs = diff_json("", &old, &new);
        assert_eq!(diffs.len(), 2);
        let paths: Vec<&str> = diffs.iter().map(|d| d.path.as_str()).collect();
        assert!(paths.contains(&"status.code"));
        assert!(paths.contains(&"status.message"));
    }

    #[test]
    fn is_excluded_exact_match() {
        assert!(is_excluded("trace_id"));
        assert!(is_excluded("span_id"));
        assert!(is_excluded("hostname"));
        assert!(is_excluded("startTimeUnixNano"));
        assert!(is_excluded("traceId"));
    }

    #[test]
    fn is_excluded_prefix_match() {
        assert!(is_excluded("meta._dd.p.tid"));
        assert!(is_excluded("meta._dd.p.ftid"));
        assert!(is_excluded("metrics._dd.agent_priority_sampler.target_tps"));
        assert!(is_excluded("metrics._dd.agent_errors_sampler.target_tps"));
        assert!(is_excluded("children_ids[0]"));
        assert!(is_excluded("children_ids[3]"));
    }

    #[test]
    fn is_excluded_does_not_match_signal_fields() {
        assert!(!is_excluded("status"));
        assert!(!is_excluded("name"));
        assert!(!is_excluded("duration"));
        assert!(!is_excluded("service"));
        assert!(!is_excluded("resource"));
        assert!(!is_excluded("meta.error.message"));
        assert!(!is_excluded("meta.version"));
        assert!(!is_excluded("meta._dd.code_origin.frames.0.line"));
    }

    #[test]
    fn filtering_removes_noise_from_dd_spans() {
        // Simulate two DD raw span objects that differ in noise + signal.
        let src_raw = serde_json::json!({
            "name": "spring.handler",
            "trace_id": "111",
            "span_id": "222",
            "start": 1000.0,
            "end": 1001.0,
            "hostname": "host-a",
            "status": "ok",
            "duration": 1.0,
            "meta": {
                "_dd.agent_hostname": "host-a",
                "_dd.p.tid": "aaa",
                "version": "v1.0"
            }
        });
        let dst_raw = serde_json::json!({
            "name": "spring.handler",
            "trace_id": "999",
            "span_id": "888",
            "start": 2000.0,
            "end": 2002.0,
            "hostname": "host-b",
            "status": "error",
            "duration": 2.0,
            "meta": {
                "_dd.agent_hostname": "host-b",
                "_dd.p.tid": "bbb",
                "version": "v2.0"
            }
        });

        let spans = vec![span("1", None, "spring.handler", 0)];
        let src = SpanTree::from_spans_with_raw(spans.clone(), vec![src_raw]).unwrap();
        let dst = SpanTree::from_spans_with_raw(spans, vec![dst_raw]).unwrap();
        let mappings = match_trees(&src, &dst, &MatchConfig::default());

        // With filtering (include_all = false).
        let diff = compute_diff(&src, &dst, &mappings, false);
        let paths: Vec<&str> = diff.entries[0]
            .json_diffs
            .iter()
            .map(|d| d.path.as_str())
            .collect();

        // Signal fields should be present.
        assert!(paths.contains(&"status"), "status should be kept");
        assert!(paths.contains(&"duration"), "duration should be kept");
        assert!(
            paths.contains(&"meta.version"),
            "meta.version should be kept"
        );

        // Noise fields should be filtered out.
        assert!(!paths.contains(&"trace_id"), "trace_id should be excluded");
        assert!(!paths.contains(&"span_id"), "span_id should be excluded");
        assert!(!paths.contains(&"start"), "start should be excluded");
        assert!(!paths.contains(&"end"), "end should be excluded");
        assert!(!paths.contains(&"hostname"), "hostname should be excluded");
        assert!(
            !paths.contains(&"meta._dd.agent_hostname"),
            "_dd.agent_hostname should be excluded"
        );
        assert!(
            !paths.contains(&"meta._dd.p.tid"),
            "_dd.p.tid should be excluded"
        );
    }

    #[test]
    fn include_all_bypasses_filtering() {
        let src_raw = serde_json::json!({
            "name": "root",
            "trace_id": "111",
            "span_id": "222",
            "status": "ok"
        });
        let dst_raw = serde_json::json!({
            "name": "root",
            "trace_id": "999",
            "span_id": "888",
            "status": "error"
        });

        let spans = vec![span("1", None, "root", 0)];
        let src = SpanTree::from_spans_with_raw(spans.clone(), vec![src_raw]).unwrap();
        let dst = SpanTree::from_spans_with_raw(spans, vec![dst_raw]).unwrap();
        let mappings = match_trees(&src, &dst, &MatchConfig::default());

        let diff = compute_diff(&src, &dst, &mappings, true);
        let paths: Vec<&str> = diff.entries[0]
            .json_diffs
            .iter()
            .map(|d| d.path.as_str())
            .collect();

        // With include_all, noise fields should still be present.
        assert!(paths.contains(&"trace_id"));
        assert!(paths.contains(&"span_id"));
        assert!(paths.contains(&"status"));
    }

    #[test]
    fn is_keyed_array_detects_otlp_attributes() {
        let arr = serde_json::json!([
            {"key": "service.name", "value": {"stringValue": "my-svc"}},
            {"key": "http.method", "value": {"stringValue": "GET"}}
        ]);
        assert!(is_keyed_array(arr.as_array().unwrap()));
    }

    #[test]
    fn is_keyed_array_rejects_plain_arrays() {
        let arr = serde_json::json!([1, 2, 3]);
        assert!(!is_keyed_array(arr.as_array().unwrap()));
    }

    #[test]
    fn is_keyed_array_rejects_empty() {
        let arr = serde_json::json!([]);
        assert!(!is_keyed_array(arr.as_array().unwrap()));
    }

    #[test]
    fn is_keyed_array_rejects_objects_without_key() {
        let arr = serde_json::json!([{"name": "foo"}, {"name": "bar"}]);
        assert!(!is_keyed_array(arr.as_array().unwrap()));
    }

    #[test]
    fn keyed_array_diff_matches_by_key_not_position() {
        // Same attributes in different order — no diffs expected.
        let old = serde_json::json!([
            {"key": "a", "value": {"stringValue": "1"}},
            {"key": "b", "value": {"stringValue": "2"}}
        ]);
        let new = serde_json::json!([
            {"key": "b", "value": {"stringValue": "2"}},
            {"key": "a", "value": {"stringValue": "1"}}
        ]);
        let diffs = diff_json("attrs", &old, &new);
        assert!(
            diffs.is_empty(),
            "reordered keyed arrays should produce no diffs, got: {diffs:?}"
        );
    }

    #[test]
    fn keyed_array_diff_detects_value_change() {
        let old = serde_json::json!([
            {"key": "http.method", "value": {"stringValue": "GET"}}
        ]);
        let new = serde_json::json!([
            {"key": "http.method", "value": {"stringValue": "POST"}}
        ]);
        let diffs = diff_json("attrs", &old, &new);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].path, "attrs.http.method.value.stringValue");
    }

    #[test]
    fn keyed_array_diff_detects_added_and_removed() {
        let old = serde_json::json!([
            {"key": "a", "value": {"stringValue": "1"}},
            {"key": "b", "value": {"stringValue": "2"}}
        ]);
        let new = serde_json::json!([
            {"key": "b", "value": {"stringValue": "2"}},
            {"key": "c", "value": {"stringValue": "3"}}
        ]);
        let diffs = diff_json("attrs", &old, &new);
        assert_eq!(diffs.len(), 2);
        let paths: Vec<&str> = diffs.iter().map(|d| d.path.as_str()).collect();
        assert!(
            paths.contains(&"attrs.a"),
            "expected removal of 'a', got: {paths:?}"
        );
        assert!(
            paths.contains(&"attrs.c"),
            "expected addition of 'c', got: {paths:?}"
        );

        // Verify the types.
        let removed = diffs.iter().find(|d| d.path == "attrs.a").unwrap();
        assert!(matches!(removed.change, JsonChange::Removed { .. }));
        let added = diffs.iter().find(|d| d.path == "attrs.c").unwrap();
        assert!(matches!(added.change, JsonChange::Added { .. }));
    }

    #[test]
    fn keyed_array_falls_back_to_positional_for_non_keyed() {
        // Plain integer array — should use positional diff.
        let old = serde_json::json!({"items": [1, 2]});
        let new = serde_json::json!({"items": [2, 1]});
        let diffs = diff_json("", &old, &new);
        // Positional: items[0] changed 1→2, items[1] changed 2→1.
        assert_eq!(diffs.len(), 2);
        assert_eq!(diffs[0].path, "items[0]");
        assert_eq!(diffs[1].path, "items[1]");
    }

    /// A 2000-level deep chain must not stack-overflow.
    ///
    /// Before the recursive-to-iterative conversion, `aligned_walk` and
    /// `emit_subtree` would blow the stack for chains this deep.
    #[test]
    fn deep_chain_does_not_stack_overflow() {
        const DEPTH: usize = 2000;

        let mut spans = Vec::with_capacity(DEPTH);
        for i in 0..DEPTH {
            let id = (i + 1).to_string();
            let parent = if i == 0 { None } else { Some(i.to_string()) };
            let start = (i as u64) * 100;
            spans.push(span(&id, parent.as_deref(), &format!("node_{i}"), start));
        }

        let src = tree_with_raw(spans.clone());
        let dst = tree_with_raw(spans);

        let mappings = match_trees(&src, &dst, &MatchConfig::default());
        let diff = compute_diff(&src, &dst, &mappings, false);

        // Every node in the chain should appear as a matched entry.
        assert_eq!(diff.entries.len(), DEPTH);
        assert!(
            diff.entries.iter().all(|e| e.kind == DiffKind::Matched),
            "all entries should be matched for identical deep chains"
        );
    }
}
