//! Trace deserialization and span tree construction.
//!
//! Supports two trace formats:
//!
//! 1. **OTLP JSON** — the OpenTelemetry Protocol JSON export format with nested
//!    `resourceSpans[].scopeSpans[].spans[]` structure.
//! 2. **Datadog UI export** — the format produced by Datadog's trace viewer export
//!    button, with a `{ "trace": { "spans": { <id>: {...}, ... } } }` structure.
//!
//! Both formats are parsed into a common `SpanTree` (arena of `SpanNode`s) with
//! precomputed height, size, and structural hashes for `GumTree` matching.
//!
//! The OTLP JSON format has quirks: `intValue` and timestamps are JSON strings (not numbers),
//! `traceId`/`spanId` are hex-encoded bytes, and `kind`/`status.code` can appear as either
//! integers or string enum names. The serde model handles all variants.
//!
//! The Datadog UI export format differs from the Datadog Agent API (`/v0.3/traces`):
//! IDs are decimal strings (not int64), timestamps are float seconds (not nanos),
//! status is `"ok"`/`"error"` (not `error: 0/1`), and spans are keyed by ID in an
//! object (not a flat array).

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

use serde::{Deserialize, Deserializer, Serialize};

/// Top-level OTLP trace export. One per JSON line in a `.jsonl` file,
/// or a single object in a `.json` file.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TracesData {
    #[serde(default)]
    pub resource_spans: Vec<ResourceSpans>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSpans {
    #[serde(default)]
    pub resource: Option<Resource>,
    #[serde(default)]
    pub scope_spans: Vec<ScopeSpans>,
    #[serde(default)]
    pub schema_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Resource {
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
    #[serde(default)]
    pub dropped_attributes_count: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeSpans {
    #[serde(default)]
    pub scope: Option<InstrumentationScope>,
    #[serde(default)]
    pub spans: Vec<Span>,
    #[serde(default)]
    pub schema_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstrumentationScope {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
    #[serde(default)]
    pub dropped_attributes_count: u32,
}

/// A single span in the OTLP format.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Span {
    #[serde(default)]
    pub trace_id: String,
    #[serde(default)]
    pub span_id: String,
    #[serde(default)]
    pub trace_state: Option<String>,
    #[serde(default)]
    pub parent_span_id: Option<String>,
    #[serde(default)]
    pub flags: Option<u32>,
    #[serde(default)]
    pub name: String,
    #[serde(default, deserialize_with = "deserialize_span_kind")]
    pub kind: SpanKind,
    /// Nanosecond timestamp as a string (proto3 `fixed64` encoding).
    #[serde(default)]
    pub start_time_unix_nano: Option<String>,
    #[serde(default)]
    pub end_time_unix_nano: Option<String>,
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
    #[serde(default)]
    pub dropped_attributes_count: u32,
    #[serde(default)]
    pub events: Vec<Event>,
    #[serde(default)]
    pub dropped_events_count: u32,
    #[serde(default)]
    pub links: Vec<Link>,
    #[serde(default)]
    pub dropped_links_count: u32,
    #[serde(default)]
    pub status: Option<Status>,
}

impl Span {
    /// Parse `start_time_unix_nano` into a `u64`. Returns 0 on missing/bad data.
    #[must_use]
    pub fn start_nanos(&self) -> u64 {
        self.start_time_unix_nano
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    /// Parse `end_time_unix_nano` into a `u64`. Returns 0 on missing/bad data.
    #[must_use]
    pub fn end_nanos(&self) -> u64 {
        self.end_time_unix_nano
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    /// Duration in nanoseconds.
    #[must_use]
    pub fn duration_nanos(&self) -> u64 {
        self.end_nanos().saturating_sub(self.start_nanos())
    }

    /// Look up an attribute value by key.
    #[must_use]
    pub fn attribute(&self, key: &str) -> Option<&AnyValue> {
        self.attributes
            .iter()
            .find(|kv| kv.key == key)
            .map(|kv| &kv.value)
    }

    /// Extract the service name from attributes.
    ///
    /// Checks `service.name` (OTLP convention and DD-converted spans).
    #[must_use]
    pub fn service_name(&self) -> Option<String> {
        self.attribute("service.name")
            .map(AnyValue::as_display_string)
    }

    /// Extract a resource identifier from attributes.
    ///
    /// Checks DD-converted `dd.resource` first, then OTLP HTTP conventions
    /// (`http.route`, `http.target`, `url.path`).
    #[must_use]
    pub fn resource_name(&self) -> Option<String> {
        for key in &["dd.resource", "http.route", "http.target", "url.path"] {
            if let Some(val) = self.attribute(key) {
                return Some(val.as_display_string());
            }
        }
        None
    }

    /// Normalized status string: `"ok"`, `"error"`, or `"unset"`.
    #[must_use]
    pub fn status_str(&self) -> &'static str {
        self.status.as_ref().map_or("unset", |s| s.code.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Default)]
pub enum SpanKind {
    #[default]
    Unspecified,
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

impl SpanKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Internal => "internal",
            Self::Server => "server",
            Self::Client => "client",
            Self::Producer => "producer",
            Self::Consumer => "consumer",
        }
    }
}

fn deserialize_span_kind<'de, D: Deserializer<'de>>(d: D) -> Result<SpanKind, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        Int(u64),
        Str(String),
    }
    let raw = Raw::deserialize(d)?;
    Ok(match raw {
        Raw::Int(1) => SpanKind::Internal,
        Raw::Int(2) => SpanKind::Server,
        Raw::Int(3) => SpanKind::Client,
        Raw::Int(4) => SpanKind::Producer,
        Raw::Int(5) => SpanKind::Consumer,
        Raw::Str(s) if s.contains("INTERNAL") => SpanKind::Internal,
        Raw::Str(s) if s.contains("SERVER") => SpanKind::Server,
        Raw::Str(s) if s.contains("CLIENT") => SpanKind::Client,
        Raw::Str(s) if s.contains("PRODUCER") => SpanKind::Producer,
        Raw::Str(s) if s.contains("CONSUMER") => SpanKind::Consumer,
        _ => SpanKind::Unspecified, // OTLP 0 + unknown ints + unrecognized strings
    })
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default, deserialize_with = "deserialize_status_code")]
    pub code: StatusCode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Default)]
pub enum StatusCode {
    #[default]
    Unset,
    Ok,
    Error,
}

impl StatusCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unset => "unset",
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }
}

fn deserialize_status_code<'de, D: Deserializer<'de>>(d: D) -> Result<StatusCode, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        Int(u64),
        Str(String),
    }
    let raw = Raw::deserialize(d)?;
    Ok(match raw {
        Raw::Int(1) => StatusCode::Ok,
        Raw::Int(2) => StatusCode::Error,
        Raw::Str(s) if s.contains("OK") => StatusCode::Ok,
        Raw::Str(s) if s.contains("ERROR") => StatusCode::Error,
        _ => StatusCode::Unset,
    })
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    #[serde(default)]
    pub time_unix_nano: Option<String>,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
    #[serde(default)]
    pub dropped_attributes_count: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Link {
    #[serde(default)]
    pub trace_id: String,
    #[serde(default)]
    pub span_id: String,
    #[serde(default)]
    pub trace_state: Option<String>,
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
    #[serde(default)]
    pub dropped_attributes_count: u32,
    #[serde(default)]
    pub flags: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KeyValue {
    pub key: String,
    pub value: AnyValue,
}

/// Typed attribute value, matching OTLP's `AnyValue` oneof.
///
/// Each variant is a JSON object with exactly one typed key:
/// `{"stringValue": "x"}`, `{"intValue": "123"}`, `{"boolValue": true}`, etc.
///
/// `intValue` is a JSON string (not number) per proto3 JSON mapping.
#[derive(Debug, Clone, Serialize)]
pub enum AnyValue {
    String(String),
    Bool(bool),
    Int(String),
    Double(f64),
    Array(Vec<AnyValue>),
    KvList(Vec<KeyValue>),
    Bytes(String),
}

impl<'de> Deserialize<'de> for AnyValue {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // OTLP AnyValue is a JSON object with exactly one key indicating the type.
        let map: serde_json::Map<String, serde_json::Value> = serde_json::Map::deserialize(d)?;

        if let Some(v) = map.get("stringValue") {
            return Ok(Self::String(v.as_str().unwrap_or_default().to_owned()));
        }
        if let Some(v) = map.get("boolValue") {
            return Ok(Self::Bool(v.as_bool().unwrap_or_default()));
        }
        if let Some(v) = map.get("intValue") {
            // Proto3 encodes int64 as a JSON string.
            let s = match v {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                _ => std::string::String::new(),
            };
            return Ok(Self::Int(s));
        }
        if let Some(v) = map.get("doubleValue") {
            return Ok(Self::Double(v.as_f64().unwrap_or(0.0)));
        }
        if let Some(v) = map.get("arrayValue") {
            let vals: Vec<AnyValue> = v
                .get("values")
                .and_then(serde_json::Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(|item| serde_json::from_value(item.clone()).ok())
                        .collect()
                })
                .unwrap_or_default();
            return Ok(Self::Array(vals));
        }
        if let Some(v) = map.get("kvlistValue") {
            let vals: Vec<KeyValue> = v
                .get("values")
                .and_then(serde_json::Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(|item| serde_json::from_value(item.clone()).ok())
                        .collect()
                })
                .unwrap_or_default();
            return Ok(Self::KvList(vals));
        }
        if let Some(v) = map.get("bytesValue") {
            return Ok(Self::Bytes(v.as_str().unwrap_or_default().to_owned()));
        }

        // Fallback: empty string.
        Ok(Self::String(String::new()))
    }
}

impl AnyValue {
    /// Extract the value as a display-friendly string.
    #[must_use]
    pub fn as_display_string(&self) -> String {
        match self {
            Self::String(s) | Self::Int(s) => s.clone(),
            Self::Bool(b) => b.to_string(),
            Self::Double(d) => d.to_string(),
            Self::Array(a) => format!("[{} items]", a.len()),
            Self::KvList(kv) => format!("{{{} entries}}", kv.len()),
            Self::Bytes(b) => format!("<{} bytes>", b.len()),
        }
    }
}

/// Unique node identifier within a `SpanTree`.
pub type NodeId = usize;

/// A node in the span tree. Contains the original span data plus precomputed
/// metrics for `GumTree` matching: height, subtree size, and structural hash.
#[derive(Debug, Clone)]
pub struct SpanNode {
    pub id: NodeId,
    pub span: Span,
    /// Raw JSON value of this span, preserved for generic diffing.
    pub raw: serde_json::Value,
    pub children: Vec<NodeId>,
    pub parent: Option<NodeId>,

    // Precomputed by `SpanTree::compute_metrics`.
    pub height: usize,
    pub size: usize,
    pub structural_hash: u64,
}

/// A rooted tree of spans, stored as a flat arena with parent/child indices.
#[derive(Debug, Clone)]
pub struct SpanTree {
    pub nodes: Vec<SpanNode>,
    pub root: NodeId,
}

impl SpanTree {
    /// Build a `SpanTree` from an OTLP `TracesData` export.
    ///
    /// Flattens all spans from all resource/scope layers, reconstructs the
    /// parent-child tree from `parentSpanId`, and precomputes metrics.
    ///
    /// If the trace has multiple root spans, a synthetic root is created.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the trace contains zero spans.
    #[cfg(test)]
    pub fn from_otlp(data: &TracesData) -> Result<Self, TraceError> {
        // Flatten all spans (typed).
        let spans: Vec<Span> = data
            .resource_spans
            .iter()
            .flat_map(|rs| &rs.scope_spans)
            .flat_map(|ss| &ss.spans)
            .cloned()
            .collect();

        if spans.is_empty() {
            return Err(TraceError::EmptyTrace);
        }

        // Use null raw values — `from_otlp_raw` is the preferred entry point
        // that preserves raw JSON. This fallback keeps existing call sites working.
        let raws = vec![serde_json::Value::Null; spans.len()];
        Self::from_spans_with_raw(spans, raws)
    }

    /// Build a `SpanTree` from typed spans paired with their raw JSON values.
    ///
    /// This is the primary constructor when raw JSON preservation is needed
    /// (i.e., for generic JSON diffing of matched span pairs).
    ///
    /// # Errors
    ///
    /// Returns `Err` if the trace contains zero spans.
    pub fn from_otlp_raw(
        data: &TracesData,
        raw_root: &serde_json::Value,
    ) -> Result<Self, TraceError> {
        // Flatten typed spans.
        let spans: Vec<Span> = data
            .resource_spans
            .iter()
            .flat_map(|rs| &rs.scope_spans)
            .flat_map(|ss| &ss.spans)
            .cloned()
            .collect();

        if spans.is_empty() {
            return Err(TraceError::EmptyTrace);
        }

        // Extract raw span values by walking the same JSON structure.
        let mut raws = Vec::with_capacity(spans.len());
        if let Some(rs_arr) = raw_root.get("resourceSpans").and_then(|v| v.as_array()) {
            for rs in rs_arr {
                if let Some(ss_arr) = rs.get("scopeSpans").and_then(|v| v.as_array()) {
                    for ss in ss_arr {
                        if let Some(sp_arr) = ss.get("spans").and_then(|v| v.as_array()) {
                            for sp in sp_arr {
                                raws.push(sp.clone());
                            }
                        }
                    }
                }
            }
        }

        // Pad with Null if the walk produced fewer values than typed parse
        // (shouldn't happen, but be defensive).
        raws.resize(spans.len(), serde_json::Value::Null);

        Self::from_spans_with_raw(spans, raws)
    }

    /// Build from a flat list of spans. Reconstructs the tree via `parentSpanId`.
    /// Raw JSON values default to Null.
    ///
    /// # Errors
    ///
    /// Returns `Err` if spans is empty.
    #[cfg(test)]
    pub fn from_spans(spans: Vec<Span>) -> Result<Self, TraceError> {
        let len = spans.len();
        Self::from_spans_with_raw(spans, vec![serde_json::Value::Null; len])
    }

    /// Build from paired typed spans and raw JSON values.
    ///
    /// # Errors
    ///
    /// Returns `Err` if spans is empty.
    pub fn from_spans_with_raw(
        spans: Vec<Span>,
        raws: Vec<serde_json::Value>,
    ) -> Result<Self, TraceError> {
        if spans.is_empty() {
            return Err(TraceError::EmptyTrace);
        }

        // Assign node IDs and build span_id → node_id index.
        let mut nodes: Vec<SpanNode> = spans
            .into_iter()
            .zip(raws)
            .enumerate()
            .map(|(id, (span, raw))| SpanNode {
                id,
                span,
                raw,
                children: Vec::new(),
                parent: None,
                height: 0,
                size: 0,
                structural_hash: 0,
            })
            .collect();

        let span_id_to_node: HashMap<&str, NodeId> = nodes
            .iter()
            .map(|n| (n.span.span_id.as_str(), n.id))
            .collect();

        // Wire parent-child relationships.
        let mut roots = Vec::new();
        // Collect parent assignments first to avoid borrow conflict.
        let parent_assignments: Vec<(NodeId, Option<NodeId>)> = nodes
            .iter()
            .map(|node| {
                let parent_id = node
                    .span
                    .parent_span_id
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .and_then(|pid| span_id_to_node.get(pid).copied());
                (node.id, parent_id)
            })
            .collect();

        for &(node_id, parent_id) in &parent_assignments {
            if let Some(pid) = parent_id {
                nodes[node_id].parent = Some(pid);
                nodes[pid].children.push(node_id);
            } else {
                roots.push(node_id);
            }
        }

        // Sort children by start time for deterministic ordering.
        // Collect start times first to avoid borrowing nodes while sorting.
        let start_times: Vec<u64> = nodes.iter().map(|n| n.span.start_nanos()).collect();
        for node in &mut nodes {
            node.children.sort_by_key(|&cid| start_times[cid]);
        }

        // Determine root: single root, or create a synthetic root if multiple.
        let root = if roots.len() == 1 {
            roots[0]
        } else {
            // Synthetic root spanning all real roots.
            let synthetic_id = nodes.len();
            // Sort roots by start time.
            roots.sort_by_key(|&rid| nodes[rid].span.start_nanos());
            for &rid in &roots {
                nodes[rid].parent = Some(synthetic_id);
            }
            nodes.push(SpanNode {
                id: synthetic_id,
                span: Span {
                    trace_id: nodes
                        .first()
                        .map_or_else(String::new, |n| n.span.trace_id.clone()),
                    span_id: "__synthetic_root__".to_owned(),
                    trace_state: None,
                    parent_span_id: None,
                    flags: None,
                    name: "<root>".to_owned(),
                    kind: SpanKind::Internal,
                    start_time_unix_nano: None,
                    end_time_unix_nano: None,
                    attributes: Vec::new(),
                    dropped_attributes_count: 0,
                    events: Vec::new(),
                    dropped_events_count: 0,
                    links: Vec::new(),
                    dropped_links_count: 0,
                    status: None,
                },
                raw: serde_json::Value::Null,
                children: roots,
                parent: None,
                height: 0,
                size: 0,
                structural_hash: 0,
            });
            synthetic_id
        };

        let mut tree = Self { nodes, root };
        tree.compute_metrics(tree.root);
        Ok(tree)
    }

    /// Number of nodes in the tree.
    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the tree is empty.
    #[must_use]
    #[allow(dead_code)] // standard collection API
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Iterate node IDs in post-order (children before parents).
    #[must_use]
    pub fn post_order(&self) -> Vec<NodeId> {
        let mut result = Vec::with_capacity(self.nodes.len());
        let mut stack = vec![(self.root, false)];
        while let Some((nid, visited)) = stack.pop() {
            if visited {
                result.push(nid);
            } else {
                stack.push((nid, true));
                // Push children in reverse so leftmost is processed first.
                for &cid in self.nodes[nid].children.iter().rev() {
                    stack.push((cid, false));
                }
            }
        }
        result
    }

    /// Iterate node IDs in pre-order (parents before children).
    #[must_use]
    #[allow(dead_code)] // used in tests, useful public API
    pub fn pre_order(&self) -> Vec<NodeId> {
        let mut result = Vec::with_capacity(self.nodes.len());
        let mut stack = vec![self.root];
        while let Some(nid) = stack.pop() {
            result.push(nid);
            for &cid in self.nodes[nid].children.iter().rev() {
                stack.push(cid);
            }
        }
        result
    }

    /// Get all descendants of a node (not including the node itself).
    #[must_use]
    pub fn descendants(&self, nid: NodeId) -> Vec<NodeId> {
        let mut result = Vec::new();
        let mut stack: Vec<NodeId> = self.nodes[nid].children.clone();
        while let Some(curr) = stack.pop() {
            result.push(curr);
            for &cid in self.nodes[curr].children.iter().rev() {
                stack.push(cid);
            }
        }
        result
    }

    /// Is this node a leaf (no children)?
    #[must_use]
    #[allow(dead_code)] // standard tree API
    pub fn is_leaf(&self, nid: NodeId) -> bool {
        self.nodes[nid].children.is_empty()
    }

    /// Compute height, size, and structural hash for every node (post-order).
    fn compute_metrics(&mut self, root: NodeId) {
        // Post-order traversal: children before parents.
        let order = self.post_order_from(root);
        for nid in order {
            let children = self.nodes[nid].children.clone();
            if children.is_empty() {
                self.nodes[nid].height = 1;
                self.nodes[nid].size = 1;
            } else {
                let max_child_height = children
                    .iter()
                    .map(|&cid| self.nodes[cid].height)
                    .max()
                    .unwrap_or(0);
                let total_child_size: usize =
                    children.iter().map(|&cid| self.nodes[cid].size).sum();
                self.nodes[nid].height = 1 + max_child_height;
                self.nodes[nid].size = 1 + total_child_size;
            }

            // Merkle-style structural hash: hash(name, kind, [child_hashes]).
            //
            // NOTE: `DefaultHasher` is not guaranteed to be stable across Rust
            // versions or compiler builds.  This is acceptable because both the
            // src and dst trees are always constructed in the same process (same
            // binary), so the hashes are consistent within a single diff run.
            // If structural hashes ever need to be persisted or compared across
            // builds, replace `DefaultHasher` with a deterministic hasher such
            // as `SipHasher13` from the `siphasher` crate.
            let mut hasher = DefaultHasher::new();
            self.nodes[nid].span.name.hash(&mut hasher);
            self.nodes[nid].span.kind.as_str().hash(&mut hasher);
            for &cid in &children {
                self.nodes[cid].structural_hash.hash(&mut hasher);
            }
            self.nodes[nid].structural_hash = hasher.finish();
        }
    }

    /// Post-order from a specific root (for subtree metrics).
    fn post_order_from(&self, root: NodeId) -> Vec<NodeId> {
        let mut result = Vec::new();
        let mut stack = vec![(root, false)];
        while let Some((nid, visited)) = stack.pop() {
            if visited {
                result.push(nid);
            } else {
                stack.push((nid, true));
                for &cid in self.nodes[nid].children.iter().rev() {
                    stack.push((cid, false));
                }
            }
        }
        result
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TraceError {
    #[error("trace contains no spans")]
    EmptyTrace,

    #[error("input too large: {bytes} bytes exceeds {max_bytes} byte limit")]
    InputTooLarge { bytes: usize, max_bytes: usize },

    #[error("invalid JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),

    #[error(
        "unrecognized trace format: expected OTLP (with 'resourceSpans') \
         or Datadog UI export (with 'trace.spans')"
    )]
    UnrecognizedFormat,

    #[error("serialization failed: {0}")]
    Serialization(serde_json::Error),
}

/// Parse an OTLP JSON file. Supports both single-object and JSON-lines format.
///
/// For JSON-lines, each line must be a complete JSON object on one line (minified).
/// Pretty-printed JSON is handled as a single object.
///
/// # Errors
///
/// Returns `Err` on invalid JSON.
#[cfg(test)]
pub fn parse_otlp_file(contents: &str) -> Result<TracesData, TraceError> {
    let trimmed = contents.trim();

    // Try single JSON object first (handles pretty-printed).
    if let Ok(data) = serde_json::from_str::<TracesData>(trimmed) {
        return Ok(data);
    }

    // Try JSON-lines: each line is a minified `TracesData`, merge them.
    let mut merged = TracesData {
        resource_spans: Vec::new(),
    };
    for line in trimmed.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let data: TracesData = serde_json::from_str(line)?;
        merged.resource_spans.extend(data.resource_spans);
    }
    Ok(merged)
}

/// Parse an OTLP JSON file, returning both the typed `TracesData` and the raw
/// `serde_json::Value` so that per-span raw JSON can be preserved for generic
/// diffing.
///
/// Parses the input string twice (once into the typed model, once into a raw
/// `Value` tree) rather than parsing once and deep-cloning the `Value`. Two
/// streaming parses from the same string are faster than one parse plus an
/// O(n) heap-allocating `Value::clone()`.
///
/// # Errors
///
/// Returns `Err` on invalid JSON.
pub fn parse_otlp_file_raw(contents: &str) -> Result<(TracesData, serde_json::Value), TraceError> {
    let trimmed = contents.trim();

    // Try single JSON object first.
    if let Ok(data) = serde_json::from_str::<TracesData>(trimmed) {
        let raw: serde_json::Value = serde_json::from_str(trimmed)?;
        return Ok((data, raw));
    }

    // JSON-lines: merge both typed and raw.
    let mut merged_data = TracesData {
        resource_spans: Vec::new(),
    };
    let mut merged_rs: Vec<serde_json::Value> = Vec::new();
    for line in trimmed.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let data: TracesData = serde_json::from_str(line)?;
        let raw: serde_json::Value = serde_json::from_str(line)?;
        if let Some(arr) = raw.get("resourceSpans").and_then(|v| v.as_array()) {
            merged_rs.extend(arr.iter().cloned());
        }
        merged_data.resource_spans.extend(data.resource_spans);
    }
    let merged_raw = serde_json::json!({ "resourceSpans": merged_rs });
    Ok((merged_data, merged_raw))
}

/// Top-level wrapper for a Datadog UI trace export.
///
/// Structure: `{ "trace": { "root_id": "...", "spans": { "<id>": {...}, ... } }, ... }`
#[derive(Debug, Clone, Deserialize)]
pub struct DatadogUiExport {
    pub trace: DatadogUiTrace,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatadogUiTrace {
    /// Span ID of the trace root (not currently used — tree root is inferred
    /// from `parent_id == "0"`).
    #[allow(dead_code)]
    pub root_id: String,
    /// Spans keyed by span ID (decimal string).
    pub spans: HashMap<String, serde_json::Value>,
}

/// Parse a Datadog UI export JSON file, returning per-span typed `Span`s and
/// their raw `serde_json::Value`s for generic diffing.
///
/// Converts the Datadog schema into the canonical `Span` representation:
/// - `name` (operation type, e.g. "spring.handler") → `span.name`
/// - Decimal string IDs → hex-encoded string IDs
/// - Float-seconds timestamps → nanosecond strings
/// - `"status": "error"` → `Status { code: Error }`
///
/// # Errors
///
/// Returns `Err` on invalid JSON or if the export contains no spans.
pub fn parse_datadog_ui_file_raw(
    contents: &str,
) -> Result<(Vec<Span>, Vec<serde_json::Value>), TraceError> {
    let export: DatadogUiExport = serde_json::from_str(contents.trim())?;

    let trace = &export.trace;
    if trace.spans.is_empty() {
        return Err(TraceError::EmptyTrace);
    }

    let mut pairs: Vec<(Span, serde_json::Value)> = trace
        .spans
        .values()
        .map(|raw_value| (dd_raw_to_span(raw_value), raw_value.clone()))
        .collect();

    // Sort by start time for deterministic ordering (HashMap iteration order
    // is not guaranteed).
    pairs.sort_by(|(a, _), (b, _)| a.start_nanos().cmp(&b.start_nanos()));

    let (spans, raws) = pairs.into_iter().unzip();

    Ok((spans, raws))
}

/// Convert a single Datadog UI export span JSON value into the canonical `Span`.
fn dd_raw_to_span(raw: &serde_json::Value) -> Span {
    // Extract fields with sensible defaults.
    let name = raw
        .get("name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_owned();
    let span_id_dec = raw
        .get("span_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("0");
    let parent_id_dec = raw
        .get("parent_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("0");
    let trace_id_dec = raw
        .get("trace_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("0");

    // Convert decimal string IDs to hex for internal consistency.
    let span_id_hex = dec_str_to_hex(span_id_dec);
    let parent_id_hex = if parent_id_dec == "0" {
        None
    } else {
        Some(dec_str_to_hex(parent_id_dec))
    };
    let trace_id_hex = dec_str_to_hex(trace_id_dec);

    // Timestamps: float seconds → nanosecond strings.
    let start_secs = raw.get("start").and_then(serde_json::Value::as_f64);
    let end_secs = raw.get("end").and_then(serde_json::Value::as_f64);

    let start_nanos_str = start_secs.map(float_secs_to_nanos_string);
    let end_nanos_str = end_secs.map(float_secs_to_nanos_string);

    // Status: "ok" / "error" → StatusCode.
    let status_str = raw
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let status_code = match status_str {
        "ok" => StatusCode::Ok,
        "error" => StatusCode::Error,
        _ => StatusCode::Unset,
    };

    // Map DD `type` to SpanKind (best-effort).
    let dd_type = raw
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let kind = match dd_type {
        "web" => SpanKind::Server,
        "http" => SpanKind::Client,
        _ => SpanKind::Internal,
    };

    // Build attributes from `service`, `resource`, `type`, and `meta` fields
    // so they appear in the diff output alongside the raw JSON.
    let mut attributes = Vec::new();

    if let Some(service) = raw.get("service").and_then(serde_json::Value::as_str) {
        attributes.push(KeyValue {
            key: "service.name".to_owned(),
            value: AnyValue::String(service.to_owned()),
        });
    }
    if let Some(resource) = raw.get("resource").and_then(serde_json::Value::as_str) {
        attributes.push(KeyValue {
            key: "dd.resource".to_owned(),
            value: AnyValue::String(resource.to_owned()),
        });
    }
    if !dd_type.is_empty() {
        attributes.push(KeyValue {
            key: "dd.type".to_owned(),
            value: AnyValue::String(dd_type.to_owned()),
        });
    }

    // Flatten `meta` map into attributes.
    if let Some(meta) = raw.get("meta").and_then(serde_json::Value::as_object) {
        for (k, v) in meta {
            if let Some(s) = v.as_str() {
                attributes.push(KeyValue {
                    key: k.clone(),
                    value: AnyValue::String(s.to_owned()),
                });
            }
        }
    }

    Span {
        trace_id: trace_id_hex,
        span_id: span_id_hex,
        trace_state: None,
        parent_span_id: parent_id_hex,
        flags: None,
        name,
        kind,
        start_time_unix_nano: start_nanos_str,
        end_time_unix_nano: end_nanos_str,
        attributes,
        dropped_attributes_count: 0,
        events: Vec::new(),
        dropped_events_count: 0,
        links: Vec::new(),
        dropped_links_count: 0,
        status: Some(Status {
            message: None,
            code: status_code,
        }),
    }
}

/// Convert a decimal string (e.g. "5281031457665490589") to a hex string.
fn dec_str_to_hex(dec: &str) -> String {
    dec.parse::<u64>()
        .map_or_else(|_| dec.to_owned(), |n| format!("{n:016x}"))
}

/// Convert float seconds to a nanosecond string (for internal timestamp fields).
fn float_secs_to_nanos_string(secs: f64) -> String {
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let nanos = (secs * 1e9) as u64;
    nanos.to_string()
}

/// Detected trace format from a JSON file's structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectedFormat {
    Otlp,
    DatadogUi,
}

/// Sniff the top-level JSON structure to determine the trace format.
///
/// - `"resourceSpans"` key → OTLP
/// - `"trace"` key with `"spans"` sub-object → Datadog UI export
///
/// # Errors
///
/// Returns `Err` if the JSON is invalid or the format cannot be determined.
pub fn detect_format(contents: &str) -> Result<DetectedFormat, TraceError> {
    let trimmed = contents.trim();

    // Parse just enough to inspect top-level keys.
    let val: serde_json::Value = serde_json::from_str(trimmed)?;

    if val.get("resourceSpans").is_some() {
        return Ok(DetectedFormat::Otlp);
    }

    if let Some(trace_obj) = val.get("trace")
        && trace_obj.get("spans").is_some()
    {
        return Ok(DetectedFormat::DatadogUi);
    }

    // OTLP JSON-lines: first line should parse and have resourceSpans.
    if let Some(first_line) = trimmed.lines().next()
        && let Ok(line_val) = serde_json::from_str::<serde_json::Value>(first_line.trim())
        && line_val.get("resourceSpans").is_some()
    {
        return Ok(DetectedFormat::Otlp);
    }

    Err(TraceError::UnrecognizedFormat)
}

/// Parse a trace file in any supported format, returning a `SpanTree`.
///
/// Auto-detects the format and dispatches to the appropriate parser.
///
/// # Errors
///
/// Returns `Err` on parse failure, empty traces, or unrecognized formats.
pub fn parse_trace_file(contents: &str) -> Result<SpanTree, TraceError> {
    let format = detect_format(contents)?;
    match format {
        DetectedFormat::Otlp => {
            let (data, raw) = parse_otlp_file_raw(contents)?;
            Ok(SpanTree::from_otlp_raw(&data, &raw)?)
        }
        DetectedFormat::DatadogUi => {
            let (spans, raws) = parse_datadog_ui_file_raw(contents)?;
            Ok(SpanTree::from_spans_with_raw(spans, raws)?)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_otlp_json() -> &'static str {
        r#"{
  "resourceSpans": [
    {
      "resource": {
        "attributes": [
          {"key": "service.name", "value": {"stringValue": "agent-service"}}
        ]
      },
      "scopeSpans": [
        {
          "scope": {"name": "agent-lib", "version": "0.1.0"},
          "spans": [
            {
              "traceId": "5b8aa5a2d2c872e8321cf37308d69df2",
              "spanId": "0000000000000001",
              "name": "agent.execute",
              "kind": 1,
              "startTimeUnixNano": "1000000000",
              "endTimeUnixNano": "5000000000",
              "attributes": [
                {"key": "agent.task", "value": {"stringValue": "fix-bug-42"}}
              ],
              "status": {"code": 1}
            },
            {
              "traceId": "5b8aa5a2d2c872e8321cf37308d69df2",
              "spanId": "0000000000000002",
              "parentSpanId": "0000000000000001",
              "name": "llm.call",
              "kind": 3,
              "startTimeUnixNano": "1000000000",
              "endTimeUnixNano": "2000000000",
              "attributes": [
                {"key": "llm.model", "value": {"stringValue": "claude-3.5-sonnet"}},
                {"key": "llm.prompt_tokens", "value": {"intValue": "1200"}}
              ],
              "status": {}
            },
            {
              "traceId": "5b8aa5a2d2c872e8321cf37308d69df2",
              "spanId": "0000000000000003",
              "parentSpanId": "0000000000000001",
              "name": "tool.call",
              "kind": 3,
              "startTimeUnixNano": "2000000000",
              "endTimeUnixNano": "2500000000",
              "attributes": [
                {"key": "tool.name", "value": {"stringValue": "read_file"}},
                {"key": "tool.params.path", "value": {"stringValue": "auth.py"}}
              ],
              "status": {"code": 1}
            },
            {
              "traceId": "5b8aa5a2d2c872e8321cf37308d69df2",
              "spanId": "0000000000000004",
              "parentSpanId": "0000000000000001",
              "name": "llm.call",
              "kind": 3,
              "startTimeUnixNano": "2500000000",
              "endTimeUnixNano": "3500000000",
              "attributes": [
                {"key": "llm.model", "value": {"stringValue": "claude-3.5-sonnet"}},
                {"key": "llm.prompt_tokens", "value": {"intValue": "3400"}}
              ],
              "status": {}
            },
            {
              "traceId": "5b8aa5a2d2c872e8321cf37308d69df2",
              "spanId": "0000000000000005",
              "parentSpanId": "0000000000000001",
              "name": "tool.call",
              "kind": 3,
              "startTimeUnixNano": "3500000000",
              "endTimeUnixNano": "4000000000",
              "attributes": [
                {"key": "tool.name", "value": {"stringValue": "edit_file"}},
                {"key": "tool.params.path", "value": {"stringValue": "auth.py"}}
              ],
              "status": {"code": 1}
            },
            {
              "traceId": "5b8aa5a2d2c872e8321cf37308d69df2",
              "spanId": "0000000000000006",
              "parentSpanId": "0000000000000001",
              "name": "agent.complete",
              "kind": 1,
              "startTimeUnixNano": "4000000000",
              "endTimeUnixNano": "5000000000",
              "attributes": [
                {"key": "agent.status", "value": {"stringValue": "success"}}
              ],
              "status": {"code": 1}
            }
          ]
        }
      ]
    }
  ]
}"#
    }

    #[test]
    fn parse_otlp_sample() {
        let data: TracesData = serde_json::from_str(sample_otlp_json()).unwrap();
        assert_eq!(data.resource_spans.len(), 1);
        let spans = &data.resource_spans[0].scope_spans[0].spans;
        assert_eq!(spans.len(), 6);
        assert_eq!(spans[0].name, "agent.execute");
        assert_eq!(spans[0].kind, SpanKind::Internal);
        assert_eq!(spans[1].kind, SpanKind::Client);
    }

    #[test]
    fn parse_span_timestamps() {
        let data: TracesData = serde_json::from_str(sample_otlp_json()).unwrap();
        let root = &data.resource_spans[0].scope_spans[0].spans[0];
        assert_eq!(root.start_nanos(), 1_000_000_000);
        assert_eq!(root.end_nanos(), 5_000_000_000);
        assert_eq!(root.duration_nanos(), 4_000_000_000);
    }

    #[test]
    fn parse_attribute_types() {
        let data: TracesData = serde_json::from_str(sample_otlp_json()).unwrap();
        let llm_span = &data.resource_spans[0].scope_spans[0].spans[1];
        let model = llm_span.attribute("llm.model").unwrap();
        assert_eq!(model.as_display_string(), "claude-3.5-sonnet");

        let tokens = llm_span.attribute("llm.prompt_tokens").unwrap();
        assert_eq!(tokens.as_display_string(), "1200");
    }

    #[test]
    fn build_span_tree() {
        let data: TracesData = serde_json::from_str(sample_otlp_json()).unwrap();
        let tree = SpanTree::from_otlp(&data).unwrap();

        // 6 spans total.
        assert_eq!(tree.len(), 6);

        // Root is "agent.execute".
        assert_eq!(tree.nodes[tree.root].span.name, "agent.execute");

        // Root has 5 children.
        assert_eq!(tree.nodes[tree.root].children.len(), 5);

        // Root height = 2 (root → children), size = 6.
        assert_eq!(tree.nodes[tree.root].height, 2);
        assert_eq!(tree.nodes[tree.root].size, 6);

        // Children are leaves: height = 1, size = 1.
        for &cid in &tree.nodes[tree.root].children {
            assert_eq!(tree.nodes[cid].height, 1);
            assert_eq!(tree.nodes[cid].size, 1);
        }
    }

    #[test]
    fn tree_traversal_orders() {
        let data: TracesData = serde_json::from_str(sample_otlp_json()).unwrap();
        let tree = SpanTree::from_otlp(&data).unwrap();

        let pre = tree.pre_order();
        let post = tree.post_order();

        // Both have all nodes.
        assert_eq!(pre.len(), 6);
        assert_eq!(post.len(), 6);

        // Pre-order: root first.
        assert_eq!(pre[0], tree.root);

        // Post-order: root last.
        assert_eq!(*post.last().unwrap(), tree.root);
    }

    #[test]
    fn descendants_of_root() {
        let data: TracesData = serde_json::from_str(sample_otlp_json()).unwrap();
        let tree = SpanTree::from_otlp(&data).unwrap();

        let desc = tree.descendants(tree.root);
        assert_eq!(desc.len(), 5); // all children, root not included
    }

    #[test]
    fn identical_trees_same_hash() {
        let data: TracesData = serde_json::from_str(sample_otlp_json()).unwrap();
        let tree1 = SpanTree::from_otlp(&data).unwrap();
        let tree2 = SpanTree::from_otlp(&data).unwrap();

        assert_eq!(
            tree1.nodes[tree1.root].structural_hash,
            tree2.nodes[tree2.root].structural_hash,
        );
    }

    #[test]
    fn parse_status_codes() {
        let json = r#"{"code": 2, "message": "something went wrong"}"#;
        let status: Status = serde_json::from_str(json).unwrap();
        assert_eq!(status.code, StatusCode::Error);
        assert_eq!(status.message.as_deref(), Some("something went wrong"));

        // String variant.
        let json2 = r#"{"code": "STATUS_CODE_OK"}"#;
        let status2: Status = serde_json::from_str(json2).unwrap();
        assert_eq!(status2.code, StatusCode::Ok);
    }

    #[test]
    fn parse_jsonlines_format() {
        // JSON-lines must be minified (one object per line).
        let data1: TracesData = serde_json::from_str(sample_otlp_json()).unwrap();
        let line1 = serde_json::to_string(&data1).unwrap();
        let mut data2 = data1;
        data2.resource_spans[0].scope_spans[0].spans[0].name = "agent.run".to_owned();
        let line2 = serde_json::to_string(&data2).unwrap();
        let combined = format!("{line1}\n{line2}");

        let merged = parse_otlp_file(&combined).unwrap();
        // Two resource spans merged.
        assert_eq!(merged.resource_spans.len(), 2);
    }

    #[test]
    fn empty_trace_is_error() {
        let json = r#"{"resourceSpans": []}"#;
        let data: TracesData = serde_json::from_str(json).unwrap();
        assert!(SpanTree::from_otlp(&data).is_err());
    }

    fn sample_dd_ui_json() -> &'static str {
        r#"{
  "trace": {
    "root_id": "100",
    "spans": {
      "100": {
        "trace_id": "999",
        "span_id": "100",
        "parent_id": "0",
        "start": 1772723808.972,
        "end": 1772723808.996,
        "duration": 0.024,
        "status": "ok",
        "type": "web",
        "service": "my-service",
        "name": "spring.handler",
        "resource": "UsersController.create",
        "resource_hash": "abc123",
        "meta": { "http.method": "POST", "http.url": "/users" },
        "metrics": { "_dd.measured": 1 },
        "hostname": "host-1",
        "env": "production",
        "children_ids": ["200", "300"]
      },
      "200": {
        "trace_id": "999",
        "span_id": "200",
        "parent_id": "100",
        "start": 1772723808.973,
        "end": 1772723808.990,
        "duration": 0.017,
        "status": "ok",
        "type": "http",
        "service": "my-service",
        "name": "http.request",
        "resource": "GET /api/data",
        "resource_hash": "def456",
        "meta": {},
        "metrics": {},
        "hostname": "host-1",
        "env": "production",
        "children_ids": []
      },
      "300": {
        "trace_id": "999",
        "span_id": "300",
        "parent_id": "100",
        "start": 1772723808.991,
        "end": 1772723808.995,
        "duration": 0.004,
        "status": "error",
        "type": "cache",
        "service": "my-service",
        "name": "redis.command",
        "resource": "SET user:123",
        "resource_hash": "ghi789",
        "meta": { "error.message": "connection refused" },
        "metrics": {},
        "hostname": "host-1",
        "env": "production",
        "children_ids": []
      }
    }
  },
  "is_truncated": false,
  "is_summary": false,
  "retention_reason": "diversity-sampling"
}"#
    }

    #[test]
    fn parse_dd_ui_basic() {
        let (spans, raws) = parse_datadog_ui_file_raw(sample_dd_ui_json()).unwrap();
        assert_eq!(spans.len(), 3);
        assert_eq!(raws.len(), 3);

        // All spans should have non-empty names.
        for span in &spans {
            assert!(!span.name.is_empty());
        }
    }

    #[test]
    fn dd_span_id_conversion() {
        let (spans, _) = parse_datadog_ui_file_raw(sample_dd_ui_json()).unwrap();
        // Decimal "100" → hex "0000000000000064".
        let root = spans.iter().find(|s| s.name == "spring.handler").unwrap();
        assert_eq!(root.span_id, "0000000000000064");
        // parent_id "0" → None (root).
        assert!(root.parent_span_id.is_none());
    }

    #[test]
    fn dd_timestamp_conversion() {
        let (spans, _) = parse_datadog_ui_file_raw(sample_dd_ui_json()).unwrap();
        let root = spans.iter().find(|s| s.name == "spring.handler").unwrap();
        // Start: 1772723808.972 → ~1772723808972000000 nanos.
        let start = root.start_nanos();
        assert!(start > 1_772_723_808_000_000_000);
        assert!(start < 1_772_723_809_000_000_000);
        // Duration should be ~24ms (0.024 seconds).
        let dur = root.duration_nanos();
        assert!(dur > 20_000_000); // > 20ms
        assert!(dur < 30_000_000); // < 30ms
    }

    #[test]
    fn dd_status_mapping() {
        let (spans, _) = parse_datadog_ui_file_raw(sample_dd_ui_json()).unwrap();
        let root = spans.iter().find(|s| s.name == "spring.handler").unwrap();
        assert_eq!(root.status.as_ref().unwrap().code, StatusCode::Ok);
        let error_span = spans.iter().find(|s| s.name == "redis.command").unwrap();
        assert_eq!(error_span.status.as_ref().unwrap().code, StatusCode::Error);
    }

    #[test]
    fn dd_kind_mapping() {
        let (spans, _) = parse_datadog_ui_file_raw(sample_dd_ui_json()).unwrap();
        let web_span = spans.iter().find(|s| s.name == "spring.handler").unwrap();
        assert_eq!(web_span.kind, SpanKind::Server);
        let http_span = spans.iter().find(|s| s.name == "http.request").unwrap();
        assert_eq!(http_span.kind, SpanKind::Client);
    }

    #[test]
    fn dd_attributes_populated() {
        let (spans, _) = parse_datadog_ui_file_raw(sample_dd_ui_json()).unwrap();
        let root = spans.iter().find(|s| s.name == "spring.handler").unwrap();
        // Should have service.name, dd.resource, dd.type, and meta keys.
        assert!(root.attribute("service.name").is_some());
        assert!(root.attribute("dd.resource").is_some());
        assert!(root.attribute("http.method").is_some());
    }

    #[test]
    fn dd_build_span_tree() {
        let (spans, raws) = parse_datadog_ui_file_raw(sample_dd_ui_json()).unwrap();
        let tree = SpanTree::from_spans_with_raw(spans, raws).unwrap();

        // 3 spans total.
        assert_eq!(tree.len(), 3);

        // Root should be "spring.handler" (parent_id = "0").
        assert_eq!(tree.nodes[tree.root].span.name, "spring.handler");

        // Root should have 2 children.
        assert_eq!(tree.nodes[tree.root].children.len(), 2);

        // Height = 2 (root → children), size = 3.
        assert_eq!(tree.nodes[tree.root].height, 2);
        assert_eq!(tree.nodes[tree.root].size, 3);
    }

    #[test]
    fn dd_raw_json_preserved() {
        let (spans, raws) = parse_datadog_ui_file_raw(sample_dd_ui_json()).unwrap();
        let tree = SpanTree::from_spans_with_raw(spans, raws).unwrap();

        // Root node should have the original DD JSON with DD-specific fields.
        let root_raw = &tree.nodes[tree.root].raw;
        assert!(root_raw.get("service").is_some());
        assert!(root_raw.get("resource").is_some());
        assert!(root_raw.get("meta").is_some());
        assert!(root_raw.get("duration").is_some());
    }

    #[test]
    fn detect_otlp_format() {
        assert_eq!(
            detect_format(sample_otlp_json()).unwrap(),
            DetectedFormat::Otlp
        );
    }

    #[test]
    fn detect_dd_ui_format() {
        assert_eq!(
            detect_format(sample_dd_ui_json()).unwrap(),
            DetectedFormat::DatadogUi
        );
    }

    #[test]
    fn detect_unknown_format_is_error() {
        assert!(detect_format(r#"{"foo": "bar"}"#).is_err());
    }

    #[test]
    fn parse_trace_file_otlp() {
        let tree = parse_trace_file(sample_otlp_json()).unwrap();
        assert_eq!(tree.len(), 6);
        assert_eq!(tree.nodes[tree.root].span.name, "agent.execute");
    }

    #[test]
    fn parse_trace_file_dd() {
        let tree = parse_trace_file(sample_dd_ui_json()).unwrap();
        assert_eq!(tree.len(), 3);
        assert_eq!(tree.nodes[tree.root].span.name, "spring.handler");
    }
}
