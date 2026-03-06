//! GumTree-style tree matching for span trees.
//!
//! Two-phase algorithm:
//! 1. **Top-down**: match identical subtrees by structural hash, largest first.
//! 2. **Bottom-up**: match unmatched inner nodes by Dice similarity of
//!    already-matched descendants.
//!
//! Produces a `MappingStore` — a bidirectional map of `(NodeId, NodeId)` pairs
//! between the baseline ("src") and candidate ("dst") trees.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

use crate::model::{NodeId, SpanTree};

/// Bidirectional mapping between nodes of two span trees.
#[derive(Debug, Clone)]
pub struct MappingStore {
    /// src node → dst node.
    src_to_dst: HashMap<NodeId, NodeId>,
    /// dst node → src node.
    dst_to_src: HashMap<NodeId, NodeId>,
}

impl MappingStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            src_to_dst: HashMap::new(),
            dst_to_src: HashMap::new(),
        }
    }

    /// Map src node to dst node. Overwrites any existing mapping for either side.
    pub fn link(&mut self, src: NodeId, dst: NodeId) {
        self.src_to_dst.insert(src, dst);
        self.dst_to_src.insert(dst, src);
    }

    /// Is this src node already mapped?
    #[must_use]
    pub fn has_src(&self, src: NodeId) -> bool {
        self.src_to_dst.contains_key(&src)
    }

    /// Is this dst node already mapped?
    #[must_use]
    pub fn has_dst(&self, dst: NodeId) -> bool {
        self.dst_to_src.contains_key(&dst)
    }

    /// Get the dst node mapped to this src node.
    #[must_use]
    pub fn get_dst(&self, src: NodeId) -> Option<NodeId> {
        self.src_to_dst.get(&src).copied()
    }

    /// Get the src node mapped to this dst node.
    #[must_use]
    pub fn get_src(&self, dst: NodeId) -> Option<NodeId> {
        self.dst_to_src.get(&dst).copied()
    }

    /// Number of mapped pairs.
    #[must_use]
    pub fn len(&self) -> usize {
        self.src_to_dst.len()
    }

    /// Whether the mapping is empty.
    #[must_use]
    #[allow(dead_code)] // standard collection API
    pub fn is_empty(&self) -> bool {
        self.src_to_dst.is_empty()
    }

    /// Iterate over all (src, dst) pairs.
    #[allow(dead_code)] // useful public API
    pub fn iter(&self) -> impl Iterator<Item = (NodeId, NodeId)> + '_ {
        self.src_to_dst.iter().map(|(&s, &d)| (s, d))
    }
}

impl Default for MappingStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for the `GumTree` matching algorithm.
#[derive(Debug, Clone)]
pub struct MatchConfig {
    /// Minimum subtree height for top-down matching (default: 1 = match leaves too).
    pub min_height: usize,
    /// Minimum Dice coefficient for bottom-up matching (default: 0.5).
    pub min_dice: f64,
}

impl Default for MatchConfig {
    fn default() -> Self {
        Self {
            min_height: 1,
            min_dice: 0.5,
        }
    }
}

/// Match two span trees using the `GumTree` algorithm.
///
/// Returns a `MappingStore` containing pairs of (src, dst) node IDs that
/// correspond to each other.
#[must_use]
pub fn match_trees(src: &SpanTree, dst: &SpanTree, config: &MatchConfig) -> MappingStore {
    let mut mappings = MappingStore::new();

    // Phase 1: top-down subtree matching by structural hash.
    top_down_match(src, dst, &mut mappings, config);

    // Phase 2: bottom-up recovery matching by Dice similarity.
    bottom_up_match(src, dst, &mut mappings, config);

    mappings
}

/// Entry in the priority queue: (height, `node_id`).
/// We process tallest subtrees first.
#[derive(Debug, Clone, Eq, PartialEq)]
struct HeightEntry {
    height: usize,
    node_id: NodeId,
}

impl Ord for HeightEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.height.cmp(&other.height)
    }
}

impl PartialOrd for HeightEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn top_down_match(
    src: &SpanTree,
    dst: &SpanTree,
    mappings: &mut MappingStore,
    config: &MatchConfig,
) {
    // Build priority queues.
    let mut src_queue = BinaryHeap::new();
    let mut dst_queue = BinaryHeap::new();

    src_queue.push(HeightEntry {
        height: src.nodes[src.root].height,
        node_id: src.root,
    });
    dst_queue.push(HeightEntry {
        height: dst.nodes[dst.root].height,
        node_id: dst.root,
    });

    while !src_queue.is_empty() && !dst_queue.is_empty() {
        let src_max = src_queue.peek().map_or(0, |e| e.height);
        let dst_max = dst_queue.peek().map_or(0, |e| e.height);

        if src_max < config.min_height && dst_max < config.min_height {
            break;
        }

        // If one side is taller, open (expand) its top entries.
        if src_max > dst_max {
            let to_open = pop_all_at_height(&mut src_queue, src_max);
            for nid in to_open {
                open_node(src, nid, &mut src_queue);
            }
            continue;
        }
        if dst_max > src_max {
            let to_open = pop_all_at_height(&mut dst_queue, dst_max);
            for nid in to_open {
                open_node(dst, nid, &mut dst_queue);
            }
            continue;
        }

        // Same height on both sides. Group by structural hash.
        let src_at_height = pop_all_at_height(&mut src_queue, src_max);
        let dst_at_height = pop_all_at_height(&mut dst_queue, dst_max);

        let mut src_by_hash: HashMap<u64, Vec<NodeId>> = HashMap::new();
        for &nid in &src_at_height {
            src_by_hash
                .entry(src.nodes[nid].structural_hash)
                .or_default()
                .push(nid);
        }

        let mut dst_by_hash: HashMap<u64, Vec<NodeId>> = HashMap::new();
        for &nid in &dst_at_height {
            dst_by_hash
                .entry(dst.nodes[nid].structural_hash)
                .or_default()
                .push(nid);
        }

        let mut matched_src: HashSet<NodeId> = HashSet::new();
        let mut matched_dst: HashSet<NodeId> = HashSet::new();

        // Unique 1:1 hash matches — map entire subtrees.
        for (&hash, src_nodes) in &src_by_hash {
            if let Some(dst_nodes) = dst_by_hash.get(&hash) {
                if src_nodes.len() == 1 && dst_nodes.len() == 1 {
                    let s = src_nodes[0];
                    let d = dst_nodes[0];
                    if !mappings.has_src(s) && !mappings.has_dst(d) {
                        map_subtrees(src, dst, s, d, mappings);
                        matched_src.insert(s);
                        matched_dst.insert(d);
                    }
                }
                // Ambiguous matches (N:M): try to disambiguate by parent similarity.
                else if src_nodes.len() > 1 || dst_nodes.len() > 1 {
                    disambiguate_matches(
                        src,
                        dst,
                        src_nodes,
                        dst_nodes,
                        mappings,
                        &mut matched_src,
                        &mut matched_dst,
                    );
                }
            }
        }

        // Open unmatched nodes (push their children into the queue).
        for &nid in &src_at_height {
            if !matched_src.contains(&nid) {
                open_node(src, nid, &mut src_queue);
            }
        }
        for &nid in &dst_at_height {
            if !matched_dst.contains(&nid) {
                open_node(dst, nid, &mut dst_queue);
            }
        }
    }
}

/// Pop all entries at a given height from the priority queue.
fn pop_all_at_height(queue: &mut BinaryHeap<HeightEntry>, height: usize) -> Vec<NodeId> {
    let mut result = Vec::new();
    while queue.peek().is_some_and(|e| e.height == height) {
        if let Some(entry) = queue.pop() {
            result.push(entry.node_id);
        }
    }
    result
}

/// Push a node's children into the priority queue.
fn open_node(tree: &SpanTree, nid: NodeId, queue: &mut BinaryHeap<HeightEntry>) {
    for &cid in &tree.nodes[nid].children {
        queue.push(HeightEntry {
            height: tree.nodes[cid].height,
            node_id: cid,
        });
    }
}

/// Map two subtrees iteratively (pre-order alignment by position).
///
/// Uses an explicit stack instead of recursion to avoid stack overflow on
/// deeply nested traces (especially relevant in WASM's 1 MB default stack).
fn map_subtrees(src: &SpanTree, dst: &SpanTree, s: NodeId, d: NodeId, mappings: &mut MappingStore) {
    let mut stack = vec![(s, d)];

    while let Some((s_node, d_node)) = stack.pop() {
        mappings.link(s_node, d_node);

        let src_children = &src.nodes[s_node].children;
        let dst_children = &dst.nodes[d_node].children;

        // Push children in reverse so that the first pair is processed first (pre-order).
        for (sc, dc) in src_children.iter().zip(dst_children.iter()).rev() {
            stack.push((*sc, *dc));
        }
    }
}

/// Disambiguate ambiguous hash matches by picking pairs with most similar parents.
fn disambiguate_matches(
    src: &SpanTree,
    dst: &SpanTree,
    src_nodes: &[NodeId],
    dst_nodes: &[NodeId],
    mappings: &mut MappingStore,
    matched_src: &mut HashSet<NodeId>,
    matched_dst: &mut HashSet<NodeId>,
) {
    // Score each candidate pair by parent name similarity + position similarity.
    let mut candidates: Vec<(NodeId, NodeId, i32)> = Vec::new();

    for &s in src_nodes {
        if mappings.has_src(s) {
            continue;
        }
        for &d in dst_nodes {
            if mappings.has_dst(d) {
                continue;
            }
            let score = similarity_score(src, dst, s, d);
            candidates.push((s, d, score));
        }
    }

    // Sort by score descending, then greedily pick best pairs.
    candidates.sort_by_key(|&(_, _, score)| Reverse(score));

    for (s, d, _) in candidates {
        if !mappings.has_src(s)
            && !mappings.has_dst(d)
            && !matched_src.contains(&s)
            && !matched_dst.contains(&d)
        {
            map_subtrees(src, dst, s, d, mappings);
            matched_src.insert(s);
            matched_dst.insert(d);
        }
    }
}

/// Heuristic similarity score between two nodes based on context.
fn similarity_score(src: &SpanTree, dst: &SpanTree, s: NodeId, d: NodeId) -> i32 {
    let mut score = 0i32;

    // Same parent name.
    if let (Some(sp), Some(dp)) = (src.nodes[s].parent, dst.nodes[d].parent)
        && src.nodes[sp].span.name == dst.nodes[dp].span.name
    {
        score += 10;
    }

    // Same position among siblings.
    if let (Some(sp), Some(dp)) = (src.nodes[s].parent, dst.nodes[d].parent) {
        let s_pos = src.nodes[sp].children.iter().position(|&c| c == s);
        let d_pos = dst.nodes[dp].children.iter().position(|&c| c == d);
        if s_pos == d_pos {
            score += 5;
        }
    }

    // Agent-specific: same tool.name attribute for tool.call spans.
    if src.nodes[s].span.name == "tool.call" {
        let s_tool = src.nodes[s].span.attribute("tool.name");
        let d_tool = dst.nodes[d].span.attribute("tool.name");
        if let (Some(st), Some(dt)) = (s_tool, d_tool)
            && st.as_display_string() == dt.as_display_string()
        {
            score += 20;
        }
    }

    score
}

fn bottom_up_match(
    src: &SpanTree,
    dst: &SpanTree,
    mappings: &mut MappingStore,
    config: &MatchConfig,
) {
    // Post-order traversal of src: leaves before parents.
    let post_order = src.post_order();

    for &s in &post_order {
        // Skip already-mapped and leaf nodes (leaves should be matched top-down).
        if mappings.has_src(s) || src.nodes[s].children.is_empty() {
            continue;
        }

        // Collect candidate dst nodes: ancestors of mapped descendants' dst targets.
        let candidates = find_bottom_up_candidates(src, dst, s, mappings);

        let mut best: Option<(NodeId, f64)> = None;
        for d in candidates {
            if mappings.has_dst(d) {
                continue;
            }
            // Must have same span name (type).
            if src.nodes[s].span.name != dst.nodes[d].span.name {
                continue;
            }
            let dice = dice_coefficient(src, dst, s, d, mappings);
            if dice >= config.min_dice && best.as_ref().is_none_or(|(_, best_d)| dice > *best_d) {
                best = Some((d, dice));
            }
        }

        if let Some((d, _)) = best {
            mappings.link(s, d);

            // "Last chance" leaf matching: match remaining unmatched leaves
            // between these two now-matched inner nodes.
            last_chance_match(src, dst, s, d, mappings);
        }
    }

    // Force-map roots if not already mapped.
    if !mappings.has_src(src.root) && !mappings.has_dst(dst.root) {
        mappings.link(src.root, dst.root);
    }
}

/// Find candidate dst nodes for bottom-up matching of src node `s`.
///
/// Walk up from each mapped dst descendant to find unmapped ancestors
/// with the same span name.
fn find_bottom_up_candidates(
    src: &SpanTree,
    dst: &SpanTree,
    s: NodeId,
    mappings: &MappingStore,
) -> Vec<NodeId> {
    let mut candidates = HashSet::new();
    let src_name = &src.nodes[s].span.name;

    for &child in &src.nodes[s].children {
        // Check this child and its descendants for mapped nodes.
        let subtree = std::iter::once(child).chain(src.descendants(child));
        for desc in subtree {
            if let Some(dst_mapped) = mappings.get_dst(desc) {
                // Walk up dst tree from the mapped node.
                let mut current = dst.nodes[dst_mapped].parent;
                while let Some(p) = current {
                    if p == dst.root {
                        break;
                    }
                    if dst.nodes[p].span.name == *src_name && !mappings.has_dst(p) {
                        candidates.insert(p);
                    }
                    current = dst.nodes[p].parent;
                }
            }
        }
    }

    candidates.into_iter().collect()
}

/// Dice coefficient measuring the overlap of matched descendants between
/// src node `s` and dst node `d`.
///
/// dice(s, d) = 2 * |common| / (|desc(s)| + |desc(d)|)
///
/// where common = descendants of `s` that map to descendants of `d`.
fn dice_coefficient(
    src: &SpanTree,
    dst: &SpanTree,
    s: NodeId,
    d: NodeId,
    mappings: &MappingStore,
) -> f64 {
    let src_desc = src.descendants(s);
    let dst_desc_set: HashSet<NodeId> = dst.descendants(d).into_iter().collect();

    let src_size = src_desc.len();
    let dst_size = dst_desc_set.len();

    if src_size == 0 && dst_size == 0 {
        return 1.0;
    }
    if src_size == 0 || dst_size == 0 {
        return 0.0;
    }

    let common = src_desc
        .iter()
        .filter(|&&sd| {
            mappings
                .get_dst(sd)
                .is_some_and(|dd| dst_desc_set.contains(&dd))
        })
        .count();

    #[allow(clippy::cast_precision_loss)]
    let dice = (2 * common) as f64 / (src_size + dst_size) as f64;
    dice
}

/// Match remaining unmatched leaves between two matched inner nodes.
fn last_chance_match(
    src: &SpanTree,
    dst: &SpanTree,
    s: NodeId,
    d: NodeId,
    mappings: &mut MappingStore,
) {
    // Only match direct children that are leaves with the same name.
    let src_unmatched: Vec<NodeId> = src.nodes[s]
        .children
        .iter()
        .copied()
        .filter(|&c| !mappings.has_src(c) && src.nodes[c].children.is_empty())
        .collect();

    let dst_unmatched: Vec<NodeId> = dst.nodes[d]
        .children
        .iter()
        .copied()
        .filter(|&c| !mappings.has_dst(c) && dst.nodes[c].children.is_empty())
        .collect();

    for &sc in &src_unmatched {
        let src_name = &src.nodes[sc].span.name;
        // Find best match by name + tool.name attribute.
        let mut best: Option<(NodeId, i32)> = None;
        for &dc in &dst_unmatched {
            if mappings.has_dst(dc) {
                continue;
            }
            if dst.nodes[dc].span.name != *src_name {
                continue;
            }
            let score = similarity_score(src, dst, sc, dc);
            if best.as_ref().is_none_or(|(_, bs)| score > *bs) {
                best = Some((dc, score));
            }
        }
        if let Some((dc, _)) = best {
            mappings.link(sc, dc);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Span, SpanKind, SpanTree};

    /// Helper: build a simple span with given name and `span_id`.
    fn span(id: &str, parent: Option<&str>, name: &str, start: u64) -> Span {
        Span {
            trace_id: "trace1".to_owned(),
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
            status: None,
        }
    }

    #[test]
    fn identical_trees_fully_matched() {
        let spans = vec![
            span("1", None, "root", 0),
            span("2", Some("1"), "child_a", 100),
            span("3", Some("1"), "child_b", 200),
        ];
        let src = SpanTree::from_spans(spans.clone()).unwrap();
        let dst = SpanTree::from_spans(spans).unwrap();

        let mappings = match_trees(&src, &dst, &MatchConfig::default());

        // All 3 nodes should be matched.
        assert_eq!(mappings.len(), 3);

        // Root maps to root.
        assert_eq!(mappings.get_dst(src.root), Some(dst.root));
    }

    #[test]
    fn inserted_span_detected() {
        let src_spans = vec![
            span("1", None, "root", 0),
            span("2", Some("1"), "child_a", 100),
            span("3", Some("1"), "child_b", 200),
        ];
        let dst_spans = vec![
            span("1", None, "root", 0),
            span("2", Some("1"), "child_a", 100),
            span("4", Some("1"), "child_new", 150),
            span("3", Some("1"), "child_b", 200),
        ];

        let src = SpanTree::from_spans(src_spans).unwrap();
        let dst = SpanTree::from_spans(dst_spans).unwrap();

        let mappings = match_trees(&src, &dst, &MatchConfig::default());

        // src has 3 nodes, all should be matched.
        assert_eq!(src.len(), 3);
        assert_eq!(dst.len(), 4);

        // All src nodes should have a mapping.
        for nid in 0..src.len() {
            assert!(mappings.has_src(nid), "src node {nid} should be mapped");
        }

        // The new node in dst should NOT have a src mapping.
        let new_node = dst
            .nodes
            .iter()
            .find(|n| n.span.name == "child_new")
            .unwrap();
        assert!(
            !mappings.has_dst(new_node.id),
            "inserted node should not be mapped"
        );
    }

    #[test]
    fn deleted_span_detected() {
        let src_spans = vec![
            span("1", None, "root", 0),
            span("2", Some("1"), "child_a", 100),
            span("3", Some("1"), "child_b", 200),
            span("4", Some("1"), "child_c", 300),
        ];
        let dst_spans = vec![
            span("1", None, "root", 0),
            span("2", Some("1"), "child_a", 100),
            span("4", Some("1"), "child_c", 300),
        ];

        let src = SpanTree::from_spans(src_spans).unwrap();
        let dst = SpanTree::from_spans(dst_spans).unwrap();

        let mappings = match_trees(&src, &dst, &MatchConfig::default());

        // The deleted node (child_b) in src should not be mapped.
        let deleted = src.nodes.iter().find(|n| n.span.name == "child_b").unwrap();
        assert!(
            !mappings.has_src(deleted.id),
            "deleted node should not be mapped"
        );

        // All dst nodes should be mapped.
        for nid in 0..dst.len() {
            assert!(mappings.has_dst(nid), "dst node {nid} should be mapped");
        }
    }

    #[test]
    fn mapping_is_bijective() {
        let src_spans = vec![
            span("1", None, "root", 0),
            span("2", Some("1"), "llm.call", 100),
            span("3", Some("1"), "tool.call", 200),
            span("4", Some("1"), "llm.call", 300),
            span("5", Some("1"), "tool.call", 400),
        ];
        let dst_spans = vec![
            span("1", None, "root", 0),
            span("2", Some("1"), "llm.call", 100),
            span("3", Some("1"), "tool.call", 200),
            span("6", Some("1"), "llm.call", 250),
            span("4", Some("1"), "llm.call", 300),
            span("5", Some("1"), "tool.call", 400),
        ];

        let src = SpanTree::from_spans(src_spans).unwrap();
        let dst = SpanTree::from_spans(dst_spans).unwrap();

        let mappings = match_trees(&src, &dst, &MatchConfig::default());

        // Check bijectivity: no two src nodes map to the same dst node.
        let mut seen_dst: HashSet<NodeId> = HashSet::new();
        for (_, d) in mappings.iter() {
            assert!(
                seen_dst.insert(d),
                "dst node {d} mapped to multiple src nodes"
            );
        }
    }

    #[test]
    fn deep_tree_matching() {
        let src_spans = vec![
            span("1", None, "root", 0),
            span("2", Some("1"), "parent_a", 100),
            span("3", Some("2"), "leaf_1", 110),
            span("4", Some("2"), "leaf_2", 120),
            span("5", Some("1"), "parent_b", 200),
            span("6", Some("5"), "leaf_3", 210),
        ];
        let dst_spans = vec![
            span("1", None, "root", 0),
            span("2", Some("1"), "parent_a", 100),
            span("3", Some("2"), "leaf_1", 110),
            span("4", Some("2"), "leaf_2", 120),
            span("5", Some("1"), "parent_b", 200),
            span("6", Some("5"), "leaf_3", 210),
        ];

        let src = SpanTree::from_spans(src_spans).unwrap();
        let dst = SpanTree::from_spans(dst_spans).unwrap();

        let mappings = match_trees(&src, &dst, &MatchConfig::default());

        // All 6 nodes should match.
        assert_eq!(mappings.len(), 6);
    }

    /// A 2000-level deep chain must not stack-overflow.
    ///
    /// Before the recursive-to-iterative conversion, `map_subtrees` would blow
    /// the stack (especially on WASM's default 1 MB stack) for chains this deep.
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

        let src = SpanTree::from_spans(spans.clone()).unwrap();
        let dst = SpanTree::from_spans(spans).unwrap();

        let mappings = match_trees(&src, &dst, &MatchConfig::default());

        // Every node in the chain should be matched 1:1.
        assert_eq!(mappings.len(), DEPTH);
    }
}
