# Architecture

## What this is

- Structural diff engine for distributed traces
- Supports OTLP JSON and Datadog UI export formats
- Compiles to WASM, runs in the browser — no server needed
- Input: two trace JSON files. Output: JSON payload or self-contained HTML.

## The pipeline

Four steps, always in order:

1. **Parse** — detect format, deserialize, build a SpanTree per trace
2. **Match** — GumTree algorithm maps baseline spans to candidate spans
3. **Diff** — walk matched trees side by side, diff raw JSON per field, filter noise
4. **Payload** — assemble diff entries + tree data + summary stats

## The modules

### model — parsing and the span tree

- Format detection: checks top-level JSON keys (`resourceSpans` = OTLP, `data` = Datadog)
- Datadog spans normalize to the same `Span` type as OTLP (hex IDs, nanos, flat attributes)
- `SpanTree` is an arena — `Vec<SpanNode>`, parent/child as `usize` indices, no `Rc`/`Box`
- Each node holds a typed `Span` (for matching) and raw `serde_json::Value` (for diffing)
- Precomputed per-node: height, subtree size, structural hash (Merkle: name + kind + child hashes)
- Multiple root spans get a synthetic `<root>` node

### match_trees — finding correspondences

- GumTree algorithm, three phases
- **Top-down**: priority queues by subtree height, group by structural hash, map unique 1:1 matches, disambiguate N:M by parent name / sibling position / `tool.name`
- **Bottom-up**: post-order walk of unmatched inner nodes, find candidates via mapped descendants, accept if Dice coefficient >= 0.5 and names match, last-chance pass for remaining leaves
- **Root force-mapping**: links roots if still unmatched
- Output: `MappingStore` — bijective `HashMap<NodeId, NodeId>` in both directions

### diff — computing what changed

- Aligned iterative walk of both trees using the mapping
- Matched pairs: field-by-field JSON diff of raw values
- Unmatched subtrees: emitted as insertions or deletions
- OTLP attributes (arrays of `{key, value}`) matched by key, not position
- Noise filter: ~20 hardcoded path patterns (IDs, timestamps, agent internals) excluded by default. `include_all = true` bypasses it.
- Each diff path auto-categorized: `errors`, `performance`, `deployment`, `http`, `other`
- `DiffEntry`: diff kind, JSON diffs, durations, statuses, delta percentage

### web — payload and rendering

- Assembles `TraceDiffPayload` from diff + both trees
- Flattens trees to pre-order `TreeNode` lists for the UI
- HTML mode: serializes payload to JSON, injects into template via placeholder replacement
- Template embedded at compile time (`include_str!`)

### wasm — the entry point

- Single export: `diff_traces` — runs the full pipeline, returns JSON string
- 50 MiB input size guard
- Panic hook for browser console errors
- Default config: min_height=1, min_dice=0.5

## Design decisions

- **Arena trees** — flat `Vec` with index relationships, no pointers. Cache-friendly, no borrow checker fights.
- **Iterative traversals** — explicit stacks everywhere. WASM stack is 1 MB, recursion is risky.
- **Dual parsing** — typed `Span` for matching, raw `Value` for diffing. Two parses cheaper than one parse + deep clone.
- **Keyed array diff** — OTLP attributes matched by `key` field, not array position. Prevents false diffs from reordering.
- **Noise filtering on by default** — IDs, timestamps, hostnames excluded. Real changes don't get buried.
- **Self-contained HTML** — server-side injects JSON into template, client-side embeds base64 WASM. Both produce a single offline .html file.

## Build and test

- `cargo test` — 56 unit tests + 5 integration tests, no external services
- `deno run --allow-all make.ts` — compiles WASM, assembles `trace-diff.html`
  - requires: `wasm32-unknown-unknown` target, `wasm-bindgen-cli`, `deno`
- `cargo clippy -- -D warnings` — pedantic lint config

## Delivery modes

- **Server-side**: call `render_trace_html(&payload)` — serializes payload as JSON, injects into the HTML template's `<script>` tag, returns a self-contained HTML string
- **Client-side WASM**: browser calls `diff_traces`, gets back JSON over the WASM/JS boundary, renders the UI client-side
