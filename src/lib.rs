//! Structural diff engine for execution traces.
//!
//! Parses OTLP JSON and Datadog UI export traces, builds span trees,
//! matches them using the `GumTree` algorithm, computes structural + JSON-level
//! diffs, and renders the result as self-contained HTML or JSON.

pub mod diff;
pub mod match_trees;
pub mod model;
pub mod wasm;
pub mod web;
