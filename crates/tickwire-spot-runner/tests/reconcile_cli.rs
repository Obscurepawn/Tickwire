//! Stateless JOIN baseline snapshot and asymmetric `S` / `S+1` split-pair admission tests for
//! public spot BBO1 topics (`specs/implementation/spot-quote-input.md`).

#![forbid(unsafe_code)]

#[path = "reconcile_cli/market.rs"]
mod market;
