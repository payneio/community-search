//! Application-level HTTP middleware.
//!
//! Each sub-module implements an Axum middleware function that can be mounted
//! with [`axum::middleware::from_fn`] or [`axum::middleware::from_fn_with_state`].

pub mod peer_version;
