// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Authentication and authorization for flux-broker.
//!
//! This module provides:
//! - API key validation
//! - ACL-based authorization with caching
//! - Principal identification

pub mod acl;
pub mod api_key;
pub mod error;
pub mod principal;

pub use acl::{AclChecker, Operation, ResourceType};
pub use api_key::ApiKeyValidator;
pub use error::AuthError;
pub use principal::Principal;
