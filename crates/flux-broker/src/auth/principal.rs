// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Principal type representing an authenticated identity.

use uuid::Uuid;

/// An authenticated principal (user or service).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Principal {
    /// The principal identifier (e.g., "service:orders", "user:alice").
    pub id: String,
    /// The API key ID used for authentication.
    pub key_id: Uuid,
}

impl Principal {
    /// Create a new principal.
    pub fn new(id: String, key_id: Uuid) -> Self {
        Self { id, key_id }
    }

    /// Check if this principal has admin privileges.
    ///
    /// Admin principals have `id` equal to "admin" or starting with "admin:".
    pub fn is_admin(&self) -> bool {
        self.id == "admin" || self.id.starts_with("admin:")
    }

    /// Get the service type from the principal ID.
    ///
    /// For principals like "service:orders", returns Some("orders").
    /// For principals without a type prefix, returns None.
    pub fn service_type(&self) -> Option<&str> {
        if let Some(idx) = self.id.find(':') {
            Some(&self.id[idx + 1..])
        } else {
            None
        }
    }

    /// Get the principal type prefix.
    ///
    /// For principals like "service:orders", returns "service".
    /// For principals without a type prefix, returns the entire ID.
    pub fn principal_type(&self) -> &str {
        if let Some(idx) = self.id.find(':') {
            &self.id[..idx]
        } else {
            &self.id
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_admin() {
        let admin = Principal::new("admin".to_string(), Uuid::nil());
        assert!(admin.is_admin());

        let admin_sub = Principal::new("admin:super".to_string(), Uuid::nil());
        assert!(admin_sub.is_admin());

        let user = Principal::new("user:alice".to_string(), Uuid::nil());
        assert!(!user.is_admin());

        let service = Principal::new("service:orders".to_string(), Uuid::nil());
        assert!(!service.is_admin());
    }

    #[test]
    fn test_service_type() {
        let service = Principal::new("service:orders".to_string(), Uuid::nil());
        assert_eq!(service.service_type(), Some("orders"));

        let simple = Principal::new("orders".to_string(), Uuid::nil());
        assert_eq!(simple.service_type(), None);
    }

    #[test]
    fn test_principal_type() {
        let service = Principal::new("service:orders".to_string(), Uuid::nil());
        assert_eq!(service.principal_type(), "service");

        let simple = Principal::new("orders".to_string(), Uuid::nil());
        assert_eq!(simple.principal_type(), "orders");
    }
}
