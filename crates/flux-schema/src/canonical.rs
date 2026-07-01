// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Schema canonicalization and hashing.
//!
//! Canonicalization normalizes a schema for consistent hashing:
//! 1. Sort object keys alphabetically
//! 2. Remove whitespace
//! 3. Remove `doc` fields (documentation doesn't affect compatibility)
//! 4. Keep `default` values (semantically significant)

use serde_json::Value;
use sha2::{Digest, Sha256};

/// Canonicalize a schema for hashing.
///
/// Returns a deterministic JSON string representation.
pub fn canonicalize(schema: &Value) -> String {
    let normalized = normalize(schema);
    // serde_json with no formatting produces minimal whitespace
    serde_json::to_string(&normalized).unwrap_or_default()
}

/// Compute SHA-256 hash of a canonicalized schema.
pub fn schema_hash(schema: &Value) -> [u8; 32] {
    let canonical = canonicalize(schema);
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    hasher.finalize().into()
}

/// Normalize a JSON value for canonicalization.
fn normalize(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            // Sort keys alphabetically, remove 'doc' fields
            let mut sorted: Vec<_> = map
                .iter()
                .filter(|(k, _)| *k != "doc")
                .map(|(k, v)| (k.clone(), normalize(v)))
                .collect();
            sorted.sort_by(|a, b| a.0.cmp(&b.0));
            Value::Object(sorted.into_iter().collect())
        }
        Value::Array(arr) => Value::Array(arr.iter().map(normalize).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_canonicalize_sorts_keys() {
        let schema = json!({
            "z_field": 1,
            "a_field": 2,
            "m_field": 3
        });

        let canonical = canonicalize(&schema);
        assert!(canonical.find("a_field").unwrap() < canonical.find("m_field").unwrap());
        assert!(canonical.find("m_field").unwrap() < canonical.find("z_field").unwrap());
    }

    #[test]
    fn test_canonicalize_removes_doc() {
        let schema = json!({
            "type": "record",
            "name": "Test",
            "doc": "This is a test record",
            "fields": [
                {"name": "id", "type": "string", "doc": "The ID field"}
            ]
        });

        let canonical = canonicalize(&schema);
        assert!(!canonical.contains("doc"));
        assert!(!canonical.contains("This is a test record"));
        assert!(!canonical.contains("The ID field"));
    }

    #[test]
    fn test_canonicalize_keeps_default() {
        let schema = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "currency", "type": "string", "default": "USD"}
            ]
        });

        let canonical = canonicalize(&schema);
        assert!(canonical.contains("default"));
        assert!(canonical.contains("USD"));
    }

    #[test]
    fn test_schema_hash_deterministic() {
        let schema1 = json!({
            "type": "record",
            "name": "Test",
            "fields": [{"name": "id", "type": "string"}]
        });

        let schema2 = json!({
            "fields": [{"type": "string", "name": "id"}],
            "name": "Test",
            "type": "record"
        });

        // Same content, different key order -> same hash
        assert_eq!(schema_hash(&schema1), schema_hash(&schema2));
    }

    #[test]
    fn test_schema_hash_different_for_different_schemas() {
        let schema1 = json!({
            "type": "record",
            "name": "Test",
            "fields": [{"name": "id", "type": "string"}]
        });

        let schema2 = json!({
            "type": "record",
            "name": "Test",
            "fields": [{"name": "id", "type": "long"}]
        });

        assert_ne!(schema_hash(&schema1), schema_hash(&schema2));
    }

    #[test]
    fn test_schema_hash_different_defaults_different_hash() {
        let schema1 = json!({
            "type": "record",
            "name": "Test",
            "fields": [{"name": "currency", "type": "string", "default": "USD"}]
        });

        let schema2 = json!({
            "type": "record",
            "name": "Test",
            "fields": [{"name": "currency", "type": "string", "default": "EUR"}]
        });

        // Different defaults -> different hash
        assert_ne!(schema_hash(&schema1), schema_hash(&schema2));
    }

    #[test]
    fn test_schema_hash_doc_ignored() {
        let schema1 = json!({
            "type": "record",
            "name": "Test",
            "doc": "Version 1",
            "fields": [{"name": "id", "type": "string"}]
        });

        let schema2 = json!({
            "type": "record",
            "name": "Test",
            "doc": "Version 2 with updated docs",
            "fields": [{"name": "id", "type": "string"}]
        });

        // Different docs -> same hash
        assert_eq!(schema_hash(&schema1), schema_hash(&schema2));
    }
}
