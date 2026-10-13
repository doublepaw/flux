// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Schema evolution: detect diffs between Avro schema versions and produce
//! Iceberg evolution operations.
//!
//! Consumes explicit SDK metadata (`flux.renames`, `flux.deletions`)
//! from schema JSON rather than inferring renames from Avro aliases.
//!
//! Operations:
//! - `flux.renames`: `{"old_name": "new_name"}` → `RenameColumn`
//! - `flux.deletions`: `["removed_field"]` → `DeleteColumn`
//! - Type widened (int→long, float→double) → `UpdateColumnType`
//! - Field in new but not old → `AddColumn`

use std::collections::HashMap;

use serde_json::Value;

use crate::error::{IcebergError, Result};

/// A single schema evolution operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvolutionOp {
    /// Add a new optional column.
    AddColumn {
        name: String,
        /// Parent field path (empty for top-level).
        parent: Vec<String>,
    },
    /// Rename an existing column.
    RenameColumn { old_name: String, new_name: String },
    /// Widen a column type (e.g. int → long).
    UpdateColumnType {
        name: String,
        from: String,
        to: String,
    },
    /// Delete (soft-remove) a column.
    DeleteColumn { name: String },
}

/// Result of comparing two schema versions for Iceberg evolution.
#[derive(Debug, Clone)]
pub struct SchemaEvolution {
    pub ops: Vec<EvolutionOp>,
}

impl SchemaEvolution {
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

/// Compute the evolution operations needed to update an Iceberg table
/// from `old_json` to `new_json`.
///
/// Reads `flux.renames` and `flux.deletions` from the new schema's
/// metadata, then compares field lists for type widening and additions.
pub fn compute_evolution(new_json: &Value, old_json: &Value) -> Result<SchemaEvolution> {
    let mut ops = Vec::new();

    // 1. Renames from flux.renames: {"old_name": "new_name"}
    let mut renames_reverse: HashMap<&str, &str> = HashMap::new();
    if let Some(renames) = new_json.get("flux.renames").and_then(|v| v.as_object()) {
        for (old_name, new_name) in renames {
            let new_name_str = new_name.as_str().ok_or_else(|| {
                IcebergError::Schema(format!(
                    "flux.renames value for '{}' must be a string",
                    old_name
                ))
            })?;
            ops.push(EvolutionOp::RenameColumn {
                old_name: old_name.clone(),
                new_name: new_name_str.to_string(),
            });
            renames_reverse.insert(new_name_str, old_name.as_str());
        }
    }

    // 2. Deletions from flux.deletions: ["removed_field"]
    if let Some(deletions) = new_json.get("flux.deletions").and_then(|v| v.as_array()) {
        for name in deletions {
            let name_str = name.as_str().ok_or_else(|| {
                IcebergError::Schema("flux.deletions entries must be strings".into())
            })?;
            ops.push(EvolutionOp::DeleteColumn {
                name: name_str.to_string(),
            });
        }
    }

    // 3. Type widening + new fields: compare old vs new field lists
    let new_fields = get_fields(new_json)?;
    let old_fields = get_fields(old_json)?;
    let old_by_name: HashMap<&str, &Value> = old_fields
        .iter()
        .filter_map(|f| f["name"].as_str().map(|n| (n, f)))
        .collect();

    for new_field in &new_fields {
        let name = new_field["name"]
            .as_str()
            .ok_or_else(|| IcebergError::Schema("field missing 'name'".into()))?;

        // If this field was renamed, look up the old name
        let lookup_name = renames_reverse.get(name).copied().unwrap_or(name);

        match old_by_name.get(lookup_name) {
            Some(old_field) => {
                // Check type widening
                if let Some((from, to)) =
                    detect_type_widening(&old_field["type"], &new_field["type"])
                {
                    ops.push(EvolutionOp::UpdateColumnType {
                        name: name.to_string(),
                        from,
                        to,
                    });
                }
            }
            None => {
                // New field (not a rename target) → AddColumn
                ops.push(EvolutionOp::AddColumn {
                    name: name.to_string(),
                    parent: vec![],
                });
            }
        }
    }

    Ok(SchemaEvolution { ops })
}

/// Extract the "fields" array from a record schema JSON.
fn get_fields(schema: &Value) -> Result<Vec<Value>> {
    schema
        .get("fields")
        .and_then(|f| f.as_array())
        .cloned()
        .ok_or_else(|| IcebergError::Schema("schema missing 'fields' array".into()))
}

/// Detect Avro type widening between two JSON type values.
fn detect_type_widening(old_type: &Value, new_type: &Value) -> Option<(String, String)> {
    let old_name = unwrap_type_name(old_type);
    let new_name = unwrap_type_name(new_type);

    match (old_name.as_str(), new_name.as_str()) {
        ("int", "long") => Some(("int".into(), "long".into())),
        ("float", "double") => Some(("float".into(), "double".into())),
        _ => None,
    }
}

/// Get the canonical type name from a JSON type value, unwrapping nullable unions.
fn unwrap_type_name(type_val: &Value) -> String {
    match type_val {
        Value::String(s) => s.clone(),
        Value::Object(obj) => obj
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string(),
        Value::Array(arr) => {
            // Union: find the non-null type
            let non_null: Vec<_> = arr.iter().filter(|v| v.as_str() != Some("null")).collect();
            if non_null.len() == 1 {
                unwrap_type_name(non_null[0])
            } else {
                "union".to_string()
            }
        }
        _ => "unknown".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_no_changes() {
        let schema = json!({
            "type": "record", "name": "T",
            "fields": [{"name": "id", "type": "long"}]
        });
        let evo = compute_evolution(&schema, &schema).unwrap();
        assert!(evo.is_empty());
    }

    #[test]
    fn test_add_field() {
        let old = json!({
            "type": "record", "name": "T",
            "fields": [{"name": "id", "type": "long"}]
        });
        let new = json!({
            "type": "record", "name": "T",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": ["null", "string"], "default": null}
            ]
        });

        let evo = compute_evolution(&new, &old).unwrap();
        assert_eq!(evo.ops.len(), 1);
        assert_eq!(
            evo.ops[0],
            EvolutionOp::AddColumn {
                name: "name".into(),
                parent: vec![],
            }
        );
    }

    #[test]
    fn test_rename_via_metadata() {
        let old = json!({
            "type": "record", "name": "T",
            "fields": [{"name": "old_name", "type": "long"}]
        });
        let new = json!({
            "type": "record", "name": "T",
            "fields": [{"name": "new_name", "type": "long"}],
            "flux.renames": {"old_name": "new_name"}
        });

        let evo = compute_evolution(&new, &old).unwrap();
        assert_eq!(evo.ops.len(), 1);
        assert_eq!(
            evo.ops[0],
            EvolutionOp::RenameColumn {
                old_name: "old_name".into(),
                new_name: "new_name".into(),
            }
        );
    }

    #[test]
    fn test_delete_via_metadata() {
        let old = json!({
            "type": "record", "name": "T",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "deprecated", "type": "string"}
            ]
        });
        let new = json!({
            "type": "record", "name": "T",
            "fields": [{"name": "id", "type": "long"}],
            "flux.deletions": ["deprecated"]
        });

        let evo = compute_evolution(&new, &old).unwrap();
        assert_eq!(evo.ops.len(), 1);
        assert_eq!(
            evo.ops[0],
            EvolutionOp::DeleteColumn {
                name: "deprecated".into(),
            }
        );
    }

    #[test]
    fn test_type_widening() {
        let old = json!({
            "type": "record", "name": "T",
            "fields": [{"name": "count", "type": "int"}]
        });
        let new = json!({
            "type": "record", "name": "T",
            "fields": [{"name": "count", "type": "long"}]
        });

        let evo = compute_evolution(&new, &old).unwrap();
        assert_eq!(evo.ops.len(), 1);
        assert_eq!(
            evo.ops[0],
            EvolutionOp::UpdateColumnType {
                name: "count".into(),
                from: "int".into(),
                to: "long".into(),
            }
        );
    }

    #[test]
    fn test_remove_field_no_op_without_metadata() {
        let old = json!({
            "type": "record", "name": "T",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "deprecated", "type": "string"}
            ]
        });
        let new = json!({
            "type": "record", "name": "T",
            "fields": [{"name": "id", "type": "long"}]
        });

        // Without flux.deletions, removing a field produces no ops
        let evo = compute_evolution(&new, &old).unwrap();
        assert!(evo.is_empty());
    }

    #[test]
    fn test_combined_evolution() {
        let old = json!({
            "type": "record", "name": "T",
            "fields": [
                {"name": "old_id", "type": "int"},
                {"name": "removed", "type": "string"}
            ]
        });
        let new = json!({
            "type": "record", "name": "T",
            "fields": [
                {"name": "new_id", "type": "long"},
                {"name": "added", "type": ["null", "string"], "default": null}
            ],
            "flux.renames": {"old_id": "new_id"},
            "flux.deletions": ["removed"]
        });

        let evo = compute_evolution(&new, &old).unwrap();
        // rename + delete + widen + add = 4
        assert_eq!(evo.ops.len(), 4);
        assert!(
            evo.ops
                .iter()
                .any(|op| matches!(op, EvolutionOp::RenameColumn { .. }))
        );
        assert!(
            evo.ops
                .iter()
                .any(|op| matches!(op, EvolutionOp::DeleteColumn { .. }))
        );
        assert!(
            evo.ops
                .iter()
                .any(|op| matches!(op, EvolutionOp::UpdateColumnType { .. }))
        );
        assert!(
            evo.ops
                .iter()
                .any(|op| matches!(op, EvolutionOp::AddColumn { .. }))
        );
    }

    #[test]
    fn test_nullable_type_widening() {
        let old = json!({
            "type": "record", "name": "T",
            "fields": [{"name": "val", "type": ["null", "int"]}]
        });
        let new = json!({
            "type": "record", "name": "T",
            "fields": [{"name": "val", "type": ["null", "long"]}]
        });

        let evo = compute_evolution(&new, &old).unwrap();
        assert_eq!(evo.ops.len(), 1);
        assert_eq!(
            evo.ops[0],
            EvolutionOp::UpdateColumnType {
                name: "val".into(),
                from: "int".into(),
                to: "long".into(),
            }
        );
    }
}
