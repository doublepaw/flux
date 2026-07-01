// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Schema compatibility checking.
//!
//! Implements BACKWARD compatibility: new schema must be able to read
//! data written with the old schema.
//!
//! Rules are the intersection of Avro backward-compatibility and Iceberg
//! schema evolution constraints.

use apache_avro::Schema;
use serde_json::Value;

use crate::SchemaError;

/// Check if a new schema is backward compatible with an old schema.
///
/// BACKWARD compatibility means: new schema can read data written with old schema.
///
/// Compatible changes:
/// - Add field with default
/// - Add nullable field (["null", T]) with default null
/// - Remove field
/// - Widen int → long
/// - Widen float → double
///
/// Incompatible changes:
/// - Add non-nullable field without default
/// - Change field type (e.g. long → string)
/// - Rename field
/// - Narrow numeric (long → int)
/// - Widen int → float (Avro OK, Iceberg NO)
/// - Add union member beyond ["null", T]
pub fn is_backward_compatible(new_schema: &Value, old_schema: &Value) -> Result<bool, SchemaError> {
    // First check Iceberg-specific constraints on the new schema
    if let Err(msg) = check_iceberg_constraints(new_schema) {
        return Err(SchemaError::IncompatibleSchema { message: msg });
    }

    // Parse both schemas
    let new_parsed = Schema::parse(new_schema).map_err(|e| SchemaError::InvalidSchema {
        message: format!("new schema: {}", e),
    })?;

    let old_parsed = Schema::parse(old_schema).map_err(|e| SchemaError::InvalidSchema {
        message: format!("old schema: {}", e),
    })?;

    // Check if new can read old using Avro's resolution rules
    // We do this by checking if the schemas are compatible for reading
    Ok(check_read_compatibility(&new_parsed, &old_parsed))
}

/// Check Iceberg-specific constraints on a schema.
fn check_iceberg_constraints(schema: &Value) -> Result<(), String> {
    check_no_complex_unions(schema)?;
    Ok(())
}

/// Check that unions are only ["null", T] (Iceberg doesn't support complex unions).
///
/// Unions in Avro are represented as arrays in the "type" field of a field definition.
/// We need to check that any union has at most 2 types (for nullable fields: ["null", T]).
fn check_no_complex_unions(value: &Value) -> Result<(), String> {
    if let Value::Object(map) = value {
        // Check the "type" field - this is where unions appear
        if let Some(type_val) = map.get("type") {
            check_type_for_union(type_val)?;
        }
        // Recurse into "fields" if this is a record
        if let Some(Value::Array(fields)) = map.get("fields") {
            for field in fields {
                check_no_complex_unions(field)?;
            }
        }
        // Recurse into "items" for arrays
        if let Some(items) = map.get("items") {
            check_no_complex_unions(items)?;
        }
        // Recurse into "values" for maps
        if let Some(values) = map.get("values") {
            check_no_complex_unions(values)?;
        }
    }
    Ok(())
}

/// Check if a type value is a complex union.
fn check_type_for_union(type_val: &Value) -> Result<(), String> {
    match type_val {
        Value::Array(arr) => {
            // This is a union type
            if arr.len() > 2 {
                return Err(format!(
                    "unions with more than 2 types are not supported (Iceberg constraint): {:?}",
                    arr
                ));
            }
            // Recurse into union variants that might be complex types
            for variant in arr {
                if let Value::Object(_) = variant {
                    check_no_complex_unions(variant)?;
                }
            }
        }
        Value::Object(_) => {
            // Nested complex type (record, array, map, etc.)
            check_no_complex_unions(type_val)?;
        }
        _ => {}
    }
    Ok(())
}

/// Check if reader schema can read data written with writer schema.
fn check_read_compatibility(reader: &Schema, writer: &Schema) -> bool {
    // Use Apache Avro's built-in compatibility check
    // The ResolvedSchema mechanism checks if reader can read writer's data
    match (reader, writer) {
        (Schema::Record(r_rec), Schema::Record(w_rec)) => {
            // For records, check field-by-field compatibility
            check_record_compatibility(r_rec, w_rec)
        }
        // For primitive types, check promotion rules
        (Schema::Long, Schema::Int) => true, // int → long is OK
        (Schema::Double, Schema::Float) => true, // float → double is OK
        (Schema::Double, Schema::Int) => false, // int → double NOT OK (Iceberg)
        (Schema::Double, Schema::Long) => false, // long → double NOT OK (Iceberg)
        (Schema::Float, Schema::Int) => false, // int → float NOT OK (Iceberg)
        (Schema::Float, Schema::Long) => false, // long → float NOT OK (Iceberg)
        // Same types are compatible
        _ if reader == writer => true,
        // Union handling
        (Schema::Union(r_union), Schema::Union(w_union)) => {
            // Each writer variant must be readable by some reader variant
            w_union.variants().iter().all(|w_var| {
                r_union
                    .variants()
                    .iter()
                    .any(|r_var| check_read_compatibility(r_var, w_var))
            })
        }
        (Schema::Union(r_union), writer) => {
            // Writer is not a union, reader is - writer type must match a variant
            r_union
                .variants()
                .iter()
                .any(|r_var| check_read_compatibility(r_var, writer))
        }
        (reader, Schema::Union(w_union)) => {
            // Writer is a union, reader is not - all writer variants must be readable
            w_union
                .variants()
                .iter()
                .all(|w_var| w_var == &Schema::Null || check_read_compatibility(reader, w_var))
        }
        // Arrays
        (Schema::Array(r_arr), Schema::Array(w_arr)) => {
            check_read_compatibility(&r_arr.items, &w_arr.items)
        }
        // Maps
        (Schema::Map(r_map), Schema::Map(w_map)) => {
            check_read_compatibility(&r_map.types, &w_map.types)
        }
        // Enums - reader must have all writer symbols
        (Schema::Enum(r_enum), Schema::Enum(w_enum)) => {
            w_enum.symbols.iter().all(|s| r_enum.symbols.contains(s))
        }
        // Everything else is incompatible
        _ => false,
    }
}

/// Check record field-by-field compatibility.
///
/// Uses `reader.lookup` which maps both field names and aliases to field
/// positions, enabling backward-compatible field renames via Avro aliases.
fn check_record_compatibility(
    reader: &apache_avro::schema::RecordSchema,
    writer: &apache_avro::schema::RecordSchema,
) -> bool {
    let mut matched_reader_positions = std::collections::HashSet::new();

    // Rule 1: Writer fields matching reader (by name OR alias) must have compatible types
    for w_field in &writer.fields {
        if let Some(&pos) = reader.lookup.get(&w_field.name) {
            if !check_read_compatibility(&reader.fields[pos].schema, &w_field.schema) {
                return false;
            }
            matched_reader_positions.insert(pos);
        }
        // Writer field not in reader → reader ignores it (OK)
    }

    // Rule 2: Unmatched reader fields must have defaults or be nullable
    for (pos, r_field) in reader.fields.iter().enumerate() {
        if matched_reader_positions.contains(&pos) {
            continue;
        }
        if r_field.default.is_none() && !is_nullable_schema(&r_field.schema) {
            return false;
        }
    }

    true
}

/// Check if a schema is nullable (union with null as first or second element).
fn is_nullable_schema(schema: &Schema) -> bool {
    match schema {
        Schema::Union(union) => union.variants().iter().any(|v| matches!(v, Schema::Null)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_same_schema_compatible() {
        let schema = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        });

        assert!(is_backward_compatible(&schema, &schema).unwrap());
    }

    #[test]
    fn test_add_field_with_default_compatible() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": "string", "default": "unknown"}
            ]
        });

        assert!(is_backward_compatible(&new, &old).unwrap());
    }

    #[test]
    fn test_add_nullable_field_compatible() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "email", "type": ["null", "string"], "default": null}
            ]
        });

        assert!(is_backward_compatible(&new, &old).unwrap());
    }

    #[test]
    fn test_remove_field_compatible() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "deprecated", "type": "string"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        });

        assert!(is_backward_compatible(&new, &old).unwrap());
    }

    #[test]
    fn test_widen_int_to_long_compatible() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "count", "type": "int"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "count", "type": "long"}
            ]
        });

        assert!(is_backward_compatible(&new, &old).unwrap());
    }

    #[test]
    fn test_widen_float_to_double_compatible() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": "float"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": "double"}
            ]
        });

        assert!(is_backward_compatible(&new, &old).unwrap());
    }

    #[test]
    fn test_add_required_field_without_default_incompatible() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "required_field", "type": "string"}
            ]
        });

        assert!(!is_backward_compatible(&new, &old).unwrap());
    }

    #[test]
    fn test_change_field_type_incompatible() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": "long"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": "string"}
            ]
        });

        assert!(!is_backward_compatible(&new, &old).unwrap());
    }

    #[test]
    fn test_narrow_long_to_int_incompatible() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "count", "type": "long"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "count", "type": "int"}
            ]
        });

        assert!(!is_backward_compatible(&new, &old).unwrap());
    }

    #[test]
    fn test_complex_union_rejected() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "value", "type": ["null", "string", "long"]}
            ]
        });

        let result = is_backward_compatible(&new, &old);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unions"));
    }

    #[test]
    fn test_int_to_float_rejected_iceberg() {
        let old = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": "int"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": "float"}
            ]
        });

        // Avro allows this, but Iceberg doesn't
        assert!(!is_backward_compatible(&new, &old).unwrap());
    }

    // ============ Real-World Schema Evolution Scenarios ============

    #[test]
    fn test_order_schema_evolution_v1_to_v2() {
        // Version 1: Basic order
        let v1 = json!({
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "amount", "type": "long"}
            ]
        });

        // Version 2: Add currency with default
        let v2 = json!({
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "amount", "type": "long"},
                {"name": "currency", "type": "string", "default": "USD"}
            ]
        });

        assert!(is_backward_compatible(&v2, &v1).unwrap());
    }

    #[test]
    fn test_order_schema_evolution_v2_to_v3() {
        // Version 2
        let v2 = json!({
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "amount", "type": "long"},
                {"name": "currency", "type": "string", "default": "USD"}
            ]
        });

        // Version 3: Add optional customer_id, remove deprecated field
        let v3 = json!({
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "amount", "type": "long"},
                {"name": "customer_id", "type": ["null", "string"], "default": null}
            ]
        });

        // currency was removed (OK), customer_id was added with null default (OK)
        assert!(is_backward_compatible(&v3, &v2).unwrap());
    }

    #[test]
    fn test_user_schema_multiple_evolutions() {
        let v1 = json!({
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "age", "type": "int"}
            ]
        });

        let v2 = json!({
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "age", "type": "long"}  // widened int -> long
            ]
        });

        let v3 = json!({
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "age", "type": "long"},
                {"name": "email", "type": ["null", "string"], "default": null}
            ]
        });

        // Each step should be compatible
        assert!(is_backward_compatible(&v2, &v1).unwrap());
        assert!(is_backward_compatible(&v3, &v2).unwrap());

        // Transitive compatibility: v3 should be able to read v1 data
        assert!(is_backward_compatible(&v3, &v1).unwrap());
    }

    #[test]
    fn test_nested_record_evolution() {
        let v1 = json!({
            "type": "record",
            "name": "Event",
            "fields": [
                {"name": "event_id", "type": "string"},
                {
                    "name": "payload",
                    "type": {
                        "type": "record",
                        "name": "Payload",
                        "fields": [
                            {"name": "data", "type": "string"}
                        ]
                    }
                }
            ]
        });

        let v2 = json!({
            "type": "record",
            "name": "Event",
            "fields": [
                {"name": "event_id", "type": "string"},
                {
                    "name": "payload",
                    "type": {
                        "type": "record",
                        "name": "Payload",
                        "fields": [
                            {"name": "data", "type": "string"},
                            {"name": "metadata", "type": ["null", "string"], "default": null}
                        ]
                    }
                }
            ]
        });

        assert!(is_backward_compatible(&v2, &v1).unwrap());
    }

    #[test]
    fn test_array_field_evolution() {
        let v1 = json!({
            "type": "record",
            "name": "Container",
            "fields": [
                {"name": "items", "type": {"type": "array", "items": "int"}}
            ]
        });

        // Widen array items from int to long
        let v2 = json!({
            "type": "record",
            "name": "Container",
            "fields": [
                {"name": "items", "type": {"type": "array", "items": "long"}}
            ]
        });

        assert!(is_backward_compatible(&v2, &v1).unwrap());
    }

    #[test]
    fn test_map_field_evolution() {
        let v1 = json!({
            "type": "record",
            "name": "Metrics",
            "fields": [
                {"name": "counters", "type": {"type": "map", "values": "int"}}
            ]
        });

        // Widen map values from int to long
        let v2 = json!({
            "type": "record",
            "name": "Metrics",
            "fields": [
                {"name": "counters", "type": {"type": "map", "values": "long"}}
            ]
        });

        assert!(is_backward_compatible(&v2, &v1).unwrap());
    }

    #[test]
    fn test_enum_evolution_add_symbol() {
        let v1 = json!({
            "type": "record",
            "name": "Order",
            "fields": [
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "Status",
                        "symbols": ["PENDING", "COMPLETED"]
                    }
                }
            ]
        });

        let v2 = json!({
            "type": "record",
            "name": "Order",
            "fields": [
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "Status",
                        "symbols": ["PENDING", "COMPLETED", "CANCELLED"]
                    }
                }
            ]
        });

        // Adding enum symbols is backward compatible
        // (reader can still read old values)
        assert!(is_backward_compatible(&v2, &v1).unwrap());
    }

    #[test]
    fn test_enum_evolution_remove_symbol_incompatible() {
        let v1 = json!({
            "type": "record",
            "name": "Order",
            "fields": [
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "Status",
                        "symbols": ["PENDING", "COMPLETED", "CANCELLED"]
                    }
                }
            ]
        });

        let v2 = json!({
            "type": "record",
            "name": "Order",
            "fields": [
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "Status",
                        "symbols": ["PENDING", "COMPLETED"]
                    }
                }
            ]
        });

        // Removing enum symbols is NOT backward compatible
        // (reader can't handle old "CANCELLED" values)
        assert!(!is_backward_compatible(&v2, &v1).unwrap());
    }

    // ============ Invalid Schema Tests ============

    #[test]
    fn test_invalid_schema_syntax() {
        let valid = json!({
            "type": "record",
            "name": "Test",
            "fields": [{"name": "id", "type": "string"}]
        });

        let invalid = json!({
            "type": "record",
            "name": "Test"
            // missing "fields"
        });

        let result = is_backward_compatible(&invalid, &valid);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("schema"));
    }

    // ============ Alias / Rename Tests ============

    #[test]
    fn test_rename_via_alias_compatible() {
        // Writer uses old name, reader uses new name with alias
        let old = json!({
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "old_name", "type": "long"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "new_name", "type": "long", "aliases": ["old_name"]}
            ]
        });

        assert!(is_backward_compatible(&new, &old).unwrap());
    }

    #[test]
    fn test_rename_via_alias_type_mismatch() {
        let old = json!({
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "old_name", "type": "long"}
            ]
        });

        let new = json!({
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "new_name", "type": "string", "aliases": ["old_name"]}
            ]
        });

        assert!(!is_backward_compatible(&new, &old).unwrap());
    }

    #[test]
    fn test_rename_without_alias_needs_default() {
        // Without alias, rename is treated as remove + add
        let old = json!({
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "old_name", "type": "long"}
            ]
        });

        // new_name has no alias, no default, not nullable → incompatible
        let new_no_default = json!({
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "new_name", "type": "long"}
            ]
        });
        assert!(!is_backward_compatible(&new_no_default, &old).unwrap());

        // With a default → compatible (old field ignored, new field uses default)
        let new_with_default = json!({
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "new_name", "type": "long", "default": 0}
            ]
        });
        assert!(is_backward_compatible(&new_with_default, &old).unwrap());
    }

    #[test]
    fn test_incompatible_type_in_nullable() {
        let v1 = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": ["null", "string"]}
            ]
        });

        let v2 = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "value", "type": ["null", "long"]}
            ]
        });

        // Changing the type inside a nullable is incompatible
        assert!(!is_backward_compatible(&v2, &v1).unwrap());
    }
}
