// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

use super::{
    SchemaError,
    safe::{LogicalType, RegularType as SafeSchemaType, SchemaNode as SafeSchemaNode},
};

use std::{collections::HashMap, marker::PhantomData};

pub(crate) use super::{Fixed, Name};

/// Type information for a field, enabling fast traversal without schema lookups.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    // Primitives
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    // Logical types
    Uuid,
    Date,
    TimeMillis,
    TimeMicros,
    TimestampMillis,
    TimestampMicros,
    Decimal,
    BigDecimal,
    Duration,
    // Complex types
    /// Record type with its full name for index lookup
    Record(String),
    /// Enum type with its full name
    Enum(String),
    /// Fixed type with name and size
    Fixed {
        name: String,
        size: usize,
    },
    /// Array with element type
    Array(Box<FieldType>),
    /// Map with value type (Avro map keys are always strings)
    Map(Box<FieldType>),
    /// Union with variant types
    Union(Vec<FieldType>),
}

/// Index for fast field lookup in records.
///
/// Maps record names to their field indices and types, enabling O(1) field access
/// and fast traversal without storing field names in each record instance.
#[derive(Debug, Default)]
pub struct SchemaRecordIndex {
    /// Maps record full name -> (field name -> (field position, field type))
    records: HashMap<String, HashMap<String, (usize, FieldType)>>,
}

impl SchemaRecordIndex {
    /// Get the field index and type for a given record and field name.
    pub fn get_field(&self, record_name: &str, field_name: &str) -> Option<(usize, &FieldType)> {
        self.records
            .get(record_name)?
            .get(field_name)
            .map(|(idx, ft)| (*idx, ft))
    }

    /// Get the field index for a given record and field name.
    pub fn get_field_index(&self, record_name: &str, field_name: &str) -> Option<usize> {
        self.records
            .get(record_name)?
            .get(field_name)
            .map(|(idx, _)| *idx)
    }

    /// Get the field map for a given record.
    pub fn get_record_fields(
        &self,
        record_name: &str,
    ) -> Option<&HashMap<String, (usize, FieldType)>> {
        self.records.get(record_name)
    }
}

/// Main Schema type, opaque representation of an Avro schema
///
/// This is the fully pre-computed type used by the serializer and deserializer.
///
/// To achieve the ideal performance and ease of use via self-referencing
/// nodes, it is built using `unsafe`, so it can only be built through
/// [its safe counterpart](crate::schema::SchemaMut) (via
/// [`.freeze()`](crate::schema::SchemaMut::freeze) or [`TryFrom`]) because it
/// makes the conversion code simple enough that we can reasonably guarantee its
/// correctness despite the usage of `unsafe`.
///
/// It is useful to implement it this way because, due to how referencing via
/// [Names](https://avro.apache.org/docs/current/specification/#names) works in Avro,
/// the most performant representation of an Avro schema is not a tree but a
/// possibly-cyclic general directed graph.
pub struct Schema {
    // First node in the array is considered to be the root
    //
    // This lifetime is fake, but since all elements have to be accessed by the `root` function
    // which will downcast it and we never push anything more in there (which would cause
    // reallocation and invalidate all nodes) this is correct.
    nodes: Vec<SchemaNode<'static>>,
    fingerprint: [u8; 8],
    schema_json: String,
    /// Index for fast record field lookup
    record_index: SchemaRecordIndex,
}

impl Schema {
    /// This is private API, you probably intended to call that on an
    /// [`SchemaMut`](crate::schema::SchemaMut) instead of `Schema`.
    ///
    /// The Avro schema
    /// is represented internally as a directed graph of nodes, all stored in
    /// [`Schema`].
    ///
    /// The root node represents the whole schema.
    pub(crate) fn root<'a>(&'a self) -> NodeRef<'a> {
        // the signature of this function downgrades the fake 'static lifetime in a way
        // that makes it correct
        assert!(
            !self.nodes.is_empty(),
            "Schema must have at least one node (the root)"
        );
        // SAFETY: bounds checked
        unsafe { NodeRef::new(self.nodes.as_ptr() as *mut _) }
    }

    /// Obtain the JSON for this schema
    pub fn json(&self) -> &str {
        &self.schema_json
    }

    /// Obtain the Rabin fingerprint of the schema
    pub fn rabin_fingerprint(&self) -> &[u8; 8] {
        &self.fingerprint
    }

    /// Get the record field index for fast field lookups
    pub fn record_index(&self) -> &SchemaRecordIndex {
        &self.record_index
    }
}

/// A `NodeRef` is a pointer to a node in a [`Schema`]
///
/// This is morally equivalent to `&'a SchemaNode<'a>`, only Rust will not
/// assume as much when it comes to aliasing constraints.
///
/// For ease of use, it can be `Deref`d to a [`SchemaNode`],
/// so this module is responsible for ensuring that no `NodeRef`
/// is leaked that would be incorrect on that regard.
///
/// SAFETY: The invariant that we need to uphold is that with regards to
/// lifetimes, this behaves the same as an `&'a SchemaNode<'a>`.
///
/// We don't directly use references because we need to update the pointees
/// after creating refences to them when building the schema, and that doesn't
/// pass Miri's Stacked Borrows checks.
/// This abstraction should be reasonably ergonomic, but pass miri.
pub(crate) struct NodeRef<'a, N = SchemaNode<'a>> {
    node: std::ptr::NonNull<N>,
    _spooky: PhantomData<&'a N>,
}
impl<'a, N> Copy for NodeRef<'a, N> {}
impl<'a, N> Clone for NodeRef<'a, N> {
    fn clone(&self) -> Self {
        *self
    }
}
/// SAFETY: NonNull is !Send !Sync, but NodeRef is really just a reference, so
/// we can implement Sync and Send
unsafe impl<T: Sync> Sync for NodeRef<'_, T> {}
/// SAFETY: NonNull is !Send !Sync, but NodeRef is really just a reference, so
/// we can implement Sync and Send
unsafe impl<T: Sync> Send for NodeRef<'_, T> {}
impl<N> NodeRef<'static, N> {
    const unsafe fn new(ptr: *mut N) -> Self {
        Self {
            // SAFETY: callers uphold that `ptr` is non-null and points to a
            // valid node with the required lifetime invariants for NodeRef.
            node: unsafe { std::ptr::NonNull::new_unchecked(ptr) },
            _spooky: PhantomData,
        }
    }
}
impl<'a, N> NodeRef<'a, N> {
    /// Compared to `Deref`, this propagates the lifetime of the reference
    pub(crate) fn as_ref(self) -> &'a N {
        // SAFETY: this module is responsible for never leaking a `NodeRef` that
        // isn't tied to the appropriate lifetime
        unsafe { self.node.as_ref() }
    }
}
impl<'a, N> std::ops::Deref for NodeRef<'a, N> {
    type Target = N;
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

/// A node of an avro schema, borrowed from a [`Schema`].
///
/// This enum is borrowed from a [`Schema`] and is used to navigate it.
///
/// For details about the meaning of the variants, see the
/// [`SchemaNode`](crate::schema::SchemaNode) documentation.
#[non_exhaustive]
pub(crate) enum SchemaNode<'a> {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Array(NodeRef<'a>),
    Map(NodeRef<'a>),
    Union(Union<'a>),
    Record(Record<'a>),
    Enum(Enum),
    Fixed(Fixed),
    Decimal(Decimal),
    BigDecimal,
    Uuid,
    Date,
    TimeMillis,
    TimeMicros,
    TimestampMillis,
    TimestampMicros,
    Duration,
}

/// Component of a [`SchemaNode`]
#[derive(Debug)]
pub(crate) struct Union<'a> {
    pub(crate) variants: Vec<NodeRef<'a>>,
}

/// Component of a [`SchemaNode`]
#[derive(Debug)]
pub(crate) struct Record<'a> {
    pub(crate) fields: Vec<RecordField<'a>>,
    pub(crate) name: Name,
}

/// Component of a [`SchemaNode`]
#[derive(Debug)]
pub(crate) struct RecordField<'a> {
    pub(crate) name: String,
    pub(crate) schema: NodeRef<'a>,
}

/// Component of a [`SchemaNode`]
#[derive(Clone, Debug)]
pub(crate) struct Enum {
    pub(crate) symbols: Vec<String>,
    pub(crate) name: Name,
}

/// Component of a [`SchemaNode`]
#[derive(Clone, Debug)]
pub(crate) struct Decimal {
    /// Unused for now - TODO take this into account when serializing (tolerate
    /// precision loss within the limits of this)
    pub(crate) _precision: usize,
    pub(crate) scale: u32,
    pub(crate) repr: DecimalRepr,
}
#[derive(Clone, Debug)]
pub(crate) enum DecimalRepr {
    Bytes,
    Fixed(Fixed),
}

impl TryFrom<super::safe::SchemaMut> for Schema {
    type Error = SchemaError;
    fn try_from(safe: super::safe::SchemaMut) -> Result<Self, SchemaError> {
        if safe.nodes().is_empty() {
            return Err(SchemaError::new(
                "Schema must have at least one node (the root)",
            ));
        }

        // The `nodes` allocation should never be moved otherwise references will become
        // invalid
        let mut ret = Self {
            nodes: (0..safe.nodes.len()).map(|_| SchemaNode::Null).collect(),
            fingerprint: safe.canonical_form_rabin_fingerprint()?,
            schema_json: match safe.schema_json {
                None => safe.serialize_to_json()?,
                Some(json) => json,
            },
            record_index: SchemaRecordIndex::default(),
        };
        let len = ret.nodes.len();
        // Let's be extra-sure (second condition is for calls to add)
        assert!(len > 0 && len == safe.nodes.len() && len <= (isize::MAX as usize));
        let storage_start_ptr = ret.nodes.as_mut_ptr();
        let key_to_ref =
            |schema_key: super::safe::SchemaKey| -> Result<NodeRef<'static>, SchemaError> {
                let idx = schema_key.idx;
                if idx >= len {
                    return Err(SchemaError::msg(format_args!(
                        "SchemaKey index {} is out of bounds (len: {})",
                        idx, len
                    )));
                }
                // SAFETY: see below
                Ok(unsafe { NodeRef::new(storage_start_ptr.add(idx)) })
            };

        // Now we can initialize the nodes
        let mut curr_storage_node_ptr = storage_start_ptr;
        for safe_node in safe.nodes {
            // SAFETY:
            // - The nodes we create here are never moving in memory since the entire vec is
            //   preallocated, and even when moving a vec, the pointed space doesn't move.
            // - The fake `'static` lifetimes are always downgraded before being made
            //   available.
            // - We only use pointers from the point at which we call `as_mut_ptr` so the
            //   compiler will not have aliasing constraints.
            // - We don't dereference the ~references (NodeRef) we create in key_to_ref
            //   until all nodes are initialized.

            let new_node = match safe_node {
                SafeSchemaNode {
                    logical_type: Some(LogicalType::Decimal(decimal)),
                    type_: SafeSchemaType::Bytes,
                } => SchemaNode::Decimal(Decimal {
                    _precision: decimal.precision,
                    scale: decimal.scale,
                    repr: DecimalRepr::Bytes,
                }),
                SafeSchemaNode {
                    logical_type: Some(LogicalType::Decimal(decimal)),
                    type_: SafeSchemaType::Fixed(fixed),
                } => SchemaNode::Decimal(Decimal {
                    _precision: decimal.precision,
                    scale: decimal.scale,
                    repr: DecimalRepr::Fixed(fixed),
                }),
                SafeSchemaNode {
                    logical_type: Some(LogicalType::Uuid),
                    type_: SafeSchemaType::String,
                } => SchemaNode::Uuid,
                SafeSchemaNode {
                    logical_type: Some(LogicalType::Date),
                    type_: SafeSchemaType::Int,
                } => SchemaNode::Date,
                SafeSchemaNode {
                    logical_type: Some(LogicalType::TimeMillis),
                    type_: SafeSchemaType::Int,
                } => SchemaNode::TimeMillis,
                SafeSchemaNode {
                    logical_type: Some(LogicalType::TimeMicros),
                    type_: SafeSchemaType::Long,
                } => SchemaNode::TimeMicros,
                SafeSchemaNode {
                    logical_type: Some(LogicalType::TimestampMillis),
                    type_: SafeSchemaType::Long,
                } => SchemaNode::TimestampMillis,
                SafeSchemaNode {
                    logical_type: Some(LogicalType::TimestampMicros),
                    type_: SafeSchemaType::Long,
                } => SchemaNode::TimestampMicros,
                SafeSchemaNode {
                    logical_type: Some(LogicalType::Duration),
                    type_: SafeSchemaType::Fixed(fixed),
                } if fixed.size == 12 => SchemaNode::Duration,
                SafeSchemaNode {
                    logical_type: Some(LogicalType::BigDecimal),
                    type_: SafeSchemaType::Bytes,
                } => SchemaNode::BigDecimal,
                _ => match safe_node.type_ {
                    SafeSchemaType::Null => SchemaNode::Null,
                    SafeSchemaType::Boolean => SchemaNode::Boolean,
                    SafeSchemaType::Int => SchemaNode::Int,
                    SafeSchemaType::Long => SchemaNode::Long,
                    SafeSchemaType::Float => SchemaNode::Float,
                    SafeSchemaType::Double => SchemaNode::Double,
                    SafeSchemaType::Bytes => SchemaNode::Bytes,
                    SafeSchemaType::String => SchemaNode::String,
                    SafeSchemaType::Array(array) => SchemaNode::Array(key_to_ref(array.items)?),
                    SafeSchemaType::Map(map) => SchemaNode::Map(key_to_ref(map.values)?),
                    SafeSchemaType::Union(union) => SchemaNode::Union({
                        Union {
                            variants: {
                                let mut variants = Vec::with_capacity(union.variants.len());
                                for schema_key in union.variants {
                                    variants.push(key_to_ref(schema_key)?);
                                }
                                variants
                            },
                        }
                    }),
                    SafeSchemaType::Record(record) => SchemaNode::Record(Record {
                        fields: {
                            let mut fields = Vec::with_capacity(record.fields.len());
                            for field in record.fields {
                                fields.push(RecordField {
                                    name: field.name,
                                    schema: key_to_ref(field.type_)?,
                                });
                            }
                            fields
                        },
                        name: record.name,
                    }),
                    SafeSchemaType::Enum(enum_) => SchemaNode::Enum(Enum {
                        symbols: enum_.symbols,
                        name: enum_.name,
                    }),
                    SafeSchemaType::Fixed(fixed) => SchemaNode::Fixed(fixed),
                },
            };
            // SAFETY: see comment at beginning of loop
            unsafe {
                *curr_storage_node_ptr = new_node;
                curr_storage_node_ptr = curr_storage_node_ptr.add(1);
            };
        }

        // Build the record_index for fast field lookups
        // Helper function to convert SchemaNode to FieldType
        fn schema_node_to_field_type(node: &SchemaNode<'_>) -> FieldType {
            match node {
                SchemaNode::Null => FieldType::Null,
                SchemaNode::Boolean => FieldType::Boolean,
                SchemaNode::Int => FieldType::Int,
                SchemaNode::Long => FieldType::Long,
                SchemaNode::Float => FieldType::Float,
                SchemaNode::Double => FieldType::Double,
                SchemaNode::Bytes => FieldType::Bytes,
                SchemaNode::String => FieldType::String,
                SchemaNode::Uuid => FieldType::Uuid,
                SchemaNode::Date => FieldType::Date,
                SchemaNode::TimeMillis => FieldType::TimeMillis,
                SchemaNode::TimeMicros => FieldType::TimeMicros,
                SchemaNode::TimestampMillis => FieldType::TimestampMillis,
                SchemaNode::TimestampMicros => FieldType::TimestampMicros,
                SchemaNode::Decimal(_) => FieldType::Decimal,
                SchemaNode::BigDecimal => FieldType::BigDecimal,
                SchemaNode::Duration => FieldType::Duration,
                SchemaNode::Record(rec) => {
                    FieldType::Record(rec.name.fully_qualified_name().to_owned())
                }
                SchemaNode::Enum(e) => FieldType::Enum(e.name.fully_qualified_name().to_owned()),
                SchemaNode::Fixed(f) => FieldType::Fixed {
                    name: f.name.fully_qualified_name().to_owned(),
                    size: f.size,
                },
                SchemaNode::Array(elem) => {
                    FieldType::Array(Box::new(schema_node_to_field_type(elem.as_ref())))
                }
                SchemaNode::Map(val) => {
                    FieldType::Map(Box::new(schema_node_to_field_type(val.as_ref())))
                }
                SchemaNode::Union(u) => FieldType::Union(
                    u.variants
                        .iter()
                        .map(|v| schema_node_to_field_type(v.as_ref()))
                        .collect(),
                ),
            }
        }

        // Iterate through all nodes and build index for records
        for node in &ret.nodes {
            if let SchemaNode::Record(record) = node {
                let field_map: HashMap<String, (usize, FieldType)> = record
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(idx, field)| {
                        let field_type = schema_node_to_field_type(field.schema.as_ref());
                        (field.name.clone(), (idx, field_type))
                    })
                    .collect();
                ret.record_index
                    .records
                    .insert(record.name.fully_qualified_name().to_owned(), field_map);
            }
        }

        Ok(ret)
    }
}

impl std::fmt::Debug for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <SchemaNode<'_> as std::fmt::Debug>::fmt(self.root().as_ref(), f)
    }
}

impl<N: std::fmt::Debug> std::fmt::Debug for NodeRef<'_, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <N as std::fmt::Debug>::fmt(self.as_ref(), f)
    }
}

impl<'a> std::fmt::Debug for SchemaNode<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        // Avoid going into stack overflow when rendering SchemaNode's debug impl, in
        // case there are loops

        use std::cell::Cell;
        struct SchemaNodeRenderingDepthGuard;
        thread_local! {
            static DEPTH: Cell<u32> = const { Cell::new(0) };
        }
        impl Drop for SchemaNodeRenderingDepthGuard {
            fn drop(&mut self) {
                DEPTH.with(|cell| cell.set(cell.get().checked_sub(1).unwrap()));
            }
        }
        const MAX_DEPTH: u32 = 2;
        let depth = DEPTH.with(|cell| {
            let val = cell.get();
            cell.set(val + 1);
            val
        });
        let _decrement_depth_guard = SchemaNodeRenderingDepthGuard;

        match *self {
            SchemaNode::Null => f.debug_tuple("Null").finish(),
            SchemaNode::Boolean => f.debug_tuple("Boolean").finish(),
            SchemaNode::Int => f.debug_tuple("Int").finish(),
            SchemaNode::Long => f.debug_tuple("Long").finish(),
            SchemaNode::Float => f.debug_tuple("Float").finish(),
            SchemaNode::Double => f.debug_tuple("Double").finish(),
            SchemaNode::Bytes => f.debug_tuple("Bytes").finish(),
            SchemaNode::String => f.debug_tuple("String").finish(),
            SchemaNode::Array(inner) => {
                let mut d = f.debug_tuple("Array");
                if depth < MAX_DEPTH {
                    d.field(inner.as_ref());
                }
                d.finish()
            }
            SchemaNode::Map(inner) => {
                let mut d = f.debug_tuple("Map");
                if depth < MAX_DEPTH {
                    d.field(inner.as_ref());
                }
                d.finish()
            }
            SchemaNode::Union(ref inner) => {
                let mut d = f.debug_tuple("Union");
                if depth < MAX_DEPTH {
                    d.field(inner);
                }
                d.finish()
            }
            SchemaNode::Record(ref inner) => {
                let mut d = f.debug_tuple("Record");
                if depth < MAX_DEPTH {
                    d.field(inner);
                }
                d.finish()
            }
            SchemaNode::Enum(ref inner) => {
                let mut d = f.debug_tuple("Enum");
                if depth < MAX_DEPTH {
                    d.field(inner);
                }
                d.finish()
            }
            SchemaNode::Fixed(ref inner) => {
                let mut d = f.debug_tuple("Fixed");
                if depth < MAX_DEPTH {
                    d.field(inner);
                }
                d.finish()
            }
            SchemaNode::Decimal(ref inner) => {
                let mut d = f.debug_tuple("Decimal");
                if depth < MAX_DEPTH {
                    d.field(inner);
                }
                d.finish()
            }
            SchemaNode::BigDecimal => f.debug_tuple("BigDecimal").finish(),
            SchemaNode::Uuid => f.debug_tuple("Uuid").finish(),
            SchemaNode::Date => f.debug_tuple("Date").finish(),
            SchemaNode::TimeMillis => f.debug_tuple("TimeMillis").finish(),
            SchemaNode::TimeMicros => f.debug_tuple("TimeMicros").finish(),
            SchemaNode::TimestampMillis => f.debug_tuple("TimestampMillis").finish(),
            SchemaNode::TimestampMicros => f.debug_tuple("TimestampMicros").finish(),
            SchemaNode::Duration => f.debug_tuple("Duration").finish(),
        }
    }
}
