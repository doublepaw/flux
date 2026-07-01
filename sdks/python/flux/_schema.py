# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Nikhil Simha Raprolu

"""Schema generation from dataclasses via @schema decorator."""

import dataclasses
import enum
import io
import json
import types
import typing
from typing import Any, Annotated, Optional, Union, get_args, get_origin, get_type_hints

import fastavro

from .exceptions import SchemaException


class Int32:
    """Annotation marker: map int to Avro "int" instead of default "long"."""


class Float32:
    """Annotation marker: map float to Avro "float" instead of default "double"."""


class NonNull:
    """Annotation marker: field is required (not nullable)."""


# Keep old names as aliases for backwards compat during migration
AvroInt = Int32
AvroFloat = Float32


def schema(
    cls=None,
    *,
    topic: str | None = None,
    namespace: str | None = None,
    renames: dict[str, str] | None = None,
    deletions: list[str] | None = None,
):
    """Decorator that adds schema generation and serde to a dataclass.

    Can be used with or without arguments:
        @schema
        @dataclass
        class Foo: ...

        @schema(topic="orders", namespace="com.example")
        @dataclass
        class Bar: ...
    """
    renames = renames or {}
    deletions = deletions or []

    def wrap(cls):
        if topic is not None:
            cls.__flux_topic__ = topic
        return _apply_schema(cls, namespace, renames, deletions)

    if cls is not None:
        # Called without arguments: @schema
        return wrap(cls)
    # Called with arguments: @schema(...)
    return wrap


def _apply_schema(
    cls,
    namespace: str | None,
    renames: dict[str, str],
    deletions: list[str],
):
    if not dataclasses.is_dataclass(cls):
        raise SchemaException(f"{cls.__name__} must be a @dataclass")

    field_names = {f.name for f in dataclasses.fields(cls)}

    # Validate renames: targets must exist as fields
    for old, new in renames.items():
        if new not in field_names:
            raise SchemaException(
                f"rename target '{new}' is not a field on {cls.__name__}"
            )

    # Validate deletions: deleted names must NOT be current fields
    for name in deletions:
        if name in field_names:
            raise SchemaException(
                f"deletion '{name}' still exists as a field on {cls.__name__}"
            )

    # Validate no overlap between rename targets and deletions
    for old, new in renames.items():
        if old in deletions:
            raise SchemaException(
                f"'{old}' appears in both renames and deletions"
            )

    # Build and cache the schema
    avro_schema = _build_schema(cls, namespace, renames, deletions)
    parsed_schema = fastavro.parse_schema(avro_schema)

    @classmethod
    def _schema(cls) -> dict:
        return avro_schema

    @classmethod
    def _schema_json(cls) -> str:
        return json.dumps(avro_schema, indent=2)

    def _to_bytes(self) -> bytes:
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, parsed_schema, _to_dict(self))
        return buf.getvalue()

    @classmethod
    def _from_bytes(cls, data: bytes):
        buf = io.BytesIO(data)
        record = fastavro.schemaless_reader(buf, parsed_schema)
        return _from_dict(cls, record)

    cls.schema = _schema
    cls.schema_json = _schema_json
    cls.to_bytes = _to_bytes
    cls.from_bytes = _from_bytes

    return cls


def _build_schema(
    cls,
    namespace: str | None,
    renames: dict[str, str],
    deletions: list[str],
) -> dict:
    reverse_renames = {v: k for k, v in renames.items()}
    hints = get_type_hints(cls, include_extras=True)
    fields = []
    seen_types: set[str] = set()

    for f in dataclasses.fields(cls):
        hint = hints[f.name]
        is_nonnull = _has_annotation(hint, NonNull)
        avro_type = _python_type_to_avro(hint, seen_types)

        # Wrap in nullable union unless annotated with NonNull
        if not is_nonnull and not _is_optional(hint):
            avro_type = ["null", avro_type]

        field_def: dict[str, Any] = {"name": f.name, "type": avro_type}

        # Add alias if this field was renamed
        if f.name in reverse_renames:
            field_def["aliases"] = [reverse_renames[f.name]]

        # Add default for nullable fields
        if not is_nonnull:
            field_def["default"] = None
        elif f.default is not dataclasses.MISSING:
            field_def["default"] = f.default
        elif f.default_factory is not dataclasses.MISSING:
            field_def["default"] = f.default_factory()

        fields.append(field_def)

    result: dict[str, Any] = {
        "type": "record",
        "name": cls.__name__,
        "fields": fields,
    }
    if namespace:
        result["namespace"] = namespace
    if renames:
        result["flux.renames"] = renames
    if deletions:
        result["flux.deletions"] = deletions

    return result


def _has_annotation(tp, marker) -> bool:
    """Check if a type has a specific annotation marker in Annotated metadata."""
    origin = get_origin(tp)
    if origin is Annotated:
        args = get_args(tp)
        for ann in args[1:]:
            if ann is marker or (isinstance(ann, type) and issubclass(ann, marker)):
                return True
    return False


def _is_union(origin) -> bool:
    """Check if origin is a union type (typing.Union or types.UnionType)."""
    return origin is Union or origin is types.UnionType


def _is_optional(tp) -> bool:
    """Check if type is Optional[T] or T | None."""
    origin = get_origin(tp)
    if _is_union(origin):
        args = get_args(tp)
        return type(None) in args
    # Handle Annotated wrapping an Optional
    if origin is Annotated:
        inner = get_args(tp)[0]
        return _is_optional(inner)
    return False


def _python_type_to_avro(tp, seen_types: set[str]) -> Any:
    """Convert a Python type hint to an Avro schema type."""
    origin = get_origin(tp)

    # Handle Annotated types first
    if origin is Annotated:
        args = get_args(tp)
        inner_type = args[0]
        annotations = args[1:]

        for ann in annotations:
            if ann is Int32 or isinstance(ann, type) and issubclass(ann, Int32):
                if inner_type is not int:
                    raise SchemaException(
                        f"Int32 can only annotate int, got {inner_type}"
                    )
                return "int"
            if ann is Float32 or isinstance(ann, type) and issubclass(ann, Float32):
                if inner_type is not float:
                    raise SchemaException(
                        f"Float32 can only annotate float, got {inner_type}"
                    )
                return "float"

        # No recognized type annotation, process inner type
        return _python_type_to_avro(inner_type, seen_types)

    # Handle Optional[T] / T | None
    if _is_union(origin):
        args = get_args(tp)
        non_none = [a for a in args if a is not type(None)]
        if len(args) - len(non_none) != 1:
            raise SchemaException(f"unsupported union type: {tp}")
        if len(non_none) != 1:
            raise SchemaException(
                f"complex unions are not supported (Iceberg constraint): {tp}"
            )
        inner_avro = _python_type_to_avro(non_none[0], seen_types)
        return ["null", inner_avro]

    # Primitives
    if tp is str:
        return "string"
    if tp is int:
        return "long"
    if tp is float:
        return "double"
    if tp is bool:
        return "boolean"
    if tp is bytes:
        return "bytes"

    # list[T]
    if origin is list:
        (item_type,) = get_args(tp)
        return {"type": "array", "items": _python_type_to_avro(item_type, seen_types)}

    # dict[str, T]
    if origin is dict:
        key_type, val_type = get_args(tp)
        if key_type is not str:
            raise SchemaException(
                f"Avro maps require string keys, got {key_type}"
            )
        return {"type": "map", "values": _python_type_to_avro(val_type, seen_types)}

    # Enum
    if isinstance(tp, type) and issubclass(tp, enum.Enum):
        symbols = []
        for member in tp:
            if not isinstance(member.value, str):
                raise SchemaException(
                    f"Avro enums require string values, "
                    f"{tp.__name__}.{member.name} = {member.value!r}"
                )
            symbols.append(member.value)
        return {"type": "enum", "name": tp.__name__, "symbols": symbols}

    # Nested dataclass with @schema
    if dataclasses.is_dataclass(tp) and hasattr(tp, "schema"):
        name = tp.__name__
        if name in seen_types:
            raise SchemaException(f"recursive dataclass reference: {name}")
        seen_types.add(name)
        return tp.schema()

    raise SchemaException(f"unsupported type: {tp}")


def _to_dict(obj) -> dict:
    """Convert a dataclass instance to a dict for Avro serialization."""
    result = {}
    for f in dataclasses.fields(obj):
        val = getattr(obj, f.name)
        result[f.name] = _convert_value(val)
    return result


def _convert_value(val):
    """Recursively convert values for Avro serialization."""
    if val is None:
        return None
    if dataclasses.is_dataclass(val) and not isinstance(val, type):
        return _to_dict(val)
    if isinstance(val, enum.Enum):
        return val.value
    if isinstance(val, list):
        return [_convert_value(v) for v in val]
    if isinstance(val, dict):
        return {k: _convert_value(v) for k, v in val.items()}
    return val


def _from_dict(cls, record: dict):
    """Construct a dataclass instance from an Avro-deserialized dict."""
    hints = get_type_hints(cls, include_extras=True)
    kwargs = {}
    for f in dataclasses.fields(cls):
        if f.name not in record:
            continue
        val = record[f.name]
        kwargs[f.name] = _reconstruct_value(hints[f.name], val)
    return cls(**kwargs)


def _reconstruct_value(tp, val):
    """Recursively reconstruct typed values from Avro-deserialized data."""
    if val is None:
        return None

    origin = get_origin(tp)

    # Unwrap Annotated
    if origin is Annotated:
        inner = get_args(tp)[0]
        return _reconstruct_value(inner, val)

    # Unwrap Optional
    if _is_union(origin):
        args = get_args(tp)
        non_none = [a for a in args if a is not type(None)]
        if non_none:
            return _reconstruct_value(non_none[0], val)
        return val

    # Nested dataclass
    if dataclasses.is_dataclass(tp) and isinstance(val, dict):
        return _from_dict(tp, val)

    # Enum
    if isinstance(tp, type) and issubclass(tp, enum.Enum):
        return tp(val)

    # list[T]
    if origin is list:
        (item_type,) = get_args(tp)
        return [_reconstruct_value(item_type, v) for v in val]

    # dict[str, T]
    if origin is dict:
        _, val_type = get_args(tp)
        return {k: _reconstruct_value(val_type, v) for k, v in val.items()}

    return val