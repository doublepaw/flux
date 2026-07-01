"""Tests for @schema decorator and schema generation."""

import enum
from dataclasses import dataclass
from typing import Annotated, Optional

import pytest

from flux._schema import schema, Int32, Float32, NonNull
from flux.exceptions import SchemaException


# ============ Primitive Type Mapping (nullable by default) ============


@schema
@dataclass
class Primitives:
    s: str
    i: int
    f: float
    b: bool
    raw: bytes


def test_str_maps_to_nullable_string():
    field = _field_by_name(Primitives.schema(), "s")
    assert field["type"] == ["null", "string"]
    assert field["default"] is None


def test_int_maps_to_nullable_long():
    field = _field_by_name(Primitives.schema(), "i")
    assert field["type"] == ["null", "long"]
    assert field["default"] is None


def test_float_maps_to_nullable_double():
    field = _field_by_name(Primitives.schema(), "f")
    assert field["type"] == ["null", "double"]
    assert field["default"] is None


def test_bool_maps_to_nullable_boolean():
    field = _field_by_name(Primitives.schema(), "b")
    assert field["type"] == ["null", "boolean"]
    assert field["default"] is None


def test_bytes_maps_to_nullable_bytes():
    field = _field_by_name(Primitives.schema(), "raw")
    assert field["type"] == ["null", "bytes"]
    assert field["default"] is None


# ============ NonNull ============


@schema
@dataclass
class WithNonNull:
    required: Annotated[str, NonNull]
    also_required: Annotated[int, NonNull]
    nullable: str


def test_nonnull_produces_bare_type():
    field = _field_by_name(WithNonNull.schema(), "required")
    assert field["type"] == "string"
    assert "default" not in field


def test_nonnull_int_produces_bare_long():
    field = _field_by_name(WithNonNull.schema(), "also_required")
    assert field["type"] == "long"
    assert "default" not in field


def test_unannotated_field_still_nullable():
    field = _field_by_name(WithNonNull.schema(), "nullable")
    assert field["type"] == ["null", "string"]
    assert field["default"] is None


# ============ Optional / Nullable ============


@schema
@dataclass
class WithOptional:
    required: Annotated[str, NonNull]
    optional_typing: Optional[str] = None
    optional_union: str | None = None


def test_optional_maps_to_null_union():
    field = _field_by_name(WithOptional.schema(), "optional_typing")
    assert field["type"] == ["null", "string"]
    assert field["default"] is None


def test_union_none_maps_to_null_union():
    field = _field_by_name(WithOptional.schema(), "optional_union")
    assert field["type"] == ["null", "string"]
    assert field["default"] is None


# ============ Collections (nullable by default) ============


@schema
@dataclass
class WithCollections:
    tags: list[str]
    counts: dict[str, int]


def test_list_maps_to_nullable_array():
    field = _field_by_name(WithCollections.schema(), "tags")
    assert field["type"] == ["null", {"type": "array", "items": "string"}]
    assert field["default"] is None


def test_dict_maps_to_nullable_map():
    field = _field_by_name(WithCollections.schema(), "counts")
    assert field["type"] == ["null", {"type": "map", "values": "long"}]
    assert field["default"] is None


def test_dict_non_str_key_raises():
    with pytest.raises(SchemaException, match="string keys"):
        @schema
        @dataclass
        class Bad:
            m: dict[int, str]


# ============ Enum (nullable by default) ============


class Status(enum.Enum):
    PENDING = "PENDING"
    DONE = "DONE"


@schema
@dataclass
class WithEnum:
    status: Status


def test_enum_maps_to_nullable_avro_enum():
    field = _field_by_name(WithEnum.schema(), "status")
    assert field["type"][0] == "null"
    assert field["type"][1]["type"] == "enum"
    assert field["type"][1]["symbols"] == ["PENDING", "DONE"]


def test_non_string_enum_raises():
    class IntEnum(enum.Enum):
        A = 1

    with pytest.raises(SchemaException, match="string values"):
        @schema
        @dataclass
        class Bad:
            e: IntEnum


# ============ Nested Dataclass (nullable by default) ============


@schema
@dataclass
class Address:
    city: str
    zip_code: str


@schema
@dataclass
class Person:
    name: str
    address: Address


def test_nested_dataclass_inline_record():
    field = _field_by_name(Person.schema(), "address")
    assert field["type"][0] == "null"
    assert field["type"][1]["type"] == "record"
    assert field["type"][1]["name"] == "Address"


# ============ Type Overrides ============


@schema
@dataclass
class WithOverrides:
    small_int: Annotated[int, Int32]
    small_float: Annotated[float, Float32]
    normal_int: int


def test_int32_override():
    field = _field_by_name(WithOverrides.schema(), "small_int")
    assert field["type"] == ["null", "int"]


def test_float32_override():
    field = _field_by_name(WithOverrides.schema(), "small_float")
    assert field["type"] == ["null", "float"]


def test_normal_int_still_nullable_long():
    field = _field_by_name(WithOverrides.schema(), "normal_int")
    assert field["type"] == ["null", "long"]


# ============ NonNull + Type Overrides ============


@schema
@dataclass
class WithNonNullOverrides:
    required_int32: Annotated[int, NonNull, Int32]
    nullable_int32: Annotated[int, Int32]


def test_nonnull_int32_bare():
    field = _field_by_name(WithNonNullOverrides.schema(), "required_int32")
    assert field["type"] == "int"
    assert "default" not in field


def test_nullable_int32_wrapped():
    field = _field_by_name(WithNonNullOverrides.schema(), "nullable_int32")
    assert field["type"] == ["null", "int"]
    assert field["default"] is None


# ============ Schema Evolution (renames, deletions) ============


@schema(
    namespace="com.example",
    renames={"old_name": "new_name"},
    deletions=["removed_field"],
)
@dataclass
class Evolved:
    id: str
    new_name: int


def test_renames_add_aliases():
    field = _field_by_name(Evolved.schema(), "new_name")
    assert field["aliases"] == ["old_name"]


def test_flux_renames_in_schema():
    s = Evolved.schema()
    assert s["flux.renames"] == {"old_name": "new_name"}


def test_flux_deletions_in_schema():
    s = Evolved.schema()
    assert s["flux.deletions"] == ["removed_field"]


def test_namespace_in_schema():
    assert Evolved.schema()["namespace"] == "com.example"


# ============ Serialization Roundtrip ============


def test_roundtrip_primitives():
    obj = Primitives(s="hello", i=42, f=3.14, b=True, raw=b"\x01\x02")
    data = obj.to_bytes()
    restored = Primitives.from_bytes(data)
    assert restored.s == "hello"
    assert restored.i == 42
    assert abs(restored.f - 3.14) < 1e-10
    assert restored.b is True
    assert restored.raw == b"\x01\x02"


def test_roundtrip_optional_present():
    obj = WithOptional(required="yes", optional_typing="val", optional_union="val2")
    restored = WithOptional.from_bytes(obj.to_bytes())
    assert restored.optional_typing == "val"
    assert restored.optional_union == "val2"


def test_roundtrip_optional_none():
    obj = WithOptional(required="yes")
    restored = WithOptional.from_bytes(obj.to_bytes())
    assert restored.optional_typing is None
    assert restored.optional_union is None


def test_roundtrip_collections():
    obj = WithCollections(tags=["a", "b"], counts={"x": 1, "y": 2})
    restored = WithCollections.from_bytes(obj.to_bytes())
    assert restored.tags == ["a", "b"]
    assert restored.counts == {"x": 1, "y": 2}


def test_roundtrip_enum():
    obj = WithEnum(status=Status.DONE)
    restored = WithEnum.from_bytes(obj.to_bytes())
    assert restored.status is Status.DONE


def test_roundtrip_nested():
    obj = Person(name="Alice", address=Address(city="NYC", zip_code="10001"))
    restored = Person.from_bytes(obj.to_bytes())
    assert restored.name == "Alice"
    assert restored.address.city == "NYC"
    assert restored.address.zip_code == "10001"


def test_roundtrip_overrides():
    obj = WithOverrides(small_int=7, small_float=1.5, normal_int=100)
    restored = WithOverrides.from_bytes(obj.to_bytes())
    assert restored.small_int == 7
    assert abs(restored.small_float - 1.5) < 1e-6
    assert restored.normal_int == 100


def test_roundtrip_nonnull():
    obj = WithNonNull(required="hello", also_required=42, nullable="world")
    restored = WithNonNull.from_bytes(obj.to_bytes())
    assert restored.required == "hello"
    assert restored.also_required == 42
    assert restored.nullable == "world"


# ============ Error Cases ============


def test_non_dataclass_raises():
    with pytest.raises(SchemaException, match="must be a @dataclass"):
        @schema
        class NotADataclass:
            x: int


def test_unsupported_type_raises():
    with pytest.raises(SchemaException, match="unsupported type"):
        @schema
        @dataclass
        class Bad:
            x: set


def test_rename_target_missing_raises():
    with pytest.raises(SchemaException, match="not a field"):
        @schema(renames={"old": "nonexistent"})
        @dataclass
        class Bad:
            x: str


def test_deletion_still_exists_raises():
    with pytest.raises(SchemaException, match="still exists"):
        @schema(deletions=["x"])
        @dataclass
        class Bad:
            x: str


def test_rename_and_deletion_overlap_raises():
    with pytest.raises(SchemaException, match="both renames and deletions"):
        @schema(renames={"old": "new_field"}, deletions=["old"])
        @dataclass
        class Bad:
            new_field: str


# ============ schema_json ============


def test_schema_json_is_valid_json():
    import json
    parsed = json.loads(Primitives.schema_json())
    assert parsed["type"] == "record"
    assert parsed["name"] == "Primitives"


# ============ Helpers ============


def _field_by_name(schema: dict, name: str) -> dict:
    for f in schema["fields"]:
        if f["name"] == name:
            return f
    raise KeyError(f"field {name!r} not found in schema")
