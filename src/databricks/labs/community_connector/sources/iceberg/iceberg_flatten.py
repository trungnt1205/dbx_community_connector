"""Flatten one Iceberg ``LoadTableResult`` into the connector's row streams.

``GET /v1/{prefix}/namespaces/{ns}/tables/{table}`` returns a single JSON
document that carries a table's whole metadata tree. Four Lakeflow streams are
different projections of that one document:

===========================  ==============================================
Stream                       Source
===========================  ==============================================
``table_schemas``            ``metadata.schemas[].fields[]`` (recursive)
``table_snapshots``          ``metadata.snapshots[]`` + ``refs``
``table_partition_specs``    ``metadata.partition-specs[].fields[]``
``table_properties``         ``metadata.properties``
===========================  ==============================================

Every emitter takes a ``base`` dict holding the provenance columns
(``catalog_prefix`` / ``namespace`` / ``table_name`` / ``table_uuid`` /
``metadata_location``) that the caller has already resolved, and yields plain
JSON-compatible dicts — the framework does the Spark type coercion.

Format-v1 fallbacks are handled here too: v1 metadata carries a single
``schema`` object and a bare ``partition-spec`` field list instead of the v2
``schemas`` / ``partition-specs`` arrays.
"""

from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional, Sequence

from databricks.labs.community_connector.sources.iceberg.iceberg_schemas import (
    iceberg_type_json,
    render_iceberg_type,
)

# ---------------------------------------------------------------------------
# Schema lookups
# ---------------------------------------------------------------------------


def all_schemas(metadata: dict) -> List[dict]:
    """Every schema version on the table, newest last."""
    schemas = metadata.get("schemas")
    if isinstance(schemas, list) and schemas:
        return [s for s in schemas if isinstance(s, dict)]
    # Format v1 carries a single ``schema`` object instead of a list.
    legacy = metadata.get("schema")
    return [legacy] if isinstance(legacy, dict) else []


def current_schema(metadata: dict) -> Optional[dict]:
    """The schema matching ``current-schema-id`` (last one as a fallback)."""
    schemas = all_schemas(metadata)
    if not schemas:
        return None
    current_id = metadata.get("current-schema-id")
    for schema in schemas:
        if schema.get("schema-id") == current_id:
            return schema
    return schemas[-1]


def identifier_field_names(metadata: dict) -> List[str]:
    """Iceberg identifier fields — the closest thing to a primary key.

    ``schema.identifier-field-ids`` is optional: v1 tables and append-only v2
    tables usually omit it, in which case the table has no primary key and can
    only be ingested as ``snapshot`` or ``append``.
    """
    schema = current_schema(metadata)
    if not schema:
        return []
    ids = schema.get("identifier-field-ids") or []
    if not ids:
        return []
    wanted = {int(i) for i in ids if _is_int(i)}
    names: List[str] = []
    for field in schema.get("fields") or []:
        if not isinstance(field, dict):
            continue
        field_id = as_int(field.get("id"), None)
        if field_id in wanted and field.get("name"):
            names.append(str(field["name"]))
    return names


# ---------------------------------------------------------------------------
# Row emitters
# ---------------------------------------------------------------------------


def emit_schema_rows(base: dict, metadata: dict) -> Iterator[dict]:
    """One row per ``(schema_id, field_id)``, including nested struct fields."""
    current_id = metadata.get("current-schema-id")
    format_version = as_int(metadata.get("format-version"), None)

    for schema in all_schemas(metadata):
        schema_id = as_int(schema.get("schema-id"), 0)
        identifier_ids = {
            int(i) for i in (schema.get("identifier-field-ids") or []) if _is_int(i)
        }
        for row in _flatten_fields(schema.get("fields") or [], "", None):
            yield {
                **base,
                "format_version": format_version,
                "schema_id": schema_id,
                "is_current_schema": schema.get("schema-id") == current_id,
                "is_identifier_field": row["field_id"] in identifier_ids,
                **row,
            }


def emit_snapshot_rows(
    base: dict, metadata: dict, *, since_ms: int, until_ms: int
) -> Iterator[dict]:
    """One row per snapshot whose ``timestamp-ms`` falls in ``(since, until]``.

    The REST Catalog has no server-side "snapshots since N" filter — the whole
    document is fetched and the window applied here.
    """
    current_snapshot_id = metadata.get("current-snapshot-id")
    refs_by_snapshot: Dict[int, List[str]] = {}
    for ref_name, ref in (metadata.get("refs") or {}).items():
        if not isinstance(ref, dict):
            continue
        snapshot_id = as_int(ref.get("snapshot-id"), None)
        if snapshot_id is not None:
            refs_by_snapshot.setdefault(snapshot_id, []).append(str(ref_name))

    # ``snapshots[]`` is append-only but not guaranteed sorted; emit
    # oldest-first so the cursor advances monotonically within a partition.
    snapshots = [s for s in (metadata.get("snapshots") or []) if isinstance(s, dict)]
    snapshots.sort(key=lambda s: as_int(s.get("timestamp-ms"), 0) or 0)

    for snapshot in snapshots:
        timestamp_ms = as_int(snapshot.get("timestamp-ms"), None)
        if timestamp_ms is None or timestamp_ms <= since_ms or timestamp_ms > until_ms:
            continue
        snapshot_id = as_int(snapshot.get("snapshot-id"), None)
        if snapshot_id is None:
            continue

        summary = snapshot.get("summary") or {}
        yield {
            **base,
            "snapshot_id": snapshot_id,
            "parent_snapshot_id": as_int(snapshot.get("parent-snapshot-id"), None),
            "sequence_number": as_int(snapshot.get("sequence-number"), None),
            "timestamp_ms": timestamp_ms,
            "manifest_list": as_str(snapshot.get("manifest-list")),
            "schema_id": as_int(snapshot.get("schema-id"), None),
            "operation": as_str(summary.get("operation")),
            "summary": string_map(summary),
            "is_current_snapshot": snapshot_id == as_int(current_snapshot_id, None),
            "ref_names": refs_by_snapshot.get(snapshot_id) or [],
        }


def emit_partition_spec_rows(base: dict, metadata: dict) -> Iterator[dict]:
    """One row per ``(spec_id, field_id)`` partition field."""
    default_spec_id = metadata.get("default-spec-id")
    schema = current_schema(metadata) or {}
    names_by_id = {
        as_int(field.get("id"), -1): as_str(field.get("name"))
        for field in schema.get("fields") or []
        if isinstance(field, dict)
    }

    specs = metadata.get("partition-specs")
    if not isinstance(specs, list) or not specs:
        # Format v1 carries a bare field list under ``partition-spec``.
        legacy = metadata.get("partition-spec")
        specs = [{"spec-id": 0, "fields": legacy}] if isinstance(legacy, list) else []

    for spec in specs:
        if not isinstance(spec, dict):
            continue
        spec_id = as_int(spec.get("spec-id"), 0)
        for field in spec.get("fields") or []:
            if not isinstance(field, dict):
                continue
            source_id = as_int(field.get("source-id"), None)
            yield {
                **base,
                "spec_id": spec_id,
                "is_default_spec": spec.get("spec-id") == default_spec_id,
                "field_id": as_int(field.get("field-id"), 0),
                "source_id": source_id,
                "source_field_name": names_by_id.get(source_id),
                "field_name": as_str(field.get("name")),
                "transform": as_str(field.get("transform")),
            }


def emit_property_rows(base: dict, metadata: dict) -> Iterator[dict]:
    """One row per table property key/value pair."""
    for key, value in (metadata.get("properties") or {}).items():
        yield {**base, "key": str(key), "value": as_str(value)}


# ---------------------------------------------------------------------------
# Field-tree walk
# ---------------------------------------------------------------------------


def _flatten_fields(
    fields: Sequence[Any], parent_path: str, parent_id: Optional[int]
) -> Iterator[dict]:
    """One row per field, recursing through nested struct definitions.

    Iceberg field IDs are unique within a schema (nested fields included), so
    ``(schema_id, field_id)`` stays a valid key at any depth.
    """
    for field in fields:
        if not isinstance(field, dict):
            continue
        name = as_str(field.get("name")) or ""
        field_type = field.get("type")
        path = f"{parent_path}.{name}" if parent_path else name
        field_id = as_int(field.get("id"), 0)

        yield {
            "field_id": field_id,
            "field_name": name,
            "field_path": path,
            "parent_field_id": parent_id,
            "field_type": render_iceberg_type(field_type),
            "field_type_json": iceberg_type_json(field_type),
            "field_required": bool(field.get("required", False)),
            "field_doc": as_str(field.get("doc")),
        }

        yield from _flatten_nested(field_type, path, field_id)


def _flatten_nested(field_type: Any, path: str, parent_id: Optional[int]) -> Iterator[dict]:
    """Descend into struct / list-of-struct / map-of-struct definitions."""
    if not isinstance(field_type, dict):
        return
    kind = field_type.get("type")
    if kind == "struct":
        yield from _flatten_fields(field_type.get("fields") or [], path, parent_id)
    elif kind == "list":
        yield from _flatten_nested(
            field_type.get("element"), f"{path}.element", parent_id
        )
    elif kind == "map":
        yield from _flatten_nested(field_type.get("value"), f"{path}.value", parent_id)


# ---------------------------------------------------------------------------
# Value coercion
# ---------------------------------------------------------------------------


def _is_int(value: Any) -> bool:
    try:
        int(value)
    except (TypeError, ValueError):
        return False
    return True


def as_int(value: Any, default: Optional[int]) -> Optional[int]:
    """Best-effort int coercion; ``default`` on anything non-numeric."""
    if value is None or isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def as_str(value: Any) -> Optional[str]:
    """``None``-preserving str coercion."""
    return None if value is None else str(value)


def string_map(raw: Any) -> Optional[Dict[str, Optional[str]]]:
    """Coerce a free-form property map to ``map<string,string>``."""
    if not isinstance(raw, dict):
        return None
    return {str(k): (None if v is None else str(v)) for k, v in raw.items()}
