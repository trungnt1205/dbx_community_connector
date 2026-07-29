"""Schemas, constants and Iceberg-to-Spark type mapping for the Iceberg connector.

Everything in this module is data — no HTTP, no I/O — so it can be imported
from a Spark executor without pulling in optional dependencies.

The six metadata streams are all derived from the Iceberg REST Catalog's
plain-JSON endpoints (``/v1/config``, ``/v1/{prefix}/namespaces`` and
``/v1/{prefix}/namespaces/{ns}/tables[/{table}]``). Four of them
(``table_schemas``, ``table_snapshots``, ``table_partition_specs``,
``table_properties``) are flattened views over the single ``LoadTableResult``
document, which is why they share the ``namespace`` / ``table_name`` /
``metadata_location`` provenance columns.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

try:  # pragma: no cover - depends on the runtime PySpark version
    from pyspark.sql.types import TimestampNTZType
except ImportError:  # pragma: no cover
    TimestampNTZType = TimestampType  # type: ignore[assignment,misc]


# ---------------------------------------------------------------------------
# Protocol constants
# ---------------------------------------------------------------------------

#: Default multi-level namespace separator mandated by the REST Catalog spec
#: (ASCII unit separator, 0x1F). Percent-encoded as ``%1F`` on the wire.
NAMESPACE_SEPARATOR = "\x1f"

#: Separator used when a multi-level namespace is rendered for humans / Delta.
NAMESPACE_DISPLAY_SEPARATOR = "."

#: Path prefix every REST Catalog route shares.
API_VERSION_PREFIX = "v1"

#: Endpoint that bootstraps ``prefix`` / ``defaults`` / ``overrides``.
CONFIG_PATH = "v1/config"

#: Deprecated-but-ubiquitous OAuth2 client-credentials token endpoint.
OAUTH_TOKENS_PATH = "v1/oauth/tokens"

DEFAULT_OAUTH_SCOPE = "catalog"

#: HTTP statuses worth retrying. 419 ("authentication timeout") is handled
#: separately because it needs a token refresh rather than a plain backoff.
RETRIABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
AUTH_REFRESH_STATUS_CODES = frozenset({401, 419})

MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 30.0
DEFAULT_TIMEOUT = 60

#: ``pageSize`` hint sent on every list endpoint. Servers may cap or ignore it.
DEFAULT_PAGE_SIZE = 100

#: Safety bound on the recursive namespace walk. Iceberg allows arbitrarily
#: deep namespaces but real catalogs rarely exceed three levels; the guard
#: keeps a misbehaving server (one that ignores ``parent``) from looping.
DEFAULT_MAX_NAMESPACE_DEPTH = 5

#: Rows returned per microbatch for the pyiceberg data path.
DEFAULT_MAX_RECORDS_PER_BATCH = 1000

#: Catalog objects (namespaces or tables) crawled per microbatch. This is the
#: real admission-control knob for the metadata streams: each object costs one
#: HTTP round trip and yields a small number of rows.
DEFAULT_MAX_OBJECTS_PER_BATCH = 50

#: Used only to translate a user-supplied ``max_records_per_batch`` into an
#: object count when ``max_objects_per_batch`` is not set. Deliberately coarse
#: — the metadata streams are sized by object count, not row count.
ESTIMATED_ROWS_PER_OBJECT = 20


# ---------------------------------------------------------------------------
# Stream names
# ---------------------------------------------------------------------------

NAMESPACES = "namespaces"
TABLES = "tables"
TABLE_SCHEMAS_STREAM = "table_schemas"
TABLE_SNAPSHOTS = "table_snapshots"
TABLE_PARTITION_SPECS = "table_partition_specs"
TABLE_PROPERTIES = "table_properties"

#: Streams derived from one ``LoadTableResult`` document per table.
LOAD_TABLE_STREAMS: Tuple[str, ...] = (
    TABLE_SCHEMAS_STREAM,
    TABLE_SNAPSHOTS,
    TABLE_PARTITION_SPECS,
    TABLE_PROPERTIES,
)

METADATA_TABLES: Tuple[str, ...] = (NAMESPACES, TABLES) + LOAD_TABLE_STREAMS


# ---------------------------------------------------------------------------
# Spark schemas
# ---------------------------------------------------------------------------

_PROPERTIES = MapType(StringType(), StringType())

NAMESPACES_SCHEMA = StructType(
    [
        StructField("catalog_prefix", StringType(), True),
        StructField("namespace", StringType(), False),
        StructField("levels", ArrayType(StringType()), True),
        StructField("parent", StringType(), True),
        StructField("depth", LongType(), True),
        StructField("location", StringType(), True),
        StructField("owner", StringType(), True),
        StructField("properties", _PROPERTIES, True),
    ]
)

TABLES_SCHEMA = StructType(
    [
        StructField("catalog_prefix", StringType(), True),
        StructField("namespace", StringType(), False),
        StructField("name", StringType(), False),
        StructField("identifier", StringType(), True),
    ]
)

TABLE_SCHEMAS_SCHEMA = StructType(
    [
        StructField("catalog_prefix", StringType(), True),
        StructField("namespace", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("table_uuid", StringType(), True),
        StructField("metadata_location", StringType(), True),
        StructField("format_version", LongType(), True),
        StructField("schema_id", LongType(), False),
        StructField("is_current_schema", BooleanType(), True),
        StructField("field_id", LongType(), False),
        StructField("field_name", StringType(), True),
        StructField("field_path", StringType(), True),
        StructField("parent_field_id", LongType(), True),
        StructField("field_type", StringType(), True),
        StructField("field_type_json", StringType(), True),
        StructField("field_required", BooleanType(), True),
        StructField("field_doc", StringType(), True),
        StructField("is_identifier_field", BooleanType(), True),
    ]
)

TABLE_SNAPSHOTS_SCHEMA = StructType(
    [
        StructField("catalog_prefix", StringType(), True),
        StructField("namespace", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("table_uuid", StringType(), True),
        StructField("metadata_location", StringType(), True),
        StructField("snapshot_id", LongType(), False),
        StructField("parent_snapshot_id", LongType(), True),
        StructField("sequence_number", LongType(), True),
        StructField("timestamp_ms", LongType(), False),
        StructField("manifest_list", StringType(), True),
        StructField("schema_id", LongType(), True),
        StructField("operation", StringType(), True),
        StructField("summary", _PROPERTIES, True),
        StructField("is_current_snapshot", BooleanType(), True),
        StructField("ref_names", ArrayType(StringType()), True),
    ]
)

TABLE_PARTITION_SPECS_SCHEMA = StructType(
    [
        StructField("catalog_prefix", StringType(), True),
        StructField("namespace", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("table_uuid", StringType(), True),
        StructField("metadata_location", StringType(), True),
        StructField("spec_id", LongType(), False),
        StructField("is_default_spec", BooleanType(), True),
        StructField("field_id", LongType(), False),
        StructField("source_id", LongType(), True),
        StructField("source_field_name", StringType(), True),
        StructField("field_name", StringType(), True),
        StructField("transform", StringType(), True),
    ]
)

TABLE_PROPERTIES_SCHEMA = StructType(
    [
        StructField("catalog_prefix", StringType(), True),
        StructField("namespace", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("table_uuid", StringType(), True),
        StructField("metadata_location", StringType(), True),
        StructField("key", StringType(), False),
        StructField("value", StringType(), True),
    ]
)

TABLE_SCHEMAS: Dict[str, StructType] = {
    NAMESPACES: NAMESPACES_SCHEMA,
    TABLES: TABLES_SCHEMA,
    TABLE_SCHEMAS_STREAM: TABLE_SCHEMAS_SCHEMA,
    TABLE_SNAPSHOTS: TABLE_SNAPSHOTS_SCHEMA,
    TABLE_PARTITION_SPECS: TABLE_PARTITION_SPECS_SCHEMA,
    TABLE_PROPERTIES: TABLE_PROPERTIES_SCHEMA,
}


# ---------------------------------------------------------------------------
# Ingestion metadata
# ---------------------------------------------------------------------------
#
# Only ``table_snapshots`` is incremental: ``metadata.snapshots[]`` is an
# append-only log. Everything else is re-derived in full on each run because
# the REST Catalog exposes no change cursor for namespaces, table lists,
# schemas, partition specs or properties.
#
# ``timestamp_ms`` is preferred over ``sequence_number`` as the cursor: it is
# globally comparable across tables (sequence numbers restart per table) and
# it can be capped at the connector's init time, which is what guarantees
# Trigger.AvailableNow termination.

TABLE_METADATA: Dict[str, dict] = {
    NAMESPACES: {
        "primary_keys": ["namespace"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLES: {
        "primary_keys": ["namespace", "name"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_SCHEMAS_STREAM: {
        "primary_keys": ["namespace", "table_name", "schema_id", "field_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_SNAPSHOTS: {
        "primary_keys": ["namespace", "table_name", "snapshot_id"],
        "cursor_field": "timestamp_ms",
        "ingestion_type": "append",
    },
    TABLE_PARTITION_SPECS: {
        "primary_keys": ["namespace", "table_name", "spec_id", "field_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    TABLE_PROPERTIES: {
        "primary_keys": ["namespace", "table_name", "key"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
}


# ---------------------------------------------------------------------------
# Iceberg type -> Spark type
# ---------------------------------------------------------------------------

_PRIMITIVE_TYPES: Dict[str, DataType] = {
    "boolean": BooleanType(),
    "int": IntegerType(),
    "long": LongType(),
    "float": FloatType(),
    "double": DoubleType(),
    "date": DateType(),
    # Iceberg ``time`` is a microsecond time-of-day; Spark has no TimeType.
    "time": StringType(),
    "timestamp": TimestampNTZType(),
    "timestamptz": TimestampType(),
    # v3 nanosecond variants — lossy when narrowed to Spark's microsecond
    # timestamp internals. Documented in the connector README.
    "timestamp_ns": TimestampNTZType(),
    "timestamptz_ns": TimestampType(),
    "string": StringType(),
    "uuid": StringType(),
    "binary": BinaryType(),
    "unknown": StringType(),
}

_DECIMAL_RE = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$")
_FIXED_RE = re.compile(r"^fixed\s*[\(\[]\s*(\d+)\s*[\)\]]$")


def iceberg_type_to_spark(iceberg_type: Any) -> DataType:
    """Map an Iceberg schema type to its Spark equivalent.

    ``iceberg_type`` is either a primitive type string (``"long"``,
    ``"decimal(18,2)"``, ``"fixed(16)"``) or a nested type object
    (``{"type": "struct"|"list"|"map", ...}``). Unknown types degrade to
    ``StringType`` rather than raising, so one exotic column never blocks a
    whole table.
    """
    if isinstance(iceberg_type, str):
        primitive = _PRIMITIVE_TYPES.get(iceberg_type)
        if primitive is not None:
            return primitive
        decimal_match = _DECIMAL_RE.match(iceberg_type)
        if decimal_match:
            return DecimalType(
                int(decimal_match.group(1)), int(decimal_match.group(2))
            )
        if _FIXED_RE.match(iceberg_type):
            return BinaryType()
        return StringType()

    if isinstance(iceberg_type, dict):
        kind = iceberg_type.get("type")
        if kind == "struct":
            return StructType(
                [
                    StructField(
                        str(field.get("name")),
                        iceberg_type_to_spark(field.get("type")),
                        not bool(field.get("required", False)),
                    )
                    for field in iceberg_type.get("fields") or []
                ]
            )
        if kind == "list":
            return ArrayType(
                iceberg_type_to_spark(iceberg_type.get("element")),
                not bool(iceberg_type.get("element-required", False)),
            )
        if kind == "map":
            return MapType(
                iceberg_type_to_spark(iceberg_type.get("key")),
                iceberg_type_to_spark(iceberg_type.get("value")),
                not bool(iceberg_type.get("value-required", False)),
            )

    return StringType()


def render_iceberg_type(iceberg_type: Any) -> str:
    """Short human-readable name of an Iceberg type (``long``, ``struct``…)."""
    if isinstance(iceberg_type, str):
        return iceberg_type
    if isinstance(iceberg_type, dict):
        return str(iceberg_type.get("type") or "unknown")
    return "unknown"


def iceberg_type_json(iceberg_type: Any) -> Optional[str]:
    """Full JSON rendering of a nested Iceberg type; ``None`` for primitives."""
    if isinstance(iceberg_type, (dict, list)):
        return json.dumps(iceberg_type, ensure_ascii=False, sort_keys=True)
    return None


def spark_schema_from_iceberg_schema(schema: dict) -> StructType:
    """Build a ``StructType`` from one Iceberg ``Schema`` object."""
    fields: List[StructField] = []
    for field in schema.get("fields") or []:
        fields.append(
            StructField(
                str(field.get("name")),
                iceberg_type_to_spark(field.get("type")),
                # Iceberg ``required: true`` -> Spark non-nullable.
                not bool(field.get("required", False)),
                metadata={"iceberg.field-id": int(field.get("id", 0) or 0)},
            )
        )
    return StructType(fields)
