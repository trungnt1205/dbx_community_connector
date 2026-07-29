"""Apache Iceberg REST Catalog connector.

Scope is the **open REST Catalog spec** (``apache/iceberg`` /
``open-api/rest-catalog-open-api.yaml``) — any server implementing it works
(Unity Catalog's Iceberg REST endpoint, Polaris, Nessie, Lakekeeper, Tabular,
the reference ``iceberg-rest-fixture``, …). Glue's native API and the Hive
Metastore Thrift API are explicitly out of scope.

Two read paths
--------------
1. **Catalog metadata** (six streams) — plain ``requests`` GETs against the
   REST Catalog. Because they go through ``requests``, the repo's
   ``source_simulator`` can serve them with no credentials and no network.
2. **Table row data** (opt-in, one stream per configured table) — delegated to
   ``pyiceberg``'s ``RestCatalog.load_table()`` +
   ``table.scan(...).to_arrow_batch_reader()``. ``pyiceberg`` is an *optional*
   dependency: the module imports and every metadata stream works without it.

Partitioning
------------
Implements ``SupportsPartitionedStream``. The natural unit of parallelism is a
catalog object:

* ``tables`` — one partition per namespace (one ``GET .../tables`` each).
* ``table_schemas`` / ``table_snapshots`` / ``table_partition_specs`` /
  ``table_properties`` — one partition per table (one ``GET .../tables/{t}``
  each, whose single ``LoadTableResult`` document is then flattened four
  different ways).
* ``namespaces`` — a single partition; the recursive ``parent`` walk is
  inherently sequential.
* configured data tables — ``is_partitioned`` returns ``False`` so streaming
  falls back to the single-driver ``read_table`` path (pyiceberg plans scans
  locally and the Arrow batch reader is not splittable without re-planning).

Offsets
-------
Metadata offsets are ``{"since_ms": int, "cursor": int, "object_offset": int}``:

* ``cursor`` is the run's init timestamp (ms). ``latest_offset`` never returns
  anything newer, so ``Trigger.AvailableNow`` always converges.
* ``object_offset`` is how far through the object list this run has crawled —
  this is the admission-control knob (``max_objects_per_batch``).
* ``since_ms`` is the previous run's ``cursor`` and bounds the
  ``table_snapshots`` append filter. The REST spec has **no** server-side
  "snapshots since N" filter, so the whole ``LoadTableResult`` is fetched and
  the snapshot list is filtered client-side.

Known gaps
----------
* ``cdc_with_deletes`` is not supported. Iceberg v2 positional/equality delete
  files are applied transparently by a scan; the REST spec and pyiceberg expose
  no "rows deleted since snapshot X" feed, so reconstructing deletes would mean
  diffing two full snapshots.
* ``views``, ``table_sort_orders``, ``table_refs`` and the remote
  scan-planning API (``POST .../plan``) are deferred.
* ``timestamp_ns`` / ``timestamptz_ns`` (v3) narrow to Spark's microsecond
  timestamps — a lossy mapping.
"""

from __future__ import annotations

import datetime as _dt
import re
import uuid as _uuid
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface.lakeflow_connect import (
    LakeflowConnect,
)
from databricks.labs.community_connector.interface.supports_partition import (
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.iceberg.iceberg_flatten import (
    as_int,
    as_str,
    current_schema,
    emit_partition_spec_rows,
    emit_property_rows,
    emit_schema_rows,
    emit_snapshot_rows,
    identifier_field_names,
    string_map,
)
from databricks.labs.community_connector.sources.iceberg.iceberg_rest import (
    IcebergRestError,
    RestCatalogClient,
)
from databricks.labs.community_connector.sources.iceberg.iceberg_schemas import (
    DEFAULT_MAX_NAMESPACE_DEPTH,
    DEFAULT_MAX_OBJECTS_PER_BATCH,
    DEFAULT_MAX_RECORDS_PER_BATCH,
    ESTIMATED_ROWS_PER_OBJECT,
    METADATA_TABLES,
    NAMESPACE_DISPLAY_SEPARATOR,
    NAMESPACE_SEPARATOR,
    NAMESPACES,
    TABLE_METADATA,
    TABLE_PARTITION_SPECS,
    TABLE_PROPERTIES,
    TABLE_SCHEMAS,
    TABLE_SCHEMAS_STREAM,
    TABLE_SNAPSHOTS,
    TABLES,
    spark_schema_from_iceberg_schema,
)

#: Property-key prefixes handed straight through to ``pyiceberg``'s
#: ``RestCatalog`` (storage credentials, SigV4 signing, FileIO tuning …).
_PYICEBERG_PASSTHROUGH_PREFIXES: Tuple[str, ...] = (
    "s3.",
    "gcs.",
    "adls.",
    "adlfs.",
    "azure.",
    "hdfs.",
    "rest.",
    "client.",
    "header.",
    "py-io-impl",
)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")
_COLUMN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")

_FALSEY = ("0", "false", "no", "off")

#: Sentinel stored in ``row_offset`` once a snapshot has been fully drained.
_DRAINED = -1


class IcebergLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect implementation for the Iceberg REST Catalog.

    Connection options
    ------------------
    uri                  REST Catalog root, e.g. ``https://host/iceberg`` (required)
    warehouse            Warehouse name/URI, sent as ``?warehouse=`` on ``/v1/config``
    prefix               Catalog prefix; overridden by ``/v1/config`` ``overrides``
    token                Static bearer token
    credential           ``<client_id>:<client_secret>`` for OAuth2 client credentials
    client_id            Alternative to ``credential`` (with ``client_secret``)
    client_secret        Alternative to ``credential`` (with ``client_id``)
    scope                OAuth2 scope (default ``catalog``)
    oauth2_server_uri    External IdP token endpoint (default ``{uri}/v1/oauth/tokens``)
    vended_credentials   ``true`` to request ``X-Iceberg-Access-Delegation``
    page_size            ``pageSize`` hint on list endpoints (default 100)
    timeout              Per-request timeout in seconds (default 60)
    catalog_name         Logical catalog name handed to pyiceberg (default ``iceberg``)
    data_tables          Comma-separated row-data tables, ``ns.table`` or
                         ``alias=ns.table``. Empty by default — the connector
                         exposes only the metadata streams unless configured.
    rest.sigv4-enabled / rest.signing-region / rest.signing-name / ``s3.*`` …
                         Passed straight through to pyiceberg for the data path.

    Table options
    -------------
    max_objects_per_batch          Catalog objects crawled per microbatch (default 50)
    max_records_per_batch          Rows per microbatch on the data path (default 1000);
                                   also used to derive ``max_objects_per_batch``
    max_namespace_depth            Recursion guard for the namespace walk (default 5)
    include_namespace_properties   ``false`` skips the per-namespace ``GET`` (default true)
    incremental_column             Data tables only — cursor column; absent means full scan
    row_filter                     Data tables only — pyiceberg row-filter expression
    selected_fields                Data tables only — comma-separated column projection
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        options = options or {}

        self._client = RestCatalogClient(options)
        self._catalog_name = options.get("catalog_name") or "iceberg"
        self._data_tables = _parse_data_tables(options.get("data_tables"))

        # Cap every offset at construction time so a single
        # Trigger.AvailableNow run cannot chase snapshots committed while it is
        # still catching up. The next trigger builds a fresh instance with a
        # newer cap and picks them up.
        self._init_ms = int(
            _dt.datetime.now(_dt.timezone.utc).timestamp() * 1000
        )

        # Per-instance crawl caches; a namespace/table listing is stable for
        # the lifetime of one run and is re-walked by several streams.
        self._namespace_cache: Optional[List[List[str]]] = None
        self._identifier_cache: Optional[List[Tuple[List[str], str]]] = None
        self._unit_cache: Dict[str, List[dict]] = {}

    # ------------------------------------------------------------------
    # LakeflowConnect
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        """Six metadata streams plus any configured row-data tables.

        Deliberately does no I/O: the catalog contents are exposed *inside*
        the ``namespaces`` / ``tables`` streams, not as Lakeflow objects.
        """
        return list(METADATA_TABLES) + sorted(self._data_tables)

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        self._validate_table(table_name)
        if table_name in TABLE_SCHEMAS:
            return TABLE_SCHEMAS[table_name]

        levels, name = _split_identifier(self._data_tables[table_name])
        metadata = self._load_table_metadata(levels, name)
        schema = current_schema(metadata)
        if schema is None:
            raise RuntimeError(
                f"Iceberg table '{self._data_tables[table_name]}' returned no "
                "schema in its LoadTableResult"
            )
        return spark_schema_from_iceberg_schema(schema)

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        self._validate_table(table_name)
        if table_name in TABLE_METADATA:
            return dict(TABLE_METADATA[table_name])

        table_options = table_options or {}
        levels, name = _split_identifier(self._data_tables[table_name])
        metadata = self._load_table_metadata(levels, name)
        primary_keys = identifier_field_names(metadata)

        incremental_column = table_options.get("incremental_column")
        if incremental_column:
            _validate_column(incremental_column)
            # Identifier fields are Iceberg's closest analogue to a primary
            # key; with them an upsert is safe (cdc), without them the only
            # honest option is append.
            return {
                "primary_keys": primary_keys,
                "cursor_field": incremental_column,
                "ingestion_type": "cdc" if primary_keys else "append",
            }
        return {
            "primary_keys": primary_keys,
            "cursor_field": None,
            "ingestion_type": "snapshot",
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Single-driver read. Used for data tables and as the batch fallback."""
        self._validate_table(table_name)
        table_options = table_options or {}

        if table_name in self._data_tables:
            return self._read_data_table(table_name, start_offset or {}, table_options)

        start = dict(start_offset or {})
        end = self.latest_offset(table_name, table_options, start)
        if start == end:
            return iter([]), start

        partitions = self.get_partitions(table_name, table_options, start, end)

        def _records() -> Iterator[dict]:
            for partition in partitions:
                yield from self.read_partition(table_name, partition, table_options)

        return _records(), end

    # ------------------------------------------------------------------
    # SupportsPartitionedStream
    # ------------------------------------------------------------------

    def is_partitioned(self, table_name: str) -> bool:
        """Metadata streams parallelise per catalog object; data tables don't."""
        return table_name in METADATA_TABLES

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """Advance the crawl cursor by at most ``max_objects_per_batch``.

        Metadata-only: the object list is discovered with cheap list calls and
        no ``LoadTableResult`` document is fetched here. The returned ``cursor``
        is pinned to ``self._init_ms``, so once the crawl reaches the end of
        the object list this method returns ``start_offset`` unchanged and the
        trigger terminates.
        """
        self._validate_table(table_name)
        table_options = table_options or {}

        if table_name in self._data_tables:
            # Never consulted (is_partitioned is False) but keep it total.
            return dict(start_offset or {})

        total = len(self._units(table_name, table_options))
        previous = dict(start_offset or {})
        start_index, since_ms = self._resume_point(previous)

        if self._is_same_run(previous) and start_index >= total:
            return previous

        step = self._objects_per_batch(table_options)
        return {
            "since_ms": since_ms,
            "cursor": self._init_ms,
            "object_offset": min(total, start_index + step),
        }

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> Sequence[dict]:
        """Slice the object list into one descriptor per catalog object."""
        self._validate_table(table_name)
        table_options = table_options or {}

        if table_name in self._data_tables:
            # One descriptor covering the whole table; batch reads land in
            # read_partition, streaming reads never get here.
            return [{"kind": "data"}]

        if (
            start_offset is not None
            and end_offset is not None
            and start_offset == end_offset
        ):
            return []

        units = self._units(table_name, table_options)
        total = len(units)

        previous = dict(start_offset or {})
        start_index, since_ms = self._resume_point(previous)
        if end_offset is None:
            end_index = total
            until_ms = self._init_ms
        else:
            end_index = int(end_offset.get("object_offset") or 0)
            until_ms = int(end_offset.get("cursor") or self._init_ms)
        end_index = min(end_index, total)

        if start_index >= end_index:
            return []

        partitions: List[dict] = []
        for unit in units[start_index:end_index]:
            descriptor = dict(unit)
            if table_name == TABLE_SNAPSHOTS:
                # The catalog cannot filter snapshots server-side, so the
                # window travels with the partition and is applied after the
                # LoadTableResult comes back.
                descriptor["since_ms"] = since_ms
                descriptor["until_ms"] = until_ms
            partitions.append(descriptor)
        return partitions

    def read_partition(
        self, table_name: str, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        """Read one catalog object. Runs on an executor — no driver state."""
        self._validate_table(table_name)
        table_options = table_options or {}
        partition = partition or {}

        if table_name in self._data_tables:
            records, _ = self._read_data_table(table_name, {}, table_options)
            yield from records
            return

        kind = partition.get("kind")
        if kind == "namespaces":
            yield from self._emit_namespaces(table_options)
        elif kind == "namespace":
            yield from self._emit_tables(_decode_levels(partition["namespace"]))
        elif kind == "table":
            levels = _decode_levels(partition["namespace"])
            name = str(partition["table"])
            result = self._client.load_table(levels, name)
            yield from self._emit_load_table_rows(
                table_name, levels, name, result, partition
            )
        else:  # pragma: no cover - defensive
            raise ValueError(
                f"Unrecognised partition descriptor for '{table_name}': {partition!r}"
            )

    # ------------------------------------------------------------------
    # Offset helpers
    # ------------------------------------------------------------------

    def _is_same_run(self, previous: dict) -> bool:
        """True when ``previous`` was produced by *this* connector instance."""
        cursor = previous.get("cursor")
        if cursor is None:
            return False
        try:
            return int(cursor) == self._init_ms
        except (TypeError, ValueError):
            return False

    def _resume_point(self, previous: dict) -> Tuple[int, int]:
        """Return ``(object_index, since_ms)`` implied by ``previous``.

        Mid-run the crawl resumes where it stopped and keeps the previous
        run's ``since_ms``. Across runs the crawl restarts from object 0 and
        the previous run's ``cursor`` becomes the new lower bound.
        """
        if self._is_same_run(previous):
            return (
                as_int(previous.get("object_offset"), 0),
                as_int(previous.get("since_ms"), 0),
            )
        return 0, as_int(previous.get("cursor"), 0)

    def _objects_per_batch(self, table_options: dict[str, str]) -> int:
        explicit = table_options.get("max_objects_per_batch")
        if explicit:
            return max(1, as_int(explicit, DEFAULT_MAX_OBJECTS_PER_BATCH))
        # Metadata streams are sized in objects, not rows; translate a
        # row-oriented cap with a deliberately coarse per-object estimate.
        rows = table_options.get("max_records_per_batch")
        if rows:
            return max(1, as_int(rows, DEFAULT_MAX_RECORDS_PER_BATCH) // ESTIMATED_ROWS_PER_OBJECT)
        return DEFAULT_MAX_OBJECTS_PER_BATCH

    # ------------------------------------------------------------------
    # Catalog crawl
    # ------------------------------------------------------------------

    def _units(self, table_name: str, table_options: dict[str, str]) -> List[dict]:
        """The ordered list of catalog objects a stream must visit."""
        cached = self._unit_cache.get(table_name)
        if cached is not None:
            return cached

        if table_name == NAMESPACES:
            units = [{"kind": "namespaces"}]
        elif table_name == TABLES:
            units = [
                {"kind": "namespace", "namespace": _encode_levels(levels)}
                for levels in self._walk_namespaces(table_options)
            ]
        else:
            units = [
                {
                    "kind": "table",
                    "namespace": _encode_levels(levels),
                    "table": name,
                }
                for levels, name in self._table_identifiers(table_options)
            ]

        self._unit_cache[table_name] = units
        return units

    def _walk_namespaces(self, table_options: dict[str, str]) -> List[List[str]]:
        """Breadth-first walk of the namespace tree via the ``parent`` param."""
        if self._namespace_cache is not None:
            return self._namespace_cache

        max_depth = max(
            1, as_int(table_options.get("max_namespace_depth"), DEFAULT_MAX_NAMESPACE_DEPTH)
        )
        discovered: List[List[str]] = []
        seen: set = set()
        queue: List[Optional[List[str]]] = [None]

        while queue:
            parent = queue.pop(0)
            try:
                children = list(self._client.list_namespaces(parent))
            except IcebergRestError as exc:
                if exc.status_code in (403, 404):
                    # A namespace we may not read, or one removed mid-crawl.
                    continue
                raise
            for levels in children:
                key = tuple(levels)
                # A server that ignores ``parent`` would otherwise re-queue the
                # same namespaces forever.
                if not levels or key in seen:
                    continue
                seen.add(key)
                discovered.append(list(levels))
                if len(levels) < max_depth:
                    queue.append(list(levels))

        discovered.sort()
        self._namespace_cache = discovered
        return discovered

    def _table_identifiers(
        self, table_options: dict[str, str]
    ) -> List[Tuple[List[str], str]]:
        """Every ``(namespace_levels, table_name)`` pair in the catalog."""
        if self._identifier_cache is not None:
            return self._identifier_cache

        identifiers: List[Tuple[List[str], str]] = []
        for levels in self._walk_namespaces(table_options):
            try:
                for identifier in self._client.list_table_identifiers(levels):
                    name = identifier.get("name")
                    if name:
                        identifiers.append((list(levels), str(name)))
            except IcebergRestError as exc:
                if exc.status_code in (403, 404):
                    continue
                raise

        identifiers.sort(key=lambda item: (item[0], item[1]))
        self._identifier_cache = identifiers
        return identifiers

    # ------------------------------------------------------------------
    # Row emitters
    # ------------------------------------------------------------------

    def _emit_namespaces(self, table_options: dict[str, str]) -> Iterator[dict]:
        include_properties = str(
            table_options.get("include_namespace_properties", "true")
        ).lower() not in _FALSEY
        prefix = self._client.prefix

        for levels in self._walk_namespaces(table_options):
            properties: Dict[str, Any] = {}
            if include_properties:
                try:
                    body = self._client.load_namespace(levels)
                    properties = body.get("properties") or {}
                except IcebergRestError as exc:
                    if exc.status_code not in (403, 404):
                        raise
                    properties = {}

            display = NAMESPACE_DISPLAY_SEPARATOR.join(levels)
            yield {
                "catalog_prefix": prefix,
                "namespace": display,
                "levels": list(levels),
                "parent": (
                    NAMESPACE_DISPLAY_SEPARATOR.join(levels[:-1])
                    if len(levels) > 1
                    else None
                ),
                "depth": len(levels),
                "location": as_str(properties.get("location")),
                "owner": as_str(properties.get("owner")),
                "properties": string_map(properties),
            }

    def _emit_tables(self, levels: Sequence[str]) -> Iterator[dict]:
        prefix = self._client.prefix
        display = NAMESPACE_DISPLAY_SEPARATOR.join(levels)
        for identifier in self._client.list_table_identifiers(levels):
            name = identifier.get("name")
            if not name:
                continue
            # The requested namespace is authoritative — the spec guarantees
            # every identifier in the response belongs to it.
            yield {
                "catalog_prefix": prefix,
                "namespace": display,
                "name": str(name),
                "identifier": f"{display}{NAMESPACE_DISPLAY_SEPARATOR}{name}",
            }

    def _emit_load_table_rows(
        self,
        stream: str,
        levels: Sequence[str],
        name: str,
        result: dict,
        partition: dict,
    ) -> Iterator[dict]:
        metadata = result.get("metadata") or {}
        base = {
            "catalog_prefix": self._client.prefix,
            "namespace": NAMESPACE_DISPLAY_SEPARATOR.join(levels),
            "table_name": name,
            "table_uuid": as_str(metadata.get("table-uuid")),
            "metadata_location": as_str(result.get("metadata-location")),
        }

        if stream == TABLE_SCHEMAS_STREAM:
            yield from emit_schema_rows(base, metadata)
        elif stream == TABLE_SNAPSHOTS:
            yield from emit_snapshot_rows(
                base,
                metadata,
                since_ms=as_int(partition.get("since_ms"), 0),
                until_ms=as_int(partition.get("until_ms"), self._init_ms),
            )
        elif stream == TABLE_PARTITION_SPECS:
            yield from emit_partition_spec_rows(base, metadata)
        elif stream == TABLE_PROPERTIES:
            yield from emit_property_rows(base, metadata)
        else:  # pragma: no cover - defensive
            raise ValueError(f"'{stream}' is not a LoadTableResult-derived stream")

    def _load_table_metadata(self, levels: Sequence[str], name: str) -> dict:
        return (self._client.load_table(levels, name) or {}).get("metadata") or {}

    # ------------------------------------------------------------------
    # Data path (pyiceberg)
    # ------------------------------------------------------------------

    def _pyiceberg_catalog(self):
        """Build a ``RestCatalog`` mirroring this connector's connection."""
        try:
            from pyiceberg.catalog.rest import RestCatalog
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "Reading Iceberg table rows requires the optional 'pyiceberg' "
                "dependency. Install it with "
                "`pip install 'lakeflow-community-connectors-iceberg[data]'` "
                "or `pip install 'pyiceberg[pyarrow]'`. The catalog metadata "
                "streams (namespaces, tables, table_schemas, table_snapshots, "
                "table_partition_specs, table_properties) do not need it."
            ) from exc

        options = self.options or {}
        properties: Dict[str, str] = {"uri": self._client.uri}
        for key, option in (
            ("warehouse", "warehouse"),
            ("token", "token"),
            ("credential", "credential"),
            ("scope", "scope"),
        ):
            value = options.get(option)
            if value:
                properties[key] = str(value)
        oauth_uri = options.get("oauth2_server_uri") or options.get(
            "oauth2-server-uri"
        )
        if oauth_uri:
            properties["oauth2-server-uri"] = str(oauth_uri)
        # Prefer the server-resolved prefix so pyiceberg targets the same
        # catalog the metadata streams read.
        prefix = self._client.prefix
        if prefix:
            properties["prefix"] = prefix

        for key, value in options.items():
            if value is None:
                continue
            if key.startswith(_PYICEBERG_PASSTHROUGH_PREFIXES):
                properties[key] = str(value)

        return RestCatalog(name=self._catalog_name, **properties)

    def _load_iceberg_table(self, identifier: str):
        return self._pyiceberg_catalog().load_table(identifier)

    def _read_data_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        identifier = self._data_tables[table_name]
        table = self._load_iceberg_table(identifier)

        snapshot = table.current_snapshot()
        if snapshot is None:
            # Empty table (no snapshot yet) — nothing to read, ever.
            return iter([]), {"snapshot_id": "", "row_offset": _DRAINED}
        current = str(snapshot.snapshot_id)

        max_records = max(
            1,
            as_int(
                table_options.get("max_records_per_batch"),
                DEFAULT_MAX_RECORDS_PER_BATCH,
            ),
        )
        incremental_column = table_options.get("incremental_column")
        if incremental_column:
            _validate_column(incremental_column)
            return self._read_data_incremental(
                table, current, start_offset, table_options, incremental_column
            )
        return self._read_data_snapshot(
            table, current, start_offset, table_options, max_records
        )

    def _read_data_snapshot(
        self,
        table: Any,
        current: str,
        start_offset: dict,
        table_options: dict[str, str],
        max_records: int,
    ) -> Tuple[Iterator[dict], dict]:
        """Full read of the current snapshot, paged by a synthetic row offset.

        A scan pinned to one snapshot is deterministic, so ``row_offset`` is a
        valid resume point even though Iceberg exposes no row-level cursor.
        ``row_offset == -1`` means "this snapshot is drained", which is what
        makes ``end_offset == start_offset`` and terminates the trigger.
        """
        previous_snapshot = start_offset.get("snapshot_id")
        row_offset = as_int(start_offset.get("row_offset"), 0)

        if previous_snapshot == current and row_offset == _DRAINED:
            return iter([]), dict(start_offset)
        if previous_snapshot != current:
            row_offset = 0

        rows = self._scan_rows(
            table, table_options, skip=max(row_offset, 0), take=max_records
        )
        if len(rows) < max_records:
            end = {"snapshot_id": current, "row_offset": _DRAINED}
        else:
            end = {
                "snapshot_id": current,
                "row_offset": max(row_offset, 0) + len(rows),
            }
        return iter(rows), end

    def _read_data_incremental(
        self,
        table: Any,
        current: str,
        start_offset: dict,
        table_options: dict[str, str],
        column: str,
    ) -> Tuple[Iterator[dict], dict]:
        """Cursor-column read using a pyiceberg ``row_filter`` push-down.

        The filter is strictly greater-than, so an empty result means "caught
        up" and the offset stops moving. ``max_records_per_batch`` is
        best-effort here: pyiceberg scans are unordered, so truncating a
        filtered scan would silently skip rows. Size batches with a narrower
        ``row_filter`` instead.
        """
        cursor = start_offset.get("cursor")
        clauses: List[str] = []
        extra_filter = table_options.get("row_filter")
        if extra_filter:
            clauses.append(f"({extra_filter})")
        if cursor is not None:
            clauses.append(f"{column} > {_literal(cursor)}")
        expression = " and ".join(clauses) if clauses else None

        rows = self._scan_rows(
            table, table_options, skip=0, take=None, row_filter=expression
        )
        values = [row.get(column) for row in rows if row.get(column) is not None]
        if not values:
            # Nothing past the cursor: freeze the offset so the trigger ends.
            caught_up = dict(start_offset) if start_offset else {"snapshot_id": current}
            return iter(rows), caught_up

        end = {"snapshot_id": current, "cursor": max(values)}
        if start_offset and start_offset == end:
            return iter([]), dict(start_offset)
        return iter(rows), end

    def _scan_rows(
        self,
        table: Any,
        table_options: dict[str, str],
        *,
        skip: int = 0,
        take: Optional[int] = None,
        row_filter: Optional[str] = None,
    ) -> List[dict]:
        """Stream a pyiceberg scan into JSON-compatible dicts."""
        kwargs: Dict[str, Any] = {}
        expression = (
            row_filter if row_filter is not None else table_options.get("row_filter")
        )
        if expression:
            kwargs["row_filter"] = expression
        selected = table_options.get("selected_fields")
        if selected:
            fields = tuple(part.strip() for part in selected.split(",") if part.strip())
            if fields:
                kwargs["selected_fields"] = fields
        if take is not None:
            # ``limit`` is applied before the client-side skip, so ask for both.
            kwargs["limit"] = skip + take

        reader = table.scan(**kwargs).to_arrow_batch_reader()
        rows: List[dict] = []
        skipped = 0
        try:
            for batch in reader:
                for record in batch.to_pylist():
                    if skipped < skip:
                        skipped += 1
                        continue
                    rows.append(_normalize(record))
                    if take is not None and len(rows) >= take:
                        return rows
        finally:
            close = getattr(reader, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:  # pragma: no cover - best effort
                    pass
        return rows

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_table(self, table_name: str) -> None:
        if table_name in METADATA_TABLES or table_name in self._data_tables:
            return
        raise ValueError(
            f"Table '{table_name}' is not supported. "
            f"Supported tables: {self.list_tables()}"
        )


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _parse_data_tables(raw: Optional[str]) -> Dict[str, str]:
    """Parse ``data_tables`` into ``{lakeflow_table_name: iceberg_identifier}``.

    Accepts ``ns.table`` (alias derived by replacing dots with underscores) or
    an explicit ``alias=ns.table``.
    """
    mapping: Dict[str, str] = {}
    for entry in str(raw or "").split(","):
        entry = entry.strip()
        if not entry:
            continue
        if "=" in entry:
            alias, _, identifier = entry.partition("=")
            alias, identifier = alias.strip(), identifier.strip()
        else:
            identifier = entry
            alias = re.sub(r"[^0-9A-Za-z_]+", "_", identifier).strip("_")
        if not identifier or not alias:
            continue
        if not _IDENTIFIER_RE.match(identifier) or "." not in identifier:
            raise ValueError(
                f"Invalid Iceberg table identifier {identifier!r} in 'data_tables'; "
                "expected a dotted identifier such as 'accounting.tax.invoices'"
            )
        if alias in METADATA_TABLES:
            raise ValueError(
                f"Data table alias {alias!r} collides with a built-in metadata "
                f"stream. Use 'alias=<identifier>' to pick another name."
            )
        mapping[alias] = identifier
    return mapping


def _split_identifier(identifier: str) -> Tuple[List[str], str]:
    parts = [part for part in identifier.split(".") if part]
    if len(parts) < 2:
        raise ValueError(
            f"Iceberg identifier {identifier!r} must include a namespace, "
            "e.g. 'accounting.tax.invoices'"
        )
    return parts[:-1], parts[-1]


def _validate_column(column: str) -> None:
    if not _COLUMN_RE.match(column):
        raise ValueError(
            f"Invalid column name {column!r}; expected an identifier such as "
            "'updated_at'"
        )


def _encode_levels(levels: Sequence[str]) -> str:
    """Pack namespace levels into one JSON-safe partition-descriptor string."""
    return NAMESPACE_SEPARATOR.join(levels)


def _decode_levels(packed: str) -> List[str]:
    return [level for level in str(packed).split(NAMESPACE_SEPARATOR) if level]


def _literal(value: Any) -> str:
    """Render a cursor value as a pyiceberg row-filter literal."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _normalize(value: Any) -> Any:
    """Make an Arrow-derived value JSON-compatible for the framework."""
    if isinstance(value, dict):
        return {str(k): _normalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize(v) for v in value]
    if isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        return value.isoformat()
    if isinstance(value, _uuid.UUID):
        return str(value)
    return value
