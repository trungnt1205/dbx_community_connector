# Lakeflow Iceberg (REST Catalog) Community Connector

This documentation describes how to configure and use the **Iceberg** Lakeflow community connector to ingest data from an **Apache Iceberg REST Catalog** into Databricks.

The connector targets the **open Iceberg REST Catalog specification**, not a single vendor. Any server that implements the spec works — Databricks Unity Catalog's Iceberg REST endpoint, Apache Polaris, Project Nessie, Lakekeeper, Tabular, Dell ECS, or the reference `iceberg-rest-fixture`. The AWS Glue native API and the Hive Metastore Thrift API are **out of scope**.

The connector reads on two paths:

1. **Catalog metadata** — six built-in streams (`namespaces`, `tables`, `table_schemas`, `table_snapshots`, `table_partition_specs`, `table_properties`) read over plain HTTPS calls to the REST Catalog. No extra libraries needed.
2. **Table row data** — one opt-in stream per table you list in the `data_tables` connection option. Rows are read through **pyiceberg**, which is an optional dependency. See [Reading table row data](#reading-table-row-data).

## Prerequisites

- **An Iceberg REST Catalog endpoint**: the root URL of a server implementing the REST Catalog spec, for example `https://catalog.example.com/iceberg`. This is the value you pass as `uri`; the connector bootstraps itself with `GET {uri}/v1/config`.
- **Credentials for that catalog**, one of:
  - a **static bearer token** issued out-of-band by the catalog operator, or
  - an **OAuth 2.0 client-credentials** pair (client ID + client secret) that the catalog (or its identity provider) accepts.
- **Read permission** on the namespaces and tables you want to ingest. Namespaces and tables the principal cannot read are skipped silently during the crawl (`403` / `404` responses do not fail the pipeline).
- **Network access** from the pipeline compute to the catalog host, and — only if you read table row data — to the underlying object storage (S3, GCS, ADLS, HDFS) where the data files live.
- **pyiceberg** installed on the pipeline compute, **only** if you configure `data_tables`. The metadata streams do not need it.
- **Lakeflow / Databricks environment**: a workspace where you can create a Unity Catalog connection for a community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector.

| Name | Type | Required | Description | Example |
|---|---|---|---|---|
| `uri` | string | yes | REST Catalog root URL. The bootstrap `GET /v1/config` call is made here and every catalog route is relative to it. Trailing slashes are trimmed. | `https://catalog.example.com/iceberg` |
| `token` | string | conditional | Static bearer token. Sent as `Authorization: Bearer <token>` on every request. Required when you authenticate with a token instead of OAuth. Store as a secret. | `dapi...` |
| `credential` | string | conditional | OAuth 2.0 client credentials as a single `client_id:client_secret` string. Use this **or** the `client_id` + `client_secret` pair. Store as a secret. | `svc-ingest:s3cr3t` |
| `client_id` | string | conditional | OAuth 2.0 client ID. Use together with `client_secret` as an alternative to `credential`. | `svc-ingest` |
| `client_secret` | string | conditional | OAuth 2.0 client secret. Use together with `client_id`. Store as a secret. | `s3cr3t` |
| `scope` | string | no | OAuth 2.0 scope requested from the token endpoint. Defaults to `catalog`. | `catalog` |
| `oauth2_server_uri` | string | no | Token endpoint URL when an external identity provider (for example Keycloak) issues the token. Defaults to `{uri}/v1/oauth/tokens`, or to the `oauth2-server-uri` value the server returns from `/v1/config`. The hyphenated spelling `oauth2-server-uri` is also accepted. | `https://idp.example.com/oauth/token` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names allowed to pass through to the connector. This connector uses table-specific options, so this parameter must be set. | see the exact value below |

Supply **either** `token` **or** an OAuth client credential. If both are present, the static `token` wins and no token exchange is performed.

The full, definitive value for `externalOptionsAllowList` is:

```
max_objects_per_batch,max_records_per_batch,max_namespace_depth,include_namespace_properties,incremental_column,row_filter,selected_fields
```

> **Note**: `incremental_column`, `row_filter`, `selected_fields`, and the batch-sizing options are **not** connection parameters. They are supplied per table under `table_configuration` in the pipeline spec, and each name must appear in `externalOptionsAllowList` or it is silently dropped at runtime.

### Optional Connection Parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `warehouse` | string | – | Warehouse name or URI, sent as `?warehouse=` on `/v1/config`. Lets a multi-warehouse catalog scope the session to one storage location or tenant. Omit for single-warehouse catalogs. |
| `prefix` | string | resolved from server | Catalog path prefix inserted into every resource path (`{uri}/v1/{prefix}/namespaces/...`). Normally you leave this unset: the connector resolves it from `/v1/config`. A `prefix` returned in the server's `overrides` block always wins over the value you set here. |
| `vended_credentials` | string | `false` | Set to `true` to send `X-Iceberg-Access-Delegation: vended-credentials` on table-load calls, asking the catalog to return short-lived storage credentials. Useful when the pipeline has no long-lived cloud credentials of its own. |
| `page_size` | string | `100` | `pageSize` hint sent on the list endpoints. Servers may cap or ignore it. |
| `timeout` | string | `60` | Per-request HTTP timeout in seconds. Raise it for slow networks or very large `LoadTableResult` documents. |
| `catalog_name` | string | `iceberg` | Logical catalog name handed to pyiceberg. Used only on the row-data path. |
| `data_tables` | string | – | Comma-separated list of Iceberg tables to expose as row-data streams. See [Reading table row data](#reading-table-row-data). Empty by default, so the connector exposes only the six metadata streams. |

#### Storage and signing properties (row-data path only)

Any option whose name starts with one of the following prefixes is passed straight through to pyiceberg's `RestCatalog`, so you can hand it storage credentials or request signing settings:

`s3.` · `gcs.` · `adls.` · `adlfs.` · `azure.` · `hdfs.` · `rest.` · `client.` · `header.` · `py-io-impl`

Common examples: `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`, `s3.region`, `gcs.project-id`, `adls.account-name`, `hdfs.default-fs`, `rest.sigv4-enabled`, `rest.signing-region`, `rest.signing-name`, `py-io-impl`.

> **Important**: these properties affect the **row-data path only**. The six metadata streams use the connector's own HTTP client, which authenticates with a bearer token or OAuth client credentials — it does **not** perform AWS SigV4 signing. A catalog that accepts *only* SigV4 (such as the AWS Glue Iceberg REST front door) is therefore not supported for the metadata streams. Setting `s3.*` credentials is unnecessary when the catalog vends credentials (`vended_credentials=true`).

### Obtaining the Required Parameters

**Catalog URL (`uri`)** — ask your catalog operator, or take it from the client configuration your query engines already use. You can verify it before creating the connection:

```bash
curl -s -H "Authorization: Bearer $ICEBERG_TOKEN" \
  "https://catalog.example.com/iceberg/v1/config?warehouse=my_warehouse"
```

A healthy response is a JSON document with `defaults`, `overrides`, and (on newer servers) an `endpoints` list. If `overrides` contains a `prefix`, the connector picks it up automatically.

**Static bearer token (`token`)** — issued by the catalog operator, typically as a long-lived, PAT-style token. For Databricks Unity Catalog's Iceberg REST endpoint this is a workspace personal access token or a service-principal token. Store it as a secret.

**OAuth 2.0 client credentials (`credential`, or `client_id` + `client_secret`)** — register a service/machine client in whatever issues tokens for your catalog:

1. In the catalog server or its identity provider (Polaris, Lakekeeper, Keycloak, …), create a **service principal** or **client-credentials client**. No redirect URI and no browser sign-in are involved — this is a machine-to-machine credential, so the connector obtains the access token itself and refreshes it before it expires.
2. Grant the client read access to the warehouse, namespaces, and tables you want to ingest.
3. Copy the client ID and client secret. Supply them either as two options (`client_id`, `client_secret`) or joined into one `credential` option in `client_id:client_secret` form.
4. If the token endpoint is **not** `{uri}/v1/oauth/tokens` — common when an external identity provider issues the tokens — set `oauth2_server_uri` to the endpoint's URL.
5. If the issuer requires a scope other than `catalog`, set `scope`.

You can validate the exchange before creating the connection:

```bash
curl -s -X POST "https://catalog.example.com/iceberg/v1/oauth/tokens" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&scope=catalog"
```

A successful response contains `access_token`, `token_type`, and `expires_in`.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to:
   `max_objects_per_batch,max_records_per_batch,max_namespace_depth,include_namespace_properties,incremental_column,row_filter,selected_fields`

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The connector exposes **six metadata streams**, always available, plus **one row-data stream per table** you list in `data_tables`. Stream names are lower snake case and must be written exactly as shown.

| Stream | Description | Ingestion Type | Primary Key | Cursor |
|---|---|---|---|---|
| `namespaces` | Every namespace in the catalog, discovered by a recursive walk, with its properties | `snapshot` | `namespace` | – |
| `tables` | Every table identifier in the catalog | `snapshot` | `namespace`, `name` | – |
| `table_schemas` | One row per field of every schema version of every table, including nested struct/list/map fields | `snapshot` | `namespace`, `table_name`, `schema_id`, `field_id` | – |
| `table_snapshots` | One row per Iceberg snapshot (commit) of every table | `append` | `namespace`, `table_name`, `snapshot_id` | `timestamp_ms` |
| `table_partition_specs` | One row per partition field of every partition spec of every table | `snapshot` | `namespace`, `table_name`, `spec_id`, `field_id` | – |
| `table_properties` | One row per table property key/value pair | `snapshot` | `namespace`, `table_name`, `key` | – |
| *(configured data tables)* | Actual rows of a specific Iceberg table | `snapshot`, `append`, or `cdc` | Iceberg identifier fields, when the table declares them | your `incremental_column`, when set |

Multi-level namespaces are rendered with a `.` separator in the `namespace` column (for example `accounting.tax`), even though the wire protocol joins them with the ASCII unit separator `0x1F`.

`table_schemas`, `table_snapshots`, `table_partition_specs`, and `table_properties` are four different flattened views of the **same** `LoadTableResult` document (`GET .../namespaces/{ns}/tables/{table}`). They therefore share the provenance columns `catalog_prefix`, `namespace`, `table_name`, `table_uuid`, and `metadata_location`. If you ingest all four, each table in the catalog is loaded four times — once per stream.

### Stream schemas

**`namespaces`**

| Column | Type | Notes |
|---|---|---|
| `catalog_prefix` | string | Prefix resolved from `/v1/config`; empty string when the server uses none |
| `namespace` | string | Levels joined with `.` — the primary key |
| `levels` | array&lt;string&gt; | Namespace levels, unjoined |
| `parent` | string | Parent namespace, `null` for top-level |
| `depth` | long | Number of levels |
| `location` | string | From the namespace's `location` property |
| `owner` | string | From the namespace's `owner` property |
| `properties` | map&lt;string,string&gt; | Full namespace property map |

`location`, `owner`, and `properties` are populated by an extra `GET` per namespace. Set the table option `include_namespace_properties=false` to skip that call — the columns then come back empty, but the crawl costs one request per namespace less.

**`tables`**

| Column | Type | Notes |
|---|---|---|
| `catalog_prefix` | string | |
| `namespace` | string | |
| `name` | string | Table name |
| `identifier` | string | `namespace.name`, ready to paste into `data_tables` |

**`table_schemas`**

Provenance columns (`catalog_prefix`, `namespace`, `table_name`, `table_uuid`, `metadata_location`) plus:

| Column | Type | Notes |
|---|---|---|
| `format_version` | long | Iceberg table format version (1, 2, 3) |
| `schema_id` | long | Schema version ID |
| `is_current_schema` | boolean | `true` for the schema matching `current-schema-id` |
| `field_id` | long | Iceberg field ID — stable across renames |
| `field_name` | string | Leaf field name |
| `field_path` | string | Dotted path for nested fields, e.g. `address.city`, `items.element.sku` |
| `parent_field_id` | long | `null` for top-level fields |
| `field_type` | string | Short type name (`long`, `string`, `struct`, `list`, `map`, …) |
| `field_type_json` | string | Full JSON type for nested types; `null` for primitives |
| `field_required` | boolean | Iceberg `required` flag |
| `field_doc` | string | Field comment |
| `is_identifier_field` | boolean | `true` when the field is in `identifier-field-ids` |

Nested `struct`, `list`-of-struct, and `map`-of-struct definitions are flattened recursively. Field IDs are unique within a schema at any depth, so `(schema_id, field_id)` stays a valid key.

**`table_snapshots`**

Provenance columns plus:

| Column | Type | Notes |
|---|---|---|
| `snapshot_id` | long | |
| `parent_snapshot_id` | long | `null` for the first snapshot |
| `sequence_number` | long | Per-table, restarts at 1 for each table |
| `timestamp_ms` | long | Commit time in epoch milliseconds — **the incremental cursor** |
| `manifest_list` | string | Path of the manifest list file |
| `schema_id` | long | Schema in effect at commit time |
| `operation` | string | `append`, `overwrite`, `replace`, or `delete` |
| `summary` | map&lt;string,string&gt; | Full commit summary (`added-records`, `deleted-records`, `total-data-files`, …) — all values are strings, per the Iceberg spec, even the numeric ones |
| `is_current_snapshot` | boolean | `true` for HEAD |
| `ref_names` | array&lt;string&gt; | Branch/tag names pointing at this snapshot |

**`table_partition_specs`**

Provenance columns plus:

| Column | Type | Notes |
|---|---|---|
| `spec_id` | long | Partition spec version |
| `is_default_spec` | boolean | `true` for the spec matching `default-spec-id` |
| `field_id` | long | Partition field ID |
| `source_id` | long | Schema field ID the partition is derived from |
| `source_field_name` | string | Resolved name of that source field (from the current schema) |
| `field_name` | string | Partition field name |
| `transform` | string | `identity`, `year`, `month`, `day`, `hour`, `bucket[N]`, `truncate[W]`, `void` |

Format-v1 tables that carry a bare `partition-spec` field list instead of the v2 `partition-specs` array are handled: their fields are emitted under `spec_id = 0`.

**`table_properties`**

Provenance columns plus `key` (string) and `value` (string).

**Configured data tables** — the schema is derived from the Iceberg table's current schema, mapped field by field to Spark types (see [Data Type Mapping](#data-type-mapping)). Iceberg `required: true` becomes a non-nullable Spark field, and each field carries its Iceberg field ID as column metadata (`iceberg.field-id`).

### Incremental behaviour and cursors

**Metadata streams — how the crawl is paced.** Every metadata stream walks a list of catalog objects: `namespaces` is a single unit, `tables` walks one unit per namespace, and the four `LoadTableResult` streams walk one unit per table. Each unit costs one HTTP round trip. To keep a microbatch bounded, the connector advances at most `max_objects_per_batch` objects (default `50`) per batch and remembers how far it got. A large catalog is therefore crawled over several microbatches, and a run under `Trigger.AvailableNow` terminates once the crawl reaches the end of the list.

Metadata streams also run **in parallel** across Spark tasks: one task per namespace for `tables`, one task per table for the four `LoadTableResult` streams. `namespaces` runs on a single task because the recursive namespace walk is inherently sequential.

**`table_snapshots` is the only incremental metadata stream.** `metadata.snapshots[]` is an append-only log, so the stream is `append`-typed with `timestamp_ms` as the cursor. Each run only emits snapshots whose `timestamp_ms` is greater than the previous run's watermark and no newer than the moment the run started. Two consequences worth knowing:

- The REST Catalog spec has **no server-side "snapshots since N" filter**. The connector fetches the whole `LoadTableResult` for each table and applies the window client-side, so an incremental run costs the same number of requests as a full one — it just emits fewer rows.
- Snapshot **expiry** removes old entries from the log. Expired snapshots that were already ingested stay in the destination table; they are not deleted.

Everything else (`namespaces`, `tables`, `table_schemas`, `table_partition_specs`, `table_properties`) is `snapshot`-typed and re-derived in full on every run, because the catalog exposes no change cursor for them.

**Data tables** — behaviour depends on whether you set `incremental_column`:

| `incremental_column` | Ingestion type | Behaviour |
|---|---|---|
| not set | `snapshot` | Full read of the table's current snapshot, paged across microbatches by an internal row offset of `max_records_per_batch` rows. The scan is pinned to one snapshot, so paging is deterministic. When the snapshot advances between runs, the read restarts from the new snapshot. |
| set, table **has** Iceberg identifier fields | `cdc` | Only rows with `incremental_column > last_seen` are read, using a pyiceberg row-filter push-down. Rows are upserted on the identifier fields. |
| set, table has **no** identifier fields | `append` | Same filtered read, but rows are appended — without a primary key an upsert is not safe. |

The incremental filter is strictly greater-than, so a row written with a cursor value equal to or below the watermark is never picked up. Choose a monotonically increasing column (a commit timestamp or a monotonic ID), not one that can be back-dated.

`cdc_with_deletes` is **not supported** — see [Limitations](#limitations).

## Reading table row data

Row data is opt-in. Two things are needed.

### 1. Install pyiceberg on the pipeline compute

The connector imports pyiceberg lazily, only when a data-table stream is read, so the metadata streams work without it. Install either the connector's optional extra or pyiceberg directly:

```bash
pip install 'lakeflow-community-connectors-iceberg[data]'
# or
pip install 'pyiceberg[pyarrow]>=0.7.0,<1.0'
```

Inside a pipeline, add the same dependency to the pipeline's environment or declare it at the top of the pipeline source file:

```python
%pip install "pyiceberg[pyarrow]>=0.7.0,<1.0"
```

If pyiceberg is missing, reading a data-table stream fails with a message naming the install command. The six metadata streams are unaffected.

### 2. List the tables in the `data_tables` connection option

`data_tables` is a comma-separated list. Each entry is either a dotted Iceberg identifier or an explicit alias:

```
data_tables = "accounting.tax.invoices, sales.orders, cust=analytics.dim.customers"
```

- **`namespace.table`** — the stream name is derived by replacing every non-alphanumeric character with `_`. `accounting.tax.invoices` becomes the stream `accounting_tax_invoices`.
- **`alias=namespace.table`** — you choose the stream name. Use this when the derived name is too long or collides.

Rules:

- Each identifier must include at least one namespace level. A bare `invoices` is rejected.
- Identifiers must match `letters/digits/underscore` segments separated by dots.
- An alias may not collide with a built-in metadata stream name (`namespaces`, `tables`, `table_schemas`, `table_snapshots`, `table_partition_specs`, `table_properties`). Use the `alias=` form to pick another name.

The resulting stream name is what you put in `source_table` in the pipeline spec.

Data-table streams are read on a **single Spark task**. pyiceberg plans scans locally and its Arrow batch reader is not splittable without re-planning, so these streams do not parallelise the way the metadata streams do.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Stream name — one of the six metadata streams, or a name derived from `data_tables` |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables (`table_snapshots`, and data tables read in `append` mode) do not support this option. |
| `primary_keys` | No | List of columns overriding the connector's default primary keys. Useful for a data table that declares no Iceberg identifier fields but does have a natural key. |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Source-specific `table_configuration` options

Every name below must also appear in the connection's `externalOptionsAllowList`.

| Option | Applies to | Default | Description |
|---|---|---|---|
| `max_objects_per_batch` | metadata streams | `50` | Catalog objects (namespaces or tables) crawled per microbatch. Each object costs one HTTP round trip. Lower it to shorten microbatches against a slow or heavily throttled catalog; raise it to finish a large crawl in fewer batches. |
| `max_records_per_batch` | data tables; metadata streams as a fallback | `1000` | Rows returned per microbatch on the data path. On a metadata stream, it is used **only** when `max_objects_per_batch` is unset, and is converted to an object count with a deliberately coarse estimate of 20 rows per object. Prefer setting `max_objects_per_batch` directly for metadata streams. |
| `max_namespace_depth` | metadata streams | `5` | Recursion guard for the namespace walk. Namespaces deeper than this are not discovered. Raise it if your catalog nests deeper than five levels. |
| `include_namespace_properties` | `namespaces` | `true` | Set to `false` to skip the per-namespace `GET`. Saves one request per namespace, at the cost of leaving `location`, `owner`, and `properties` empty. |
| `incremental_column` | data tables only | – | Cursor column. Absent means a full snapshot read. Must be a plain column identifier (dotted paths into structs are accepted; arbitrary expressions are rejected). |
| `row_filter` | data tables only | – | A pyiceberg row-filter expression pushed down to the scan, e.g. `status = 'PAID' and amount > 100`. Combined with the cursor predicate using `and` when `incremental_column` is also set. |
| `selected_fields` | data tables only | all columns | Comma-separated column projection, e.g. `invoice_id,customer_id,amount`. |

**Sizing note for incremental data reads.** When `incremental_column` is set, `max_records_per_batch` is best-effort and effectively ignored: pyiceberg scans are unordered, so truncating a filtered scan would silently skip rows. Control batch size with a narrower `row_filter` instead, or fall back to a snapshot read where the row-offset paging does honour the cap.

## Data Type Mapping

Applies to the row-data streams. Iceberg field types (from `schemas[].fields[].type`) map to Spark types as follows.

| Iceberg type | Spark type | Notes |
|---|---|---|
| `boolean` | `BooleanType` | |
| `int` | `IntegerType` | 32-bit signed |
| `long` | `LongType` | 64-bit signed |
| `float` | `FloatType` | 32-bit IEEE 754 |
| `double` | `DoubleType` | 64-bit IEEE 754 |
| `decimal(P,S)` | `DecimalType(P, S)` | Precision and scale taken from the type string |
| `date` | `DateType` | Days since epoch |
| `time` | `StringType` | Spark has no time-of-day type; the microsecond time is rendered as text |
| `timestamp` | `TimestampNTZType` | Microsecond precision, no timezone. Falls back to `TimestampType` on runtimes without `TimestampNTZType` |
| `timestamptz` | `TimestampType` | Microsecond precision, UTC-normalized |
| `timestamp_ns` | `TimestampNTZType` | **Lossy** — Iceberg v3 nanoseconds narrowed to Spark microseconds |
| `timestamptz_ns` | `TimestampType` | **Lossy** — same narrowing |
| `string` | `StringType` | UTF-8 |
| `uuid` | `StringType` | Canonical UUID text |
| `fixed(L)` | `BinaryType` | Fixed-length byte array |
| `binary` | `BinaryType` | Variable-length |
| `unknown` | `StringType` | |
| `struct` | `StructType` | Fields mapped recursively |
| `list` | `ArrayType` | Element type mapped recursively; `element-required` drives nullability |
| `map` | `MapType` | Key and value types mapped recursively; `value-required` drives nullability |

Notes:

- Any type the connector does not recognise degrades to `StringType` rather than failing, so one exotic column never blocks a whole table.
- Iceberg `required: true` maps to a **non-nullable** Spark field.
- Hidden-partition columns are **not** emitted. `year`/`month`/`day`/`hour`/`bucket[N]`/`truncate[W]` transforms are metadata-only partition keys; the scan returns only the table's real schema fields. Use the `table_partition_specs` stream to see the partitioning.
- Values that Arrow returns as Python `datetime`, `date`, `time`, or `UUID` objects are normalized to ISO-8601 / canonical text before being handed to the pipeline.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which guides you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

Update the `pipeline_spec` in the main pipeline file (e.g. `ingest.py`).

A metadata-only pipeline needs no table options at all:

```json
{
  "pipeline_spec": {
    "connection_name": "iceberg_connection",
    "object": [
      { "table": { "source_table": "namespaces" } },
      { "table": { "source_table": "tables" } },
      { "table": { "source_table": "table_schemas" } },
      { "table": { "source_table": "table_snapshots" } },
      { "table": { "source_table": "table_partition_specs" } },
      { "table": { "source_table": "table_properties" } }
    ]
  }
}
```

A pipeline that tunes the crawl and also reads row data — assuming the connection sets
`data_tables = "accounting.tax.invoices, cust=analytics.dim.customers"`:

```json
{
  "pipeline_spec": {
    "connection_name": "iceberg_connection",
    "object": [
      {
        "table": {
          "source_table": "namespaces",
          "table_configuration": {
            "include_namespace_properties": "false",
            "max_namespace_depth": "3"
          }
        }
      },
      {
        "table": {
          "source_table": "table_snapshots",
          "table_configuration": {
            "max_objects_per_batch": "25"
          }
        }
      },
      {
        "table": {
          "source_table": "accounting_tax_invoices",
          "destination_table": "invoices",
          "table_configuration": {
            "incremental_column": "updated_at",
            "row_filter": "status != 'DRAFT'",
            "selected_fields": "invoice_id,customer_id,amount,status,updated_at"
          }
        }
      },
      {
        "table": {
          "source_table": "cust",
          "destination_table": "dim_customers",
          "table_configuration": {
            "max_records_per_batch": "5000"
          }
        }
      }
    ]
  }
}
```

You can also customize the source connector code for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start small.** Begin with `namespaces` and `tables` to confirm the connection, the resolved `prefix`, and that your principal can see the namespaces you expect. Add the heavier `LoadTableResult` streams afterwards.
- **Budget for the crawl cost.** `table_schemas`, `table_snapshots`, `table_partition_specs`, and `table_properties` each issue one table-load request per table in the catalog. On a catalog with thousands of tables, ingest only the streams you actually consume, and pace them with `max_objects_per_batch`.
- **Use `include_namespace_properties=false`** if you do not need namespace `location`/`owner`/`properties` — it removes one request per namespace.
- **Prefer `table_snapshots` for change tracking.** It is the one incremental metadata stream, and its `summary` map is where commit statistics live.
- **Give row-data streams a cursor.** A `snapshot` data table is re-read in full whenever the table commits. Setting `incremental_column` reduces the read to new rows only.
- **Project and filter early.** `selected_fields` and `row_filter` are pushed down to pyiceberg, which prunes manifests and files using partition values and column statistics. A well-chosen filter on a partition column is far cheaper than filtering downstream.
- **Respect the operator's limits.** The Iceberg REST spec defines no rate limits — they are whatever your catalog operator enforces. The connector already honours `Retry-After` and retries `429`, `500`, `502`, `503`, and `504` up to five times with jittered exponential backoff (1 s doubling to a 30 s cap). If you still see throttling, lower `max_objects_per_batch` and widen the schedule interval.
- **Schedule sensibly.** Catalog metadata changes far more slowly than row data. It is common to run a metadata pipeline hourly or daily and the row-data pipeline on a tighter schedule.

#### Troubleshooting

**Common Issues:**

- **`Iceberg connector requires a 'uri' option ...`** — the `uri` connection option is missing. The connector also accepts `catalog_uri` or `base_url` as aliases, but `uri` is the documented name.
- **`401 Unauthorized` on every call** — the token is wrong, expired, or revoked; or the OAuth exchange failed. With a static `token` there is no refresh path, so replace it. With client credentials, the connector re-exchanges the credential once and replays the request before giving up; a persistent 401 means the client ID/secret or `scope` is wrong.
- **`419` mid-run** — the catalog signalling "token expired during this session". This is handled automatically for OAuth credentials (the token is re-fetched and the request replayed). With a static `token` it cannot be recovered — switch to client credentials for long-running pipelines.
- **`403 Forbidden` / `404 Not Found` during the crawl, but the pipeline succeeds with missing rows** — this is deliberate. A namespace or table the principal cannot read (or that was dropped mid-crawl) is skipped so one inaccessible object does not fail the whole run. If rows are missing, check the principal's grants at the catalog.
- **Wrong or empty results, paths look off** — the catalog `prefix` is probably not what you think. Call `GET {uri}/v1/config` and inspect `overrides.prefix`; a server-supplied `prefix` always overrides the one you configured. Multi-warehouse catalogs usually mint a prefix per warehouse, so set `warehouse` too.
- **Deeply nested namespaces missing** — the walk stops at `max_namespace_depth` (default `5`). Raise the option. If a namespace list never terminates, the guard is doing its job against a server that ignores the `parent` query parameter.
- **`Reading Iceberg table rows requires the optional 'pyiceberg' dependency`** — install `pyiceberg[pyarrow]` (or the `[data]` extra) on the pipeline compute. See [Reading table row data](#reading-table-row-data).
- **`Table '<name>' is not supported`** — `source_table` is neither a metadata stream nor a name derived from `data_tables`. Check for a typo, and remember that dots in an identifier become underscores in the stream name (`sales.orders` → `sales_orders`) unless you used the `alias=` form.
- **`Invalid Iceberg table identifier ... in 'data_tables'`** — the entry has no namespace (`invoices` instead of `sales.invoices`) or contains characters outside letters, digits, underscore, and dot.
- **`Data table alias ... collides with a built-in metadata stream`** — the `alias=` you chose is one of the six reserved metadata stream names (for example `tables=sales.orders`). Pick a different alias.
- **A data table returns zero rows forever** — the Iceberg table has no snapshot yet (never written to), or the `incremental_column` watermark has already passed every row. Confirm with the `table_snapshots` stream.
- **Storage access errors (`AccessDenied`, `NoSuchBucket`, credential errors) on data tables only** — the catalog metadata is reachable but the data files are not. Either set `vended_credentials=true` so the catalog issues short-lived storage credentials, or supply the appropriate `s3.*` / `gcs.*` / `adls.*` / `hdfs.*` properties on the connection.
- **`AWS4-HMAC-SHA256` / signature errors on the metadata streams** — the catalog requires SigV4 request signing, which the metadata path does not implement. See the note under [Storage and signing properties](#storage-and-signing-properties-row-data-path-only).
- **Timeouts loading a table** — a `LoadTableResult` for a table with a very long snapshot history can be large. Raise `timeout` (default 60 s).
- **A metadata pipeline seems to make no progress** — remember the crawl is paced: each microbatch advances at most `max_objects_per_batch` objects. A catalog with 5,000 tables needs 100 microbatches at the default of 50. Raise the option or let the run continue.

## Limitations

- **`cdc_with_deletes` is not supported.** Iceberg v2 positional and equality delete files are applied transparently by a scan — deleted rows simply stop appearing — but neither the REST spec nor pyiceberg exposes a "rows deleted since snapshot X" feed. Reconstructing deletes would mean diffing two full snapshots. Deletes at the source therefore do not propagate to the destination on `cdc` or `append` data tables. Use a periodic `snapshot`-mode full refresh if you need delete convergence.
- **No server-side incremental filter for snapshots.** `table_snapshots` fetches each table's full metadata document and applies the `timestamp_ms` window client-side, so an incremental run costs the same requests as a full one.
- **Deferred streams.** `views`, `table_sort_orders`, and `table_refs` are not implemented. Branch and tag names are still available as the `ref_names` column on `table_snapshots`.
- **Remote scan planning is not used.** The newer `POST .../tables/{table}/plan` server-side planning API is deferred; pyiceberg plans scans locally, which works against every REST catalog implementation.
- **AWS SigV4 applies to the row-data path only.** The metadata streams authenticate with a bearer token or OAuth client credentials.
- **Nanosecond timestamps are lossy.** Iceberg v3 `timestamp_ns` / `timestamptz_ns` are narrowed to Spark's microsecond timestamps.
- **Iceberg `time` has no Spark equivalent** and is surfaced as a string.
- **Namespace rendering is ambiguous in one edge case.** Levels are joined with `.` in the `namespace` column, so a namespace level that itself contains a dot cannot be distinguished from two levels. The `levels` array column on the `namespaces` stream preserves the true structure.
- **Row-data streams are single-task.** Only the metadata streams parallelise across Spark tasks.
- **Read-only.** No write-back, no catalog mutation. All write-path REST endpoints (commit, rename, register, credentials, sign, metrics) are out of scope.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/iceberg/iceberg.py`
- REST client: `src/databricks/labs/community_connector/sources/iceberg/iceberg_rest.py`
- Schemas and type mapping: `src/databricks/labs/community_connector/sources/iceberg/iceberg_schemas.py`
- Metadata flattening: `src/databricks/labs/community_connector/sources/iceberg/iceberg_flatten.py`
- Source API research notes: `src/databricks/labs/community_connector/sources/iceberg/iceberg_api_doc.md`
- Connector spec: `src/databricks/labs/community_connector/sources/iceberg/connector_spec.yaml`
- Apache Iceberg REST Catalog spec (rendered): https://iceberg.apache.org/rest-catalog-spec/
- Apache Iceberg REST Catalog OpenAPI source: https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
- Iceberg table spec: https://iceberg.apache.org/spec/
- pyiceberg configuration reference: https://py.iceberg.apache.org/configuration/
- pyiceberg API (`RestCatalog`, `table.scan`): https://py.iceberg.apache.org/api/
