# **Iceberg (REST Catalog) API Documentation**

Scope for this connector: the **Apache Iceberg REST Catalog** open spec (`open-api/rest-catalog-open-api.yaml` in `apache/iceberg`, published at https://iceberg.apache.org/rest-catalog-spec/). This is a protocol, not a single vendor — any server implementing it works (Tabular/Databricks Unity Catalog Iceberg REST endpoint, AWS Glue's Iceberg REST front door, Apache Polaris, Project Nessie, Lakekeeper, Dell ECS, self-hosted `iceberg-rest-fixture`, etc.). **Glue Catalog API and Hive Metastore Thrift API are out of scope** — only the REST spec.

Two distinct read paths are combined:
1. **Catalog/metadata reads** — plain HTTP GET calls against the REST Catalog API described in this document, used to enumerate namespaces, tables, schemas, snapshots, partition specs, etc.
2. **Table row-data reads** — not done by hand-rolled HTTP calls against data files. The connector uses **pyiceberg**'s `RestCatalog` to `load_table()` (which internally calls the same REST endpoints) and then `table.scan(...).to_arrow_batch_reader()` to pull actual Parquet/Avro data as Arrow record batches. pyiceberg handles manifest-list/manifest resolution, partition pruning, and storage-credential exchange.

---

## **Authorization**

The REST Catalog spec defines three auth mechanisms. A server advertises which one(s) it needs via the bootstrap `GET /v1/config` call (see below) and the connection's `oauth2-server-uri` / credential configuration; the connector should support all three and let the operator pick one via UC connection fields.

### 1. Bearer token (static)

- Header: `Authorization: Bearer <token>` on every request (including `/v1/config`).
- Configured directly as `token` — a long-lived PAT-style token issued out-of-band by the catalog operator (common for Unity Catalog's Iceberg REST endpoint and Tabular).
- The connector **stores** the token as a secret and never runs an interactive flow for it.

```bash
curl -s -H "Authorization: Bearer $ICEBERG_TOKEN" \
  "https://catalog.example.com/iceberg/v1/config?warehouse=my_warehouse"
```

### 2. OAuth2 client-credentials (dynamic token exchange)

- Token endpoint: `POST /v1/oauth/tokens` (relative to the catalog `uri`, unless overridden by `oauth2-server-uri` from `/v1/config` `defaults`/`overrides`, or by explicit config — some deployments run a separate IdP, e.g. Keycloak, and set `oauth2-server-uri` to a different host entirely). **Note:** this endpoint is marked deprecated-for-removal in the newest spec revisions in favor of external IdP-issued tokens, but it is still the mechanism most self-hosted REST catalogs (Polaris, Lakekeeper, the reference fixture) implement today, so the connector must support it.
- Request: `application/x-www-form-urlencoded` body (OAuth2 client-credentials grant):
  ```
  grant_type=client_credentials
  client_id=<client_id>
  client_secret=<client_secret>
  scope=catalog          # optional, defaults to "catalog"
  ```
  The connector **stores** `client_id`/`client_secret` (or a combined `credential` string in `client_id:client_secret` form, matching pyiceberg's `credential` property) and exchanges them for an access token at runtime; it does not run a user-facing OAuth authorization-code flow.
- Response (`application/json`, `OAuthTokenResponse`):
  ```json
  {
    "access_token": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...",
    "token_type": "Bearer",
    "expires_in": 3600,
    "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
    "scope": "catalog"
  }
  ```
- The resulting `access_token` is then sent as `Authorization: Bearer <access_token>` on subsequent catalog calls; the connector refreshes it proactively before `expires_in` elapses (pyiceberg does this internally via its `AuthManager`).

```bash
curl -s -X POST "https://catalog.example.com/iceberg/v1/oauth/tokens" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&scope=catalog"
```

### 3. AWS SigV4 request signing

- Used by AWS-hosted REST catalogs (e.g. Glue's Iceberg REST front door, `glue.<region>.amazonaws.com/iceberg`).
- No bearer token; every HTTP request is signed with AWS SigV4 using the caller's AWS credentials (access key/secret/session token, or an assumed role), with `Authorization: AWS4-HMAC-SHA256 Credential=...` and companion `X-Amz-Date` / `X-Amz-Content-SHA256` headers computed over the request.
- pyiceberg config keys: `rest.sigv4-enabled=true`, `rest.signing-region=<aws-region>`, `rest.signing-name=<service-name, e.g. glue or execute-api>`. Standard AWS credential resolution (env vars, profile, instance role) supplies the keys — the connector does not implement its own signer, it delegates to `boto3`/pyiceberg's SigV4 request signer.

### Bootstrap: `GET /v1/config`

Every session must start with this call before anything else. It tells the client the effective catalog properties (importantly the `prefix` to use in every subsequent path) and which auth the server expects.

```
GET /v1/config?warehouse=<warehouse_name_or_uri>
Authorization: <one of the schemes above, if the server requires auth even for /v1/config>
```

Response (`CatalogConfig`):
```json
{
  "overrides": {
    "prefix": "my_catalog",
    "warehouse": "s3://my-bucket/warehouse"
  },
  "defaults": {
    "clients": "4",
    "namespace-separator": "\u001F"
  },
  "endpoints": [
    "GET /v1/{prefix}/namespaces",
    "POST /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}"
  ]
}
```

- `overrides` — properties the client **must** apply, overriding anything it was configured with (most importantly `prefix`, which many multi-tenant catalogs mint per-warehouse; also can override `warehouse` itself).
- `defaults` — properties the client uses only if it doesn't already have a value (e.g. `namespace-separator`, page-size hints).
- `endpoints` (spec ≥ 1.14, optional) — the exact list of routes this server implements; lets an older/newer client detect optional features (views, plan/tasks, credentials-vending) rather than guessing and getting a 404/501. If absent, assume the default endpoint set (all endpoints in the base spec except views/plan/scan-planning).
- Merge order per spec: server `defaults` are applied first, then any client-supplied config value overrides them, then server `overrides` are applied last and always win — i.e. `overrides` always take precedence over anything the client passed.
- **`prefix`**: once resolved (from `overrides.prefix`, or a client-configured `warehouse` name used directly, or empty), every catalog resource path below is `{catalog_uri}/v1/{prefix}/...`. `prefix` is URL-path-encoded as a single opaque segment (it may itself contain `/`-like structure but is escaped, e.g. `%2F`).

Base URL shape used throughout this document:

```
{uri}/v1/config                                   # bootstrap, no prefix
{uri}/v1/oauth/tokens                              # token exchange, no prefix
{uri}/v1/{prefix}/namespaces                       # everything else, prefix required
{uri}/v1/{prefix}/namespaces/{namespace}
{uri}/v1/{prefix}/namespaces/{namespace}/tables
{uri}/v1/{prefix}/namespaces/{namespace}/tables/{table}
{uri}/v1/{prefix}/namespaces/{namespace}/views
```

`{uri}` is the operator-supplied catalog root, e.g. `https://catalog.example.com/iceberg`. `warehouse` is a connector-level config parameter — sent as the `?warehouse=` query param on `/v1/config` and echoed by pyiceberg's `RestCatalog(warehouse=...)`; it is how a multi-warehouse catalog server picks which storage location/tenant to scope the session to.

---

## **Object List**

The object list is **retrieved dynamically via API calls** — there is no static table list, since the whole point of the REST Catalog is to enumerate whatever namespaces/tables a given warehouse contains.

Object hierarchy: `catalog (uri + prefix) → namespace (possibly nested) → table | view`.

### `GET /v1/{prefix}/namespaces` — list namespaces

Query params:
| Param | Required | Description |
|---|---|---|
| `parent` | No | A multipart namespace, encoded with the namespace separator (default unit-separator `0x1F`, percent-encoded as `%1F`), to list only the direct children of that namespace. Omit to list top-level namespaces. |
| `pageToken` | No | Opaque continuation token from a prior response's `next-page-token`. |
| `pageSize` | No | Hint for page size; server may cap or ignore. |

Example — list nested namespaces under `accounting`:
```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://catalog.example.com/iceberg/v1/my_catalog/namespaces?parent=accounting"
```

Response (`ListNamespacesResponse`):
```json
{
  "namespaces": [
    ["accounting", "tax"],
    ["accounting", "reporting"]
  ],
  "next-page-token": null
}
```

Notes:
- A namespace is always represented as a **JSON array of levels** (`["accounting", "tax"]` for a nested namespace, `["accounting"]` for a top-level one) in list responses.
- When a namespace must appear as a single **path segment** (e.g. `GET /v1/{prefix}/namespaces/{namespace}`), the levels are joined with the namespace separator and the whole joined string is URL-encoded once more: `accounting%1Ftax` (the `0x1F` byte, itself percent-encoded).
- Pagination: `pageToken`/`pageSize` on the request, `next-page-token` on the response; `null` or absent means no more pages.

### `GET /v1/{prefix}/namespaces/{namespace}/tables` — list tables in a namespace

Query params: `pageToken`, `pageSize` (same semantics as above).

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://catalog.example.com/iceberg/v1/my_catalog/namespaces/accounting%1Ftax/tables"
```

Response (`ListTablesResponse`):
```json
{
  "identifiers": [
    { "namespace": ["accounting", "tax"], "name": "invoices" },
    { "namespace": ["accounting", "tax"], "name": "payments" }
  ],
  "next-page-token": null
}
```

### `GET /v1/{prefix}/namespaces/{namespace}/views` — list views (optional)

Same shape as the tables list, but for view metadata objects (`ListTablesResponse`-equivalent `identifiers` array). Only present on servers whose `/v1/config` `endpoints` include view routes (view support was added later in the spec and is not universal — e.g. some catalogs implement tables-only). The connector should probe `endpoints` (or tolerate a 404/501) before treating views as available.

**Connector object model derived from the above:**

| Lakeflow object | Source | Notes |
|---|---|---|
| `namespaces` | `GET /v1/{prefix}/namespaces` (recursive walk via `parent`) | metadata stream |
| `tables` | `GET /v1/{prefix}/namespaces/{ns}/tables` per namespace | metadata stream |
| `views` | `GET /v1/{prefix}/namespaces/{ns}/views` per namespace, if supported | metadata stream, optional |
| `table_schemas` | flattened from `LoadTableResult.metadata.schemas` | metadata stream, derived, no dedicated endpoint |
| `table_snapshots` | flattened from `LoadTableResult.metadata.snapshots` | metadata stream, derived |
| `table_partition_specs` | flattened from `LoadTableResult.metadata.partition-specs` | metadata stream, derived |
| `table_properties` | flattened from `LoadTableResult.metadata.properties` (key/value) | metadata stream, derived |
| `<user-configured table>` row data | pyiceberg `catalog.load_table(...).scan(...)` over the table's data/Parquet files | data stream, per-table opt-in |

---

## **Object Schema**

### `GET /v1/{prefix}/namespaces/{namespace}` — namespace properties

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://catalog.example.com/iceberg/v1/my_catalog/namespaces/accounting%1Ftax"
```

Response (`GetNamespaceResponse`):
```json
{
  "namespace": ["accounting", "tax"],
  "properties": {
    "owner": "finance-team",
    "location": "s3://my-bucket/warehouse/accounting/tax",
    "created-at": "2024-01-15T10:00:00Z"
  }
}
```
`properties` is a free-form string-to-string map; there is no fixed schema for it beyond a few conventional keys (`location`, `owner`, `exists`).

### `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}` — load table (the core metadata call)

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://catalog.example.com/iceberg/v1/my_catalog/namespaces/accounting%1Ftax/tables/invoices"
```

Query params: none required. `X-Iceberg-Access-Delegation: vended-credentials` request header may be sent to ask the server to include short-lived storage credentials in the response's `config` (or `storage-credentials`) block, so the client doesn't need its own long-lived cloud credentials.

Response (`LoadTableResult`), abbreviated but representative:
```json
{
  "metadata-location": "s3://my-bucket/warehouse/accounting/tax/invoices/metadata/00003-9b1d6dd8.metadata.json",
  "metadata": {
    "format-version": 2,
    "table-uuid": "9c12a76c-3c1f-4e8a-9d7d-1f7bcbb2f1a1",
    "location": "s3://my-bucket/warehouse/accounting/tax/invoices",
    "last-sequence-number": 12,
    "last-updated-ms": 1717000000000,
    "last-column-id": 6,
    "current-schema-id": 1,
    "schemas": [
      {
        "schema-id": 0,
        "type": "struct",
        "fields": [
          { "id": 1, "name": "invoice_id", "required": true, "type": "long" },
          { "id": 2, "name": "customer_id", "required": true, "type": "long" },
          { "id": 3, "name": "amount", "required": false, "type": "decimal(18,2)" },
          { "id": 4, "name": "created_at", "required": false, "type": "timestamptz" }
        ]
      },
      {
        "schema-id": 1,
        "type": "struct",
        "fields": [
          { "id": 1, "name": "invoice_id", "required": true, "type": "long" },
          { "id": 2, "name": "customer_id", "required": true, "type": "long" },
          { "id": 3, "name": "amount", "required": false, "type": "decimal(18,2)" },
          { "id": 4, "name": "created_at", "required": false, "type": "timestamptz" },
          { "id": 6, "name": "status", "required": false, "type": "string" }
        ]
      }
    ],
    "default-spec-id": 0,
    "partition-specs": [
      {
        "spec-id": 0,
        "fields": [
          { "source-id": 4, "field-id": 1000, "name": "created_at_day", "transform": "day" }
        ]
      }
    ],
    "last-partition-id": 1000,
    "default-sort-order-id": 0,
    "sort-orders": [
      {
        "order-id": 0,
        "fields": [
          { "source-id": 4, "transform": "identity", "direction": "asc", "null-order": "nulls-last" }
        ]
      }
    ],
    "properties": {
      "write.format.default": "parquet",
      "write.parquet.compression-codec": "zstd",
      "owner": "finance-team"
    },
    "current-snapshot-id": 8721340192837465000,
    "snapshots": [
      {
        "snapshot-id": 8721340192837465000,
        "parent-snapshot-id": 8721340192837460000,
        "sequence-number": 12,
        "timestamp-ms": 1717000000000,
        "manifest-list": "s3://my-bucket/warehouse/.../snap-8721340192837465000.avro",
        "summary": {
          "operation": "append",
          "added-data-files": "4",
          "added-records": "15000",
          "added-files-size": "2048000",
          "changed-partition-count": "3",
          "total-records": "150000",
          "total-data-files": "40"
        },
        "schema-id": 1
      },
      {
        "snapshot-id": 8721340192837460000,
        "parent-snapshot-id": null,
        "sequence-number": 11,
        "timestamp-ms": 1716900000000,
        "manifest-list": "s3://my-bucket/warehouse/.../snap-8721340192837460000.avro",
        "summary": { "operation": "append", "added-data-files": "40", "added-records": "135000" },
        "schema-id": 0
      }
    ],
    "snapshot-log": [
      { "timestamp-ms": 1716900000000, "snapshot-id": 8721340192837460000 },
      { "timestamp-ms": 1717000000000, "snapshot-id": 8721340192837465000 }
    ],
    "metadata-log": [
      { "timestamp-ms": 1716900000000, "metadata-file": "s3://.../00002-....metadata.json" }
    ],
    "refs": {
      "main": { "snapshot-id": 8721340192837465000, "type": "branch" }
    }
  },
  "config": {
    "s3.access-key-id": "ASIA...vended...",
    "s3.secret-access-key": "...vended...",
    "s3.session-token": "...vended...",
    "client.region": "us-east-1"
  }
}
```

`config` above is the **vended storage credentials / property overrides for this one table** — separate from catalog-level auth. Newer spec revisions instead return a top-level `storage-credentials` array (keyed by storage-location prefix, so a table spanning multiple buckets/paths can get distinct credential sets):
```json
"storage-credentials": [
  {
    "prefix": "s3://my-bucket/warehouse/accounting/tax/invoices",
    "config": {
      "s3.access-key-id": "ASIA...",
      "s3.secret-access-key": "...",
      "s3.session-token": "..."
    }
  }
]
```
pyiceberg merges whichever shape the server returns into the `FileIO` properties used for that table's scan, so the connector does not need its own logic for this — `load_table()` handles it.

### Metadata object structure — flattening guide for the connector's metadata streams

| Metadata field | Type | Flattening target stream | Notes |
|---|---|---|---|
| `metadata.schemas[]` | array of `Schema` (`schema-id`, `type`, `fields[]`) | `table_schemas` | One row per `(table, schema-id, field.id)`: `field.id`, `field.name`, `field.required`, `field.type` (may be primitive string or nested `struct`/`list`/`map` — see Field Type Mapping). `current-schema-id` marks the active one. |
| `metadata.partition-specs[]` | array of `PartitionSpec` (`spec-id`, `fields[]`) | `table_partition_specs` | One row per `(table, spec-id, field.field-id)`: `source-id`, `field-id`, `name`, `transform` (`identity`, `year`, `month`, `day`, `hour`, `bucket[N]`, `truncate[W]`, `void`). `default-spec-id` marks the active one. |
| `metadata.snapshots[]` | array of `Snapshot` | `table_snapshots` | One row per `(table, snapshot-id)`: `snapshot-id`, `parent-snapshot-id`, `sequence-number`, `timestamp-ms`, `manifest-list`, `schema-id`, `summary.operation` (`append`\|`overwrite`\|`replace`\|`delete`), plus the rest of `summary` as a flattened/JSON-text map (`added-data-files`, `added-records`, `deleted-data-files`, `deleted-records`, `total-records`, `total-data-files`, etc. — all string-typed per spec even though numeric). `current-snapshot-id` marks HEAD. |
| `metadata.snapshot-log[]` | array of `{timestamp-ms, snapshot-id}` | append onto `table_snapshots` as `logged-at` / or a separate small stream if the connector wants HEAD-history distinct from all-known-snapshots | Useful for reconstructing "what was HEAD at time T" without re-listing all snapshots. |
| `metadata.sort-orders[]` | array of `SortOrder` (`order-id`, `fields[]`) | optional `table_sort_orders` stream, or fold into doc only | `field.source-id`, `transform`, `direction` (`asc`/`desc`), `null-order`. `default-sort-order-id` marks active. |
| `metadata.properties` | map<string,string> | `table_properties` | One row per `(table, key, value)`. |
| `metadata.refs` | map<string, `SnapshotRef`> | optional, or fold into `table_snapshots` as a `ref-name` column | Branches/tags pointing at snapshot IDs (`main` branch is the default read target). |
| `metadata-location` (top-level, sibling of `metadata`) | string | carried as a column on every derived row for lineage/debugging | Points at the actual metadata JSON file on storage — changes on every commit. |

---

## **Get Object Primary Keys**

The REST Catalog spec has **no concept of primary keys** — Iceberg tables are schema+partition+snapshot metadata over immutable data files; row identity for CDC purposes is a data-layer concern (see below), not something the catalog API reports.

Primary keys for the connector's **metadata streams** are derived, not source-declared:

| Stream | Primary key (connector-derived) |
|---|---|
| `namespaces` | `namespace` (the joined multipart levels) |
| `tables` | `namespace`, `name` |
| `views` | `namespace`, `name` |
| `table_schemas` | `namespace`, `table`, `schema-id`, `field.id` |
| `table_partition_specs` | `namespace`, `table`, `spec-id`, `field-id` |
| `table_snapshots` | `namespace`, `table`, `snapshot-id` |
| `table_properties` | `namespace`, `table`, `key` |

Row-level primary keys for the **user-configured data table** stream: Iceberg v2 tables *may* declare **identifier fields** on a schema (`schema.identifier-field-ids`, a list of field IDs — the closest analogue to a primary key, used by row-level `MERGE`/upsert engines like Flink/Spark). When present, the connector should surface those columns as the table's primary key. When absent (v1 tables, or v2 tables written append-only without identifier fields), the object has **no primary key** and can only be ingested as `snapshot` or `append`.

---

## **Object's ingestion type**

| Object | Ingestion type | Why |
|---|---|---|
| `namespaces` | `snapshot` | Small, no natural cursor; catalog doesn't timestamp namespace changes. |
| `tables` (list) | `snapshot` | Same — table list has no change-cursor via this endpoint. |
| `views` | `snapshot` | Same. |
| `table_schemas` | `snapshot` | Re-derived fully on every metadata refresh (a schema's field set is small and versioned by `schema-id`, not incrementally appended). |
| `table_partition_specs` | `snapshot` | Same reasoning. |
| `table_properties` | `snapshot` | Same reasoning — small key/value set, cheap to re-pull whole each time. |
| `table_snapshots` | **`append`** | `metadata.snapshots[]` is a strictly append-only log keyed by monotonically increasing `sequence-number` / `timestamp-ms` (older snapshots are only removed by explicit expiry, which the connector should treat as a rare compaction event, not a normal delete). Cursor on `sequence-number` (integer, gapless-monotonic per table) or `timestamp-ms`. |
| `<user table>` row data | **`cdc`** when the table declares Iceberg row-level deletes/identifier fields and the connector reads snapshot-to-snapshot changes via pyiceberg's incremental append scan (`append_snapshot_id` / `to_snapshot_id`); **`append`** when only new data files are being read forward (typical for append-only fact tables, no update/delete awareness); **`snapshot`** as the fallback full-table read (first sync, or a table with no reliable snapshot lineage, e.g. after a `rewrite_manifests`/expire that breaks the parent chain the connector was tracking). `cdc_with_deletes` is **not cleanly supported**: Iceberg v2 row-level deletes (positional/equality delete files) are visible to a full table scan (rows simply disappear) but the REST spec + pyiceberg's scan API do not expose a "deleted since X" diff feed the way a CDC log would — reconstructing deletes requires comparing two full snapshots or reading the delete files directly, which is out of scope for the initial connector. Document this explicitly as a gap, matching the honesty pattern used in the `odata` connector's "what this connector does not cover" section. |

---

## **Read API for Data Retrieval**

### A. Metadata streams (catalog HTTP calls)

All metadata endpoints above are plain `GET`s. Pagination is `pageToken`/`pageSize` (opaque cursor, see Object List section) on the list endpoints (`namespaces`, `tables`, `views`); the single-object endpoints (`namespace` properties, `load table`) return one full JSON document with no pagination — the whole `metadata` blob comes back in one call, however large the snapshot list is (there is no way to fetch "snapshots since N" from the catalog API itself; the client fetches the whole document and filters client-side).

Recommended read pattern for a full metadata crawl:
```
GET /v1/config?warehouse=...                                        # once per session
GET /v1/{prefix}/namespaces                                         # top level
  → for each returned namespace, GET .../namespaces?parent=<ns>      # recurse until empty
GET /v1/{prefix}/namespaces/{ns}/tables                              # per namespace
GET /v1/{prefix}/namespaces/{ns}/tables/{table}                      # per table, full LoadTableResult
```

Incremental strategy for `table_snapshots`: keep a per-table cursor = last-seen `sequence-number` (preferred, strictly monotonic and gapless within one table's lineage) or `timestamp-ms` (fallback, monotonic but with coarser/duplicate-prone resolution across concurrent writers). Each poll re-`load_table()`s (there is no cheaper "just give me new snapshots" endpoint) and emits only `snapshots[]` entries with `sequence-number > last_seen`. This makes `table_snapshots` `append`-typed with a client-side cursor filter rather than a server-supported one — call this out clearly as a Iceberg-REST-doesn't-support-server-side-since-filtering limitation.

Everything else (`namespaces`, `tables`, `views`, `table_schemas`, `table_partition_specs`, `table_properties`) is re-pulled in full each run (`snapshot` ingestion) since there is no cursor concept for them at all.

### B. Data-row reads (pyiceberg table scan, not raw HTTP)

The connector does **not** hand-parse manifest lists/manifests or read Parquet/Avro files directly with HTTP GETs against storage. It delegates to pyiceberg:

```python
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog(
    name="my_catalog",
    **{
        "uri": "https://catalog.example.com/iceberg",
        "warehouse": "s3://my-bucket/warehouse",
        # one of the following auth blocks:
        "token": "<static bearer token>",
        # or:
        "credential": "<client_id>:<client_secret>",
        "oauth2-server-uri": "https://idp.example.com/oauth/token",
        "scope": "catalog",
        # storage props (only needed if the server does NOT vend credentials
        # via LoadTableResult.config / storage-credentials):
        "s3.region": "us-east-1",
        "s3.access-key-id": "...",
        "s3.secret-access-key": "...",
        # AWS SigV4 catalog auth instead of bearer/oauth2, if applicable:
        "rest.sigv4-enabled": "true",
        "rest.signing-region": "us-east-1",
        "rest.signing-name": "glue",
    },
)

table = catalog.load_table("accounting.tax.invoices")   # dotted identifier; pyiceberg splits on "."

# Snapshot / full read:
reader = table.scan(
    row_filter="created_at >= '2024-01-01'",   # optional predicate pushdown, translated to Iceberg expressions
    selected_fields=("invoice_id", "customer_id", "amount", "created_at", "status"),
    limit=None,
).to_arrow_batch_reader()   # -> pyarrow.RecordBatchReader, streamed lazily

for batch in reader:
    ...  # convert to Spark InternalRows / feed the Data Source API's PartitionReader

# Incremental / append read between two known snapshots:
incr = table.scan(
    from_snapshot_id=<previous_cursor_snapshot_id>,   # exclusive
    to_snapshot_id=<current_head_snapshot_id>,        # inclusive
).to_arrow_batch_reader()
```

Key facts for the connector's incremental data design:
- `table.current_snapshot().snapshot_id` (or `metadata.current-snapshot-id` from the catalog call) is the natural **cursor value** to persist between runs.
- pyiceberg's `from_snapshot_id`/`to_snapshot_id` scan mode only returns rows from **added data files** across that snapshot range (append-only semantics) — it does not synthesize before/after images for updates, and it can raise if the snapshot lineage between the two IDs contains anything other than pure appends (e.g. an intervening `overwrite`/`delete`/`rewrite` breaks the simple append-diff assumption). The connector should catch that case and fall back to a full `snapshot` re-read, or surface a clear error asking the operator to re-baseline.
- There is no pyiceberg-native "give me the deleted rows since snapshot X" call comparable to a CDC log; v2 delete files are automatically applied (rows removed) within a normal `table.scan()`, so deletions are invisible unless the connector diffs two full snapshots (expensive) or reads `row_filter`/`inspect` metadata tables (`table.inspect.snapshots()`, `.manifests()`, `.data_files()`, `.delete_files()` — useful for debugging/monitoring but not wired into this connector's initial data-read path).
- `selected_fields` is Iceberg's column projection (comparable to `$select`); `row_filter` is a string expression pyiceberg parses into its own predicate AST and pushes down to manifest/file pruning where possible (min/max stats, partition values) — this is the main "filter" mechanism, there is no query-parameter equivalent at the raw HTTP layer since scans never go through the REST Catalog API itself (only metadata does).
- `limit` caps rows returned per scan call — useful for a first "peek" during schema inference, not a pagination primitive (there is no true resumable row-level pagination token in pyiceberg's local-planning scan; batching a very large full scan into repeated `read_table` calls is a connector-level responsibility built on top of the Arrow batch reader, e.g. by paging through `to_arrow_batch_reader()` and stopping the generator once `max_records_per_batch` is hit, persisting only the snapshot-id cursor between calls — no true "resume mid-file" offset is exposed).

Newer Iceberg REST spec revisions also define a **remote scan-planning API** (`POST /v1/{prefix}/.../tables/{table}/plan`, `GET .../plan/{plan-id}`, `GET .../tables/{table}/tasks`) that lets a server do manifest planning server-side and hand back file tasks — this is an alternative to pyiceberg's local planning and is **not used** by this connector (pyiceberg's default `RestCatalog.load_table()` + local `scan()` planning is simpler, works against every REST catalog implementation regardless of whether it supports remote planning, and is what the task description asks for). Document it here only so a future maintainer knows it exists and why it was skipped.

### Rate limits

The spec itself defines no rate limits (it's a protocol, not a hosted service) — limits are operator-specific (e.g. AWS Glue's Iceberg REST endpoint inherits Glue API throttling: default ~<100s of requests/second per account with standard AWS API throttling/backoff via 429; Databricks Unity Catalog's Iceberg REST endpoint follows UC API quotas). The connector should:
- Respect `Retry-After` on 429/503 responses.
- Apply exponential backoff with jitter on 429/503/5xx, matching the pattern documented in other connectors in this repo (see `odata`'s `max_retries`/backoff behavior for the house style).
- Batch namespace/table crawls with modest concurrency (pyiceberg itself is not inherently rate-limit-aware).

### Error model

All error responses share one JSON shape (`ErrorModel`), wrapped in `{"error": {...}}`:
```json
{
  "error": {
    "message": "Namespace does not exist: accounting.tax",
    "type": "NoSuchNamespaceException",
    "code": 404
  }
}
```
| HTTP status | Meaning | Typical cause |
|---|---|---|
| 400 | Bad Request | Malformed namespace/table identifier, invalid query param |
| 401 | Unauthorized | Missing/invalid/expired bearer token, failed OAuth2 exchange, bad SigV4 signature |
| 403 | Forbidden | Authenticated but not authorized for this namespace/table/warehouse |
| 404 | Not Found | Namespace, table, or view does not exist |
| 409 | Conflict | Create-if-not-exists collision (namespace/table already exists) — relevant to write paths, rare on this connector's read-only paths |
| 419 | Authentication Timeout (non-standard, Iceberg-spec-specific) | Token expired mid-session; client should refresh via `/v1/oauth/tokens` (or its configured auth manager) and retry |
| 503 | Service Unavailable | Server overloaded/maintenance; retry with backoff, honor `Retry-After` |
| 500 | Internal Server Error | Generic catalog-server-side failure (also carries `ErrorModel`) |

---

## **Field Type Mapping**

Iceberg primitive types (from `schemas[].fields[].type`) map to Spark types as follows.

| Iceberg type | Spark type | Notes |
|---|---|---|
| `boolean` | `BooleanType` | |
| `int` | `IntegerType` | 32-bit signed |
| `long` | `LongType` | 64-bit signed |
| `float` | `FloatType` | 32-bit IEEE 754 |
| `double` | `DoubleType` | 64-bit IEEE 754 |
| `decimal(P,S)` | `DecimalType(P, S)` | Precision/scale from the type string itself |
| `date` | `DateType` | Days since epoch |
| `time` | `StringType` (no native Spark `TimeType`) | Iceberg time-of-day, microsecond precision, no timezone |
| `timestamp` | `TimestampNTZType` (or `TimestampType` if the framework has no NTZ type) | Microsecond precision, no timezone ("timestamp without timezone") |
| `timestamptz` | `TimestampType` | Microsecond precision, UTC-normalized |
| `timestamp_ns` / `timestamptz_ns` (v3 nanosecond variants) | `TimestampNTZType` / `TimestampType` | Nanosecond precision — note precision loss if downcast to microsecond-based Spark timestamp internals; document as a known lossy mapping |
| `string` | `StringType` | UTF-8 |
| `uuid` | `StringType` | Rendered as canonical UUID text |
| `fixed(L)` | `BinaryType` | Fixed-length byte array of length `L` |
| `binary` | `BinaryType` | Variable-length |
| `struct` | `StructType` (nested) | Recursively mapped fields, each carrying its own `id`/`required` |
| `list` | `ArrayType` | Element type recursively mapped; `element-required` governs nullability |
| `map` | `MapType` | Key/value types recursively mapped; Iceberg map keys are typically primitive |

Special behaviors:
- Every field carries a stable integer `id` (`field.id`) that survives renames — schema evolution in Iceberg is ID-based, not name-based. The connector should key its own internal column tracking off `id`, not `name`, when reconciling schema changes across `schema-id` versions.
- `required` (bool) maps to Spark nullability (`required: true` → non-nullable).
- Partition-derived columns are **not** separate physical columns in the data files for `identity` transforms in v2 (Iceberg hidden partitioning) — they are the same underlying source column; `year`/`month`/`day`/`hour`/`bucket[N]`/`truncate[W]` transforms are metadata-only partition keys, not extra output columns, and pyiceberg's Arrow output reflects only the table's actual schema fields, never synthetic partition columns, unless the row filter/select explicitly asks for them via Iceberg's `_partition` metadata column (special, optional, not enabled by default).
- Identifier fields (`schema.identifier-field-ids`) are the closest analogue to primary keys (see *Get Object Primary Keys*).

---

## **Recommended Streams / Tables**

Cross-referencing peer ecosystems, since none of the majors ship a first-class "Iceberg REST Catalog" *source* connector the way they do a database source — Iceberg is normally consumed by **query engines** (Trino, Spark, Flink) rather than ELT tools, so the closest signal is how those engines model catalog objects, not an Airbyte/Fivetran connector list:

- **Trino's `iceberg` connector** models `information_schema`-style system tables per Iceberg table: `"table_name$snapshots"`, `"table_name$history"`, `"table_name$partitions"`, `"table_name$files"`, `"table_name$manifests"`, `"table_name$refs"` — directly analogous to this doc's `table_snapshots` / `table_partition_specs` split.
- **Spark's Iceberg extensions** expose the same idea via `<table>.snapshots`, `<table>.history`, `<table>.files`, `<table>.manifests`, `<table>.partitions` metadata tables.
- **pyiceberg's `table.inspect`** module mirrors this: `.snapshots()`, `.history()`, `.partitions()`, `.manifests()`, `.data_files()`, `.delete_files()`, `.refs()` — all backed by reading the manifest/metadata files, no catalog-server support needed beyond `load_table()`.
- Airbyte/Fivetran have no dedicated "Iceberg REST catalog" source connector as of this research (both integrate with Iceberg only as a *destination* format); this doc's stream split is therefore based on the Trino/Spark/pyiceberg system-table convention above rather than an existing ELT connector's schema.

Final recommendation:

**Metadata streams** (homogeneous API pattern — plain GET + JSON, single pass, documented together):
- `namespaces`
- `tables`
- `table_schemas` (flattened)
- `table_snapshots` (flattened, `append` cursor on `sequence-number`)
- `table_partition_specs` (flattened)
- `table_properties` (flattened)

**Data stream:**
- One row-data stream per user-configured Iceberg table, read via pyiceberg `scan(...).to_arrow_batch_reader()`.

## Deferred Tables

- **`views`** (`GET /v1/{prefix}/namespaces/{namespace}/views`) — deferred. Not every REST catalog implementation supports views (feature added later in the spec, and the connector should probe `/v1/config`'s `endpoints` list or tolerate 404s); including it in the first metadata batch would force error-handling for an endpoint that may not exist on a given server. Low complexity to add later — same list/load shape as tables.
- **`table_sort_orders`** — deferred to a doc-only mention rather than a full connector stream. Sort orders are rarely queried independently of the schema/partition-spec streams and add another near-duplicate flattening pass for comparatively low value in a first cut.
- **Remote scan-planning API** (`POST .../tables/{table}/plan`, `.../plan/{plan-id}`, `.../tables/{table}/tasks`) — deferred entirely. This is an alternative server-side planning path to pyiceberg's default local planning; not needed for the connector's read strategy and adds a second, catalog-implementation-dependent code path for the same outcome.
- **Row-level delete reconciliation (`cdc_with_deletes`)** — deferred / explicitly unsupported for now. Reconstructing a true delete feed requires either diffing full snapshots or reading manifest/delete files directly (`table.inspect.delete_files()`), which is materially more complex than the append/snapshot/cdc paths documented above. Flagged as a known gap, not a table to research further at this time — revisit if a consuming pipeline needs deletes.
- **Table `refs` (branches/tags) as a first-class stream** — deferred; folding `refs` into `table_snapshots` as an optional `ref-name` column (noted in the Object Schema flattening table) is sufficient for now. A dedicated `table_refs` stream can be added later if branch/tag lifecycle tracking becomes a requirement.
- **`transactions/commit`, `tables/rename`, `views/rename`, `.../register`, `.../unregister`, `.../credentials`, `.../sign`, `.../metrics`, namespace-level `functions`** — all write-path or auxiliary endpoints in the OpenAPI spec, irrelevant to a read-only connector; not researched further.

---

## **Sources and References**

- Official REST Catalog spec (rendered): https://iceberg.apache.org/rest-catalog-spec/ — highest confidence.
- Official OpenAPI source of truth: https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml (and `open-api/README.md` in the same repo) — highest confidence; all endpoint paths, `CatalogConfig`, `ErrorModel`, `LoadTableResult`, namespace-separator behavior, and endpoint list in this doc were cross-checked against this file.
- pyiceberg configuration docs: https://py.iceberg.apache.org/configuration/ — high confidence; source for `RestCatalog` construction kwargs (`uri`, `token`, `credential`, `oauth2-server-uri`, `scope`, `warehouse`, `rest.sigv4-enabled`, `rest.signing-region`, `rest.signing-name`, `s3.*`).
- pyiceberg source (`pyiceberg/catalog/rest/__init__.py`, apache/iceberg-python on GitHub) — high confidence; used to confirm `RestCatalog` auth-manager behavior and OAuth2 credential-format handling.
- Trino Iceberg connector docs (metadata tables `$snapshots`, `$history`, `$partitions`, `$manifests`, `$files`, `$refs`) — medium confidence, used only to justify the metadata-stream split by analogy, not for REST Catalog wire-format details.
- Spark Iceberg extension metadata tables (`.snapshots`, `.history`, `.files`, `.manifests`, `.partitions`) — medium confidence, same purpose as above.
- DeepWiki `apache/iceberg` REST catalog overview page — medium confidence, used only as a cross-check/summary, not as a primary source for exact field names.
- No Airbyte/Fivetran-published "Iceberg REST Catalog" source connector was found; both vendors currently support Iceberg only as a write **destination/format**, not as an ELT **source**. This is called out explicitly rather than assumed — the stream-split recommendation in this doc is derived from query-engine metadata-table conventions (Trino/Spark/pyiceberg) instead.
- Where the OpenAPI spec and general web-search summaries disagreed on fine details (e.g. exact `config`-vs-`storage-credentials` shape across spec versions, or the precise `/v1/oauth/tokens` deprecation note), the OpenAPI YAML file content was treated as authoritative since it is the literal contract servers/clients implement against.
