"""Tests for the Iceberg REST Catalog connector.

Two layers:

* ``TestIcebergConnector`` runs the shared ``LakeflowConnectTests`` +
  ``SupportsPartitionedStreamTests`` contract suites against the in-process
  simulator at ``source_simulator/specs/iceberg/`` (a ``sim_catalog``-prefixed
  REST Catalog with a four-namespace tree and a two-table
  ``accounting.tax`` namespace). This is what CI runs — no credentials,
  no network.

* The module-level tests below cover behaviour the contract suite cannot
  reach: the init-time snapshot cursor cap (the corpus carries a year-2099
  snapshot that must never be emitted), the ``/v1/config`` prefix-override
  merge rule, and the opt-in pyiceberg data-table stream, whose row read is
  guarded behind an optional import.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Iterator

import pytest
import requests.sessions
from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    LongType,
    StringType,
    StructType,
)

from databricks.labs.community_connector.source_simulator import (
    MODE_SIMULATE,
    Simulator,
)
from databricks.labs.community_connector.sources.iceberg.iceberg import (
    IcebergLakeflowConnect,
)
from tests.unit.sources.test_partition_suite import SupportsPartitionedStreamTests
from tests.unit.sources.test_suite import LakeflowConnectTests

# The simulator spec anchors its path patterns and allows at most two base
# segments ahead of ``/v1``, so the catalog root here keeps a single one.
_URI = "https://iceberg.example.test/iceberg"
_TOKEN = "simulator-fake-token"
_WAREHOUSE = "s3://sim-bucket/warehouse"

_SPEC_PATH = (
    Path(__file__).resolve().parents[4]
    / "src/databricks/labs/community_connector/source_simulator/specs/iceberg/endpoints.yaml"
)
_CORPUS_DIR = (
    Path(__file__).resolve().parents[4]
    / "src/databricks/labs/community_connector/source_simulator/specs/iceberg/corpus"
)

#: ``load_table.json`` carries a snapshot dated 2099-01-01 specifically so a
#: connector that fails to cap its cursor at init time leaks it into
#: ``table_snapshots``.
_FUTURE_SNAPSHOT_TIMESTAMP_MS = 4070908800000
_FUTURE_SNAPSHOT_ID = 8721340192837470000

_HAS_PYICEBERG = importlib.util.find_spec("pyiceberg") is not None


class TestIcebergConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    """Contract suite for the six catalog-metadata streams.

    ``data_tables`` is deliberately left unset: the row-data stream needs
    ``pyiceberg``, which talks to object storage rather than going through
    ``requests``, so the simulator cannot stand in for it. That path is
    covered by ``TestDataTableStream`` below.

    Every metadata stream is partitioned (``is_partitioned`` returns True for
    all of ``METADATA_TABLES``), so the ``read_table``-shaped tests in
    ``LakeflowConnectTests`` skip and the partition mixin does the work.
    """

    connector_class = IcebergLakeflowConnect
    simulator_source = "iceberg"
    # The simulator never validates credentials — these only have to satisfy
    # ``RestCatalogClient.__init__`` so a bearer header can be built.
    replay_config = {
        "uri": _URI,
        "token": _TOKEN,
        "warehouse": _WAREHOUSE,
    }


# ---------------------------------------------------------------------------
# Shared simulator fixtures for the scenario tests
# ---------------------------------------------------------------------------


@pytest.fixture
def simulator() -> Iterator[Simulator]:
    """A ``Simulator(SIMULATE)`` bound to the committed Iceberg corpus."""
    with Simulator(
        mode=MODE_SIMULATE, spec_path=_SPEC_PATH, corpus_dir=_CORPUS_DIR
    ) as sim:
        yield sim


@pytest.fixture
def connector(simulator: Simulator) -> IcebergLakeflowConnect:  # noqa: ARG001
    return IcebergLakeflowConnect(
        {"uri": _URI, "token": _TOKEN, "warehouse": _WAREHOUSE}
    )


@pytest.fixture
def sent_requests(simulator: Simulator) -> Iterator[list]:  # noqa: ARG001
    """Capture every ``PreparedRequest`` on its way into the simulator.

    Layered on top of the simulator's own ``Session.send`` patch (hence the
    ``simulator`` dependency — the wrapper must be installed second and
    removed first) so tests can assert on headers and query params, which
    the simulator does not otherwise expose in simulate mode.
    """
    captured: list = []
    original = requests.sessions.Session.send

    def spy(self, request, **kwargs):
        captured.append(request)
        return original(self, request, **kwargs)

    requests.sessions.Session.send = spy
    try:
        yield captured
    finally:
        requests.sessions.Session.send = original


# ---------------------------------------------------------------------------
# 0. Authentication
# ---------------------------------------------------------------------------


class TestAuthentication:
    """Static bearer token vs. the OAuth2 client-credentials exchange."""

    def test_static_token_never_calls_the_token_endpoint(self, sent_requests):
        conn = IcebergLakeflowConnect({"uri": _URI, "token": _TOKEN})
        _read_all(conn, "namespaces")
        assert not [r for r in sent_requests if r.method == "POST"]
        gets = [r for r in sent_requests if r.method == "GET"]
        assert gets
        assert all(r.headers["Authorization"] == f"Bearer {_TOKEN}" for r in gets)

    def test_credential_is_exchanged_for_a_bearer_token(self, sent_requests):
        conn = IcebergLakeflowConnect(
            {"uri": _URI, "credential": "sim-client:sim-secret"}
        )
        _read_all(conn, "namespaces")

        posts = [r for r in sent_requests if r.method == "POST"]
        assert len(posts) == 1, "the exchanged token must be cached, not re-fetched"
        assert posts[0].url.endswith("/v1/oauth/tokens")
        body = posts[0].body or ""
        assert "grant_type=client_credentials" in body
        assert "client_id=sim-client" in body
        assert "client_secret=sim-secret" in body
        assert "scope=catalog" in body

        gets = [r for r in sent_requests if r.method == "GET"]
        assert gets
        assert all(
            r.headers["Authorization"] == "Bearer simulated-iceberg-access-token"
            for r in gets
        )

    def test_client_id_and_secret_are_equivalent_to_credential(self, sent_requests):
        conn = IcebergLakeflowConnect(
            {
                "uri": _URI,
                "client_id": "sim-client",
                "client_secret": "sim-secret",
                "scope": "PRINCIPAL_ROLE:ALL",
            }
        )
        _read_all(conn, "namespaces")
        posts = [r for r in sent_requests if r.method == "POST"]
        assert len(posts) == 1
        assert "client_id=sim-client" in (posts[0].body or "")
        assert "scope=PRINCIPAL_ROLE%3AALL" in (posts[0].body or "")

    def test_sigv4_signs_every_metadata_request(self, sent_requests):
        """AWS S3 Tables rejects anything unsigned, including the bootstrap.

        The row-data path forwards these options to pyiceberg, but the six
        metadata streams are plain ``requests`` calls — if the client does not
        sign them itself, every one of them 403s.
        """
        pytest.importorskip("botocore")
        conn = IcebergLakeflowConnect(
            {
                "uri": _URI,
                "rest.sigv4-enabled": "true",
                "rest.signing-name": "s3tables",
                "rest.signing-region": "ap-southeast-1",
                "s3.access-key-id": "AKIAIOSFODNN7EXAMPLE",
                "s3.secret-access-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            }
        )
        _read_all(conn, "namespaces")

        gets = [r for r in sent_requests if r.method == "GET"]
        assert gets
        for request in gets:
            authorization = request.headers.get("Authorization", "")
            assert authorization.startswith("AWS4-HMAC-SHA256 "), (
                f"unsigned request to {request.url}"
            )
            assert "Credential=AKIAIOSFODNN7EXAMPLE/" in authorization
            assert "/ap-southeast-1/s3tables/aws4_request" in authorization
            assert "Signature=" in authorization
            assert "X-Amz-Date" in request.headers

    def test_sigv4_suppresses_the_bearer_token(self, sent_requests):
        """A bearer header alongside a signature is dead weight at best.

        It also means a catalog configured with both would burn an OAuth
        exchange on every client for a token nothing reads.
        """
        pytest.importorskip("botocore")
        conn = IcebergLakeflowConnect(
            {
                "uri": _URI,
                "token": _TOKEN,
                "rest.sigv4-enabled": "true",
                "rest.signing-name": "s3tables",
                "rest.signing-region": "ap-southeast-1",
                "s3.access-key-id": "AKIAIOSFODNN7EXAMPLE",
                "s3.secret-access-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            }
        )
        _read_all(conn, "namespaces")
        gets = [r for r in sent_requests if r.method == "GET"]
        assert gets
        assert not any(
            r.headers.get("Authorization", "").startswith("Bearer") for r in gets
        )

    def test_sigv4_without_a_region_fails_fast(self):
        """A missing region yields an unusable signature, so refuse up front."""
        with pytest.raises(ValueError) as excinfo:
            IcebergLakeflowConnect(
                {"uri": _URI, "rest.sigv4-enabled": "true", "token": _TOKEN}
            )
        assert "rest.signing-region" in str(excinfo.value)

    def test_sigv4_off_by_default_keeps_bearer_auth(self, sent_requests):
        """The signing options must not change behaviour unless switched on."""
        conn = IcebergLakeflowConnect(
            {
                "uri": _URI,
                "token": _TOKEN,
                "rest.signing-name": "s3tables",
                "rest.signing-region": "ap-southeast-1",
            }
        )
        _read_all(conn, "namespaces")
        gets = [r for r in sent_requests if r.method == "GET"]
        assert gets
        assert all(r.headers["Authorization"] == f"Bearer {_TOKEN}" for r in gets)

    def test_warehouse_is_sent_on_the_config_bootstrap(self, sent_requests):
        conn = IcebergLakeflowConnect(
            {"uri": _URI, "token": _TOKEN, "warehouse": _WAREHOUSE}
        )
        # Every catalog URL is built from the resolved prefix, so a full crawl
        # reads the config dozens of times — it must still hit the wire once.
        _read_all(conn, "table_properties")
        config_calls = [r for r in sent_requests if "/v1/config" in r.url]
        assert len(config_calls) == 1, (
            f"/v1/config must be fetched once and cached, saw {len(config_calls)}"
        )
        assert "warehouse=" in config_calls[0].url

    def test_vended_credentials_adds_the_delegation_header(self, sent_requests):
        conn = IcebergLakeflowConnect(
            {"uri": _URI, "token": _TOKEN, "vended_credentials": "true"}
        )
        _read_all(conn, "table_properties")
        load_calls = [r for r in sent_requests if "/tables/" in r.url]
        assert load_calls
        assert all(
            r.headers.get("X-Iceberg-Access-Delegation") == "vended-credentials"
            for r in load_calls
        )
        # The header is opt-in; nothing else should carry it.
        other = [r for r in sent_requests if "/tables/" not in r.url]
        assert all("X-Iceberg-Access-Delegation" not in r.headers for r in other)

    def test_missing_uri_is_rejected_at_construction(self, simulator: Simulator):  # noqa: ARG002
        with pytest.raises(ValueError, match="requires a 'uri' option"):
            IcebergLakeflowConnect({"token": _TOKEN})


def _read_all(conn: IcebergLakeflowConnect, table: str, opts: dict = None) -> list:
    """Drain every partition of a metadata stream into one list."""
    opts = opts or {}
    rows = []
    for partition in conn.get_partitions(table, opts):
        rows.extend(conn.read_partition(table, partition, opts))
    return rows


# ---------------------------------------------------------------------------
# 1. Config bootstrap — the server's ``overrides`` win over client config
# ---------------------------------------------------------------------------


class TestConfigBootstrap:
    """``GET /v1/config`` resolves the prefix every later route is built on."""

    def test_server_override_beats_client_prefix(self, simulator: Simulator):  # noqa: ARG002
        # The corpus returns ``overrides.prefix = sim_catalog``. Per the REST
        # spec's merge order (defaults < client < overrides) the client's
        # ``prefix`` must lose.
        conn = IcebergLakeflowConnect(
            {"uri": _URI, "token": _TOKEN, "prefix": "client_supplied"}
        )
        assert conn._client.prefix == "sim_catalog"

    def test_namespace_separator_from_defaults(self, connector):
        assert connector._client.namespace_separator == "\x1f"

    def test_catalog_prefix_stamped_on_rows(self, connector):
        rows = _read_all(connector, "namespaces")
        assert rows
        assert {row["catalog_prefix"] for row in rows} == {"sim_catalog"}


# ---------------------------------------------------------------------------
# 2. Namespace walk — recursion via ``?parent=`` + handler pagination
# ---------------------------------------------------------------------------


class TestNamespaceWalk:
    """The connector discovers nested namespaces by re-querying with ``parent``."""

    def test_walk_finds_nested_namespaces(self, connector):
        rows = _read_all(connector, "namespaces")
        assert sorted(row["namespace"] for row in rows) == [
            "accounting",
            "accounting.reporting",
            "accounting.tax",
            "sales",
        ]

    def test_parent_and_depth_derived_from_levels(self, connector):
        by_name = {row["namespace"]: row for row in _read_all(connector, "namespaces")}
        assert by_name["accounting"]["parent"] is None
        assert by_name["accounting"]["depth"] == 1
        assert by_name["accounting.tax"]["parent"] == "accounting"
        assert by_name["accounting.tax"]["depth"] == 2
        assert by_name["accounting.tax"]["levels"] == ["accounting", "tax"]

    def test_namespace_properties_can_be_skipped(self, connector):
        """``include_namespace_properties=false`` drops the per-namespace GET."""
        rows = _read_all(
            connector, "namespaces", {"include_namespace_properties": "false"}
        )
        assert rows
        assert all(row["owner"] is None for row in rows)
        assert all(row["location"] is None for row in rows)

    def test_max_namespace_depth_truncates_the_walk(self, simulator: Simulator):  # noqa: ARG002
        conn = IcebergLakeflowConnect({"uri": _URI, "token": _TOKEN})
        rows = _read_all(conn, "namespaces", {"max_namespace_depth": "1"})
        # Depth 1 means "list the roots, never recurse into them".
        assert sorted(row["namespace"] for row in rows) == ["accounting", "sales"]

    def test_pagination_walk_returns_every_namespace_once(
        self, simulator: Simulator  # noqa: ARG002
    ):
        """``page_size=1`` forces the handler's ``next-page-token`` loop."""
        conn = IcebergLakeflowConnect(
            {"uri": _URI, "token": _TOKEN, "page_size": "1"}
        )
        names = [row["namespace"] for row in _read_all(conn, "namespaces")]
        assert sorted(names) == [
            "accounting",
            "accounting.reporting",
            "accounting.tax",
            "sales",
        ]
        assert len(names) == len(set(names)), f"duplicate namespaces: {names}"


# ---------------------------------------------------------------------------
# 3. Table listing
# ---------------------------------------------------------------------------


class TestTableListing:
    def test_identifiers_are_stamped_with_the_requested_namespace(self, connector):
        rows = _read_all(connector, "tables")
        assert rows
        for row in rows:
            assert row["identifier"] == f"{row['namespace']}.{row['name']}"
        # One partition per namespace, two identifiers served per namespace.
        assert len(rows) == 4 * 2
        keys = [(row["namespace"], row["name"]) for row in rows]
        assert len(keys) == len(set(keys)), f"duplicate (namespace, name): {keys}"

    def test_one_partition_per_namespace(self, connector):
        partitions = connector.get_partitions("tables", {})
        assert [p["kind"] for p in partitions] == ["namespace"] * 4


# ---------------------------------------------------------------------------
# 4. LoadTableResult flattening
# ---------------------------------------------------------------------------


class TestLoadTableFlattening:
    def test_schema_rows_cover_nested_fields(self, connector):
        rows = _read_all(connector, "table_schemas")
        current = [
            row
            for row in rows
            if row["is_current_schema"]
            and row["namespace"] == "accounting.tax"
            and row["table_name"] == "invoices"
        ]
        by_path = {row["field_path"]: row for row in current}
        # Nested struct members are flattened with a dotted path and carry a
        # parent field id.
        assert "billing_address.city" in by_path
        assert by_path["billing_address.city"]["parent_field_id"] == 7
        # ``identifier-field-ids: [1]`` on schema 1.
        assert by_path["invoice_id"]["is_identifier_field"] is True
        assert by_path["customer_id"]["is_identifier_field"] is False
        assert by_path["status"]["field_doc"] == "Lifecycle state of the invoice."
        assert by_path["line_item_ids"]["field_type"] == "list"

    def test_partition_spec_rows_resolve_source_field_names(self, connector):
        rows = _read_all(connector, "table_partition_specs")
        by_spec = {row["spec_id"]: row for row in rows}
        assert by_spec[0]["is_default_spec"] is True
        assert by_spec[0]["source_field_name"] == "created_at"
        assert by_spec[0]["transform"] == "day"
        assert by_spec[1]["is_default_spec"] is False
        assert by_spec[1]["source_field_name"] == "customer_id"

    def test_property_rows_are_one_per_key(self, connector):
        rows = _read_all(connector, "table_properties")
        first_table = [
            row
            for row in rows
            if row["namespace"] == "accounting.tax" and row["table_name"] == "invoices"
        ]
        assert {row["key"] for row in first_table} == {
            "write.format.default",
            "write.parquet.compression-codec",
            "owner",
        }


# ---------------------------------------------------------------------------
# 5. Snapshot cursor cap — the reason ``load_table.json`` holds a 2099 snapshot
# ---------------------------------------------------------------------------


class TestSnapshotCursorCap:
    """``latest_offset``/``get_partitions`` pin the window at instance init.

    Without the cap a ``Trigger.AvailableNow`` run would keep chasing
    snapshots committed while it catches up. The corpus proves the cap is
    live by carrying a snapshot dated 2099-01-01.
    """

    def test_future_snapshot_is_excluded(self, connector):
        rows = _read_all(connector, "table_snapshots")
        assert rows, "expected snapshots from the committed corpus"
        assert all(row["timestamp_ms"] <= connector._init_ms for row in rows)
        assert _FUTURE_SNAPSHOT_ID not in {row["snapshot_id"] for row in rows}
        assert _FUTURE_SNAPSHOT_TIMESTAMP_MS not in {
            row["timestamp_ms"] for row in rows
        }

    def test_partition_carries_the_window(self, connector):
        partitions = connector.get_partitions("table_snapshots", {})
        assert partitions
        for partition in partitions:
            assert partition["since_ms"] == 0
            assert partition["until_ms"] == connector._init_ms

    def test_second_run_only_sees_snapshots_after_the_first_cursor(
        self, simulator: Simulator  # noqa: ARG002
    ):
        """A fresh instance treats the previous run's ``cursor`` as ``since``."""
        first = IcebergLakeflowConnect({"uri": _URI, "token": _TOKEN})
        end_offset = first.latest_offset("table_snapshots", {})

        second = IcebergLakeflowConnect({"uri": _URI, "token": _TOKEN})
        partitions = second.get_partitions("table_snapshots", {}, end_offset, None)
        assert partitions
        assert all(p["since_ms"] == end_offset["cursor"] for p in partitions)
        # Every corpus snapshot predates the first run, so the second run
        # finds nothing new.
        rows = [
            row
            for p in partitions
            for row in second.read_partition("table_snapshots", p, {})
        ]
        assert rows == []

    def test_snapshot_refs_and_current_flag(self, connector):
        rows = _read_all(connector, "table_snapshots")
        current = [row for row in rows if row["is_current_snapshot"]]
        assert current, "expected exactly one snapshot flagged current per table"
        assert all(row["ref_names"] == ["main"] for row in current)


# ---------------------------------------------------------------------------
# 6. Crawl admission control
# ---------------------------------------------------------------------------


class TestObjectBatching:
    """``max_objects_per_batch`` is the metadata streams' admission-control knob."""

    def test_crawl_advances_one_object_at_a_time(self, connector):
        opts = {"max_objects_per_batch": "1"}
        total = len(connector.get_partitions("table_properties", opts))
        assert total == 8  # 4 namespaces x 2 table identifiers each

        offset = {}
        seen = 0
        for step in range(1, total + 1):
            end = connector.latest_offset("table_properties", opts, offset)
            assert end["object_offset"] == step
            partitions = connector.get_partitions(
                "table_properties", opts, offset, end
            )
            assert len(partitions) == 1
            seen += 1
            offset = end
        # Drained: the offset stops moving, which is what ends the trigger.
        assert connector.latest_offset("table_properties", opts, offset) == offset
        assert seen == total

    def test_max_records_per_batch_derives_an_object_count(self, connector):
        # 100 rows / ESTIMATED_ROWS_PER_OBJECT (20) => 5 objects.
        end = connector.latest_offset(
            "table_properties", {"max_records_per_batch": "100"}, {}
        )
        assert end["object_offset"] == 5


# ---------------------------------------------------------------------------
# 7. The single-driver ``read_table`` fallback
# ---------------------------------------------------------------------------


class TestBatchReadTable:
    """``read_table`` is the batch path for the metadata streams.

    ``LakeflowConnectTests.test_read_table`` / ``test_read_terminates`` skip
    for this connector because ``is_partitioned`` is True for all six
    streams, so the contract they check is re-asserted here by hand.
    """

    @pytest.mark.parametrize(
        "table",
        [
            "namespaces",
            "tables",
            "table_schemas",
            "table_snapshots",
            "table_partition_specs",
            "table_properties",
        ],
    )
    def test_read_table_returns_the_same_rows_as_the_partitioned_path(
        self, simulator: Simulator, table: str  # noqa: ARG002
    ):
        conn = IcebergLakeflowConnect({"uri": _URI, "token": _TOKEN})
        records, offset = conn.read_table(table, {}, {})
        rows = list(records)
        assert rows, f"{table} produced no rows on the first read"
        assert isinstance(offset, dict)
        # Same instance, so the crawl is already cached — compare against a
        # fresh instance's partitioned drain.
        other = IcebergLakeflowConnect({"uri": _URI, "token": _TOKEN})
        assert rows == _read_all(other, table)

    @pytest.mark.parametrize(
        "table",
        [
            "namespaces",
            "tables",
            "table_schemas",
            "table_snapshots",
            "table_partition_specs",
            "table_properties",
        ],
    )
    def test_read_table_converges(self, simulator: Simulator, table: str):  # noqa: ARG002
        """Trigger.AvailableNow ends when the offset stops changing."""
        conn = IcebergLakeflowConnect({"uri": _URI, "token": _TOKEN})
        offset: dict = {}
        for iteration in range(10):
            records, new_offset = conn.read_table(table, offset, {})
            list(records)
            if new_offset == offset:
                assert iteration > 0, "converged before reading anything"
                return
            offset = new_offset
        pytest.fail(f"{table} did not converge in 10 iterations (last: {offset})")

    def test_read_table_drains_across_several_microbatches(
        self, simulator: Simulator  # noqa: ARG002
    ):
        """One object per batch: every row still arrives exactly once."""
        conn = IcebergLakeflowConnect({"uri": _URI, "token": _TOKEN})
        expected = _read_all(
            IcebergLakeflowConnect({"uri": _URI, "token": _TOKEN}), "table_properties"
        )

        opts = {"max_objects_per_batch": "1"}
        collected: list = []
        offset: dict = {}
        for _ in range(20):
            records, new_offset = conn.read_table("table_properties", offset, opts)
            collected.extend(records)
            if new_offset == offset:
                break
            offset = new_offset
        assert collected == expected


# ---------------------------------------------------------------------------
# 8. The opt-in pyiceberg data-table stream
# ---------------------------------------------------------------------------


@pytest.fixture
def data_connector(simulator: Simulator) -> IcebergLakeflowConnect:  # noqa: ARG001
    return IcebergLakeflowConnect(
        {
            "uri": _URI,
            "token": _TOKEN,
            "data_tables": "accounting.tax.invoices",
        }
    )


class TestDataTableStream:
    """Everything up to the actual row scan runs on the REST metadata path.

    ``get_table_schema`` and ``read_table_metadata`` for a data table are
    derived from the ``LoadTableResult`` document, so the simulator serves
    them. Only the row read reaches for ``pyiceberg``.
    """

    def test_alias_is_derived_and_listed(self, data_connector):
        tables = data_connector.list_tables()
        assert "accounting_tax_invoices" in tables
        # The six metadata streams are still there.
        assert {"namespaces", "tables", "table_schemas"} <= set(tables)

    def test_explicit_alias(self, simulator: Simulator):  # noqa: ARG002
        conn = IcebergLakeflowConnect(
            {
                "uri": _URI,
                "token": _TOKEN,
                "data_tables": "invoices=accounting.tax.invoices",
            }
        )
        assert "invoices" in conn.list_tables()

    def test_alias_colliding_with_a_metadata_stream_is_rejected(
        self, simulator: Simulator  # noqa: ARG002
    ):
        with pytest.raises(ValueError, match="collides"):
            IcebergLakeflowConnect(
                {
                    "uri": _URI,
                    "token": _TOKEN,
                    "data_tables": "tables=accounting.tax.invoices",
                }
            )

    def test_identifier_without_a_namespace_is_rejected(
        self, simulator: Simulator  # noqa: ARG002
    ):
        with pytest.raises(ValueError, match="dotted identifier"):
            IcebergLakeflowConnect(
                {"uri": _URI, "token": _TOKEN, "data_tables": "invoices"}
            )

    def test_data_table_is_not_partitioned(self, data_connector):
        assert data_connector.is_partitioned("accounting_tax_invoices") is False
        assert data_connector.is_partitioned("namespaces") is True

    def test_schema_comes_from_the_current_iceberg_schema(self, data_connector):
        schema = data_connector.get_table_schema("accounting_tax_invoices", {})
        assert isinstance(schema, StructType)
        assert schema.fieldNames() == [
            "invoice_id",
            "customer_id",
            "amount",
            "created_at",
            "status",
            "billing_address",
            "line_item_ids",
        ]
        assert schema["invoice_id"].dataType == LongType()
        # Iceberg ``required: true`` -> Spark non-nullable.
        assert schema["invoice_id"].nullable is False
        assert schema["amount"].dataType == DecimalType(18, 2)
        assert schema["status"].dataType == StringType()
        assert schema["line_item_ids"].dataType == ArrayType(LongType(), False)
        assert isinstance(schema["billing_address"].dataType, StructType)

    def test_metadata_uses_identifier_fields_as_primary_keys(self, data_connector):
        meta = data_connector.read_table_metadata("accounting_tax_invoices", {})
        assert meta == {
            "primary_keys": ["invoice_id"],
            "cursor_field": None,
            "ingestion_type": "snapshot",
        }

    def test_incremental_column_promotes_the_stream_to_cdc(self, data_connector):
        meta = data_connector.read_table_metadata(
            "accounting_tax_invoices", {"incremental_column": "created_at"}
        )
        assert meta == {
            "primary_keys": ["invoice_id"],
            "cursor_field": "created_at",
            "ingestion_type": "cdc",
        }

    def test_invalid_incremental_column_is_rejected(self, data_connector):
        with pytest.raises(ValueError, match="Invalid column name"):
            data_connector.read_table_metadata(
                "accounting_tax_invoices", {"incremental_column": "1; DROP TABLE"}
            )

    @pytest.mark.skipif(
        _HAS_PYICEBERG,
        reason="pyiceberg is installed — the optional-dependency guard is not reachable",
    )
    def test_row_read_without_pyiceberg_raises_an_actionable_error(
        self, data_connector
    ):
        """Row reads are the only path that needs the optional dependency.

        The simulator cannot stand in for it (pyiceberg reads data files from
        object storage, not through ``requests``), so simulate-mode coverage
        stops at the guard — which must name the extra to install and make
        clear the metadata streams still work.
        """
        with pytest.raises(RuntimeError) as excinfo:
            data_connector.read_table("accounting_tax_invoices", {}, {})
        message = str(excinfo.value)
        assert "pyiceberg" in message
        assert "namespaces" in message

    def test_metadata_streams_still_work_alongside_a_data_table(self, data_connector):
        """The optional dependency must not poison the six metadata streams."""
        rows = _read_all(data_connector, "namespaces")
        assert len(rows) == 4
