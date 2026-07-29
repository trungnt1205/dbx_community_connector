"""Custom simulator handler for the Iceberg REST Catalog namespace listing.

``GET /v1/{prefix}/namespaces`` is the one route the declarative spec cannot
express:

* the response is an array of **arrays** (``{"namespaces": [["a"], ["a","b"]]}``)
  rather than an array of record objects, so ``wrapper.records_key`` plus the
  filter/sort pipeline has nothing to work with;
* ``?parent=`` selects the *direct children* of a multi-level namespace whose
  levels are joined with the 0x1F unit separator — a hierarchy query, not a
  field comparison.

Everything else in ``endpoints.yaml`` stays declarative.

The namespace tree comes from the ``namespace_levels`` corpus file (a JSON
array of level arrays); a small built-in tree is used when that file is
missing so the spec keeps working if the corpus is regenerated from
``TABLE_SCHEMAS`` alone.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlsplit

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

#: Iceberg's default multipart-namespace separator (ASCII unit separator).
NAMESPACE_SEPARATOR = chr(0x1F)

CORPUS_KEY = "namespace_levels"

_DEFAULT_NAMESPACES: List[List[str]] = [
    ["accounting"],
    ["accounting", "tax"],
    ["accounting", "reporting"],
    ["sales"],
]

_DEFAULT_PAGE_SIZE = 100


def list_namespaces(prep: PreparedRequest, spec: Any, corpus: Any) -> Response:  # noqa: ARG001
    """Serve ``GET /v1/{prefix}/namespaces`` with ``parent`` + page tokens."""
    query = _query(prep.url or "")
    namespaces = _namespaces(corpus)

    parent_raw = query.get("parent")
    parent = _split(parent_raw) if parent_raw else []
    depth = len(parent) + 1
    children = [
        levels
        for levels in namespaces
        if len(levels) == depth and levels[: len(parent)] == parent
    ]

    page_size = _int(query.get("pageSize"), _DEFAULT_PAGE_SIZE)
    offset = _int(query.get("pageToken"), 0)
    page = children[offset : offset + page_size]
    next_offset = offset + len(page)
    next_token = str(next_offset) if next_offset < len(children) else None

    return _json_response(
        prep, {"namespaces": page, "next-page-token": next_token}
    )


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _namespaces(corpus: Any) -> List[List[str]]:
    raw = corpus.get(CORPUS_KEY) if corpus is not None else None
    if not isinstance(raw, list) or not raw:
        return [list(levels) for levels in _DEFAULT_NAMESPACES]
    out: List[List[str]] = []
    for entry in raw:
        if isinstance(entry, list):
            out.append([str(level) for level in entry])
        elif isinstance(entry, str):
            out.append(_split(entry))
    return out or [list(levels) for levels in _DEFAULT_NAMESPACES]


def _query(url: str) -> Dict[str, str]:
    parsed = parse_qs(urlsplit(url).query, keep_blank_values=True)
    return {key: values[0] for key, values in parsed.items() if values}


def _split(raw: str) -> List[str]:
    # Tolerate the canonical 0x1F join as well as a dotted fallback.
    separator = NAMESPACE_SEPARATOR if NAMESPACE_SEPARATOR in raw else "."
    return [level for level in raw.split(separator) if level]


def _int(raw: Any, default: int) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value >= 0 else default


def _json_response(prep: PreparedRequest, payload: dict) -> Response:
    body = json.dumps(payload, ensure_ascii=False)
    record = ResponseRecord(
        status_code=200,
        headers={"Content-Type": "application/json"},
        body_text=body,
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(record, prep)
