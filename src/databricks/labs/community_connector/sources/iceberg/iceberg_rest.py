"""Thin HTTP client for the Apache Iceberg REST Catalog open spec.

Only the read routes the connector needs are implemented:

    GET  {uri}/v1/config                                        (bootstrap)
    POST {uri}/v1/oauth/tokens                                  (client creds)
    GET  {uri}/v1/{prefix}/namespaces[?parent=&pageToken=&pageSize=]
    GET  {uri}/v1/{prefix}/namespaces/{namespace}
    GET  {uri}/v1/{prefix}/namespaces/{namespace}/tables[?pageToken=&pageSize=]
    GET  {uri}/v1/{prefix}/namespaces/{namespace}/tables/{table}

Every call goes through ``requests`` so the repo's ``source_simulator`` — which
patches ``requests.sessions.Session.send`` — can stand in for a live catalog.

Design notes
------------
- ``prefix`` is never assumed: it is resolved from ``GET /v1/config`` using the
  spec's merge order (server ``defaults`` < client config < server
  ``overrides``) and cached for the life of the client.
- Multi-level namespaces are joined with the 0x1F unit separator and then
  percent-encoded as a single opaque path segment.
- Errors carry the spec's ``ErrorModel`` shape (``{"error": {...}}``).
- The client is picklable: the ``requests.Session`` is created lazily and
  dropped from the pickled state, so a connector holding one can be shipped to
  a Spark executor.
"""

from __future__ import annotations

import random
import time
from typing import Any, Dict, Iterator, List, Optional, Sequence
from urllib.parse import quote

import requests

from databricks.labs.community_connector.sources.iceberg.iceberg_schemas import (
    AUTH_REFRESH_STATUS_CODES,
    CONFIG_PATH,
    DEFAULT_OAUTH_SCOPE,
    DEFAULT_PAGE_SIZE,
    DEFAULT_TIMEOUT,
    INITIAL_BACKOFF,
    MAX_BACKOFF,
    MAX_RETRIES,
    NAMESPACE_SEPARATOR,
    OAUTH_TOKENS_PATH,
    RETRIABLE_STATUS_CODES,
)

_ERROR_BODY_LIMIT = 512

#: Default SigV4 service name from the Iceberg REST spec. AWS S3 Tables wants
#: ``s3tables``; an API-Gateway-fronted catalog wants the default.
DEFAULT_SIGNING_NAME = "execute-api"


class SigV4Auth(requests.auth.AuthBase):
    """Sign each request with AWS SigV4.

    Signing happens on the *prepared* request so the signature covers the final
    URL, query string, and body — signing earlier would miss the query
    parameters ``requests`` appends from ``params``.

    Credentials come from botocore's default chain (environment, shared config,
    container/instance metadata, assumed role), so an instance profile works
    without any key material in the connection.
    """

    def __init__(
        self,
        service_name: str,
        region: str,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        session_token: Optional[str] = None,
    ) -> None:
        self._service_name = service_name
        self._region = region
        self._access_key = access_key
        self._secret_key = secret_key
        self._session_token = session_token
        self._signer = None  # built lazily; botocore objects do not pickle

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        state["_signer"] = None
        return state

    def _build_signer(self):
        try:
            from botocore.auth import SigV4Auth as _BotoSigV4Auth
            from botocore.credentials import Credentials
            from botocore.session import Session as BotoSession
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "SigV4 signing requires 'botocore'. Install it with "
                "`pip install 'lakeflow-community-connectors-iceberg[sigv4]'` "
                "or `pip install botocore`. It is preinstalled on Databricks "
                "runtimes. Set rest.sigv4-enabled=false to use bearer-token "
                "authentication instead."
            ) from exc

        if self._access_key and self._secret_key:
            credentials = Credentials(
                access_key=self._access_key,
                secret_key=self._secret_key,
                token=self._session_token,
            )
        else:
            credentials = BotoSession().get_credentials()
            if credentials is None:
                raise RuntimeError(
                    "SigV4 signing is enabled but no AWS credentials were "
                    "found. Provide s3.access-key-id / s3.secret-access-key on "
                    "the connection, or give the compute an instance profile "
                    "or assumed role."
                )
        return _BotoSigV4Auth(credentials, self._service_name, self._region)

    def __call__(self, request):
        try:
            from botocore.awsrequest import AWSRequest
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "SigV4 signing requires 'botocore'; install it or set "
                "rest.sigv4-enabled=false."
            ) from exc

        if self._signer is None:
            self._signer = self._build_signer()

        # Only headers that participate in the signature travel to the signer;
        # hop-by-hop headers requests adds later would invalidate it.
        aws_request = AWSRequest(
            method=request.method,
            url=request.url,
            data=request.body,
            headers={"Accept": request.headers.get("Accept", "application/json")},
        )
        self._signer.add_auth(aws_request)
        request.headers.update(dict(aws_request.headers))
        return request


class IcebergRestError(RuntimeError):
    """An Iceberg REST Catalog ``ErrorModel`` response."""

    def __init__(
        self,
        status_code: int,
        message: str,
        error_type: Optional[str] = None,
        url: Optional[str] = None,
    ) -> None:
        self.status_code = status_code
        self.error_type = error_type
        self.url = url
        detail = f"{error_type}: " if error_type else ""
        super().__init__(
            f"Iceberg REST Catalog error {status_code} for {url}: {detail}{message}"
        )


def _redact(text: str) -> str:
    """Flatten and truncate a response body for safe inclusion in an error."""
    if not text:
        return ""
    flat = text.replace("\r", " ").replace("\n", " ")
    if len(flat) <= _ERROR_BODY_LIMIT:
        return flat
    return flat[:_ERROR_BODY_LIMIT] + "...[truncated]"


def encode_namespace(levels: Sequence[str], separator: str = NAMESPACE_SEPARATOR) -> str:
    """Join namespace levels with ``separator`` and encode as one path segment.

    ``["accounting", "tax"]`` -> ``accounting%1Ftax``.
    """
    return quote(separator.join(levels), safe="")


class RestCatalogClient:
    """Read-only client for an Iceberg REST Catalog server."""

    def __init__(self, options: Dict[str, str]) -> None:
        uri = options.get("uri") or options.get("catalog_uri") or options.get("base_url")
        if not uri:
            raise ValueError(
                "Iceberg connector requires a 'uri' option pointing at the REST "
                "Catalog root, e.g. https://catalog.example.com/iceberg"
            )
        self._uri = str(uri).rstrip("/")
        self._warehouse = options.get("warehouse") or None
        self._configured_prefix = options.get("prefix") or None

        self._token = options.get("token") or None
        self._credential = options.get("credential") or None
        if not self._credential:
            client_id = options.get("client_id")
            client_secret = options.get("client_secret")
            if client_id and client_secret:
                self._credential = f"{client_id}:{client_secret}"
        self._scope = options.get("scope") or DEFAULT_OAUTH_SCOPE
        self._configured_oauth_uri = (
            options.get("oauth2_server_uri")
            or options.get("oauth2-server-uri")
            or None
        )

        self._vend_credentials = str(
            options.get("vended_credentials", "false")
        ).lower() in ("1", "true", "yes")

        self._timeout = _positive_int(options.get("timeout"), DEFAULT_TIMEOUT)
        self._page_size = _positive_int(options.get("page_size"), DEFAULT_PAGE_SIZE)

        # -- SigV4 (AWS S3 Tables, Glue's REST front door, API Gateway) ------
        # These same options are forwarded to pyiceberg for the row-data path;
        # honouring them here is what makes the metadata streams work too.
        self._sigv4_enabled = str(
            options.get("rest.sigv4-enabled")
            or options.get("rest.sigv4_enabled")
            or "false"
        ).lower() in ("1", "true", "yes")
        self._signing_name = (
            options.get("rest.signing-name")
            or options.get("rest.signing_name")
            or DEFAULT_SIGNING_NAME
        )
        self._signing_region = (
            options.get("rest.signing-region")
            or options.get("rest.signing_region")
            or options.get("s3.region")
            or options.get("aws.region")
            or None
        )
        if self._sigv4_enabled and not self._signing_region:
            raise ValueError(
                "rest.sigv4-enabled is true but no signing region was given. "
                "Set rest.signing-region (e.g. ap-southeast-1)."
            )
        self._aws_access_key = (
            options.get("s3.access-key-id") or options.get("aws.access-key-id") or None
        )
        self._aws_secret_key = (
            options.get("s3.secret-access-key")
            or options.get("aws.secret-access-key")
            or None
        )
        self._aws_session_token = (
            options.get("s3.session-token") or options.get("aws.session-token") or None
        )

        # Resolved lazily and cached; all plain values so they survive pickling.
        self._config_cache: Optional[dict] = None
        self._access_token: Optional[str] = None
        self._access_token_expiry: float = 0.0

    # -- pickling ---------------------------------------------------------

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        # A live socket pool must not travel to an executor.
        state.pop("_session_obj", None)
        return state

    @property
    def _session(self) -> requests.Session:
        session = getattr(self, "_session_obj", None)
        if session is None:
            session = requests.Session()
            self._session_obj = session
        return session

    # -- auth -------------------------------------------------------------

    @property
    def _oauth_token_url(self) -> str:
        if self._configured_oauth_uri:
            return self._configured_oauth_uri
        # The server may point us at an external IdP via config properties.
        # Read the *cache* rather than calling config(): the bootstrap request
        # itself may need a bearer token, and going through config() here
        # would recurse.
        if self._config_cache is not None:
            for block in ("overrides", "defaults"):
                from_server = (self._config_cache.get(block) or {}).get(
                    "oauth2-server-uri"
                )
                if from_server:
                    return str(from_server)
        return f"{self._uri}/{OAUTH_TOKENS_PATH}"

    def _fetch_oauth_token(self) -> Optional[str]:
        """Exchange stored client credentials for a bearer access token."""
        if not self._credential:
            return None
        client_id, _, client_secret = self._credential.partition(":")
        payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": self._scope,
        }
        url = self._oauth_token_url
        resp = self._session.post(
            url,
            data=payload,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            timeout=self._timeout,
        )
        if resp.status_code >= 400:
            raise _error_from_response(resp, url)
        body = _safe_json(resp)
        token = body.get("access_token")
        if not token:
            raise IcebergRestError(
                resp.status_code,
                "OAuth2 token response contained no 'access_token'",
                url=url,
            )
        expires_in = body.get("expires_in")
        try:
            ttl = float(expires_in) if expires_in is not None else 3600.0
        except (TypeError, ValueError):
            ttl = 3600.0
        # Refresh a minute early so a long read never trips a 419 mid-flight.
        self._access_token_expiry = time.time() + max(ttl - 60.0, 30.0)
        self._access_token = str(token)
        return self._access_token

    def _bearer(self) -> Optional[str]:
        if self._token:
            return self._token
        if not self._credential:
            return None
        if self._access_token and time.time() < self._access_token_expiry:
            return self._access_token
        return self._fetch_oauth_token()

    def _invalidate_token(self) -> bool:
        """Drop a cached OAuth token. Returns True when a retry makes sense."""
        if self._credential:
            self._access_token = None
            self._access_token_expiry = 0.0
            return True
        # A static bearer token cannot be refreshed.
        return False

    def _auth_headers(self) -> Dict[str, str]:
        # Under SigV4 the Authorization header is the signature, so a bearer
        # token must not be set: it would be overwritten by the signer anyway,
        # and asking for one would trigger a pointless OAuth exchange.
        if self._sigv4_enabled:
            return {}
        bearer = self._bearer()
        return {"Authorization": f"Bearer {bearer}"} if bearer else {}

    @property
    def _request_auth(self) -> Optional[requests.auth.AuthBase]:
        """The ``requests`` auth hook, or None for bearer/anonymous calls."""
        if not self._sigv4_enabled:
            return None
        auth = getattr(self, "_sigv4_auth_obj", None)
        if auth is None:
            auth = SigV4Auth(
                service_name=self._signing_name,
                region=self._signing_region,
                access_key=self._aws_access_key,
                secret_key=self._aws_secret_key,
                session_token=self._aws_session_token,
            )
            self._sigv4_auth_obj = auth
        return auth

    # -- transport --------------------------------------------------------

    def _request(
        self,
        method: str,
        url: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
    ) -> dict:
        """Issue one request, retrying on throttling / transient failures."""
        backoff = INITIAL_BACKOFF
        auth_retry_available = True
        last_error: Optional[BaseException] = None

        for attempt in range(MAX_RETRIES):
            request_headers = {"Accept": "application/json"}
            request_headers.update(self._auth_headers())
            if headers:
                request_headers.update(headers)

            try:
                resp = self._session.request(
                    method,
                    url,
                    params=params,
                    headers=request_headers,
                    timeout=self._timeout,
                    auth=self._request_auth,
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = exc
                if attempt < MAX_RETRIES - 1:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)
                    continue
                raise

            if resp.status_code < 400:
                return _safe_json(resp)

            if (
                resp.status_code in AUTH_REFRESH_STATUS_CODES
                and auth_retry_available
                and self._invalidate_token()
            ):
                # 419 (and sometimes 401) means "token expired mid-session":
                # re-exchange the credential once and replay the request.
                auth_retry_available = False
                continue

            if resp.status_code in RETRIABLE_STATUS_CODES and attempt < MAX_RETRIES - 1:
                time.sleep(_retry_delay(resp, backoff))
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue

            raise _error_from_response(resp, url)

        if last_error is not None:  # pragma: no cover - defensive
            raise last_error
        raise IcebergRestError(  # pragma: no cover - defensive
            0, f"Exhausted {MAX_RETRIES} attempts", url=url
        )

    # -- config bootstrap -------------------------------------------------

    def config(self) -> dict:
        """``GET /v1/config`` — cached for the life of the client."""
        if self._config_cache is None:
            params = {}
            if self._warehouse:
                params["warehouse"] = self._warehouse
            self._config_cache = (
                self._request("GET", f"{self._uri}/{CONFIG_PATH}", params=params) or {}
            )
        return self._config_cache

    def _merged_config_properties(self) -> Dict[str, Any]:
        """Spec merge order: ``defaults`` < client config < ``overrides``."""
        cfg = self.config()
        merged: Dict[str, Any] = dict(cfg.get("defaults") or {})
        if self._configured_prefix:
            merged["prefix"] = self._configured_prefix
        if self._warehouse:
            merged["warehouse"] = self._warehouse
        merged.update(cfg.get("overrides") or {})
        return merged

    @property
    def uri(self) -> str:
        """The catalog root URL, without a trailing slash."""
        return self._uri

    @property
    def prefix(self) -> str:
        """The catalog path prefix, ``""`` when the server does not use one."""
        value = self._merged_config_properties().get("prefix")
        return str(value) if value else ""

    @property
    def namespace_separator(self) -> str:
        value = self._merged_config_properties().get("namespace-separator")
        return str(value) if value else NAMESPACE_SEPARATOR

    @property
    def endpoints(self) -> List[str]:
        """Routes advertised by the server (spec >= 1.14); ``[]`` when absent."""
        raw = self.config().get("endpoints") or []
        return [str(item) for item in raw]

    def supports_endpoint(self, route: str) -> bool:
        """True when the server advertises ``route`` or advertises nothing."""
        advertised = self.endpoints
        if not advertised:
            # Pre-1.14 servers omit the list; assume the base endpoint set.
            return True
        return route in advertised

    # -- URL building -----------------------------------------------------

    def _catalog_url(self, *segments: str) -> str:
        parts = [self._uri, "v1"]
        prefix = self.prefix
        if prefix:
            # The spec allows a multi-segment prefix, and real servers use one:
            # Unity Catalog returns "catalogs/{catalog}". Percent-encoding the
            # separator would 404 every catalog route, so "/" stays safe.
            parts.append(quote(prefix.strip("/"), safe="/"))
        parts.extend(segments)
        return "/".join(parts)

    def _encode_namespace(self, levels: Sequence[str]) -> str:
        return encode_namespace(levels, self.namespace_separator)

    # -- read routes ------------------------------------------------------

    def list_namespaces(
        self, parent: Optional[Sequence[str]] = None
    ) -> Iterator[List[str]]:
        """Yield the direct children of ``parent`` (top-level when None)."""
        url = self._catalog_url("namespaces")
        params: Dict[str, Any] = {"pageSize": self._page_size}
        if parent:
            params["parent"] = self.namespace_separator.join(parent)

        for body in self._paginate(url, params):
            for levels in body.get("namespaces") or []:
                if isinstance(levels, list):
                    yield [str(level) for level in levels]
                elif levels is not None:
                    # Tolerate servers that emit a pre-joined string.
                    yield str(levels).split(self.namespace_separator)

    def load_namespace(self, levels: Sequence[str]) -> dict:
        """``GET /v1/{prefix}/namespaces/{namespace}`` — properties map."""
        url = self._catalog_url("namespaces", self._encode_namespace(levels))
        return self._request("GET", url) or {}

    def list_table_identifiers(self, levels: Sequence[str]) -> Iterator[dict]:
        """Yield ``{"namespace": [...], "name": ...}`` for one namespace."""
        url = self._catalog_url(
            "namespaces", self._encode_namespace(levels), "tables"
        )
        params: Dict[str, Any] = {"pageSize": self._page_size}
        for body in self._paginate(url, params):
            for identifier in body.get("identifiers") or []:
                if isinstance(identifier, dict):
                    yield identifier

    def load_table(self, levels: Sequence[str], table: str) -> dict:
        """``GET .../tables/{table}`` — the full ``LoadTableResult``."""
        url = self._catalog_url(
            "namespaces",
            self._encode_namespace(levels),
            "tables",
            quote(table, safe=""),
        )
        headers = (
            {"X-Iceberg-Access-Delegation": "vended-credentials"}
            if self._vend_credentials
            else None
        )
        return self._request("GET", url, headers=headers) or {}

    # -- pagination -------------------------------------------------------

    def _paginate(self, url: str, params: Dict[str, Any]) -> Iterator[dict]:
        """Walk ``pageToken`` / ``next-page-token`` until the server stops."""
        page_params = dict(params)
        seen_tokens = set()
        while True:
            body = self._request("GET", url, params=page_params) or {}
            yield body
            token = body.get("next-page-token")
            if not token:
                return
            token = str(token)
            if token in seen_tokens:
                # A server that echoes the same token would loop forever.
                return
            seen_tokens.add(token)
            page_params = dict(params)
            page_params["pageToken"] = token


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _positive_int(raw: Any, default: int) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _safe_json(resp: requests.Response) -> dict:
    try:
        body = resp.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _retry_delay(resp: requests.Response, backoff: float) -> float:
    """Honour ``Retry-After`` when present, else exponential backoff + jitter."""
    retry_after = resp.headers.get("Retry-After")
    if retry_after:
        try:
            return min(float(retry_after), MAX_BACKOFF)
        except (TypeError, ValueError):
            pass
    return min(backoff, MAX_BACKOFF) * (1.0 + random.random() * 0.1)


def _error_from_response(resp: requests.Response, url: str) -> IcebergRestError:
    """Translate an ``ErrorModel`` body (or any body) into an exception."""
    message = _redact(resp.text or "")
    error_type = None
    try:
        payload = resp.json()
    except ValueError:
        payload = None
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            message = str(error.get("message") or message)
            error_type = error.get("type")
            stack = error.get("stack")
            if stack:
                message = f"{message} (stack omitted, {len(stack)} frames)"
    return IcebergRestError(resp.status_code, message, error_type, url)
