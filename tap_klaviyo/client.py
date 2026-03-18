"""REST client handling, including KlaviyoStream base class."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from urllib.parse import ParseResult, parse_qs

from singer_sdk import SchemaDirectory, StreamSchema
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.pagination import BaseHATEOASPaginator
from singer_sdk.streams import RESTStream

from tap_klaviyo import schemas

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from typing import TypedDict

    import requests
    from singer_sdk.helpers.types import Context

    class KlaviyoPaginatedResponse(TypedDict):
        """Klaviyo response dict for paginated endpoints."""

        data: list[dict[str, Any]]
        links: dict[str, str]


SCHEMAS_DIR = SchemaDirectory(schemas)
UTC = timezone.utc
DEFAULT_START_DATE = datetime(2000, 1, 1, tzinfo=UTC).isoformat()


class KlaviyoPaginator(BaseHATEOASPaginator):
    """HATEOAS paginator for the Klaviyo API."""

    @override
    def get_next_url(self, response: requests.Response) -> str | None:
        data: KlaviyoPaginatedResponse = response.json()
        return data.get("links", {}).get("next")


class KlaviyoStream(RESTStream[ParseResult]):
    """Klaviyo stream class."""

    url_base = "https://a.klaviyo.com/api"
    records_jsonpath = "$[data][*]"
    max_page_size: int | None = None
    schema = StreamSchema(SCHEMAS_DIR)

    @override
    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return APIKeyAuthenticator(
            key="Authorization",
            value=f"Klaviyo-API-Key {self.config['auth_token']}",
            location="header",
        )

    @override
    @property
    def http_headers(self) -> dict[str, str]:
        """Return the http headers needed."""
        return {"revision": self.config["revision"]}

    @override
    def get_new_paginator(self) -> BaseHATEOASPaginator:
        return KlaviyoPaginator()

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, Any]:

        if next_page_token:
            return parse_qs(next_page_token.query)

        params: dict[str, Any] = {}
        if self.replication_key:
            if start_date := self.get_starting_timestamp(context):
                filter_timestamp = start_date.isoformat()
            else:
                filter_timestamp = DEFAULT_START_DATE

            if self.is_sorted:
                params["sort"] = self.replication_key

            params["filter"] = f"greater-than({self.replication_key},{filter_timestamp})"

        if self.max_page_size:
            params["page[size]"] = self.max_page_size
        return params
