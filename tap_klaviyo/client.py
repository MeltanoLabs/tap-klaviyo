"""REST client handling, including KlaviyoStream base class."""

from __future__ import annotations

import typing as t
from pathlib import Path
from urllib.parse import parse_qsl

from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.pagination import BaseHATEOASPaginator
from singer_sdk.streams import RESTStream

if t.TYPE_CHECKING:
    from urllib.parse import ParseResult

    import requests

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class KlaviyoPaginator(BaseHATEOASPaginator):
    """HATEOAS paginator for the Klaviyo API."""

    def get_next_url(self, response: requests.Response) -> str:
        data = response.json()
        return data.get("links").get("next")  # type: ignore[no-any-return]


class KlaviyoStream(RESTStream):
    """Klaviyo stream class."""

    url_base = "https://a.klaviyo.com/api"
    records_jsonpath = "$[data][*]"
    max_page_size: int | None = None

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="Authorization",
            value=f'Klaviyo-API-Key {self.config.get("auth_token", "")}',
            location="header",
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        if "revision" in self.config:
            headers["revision"] = self.config.get("revision")
        return headers

    def get_new_paginator(self) -> BaseHATEOASPaginator:
        return KlaviyoPaginator()

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, t.Any]:
        params: dict[str, t.Any] = {}

        if next_page_token:
            params.update(parse_qsl(next_page_token.query))

        if self.replication_key:
            filter_timestamp = self.get_starting_timestamp(context)

            if self.is_sorted:
                params["sort"] = self.replication_key

            params["filter"] = f"greater-than({self.replication_key},{filter_timestamp.isoformat()})"

        if self.max_page_size:
            params["page[size]"] = self.max_page_size
        return params
