"""REST client handling, including KlaviyoStream base class."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl

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
    from collections.abc import Iterable
    from urllib.parse import ParseResult

    import requests
    from singer_sdk.helpers.types import Context, Record

SCHEMAS_DIR = SchemaDirectory(schemas)
UTC = timezone.utc
DEFAULT_START_DATE = datetime(2000, 1, 1, tzinfo=UTC).isoformat()


def _isodate_from_date_string(date_string: str) -> str:
    """Convert a date or datetime string to an ISO datetime string in UTC.

    Accepts both date-only (YYYY-MM-DD) and full datetime strings
    (e.g. 2026-02-26T10:00:00Z or 2026-02-26T10:00:00+00:00).

    Args:
        date_string: The date or datetime string to convert.

    Returns:
        An ISO datetime string with UTC timezone.
    """
    # Normalize Z suffix so fromisoformat can handle it (Python < 3.11 compat)
    normalized = date_string.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.isoformat()
    except ValueError:
        # Fall back for plain date strings like "2026-02-26"
        return datetime.strptime(date_string, "%Y-%m-%d").replace(tzinfo=UTC).isoformat()


class KlaviyoPaginator(BaseHATEOASPaginator):
    """HATEOAS paginator for the Klaviyo API."""

    @override
    def get_next_url(self, response: requests.Response) -> str:
        data = response.json()
        return data.get("links").get("next")  # type: ignore[no-any-return]


class KlaviyoStream(RESTStream):
    """Klaviyo stream class."""

    url_base = "https://a.klaviyo.com/api"
    records_jsonpath = "$[data][*]"
    max_page_size: int | None = None
    schema = StreamSchema(SCHEMAS_DIR)
    # Set to False on streams whose API endpoint does not support less-than filtering
    apply_end_date_filter: bool = True

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
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "revision" in self.config:
            headers["revision"] = self.config.get("revision")
        return headers

    @override
    def get_new_paginator(self) -> BaseHATEOASPaginator:
        return KlaviyoPaginator()

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}

        if next_page_token:
            params.update(parse_qsl(next_page_token.query))

        if self.replication_key:
            if start_date := self.get_starting_timestamp(context):
                filter_timestamp = start_date.isoformat()
            elif start_date := self.config.get("start_date"):
                filter_timestamp = _isodate_from_date_string(start_date)
            else:
                filter_timestamp = DEFAULT_START_DATE

            if self.is_sorted:
                params["sort"] = self.replication_key

            filter_expr = f"greater-than({self.replication_key},{filter_timestamp})"
            if self.apply_end_date_filter and (end_date := self.config.get("end_date")):
                end_timestamp = _isodate_from_date_string(end_date)
                filter_expr = f"and({filter_expr},less-than({self.replication_key},{end_timestamp}))"
            params["filter"] = filter_expr

        if self.max_page_size:
            params["page[size]"] = self.max_page_size
        return params

    @override
    def get_records(self, context: Context | None) -> Iterable[Record]:
        """Skip partition if bookmark is already past the end_date.

        Prevents 400 errors from the Klaviyo API when a stream's replication
        key bookmark (from a previous run) is already ahead of the requested
        end_date, which would produce an invalid start > end filter expression.
        """
        end_date = self.config.get("end_date")
        if end_date and self.apply_end_date_filter and self.replication_key:
            start_ts = self.get_starting_timestamp(context)
            if start_ts is not None:
                end_ts = datetime.fromisoformat(_isodate_from_date_string(end_date))
                if end_ts.tzinfo is None:
                    end_ts = end_ts.replace(tzinfo=UTC)
                if start_ts >= end_ts:
                    self.logger.info(
                        "Skipping stream '%s' (context: %s): bookmark %s is already "
                        "at or past end_date %s — no records to fetch.",
                        self.name,
                        context,
                        start_ts.isoformat(),
                        end_date,
                    )
                    return
        yield from super().get_records(context)
