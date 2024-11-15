"""REST client handling, including KlaviyoStream base class."""

from __future__ import annotations

import typing as t
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import sleep
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
    filter_compare = "greater-than"

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

            params["filter"] = f"{self.filter_compare}({self.replication_key},{filter_timestamp.isoformat()})"

        if self.max_page_size:
            params["page[size]"] = self.max_page_size
        self.logger.debug("QUERY PARAMS: %s", params)
        return params

    def backoff_wait_generator(self) -> t.Generator[float, None, None]:
        def _backoff_from_headers(retriable_api_error):
            response_headers = retriable_api_error.response.headers
            return int(response_headers.get("Retry-After", 60))

        return self.backoff_runtime(value=_backoff_from_headers)


class KlaviyoReportStream(KlaviyoStream):
    rest_method = "POST"
    replication_key = 'report_date'
    backoff_max_tries = 10
    records_jsonpath = "$[data][attributes][results][*]"
    is_sorted = True

    @property
    def report_type(self):
        raise NotImplemented('report type needs to be defined')

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        row["generated_at"] = datetime.now().isoformat()
        row["report_date"] = context['end']
        return row

    def get_records(self, context) -> t.Iterable[dict[str, t.Any]]:
        context = context or {}
        # Only yesterday reports are completed
        report_limit_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        current_date = datetime.fromtimestamp(
            self.get_starting_timestamp(context).replace(hour=0, minute=0, second=0, microsecond=0).timestamp(),
            timezone.utc)
        # when it's not the first run we start collect from next day
        if 'replication_key_value' in self.get_context_state(context):
            current_date += timedelta(days=1)
        while current_date <= report_limit_date:
            context['start'] = current_date - timedelta(
                days=self.config.get('reports_attributes', {}).get(self.name, {}).get('timeframe_last_n_days', 1))
            context['end'] = current_date
            self.logger.info(f'Processing {self.name} report from {context["start"]} to {context["end"]}')
            yield from super().get_records(context)
            current_date += timedelta(days=1)
            sleep(31)  # 2/m rate limit
        else:
            self.logger.info(f'Last available daily report already extracted')

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, t.Any]:
        params: dict[str, t.Any] = {}
        if next_page_token:
            params.update(parse_qsl(next_page_token.query))

        if self.max_page_size:
            params["page[size]"] = self.max_page_size
        self.logger.debug("QUERY PARAMS: %s", params)
        return params

    def prepare_request_payload(
            self,
            context: dict | None,
            next_page_token: t.Optional[t.Any],
    ) -> dict | None:
        report_attributes = self.config.get('reports_attributes', {}).get(self.name, {})
        return {
            "data": {
                "type": self.report_type,
                "attributes": {
                    "statistics": report_attributes.get('statistics', [
                        "click_rate",
                        "click_to_open_rate",
                        "clicks",
                        "clicks_unique",
                        "delivered",
                        "delivery_rate",
                        "open_rate",
                        "opens",
                        "opens_unique",
                        "recipients",
                        "unsubscribe_rate",
                        "unsubscribe_uniques",
                        "unsubscribes",
                        "bounced",
                        "bounce_rate"
                    ]),
                    "timeframe": report_attributes.get('timeframe', {
                        "start": context['start'].isoformat(),
                        "end": context['end'].isoformat()
                    }),
                    "conversion_metric_id": report_attributes.get(
                        'conversion_metric_id', "WcGvVS"),
                }
            }
        }
