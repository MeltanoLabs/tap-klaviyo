"""Stream type classes for tap-klaviyo."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from tap_klaviyo.client import KlaviyoStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable
    from urllib.parse import ParseResult

    from singer_sdk.helpers.types import Context, Record


class EventsStream(KlaviyoStream):
    """Define custom stream."""

    name = "events"
    path = "/events"
    primary_keys = ["id"]
    replication_key = "datetime"

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        row["datetime"] = row["attributes"]["datetime"]
        return row

    @override
    @property
    def is_sorted(self) -> bool:
        return True


class CampaignsStream(KlaviyoStream):
    """Define custom stream."""

    name = "campaigns"
    path = "/campaigns"
    primary_keys = ["id"]
    replication_key = "updated_at"

    @override
    @property
    def partitions(self) -> list[dict[str, Any]] | None:
        return [
            {
                "filter": "equals(messages.channel,'email')",
            },
            {
                "filter": "equals(messages.channel,'sms')",
            },
            {
                "filter": "equals(messages.channel,'mobile_push')",
            },
        ]

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, Any]:
        url_params = super().get_url_params(context, next_page_token)

        # Apply channel filters
        if context:
            parent_filter = url_params["filter"]
            url_params["filter"] = f"and({parent_filter},{context['filter']})"

        return url_params

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        row["updated_at"] = row["attributes"]["updated_at"]
        return row

    @override
    @property
    def is_sorted(self) -> bool:
        # https://github.com/MeltanoLabs/tap-klaviyo/issues/156#issuecomment-4099218378
        return False


class ProfilesStream(KlaviyoStream):
    """Define custom stream."""

    name = "profiles"
    path = "/profiles"
    primary_keys = ["id"]
    replication_key = "updated"
    max_page_size = 100

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        row["updated"] = row["attributes"]["updated"]
        return row

    @override
    @property
    def is_sorted(self) -> bool:
        return True


class MetricsStream(KlaviyoStream):
    """Define custom stream."""

    name = "metrics"
    path = "/metrics"
    primary_keys = ["id"]
    replication_key = None

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        row["updated"] = row["attributes"]["updated"]
        integration = row.get("attributes", {}).get("integration")
        if isinstance(integration, dict):
            category = integration.get("category")
            if isinstance(category, dict):
                integration["category"] = category.get("category")
            elif category is not None and not isinstance(category, str):
                integration["category"] = str(category)
        return row


class ListsStream(KlaviyoStream):
    """Define custom stream."""

    name = "lists"
    path = "/lists"
    primary_keys = ["id"]
    replication_key = "updated"

    @override
    def get_child_context(self, record: Record, context: Context | None) -> Context | None:
        context = dict(context) if context else {}
        context["list_id"] = record["id"]

        return super().get_child_context(record, context)

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        row["updated"] = row["attributes"]["updated"]
        return row


class ListPersonStream(KlaviyoStream):
    """Define custom stream."""

    name = "listperson"
    path = "/lists/{list_id}/relationships/profiles/"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = ListsStream
    max_page_size = 1000

    @override
    def post_process(self, row: Record, context: Context | None = None) -> Record | None:
        row["list_id"] = context["list_id"] if context else None
        return row


class FlowsStream(KlaviyoStream):
    """Define custom stream."""

    name = "flows"
    path = "/flows"
    primary_keys = ["id"]
    replication_key = "updated"
    is_sorted = True

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        row["updated"] = row["attributes"]["updated"]
        return row


class SegmentsStream(KlaviyoStream):
    """Define segments stream."""

    name = "segments"
    path = "/segments"
    primary_keys = ["id"]
    replication_key = "updated"

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        row["updated"] = row.get("attributes", {}).get("updated")
        return row

    @override
    @property
    def is_sorted(self) -> bool:
        return True


class TemplatesStream(KlaviyoStream):
    """Define custom stream."""

    name = "templates"
    path = "/templates"
    primary_keys = ["id"]
    replication_key = "updated"

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        row["updated"] = row["attributes"]["updated"]
        return row

    @property
    def is_sorted(self) -> bool:
        return True

class SegmentSeriesReportStream(KlaviyoStream):
    """Define custom stream that uses a POST request.

    The Klaviyo endpoint for segment series reports requires a POST call with a
    JSON body; the default RESTStream implementation issues GET requests, so we
    override the method and enable JSON payloads here.
    """

    name = "segment_series_report"
    path = "/segment-series-reports"
    primary_keys = ["segment_id", "date", "statistic_name"]
    records_jsonpath = "$"  # Pass entire response

    # force POST instead of the default GET
    http_method = "POST"
    # tell the base class to send the prepared payload as JSON
    payload_as_json = True

    @override
    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        """Get records by flattening segment series report data.

        Override to handle custom extraction logic for this stream.
        """
        # Call the parent to get responses, then flatten them
        for record in super().get_records(context):
            # record here is the full response object due to records_jsonpath
            # We need to flatten it into individual records
            yield from self._flatten_response_record(record)

    def _flatten_response_record(
        self,
        response_record: dict[str, Any],
    ) -> Iterable[dict[str, Any]]:
        """Flatten a segment series report response record."""
        # Check if this is the full response structure
        if "data" in response_record and isinstance(response_record.get("data"), dict):
            attributes = response_record.get("data", {}).get("attributes", {})
            results = attributes.get("results", [])
            date_times = attributes.get("date_times", [])

            for result in results:
                segment_id = result.get("groupings", {}).get("segment_id")
                statistics = result.get("statistics", {})

                for stat_name, stat_values in statistics.items():
                    for date_idx, date in enumerate(date_times):
                        yield {
                            "date": date,
                            "segment_id": segment_id,
                            "statistic_name": stat_name,
                            "statistic_value": stat_values[date_idx],
                        }
        else:
            # Pass through if not the expected structure
            yield response_record

    @override
    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, Any] | None:
        # The Klaviyo segment series report endpoint *requires* a JSON body
        # describing details such as the statistics to compute, the
        # interval, and the timeframe.
        #
        # These can be configured in the config under "segment_series_report_config"
        # or use defaults for: statistics, interval, and timeframe.
        config = self.config.get("segment_series_report_config", {})
        return {
            "data": {
                "type": "segment-series-report",
                "attributes": {
                    "statistics": config.get("statistics", [
                        "members_added",
                        "total_members",
                        "members_removed",
                        "net_members_changed",
                    ]),
                    "interval": config.get("interval", "daily"),
                    "timeframe": config.get("timeframe", {"key": "last_7_days"}),
                },
            }
        }

class CampaignValuesReportStream(KlaviyoStream):
    """The Klaviyo endpoint for campaign values reports requires a POST call with a JSON body.

    The default RESTStream implementation issues GET requests, so we
    override the method and enable JSON payloads here.
    """

    name = "campaign_values_report"
    path = "/campaign-values-reports"
    primary_keys = [
        "campaign_id",
        "campaign_message_id",
        "send_channel",
        "statistic_name",
    ]
    records_jsonpath = "$"  # Pass entire response
    # force POST instead of the default GET
    http_method = "POST"
    # tell the base class to send the prepared payload as JSON
    payload_as_json = True

    @override
    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        """Get records by flattening campaign values report data.

        Override to handle custom extraction logic for this stream.
        """
        # Call the parent to get responses, then flatten them
        for record in super().get_records(context):
            # record here is the full response object due to records_jsonpath
            # We need to flatten it into individual records
            yield from self._flatten_response_record(record)

    def _flatten_response_record(
        self,
        response_record: dict[str, Any],
    ) -> Iterable[dict[str, Any]]:
        """Flatten a campaign values report response record."""
        if "data" in response_record and isinstance(response_record.get("data"), dict):
            attributes = response_record.get("data", {}).get("attributes", {})
            results = attributes.get("results", [])
            date_times = attributes.get("date_times", [])

            for result in results:
                groupings = result.get("groupings", {}) or {}
                campaign_id = groupings.get("campaign_id")
                campaign_message_id = groupings.get("campaign_message_id")
                send_channel = groupings.get("send_channel")
                statistics = result.get("statistics", {})
                for stat_name, stat_value in statistics.items():
                    if date_times:
                        for date_idx, date in enumerate(date_times):
                            yield {
                                "date": date,
                                "campaign_id": campaign_id,
                                "campaign_message_id": campaign_message_id,
                                "send_channel": send_channel,
                                "statistic_name": stat_name,
                                "statistic_value": (
                                    stat_value[date_idx]
                                    if isinstance(stat_value, list)
                                    and date_idx < len(stat_value)
                                    else None
                                    if isinstance(stat_value, list)
                                    else stat_value
                                ),
                            }
                    else:
                        yield {
                            "campaign_id": campaign_id,
                            "campaign_message_id": campaign_message_id,
                            "send_channel": send_channel,
                            "statistic_name": stat_name,
                            "statistic_value": stat_value,
                        }
        else:
            # Pass through if not the expected structure
            yield response_record

    @override
    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, Any] | None:
        # The Klaviyo campaign values report endpoint requires a JSON body
        # describing details such as the statistics to compute, the
        # interval, and the timeframe.
        #
        # These can be configured in the config under "campaign_values_report_config"
        # or use defaults for: statistics, interval, and timeframe.
        config = self.config.get("campaign_values_report_config", {})
        return {
            "data": {
                "type": "campaign-values-report",
                "attributes": {
                    "statistics": config.get("statistics", [
                        "opens_unique",
                        "open_rate",
                        "delivered",
                        "clicks_unique",
                        "click_to_open_rate",
                        "revenue_per_recipient",
                    ]),
                    "conversion_metric_id": config.get("conversion_metric_id", "WcYbF8"),
                    "timeframe": config.get("timeframe", {"key": "last_7_days"}),
                },
            }
        }

    @property
    def is_sorted(self) -> bool:
        return True


class FlowValuesReportStream(KlaviyoStream):
    """The Klaviyo endpoint for flow values reports requires a POST call with a JSON body.

    The default RESTStream implementation issues GET requests, so we
    override the method and enable JSON payloads here.
    """

    name = "flow_values_report"
    path = "/flow-values-reports"
    primary_keys = [
        "flow_id",
        "flow_message_id",
        "send_channel",
        "statistic_name",
    ]
    records_jsonpath = "$"  # Pass entire response
    # force POST instead of the default GET
    http_method = "POST"
    # tell the base class to send the prepared payload as JSON
    payload_as_json = True

    @override
    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        """Get records by flattening flow values report data.

        Override to handle custom extraction logic for this stream.
        """
        # Call the parent to get responses, then flatten them
        for record in super().get_records(context):
            # record here is the full response object due to records_jsonpath
            # We need to flatten it into individual records
            yield from self._flatten_response_record(record)

    def _flatten_response_record(
        self,
        response_record: dict[str, Any],
    ) -> Iterable[dict[str, Any]]:
        """Flatten a flow values report response record."""
        if "data" in response_record and isinstance(response_record.get("data"), dict):
            attributes = response_record.get("data", {}).get("attributes", {})
            results = attributes.get("results", [])
            date_times = attributes.get("date_times", [])

            for result in results:
                groupings = result.get("groupings", {}) or {}
                flow_id = groupings.get("flow_id")
                flow_message_id = groupings.get("flow_message_id")
                send_channel = groupings.get("send_channel")
                statistics = result.get("statistics", {})
                for stat_name, stat_value in statistics.items():
                    if date_times:
                        for date_idx, date in enumerate(date_times):
                            yield {
                                "date": date,
                                "flow_id": flow_id,
                                "flow_message_id": flow_message_id,
                                "send_channel": send_channel,
                                "statistic_name": stat_name,
                                "statistic_value": (
                                    stat_value[date_idx]
                                    if isinstance(stat_value, list)
                                    and date_idx < len(stat_value)
                                    else None
                                    if isinstance(stat_value, list)
                                    else stat_value
                                ),
                            }
                    else:
                        yield {
                            "flow_id": flow_id,
                            "flow_message_id": flow_message_id,
                            "send_channel": send_channel,
                            "statistic_name": stat_name,
                            "statistic_value": stat_value,
                        }
        else:
            # Pass through if not the expected structure
            yield response_record

    @override
    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, Any] | None:
        # The Klaviyo flow values report endpoint requires a JSON body
        # describing details such as the statistics to compute and timeframe.
        #
        # These can be configured in the config under "flow_values_report_config"
        # or use defaults for: statistics and timeframe.
        config = self.config.get("flow_values_report_config", {})
        return {
            "data": {
                "type": "flow-values-report",
                "attributes": {
                    "statistics": config.get("statistics", [
                        "opens",
                        "open_rate",
                        "delivered",
                        "clicks",
                        "click_rate",
                        "click_to_open_rate",
                        "unsubscribe_rate",
                        "conversion_rate",
                        "revenue_per_recipient",
                    ]),
                    "conversion_metric_id": config.get("conversion_metric_id", "WcYbF8"),
                    "timeframe": config.get("timeframe", {"key": "last_7_days"}),
                },
            }
        }

    @property
    def is_sorted(self) -> bool:
        return True
