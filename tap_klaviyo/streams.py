"""Stream type classes for tap-klaviyo."""

from __future__ import annotations

import json
import sys
from datetime import datetime, time, timedelta, timezone
from importlib import resources
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl
from zoneinfo import ZoneInfo

from tap_klaviyo import schemas
from tap_klaviyo.client import DEFAULT_START_DATE, KlaviyoStream, _isodate_from_date_string

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable
    from urllib.parse import ParseResult

    from singer_sdk.helpers.types import Context, Record


def _get_report_config_value(config: dict[str, Any], key: str) -> dict[str, Any]:
    value = config.get(key, {})
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed_value = json.loads(value)
        if isinstance(parsed_value, dict):
            return parsed_value
        msg = f"Expected '{key}' JSON to decode to an object."
        raise TypeError(msg)
    msg = f"Expected '{key}' to be an object or JSON string."
    raise TypeError(msg)


def _get_report_config_list_value(config: dict[str, Any], key: str) -> list[dict[str, Any]]:
    value = config.get(key, [])
    if value is None:
        return []
    if isinstance(value, list):
        if all(isinstance(item, dict) for item in value):
            return value
        msg = f"Expected all '{key}' items to be objects."
        raise TypeError(msg)
    if isinstance(value, str):
        parsed_value = json.loads(value)
        if isinstance(parsed_value, list) and all(
            isinstance(item, dict) for item in parsed_value
        ):
            return parsed_value
        msg = f"Expected '{key}' JSON to decode to an array of objects."
        raise TypeError(msg)
    msg = f"Expected '{key}' to be an array of objects or JSON string."
    raise TypeError(msg)


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
    def partitions(self) -> list[dict] | None:
        return [
            {
                "filter": "equals(messages.channel,'email')",
            },
            {
                "filter": "equals(messages.channel,'sms')",
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
        return True


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
    primary_keys = ["report_name", "segment_id", "date", "statistic_name"]
    records_jsonpath = "$"  # Pass entire response

    # force POST instead of the default GET
    http_method = "POST"
    # tell the base class to send the prepared payload as JSON
    payload_as_json = True
    _schema_path = resources.files(schemas).joinpath("segment_series_report.json")

    def __init__(
        self,
        tap: Any,
        *,
        report_config: dict[str, Any] | None = None,
        report_name: str | None = None,
    ) -> None:
        self._report_config = report_config
        super().__init__(tap=tap, name=report_name or self.name)

    @property
    def schema(self) -> dict[str, Any]:
        """Return the shared schema for all named segment series streams."""
        return json.loads(self._schema_path.read_text(encoding="utf-8"))

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
                            "report_name": self.name,
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
        config = self._report_config or _get_report_config_value(
            self.config,
            "segment_series_report_config",
        )
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

    @classmethod
    def from_config(cls, tap: Any) -> list["SegmentSeriesReportStream"]:
        """Build zero or more segment series report streams from tap config."""
        named_reports = _get_report_config_list_value(tap.config, "segment_series_reports")
        streams: list[SegmentSeriesReportStream] = []

        for report in named_reports:
            report_name = report.get("name")
            if not isinstance(report_name, str) or not report_name:
                msg = (
                    "Each 'segment_series_reports' entry must include a non-empty 'name'."
                )
                raise ValueError(msg)
            streams.append(cls(tap, report_config=report, report_name=report_name))

        if streams:
            return streams

        legacy_config = _get_report_config_value(tap.config, "segment_series_report_config")
        if legacy_config:
            return [cls(tap, report_config=legacy_config, report_name=cls.name)]

        return []

class CampaignValuesReportStream(KlaviyoStream):
    """The Klaviyo endpoint for campaign values reports requires a POST call with a JSON body.

    The default RESTStream implementation issues GET requests, so we
    override the method and enable JSON payloads here.
    """

    name = "campaign_values_report"
    path = "/campaign-values-reports"
    primary_keys = [
        "report_name",
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
    _schema_path = resources.files(schemas).joinpath("campaign_values_report.json")

    def __init__(
        self,
        tap: Any,
        *,
        report_config: dict[str, Any] | None = None,
        report_name: str | None = None,
    ) -> None:
        self._report_config = report_config
        super().__init__(tap=tap, name=report_name or self.name)

    @property
    def schema(self) -> dict[str, Any]:
        """Return the shared schema for all named campaign value streams."""
        return json.loads(self._schema_path.read_text(encoding="utf-8"))

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
                                "report_name": self.name,
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
                            "report_name": self.name,
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
        # timeframe, and optionally a conversion metric id.
        #
        # These can be configured in the config under
        # "campaign_values_report_config" or use defaults for: statistics
        # and timeframe. conversion_metric_id is intentionally not defaulted
        # because it is account-specific.
        config = self._report_config or _get_report_config_value(
            self.config,
            "campaign_values_report_config",
        )
        attributes = {
            "statistics": config.get("statistics", [
                "opens_unique",
                "open_rate",
                "delivered",
                "clicks_unique",
                "click_to_open_rate",
                "revenue_per_recipient",
            ]),
            "timeframe": config.get("timeframe", {"key": "last_7_days"}),
        }
        if config.get("conversion_metric_id"):
            attributes["conversion_metric_id"] = config["conversion_metric_id"]

        return {
            "data": {
                "type": "campaign-values-report",
                "attributes": attributes,
            }
        }

    @classmethod
    def from_config(cls, tap: Any) -> list["CampaignValuesReportStream"]:
        """Build zero or more campaign values report streams from tap config."""
        named_reports = _get_report_config_list_value(tap.config, "campaign_values_reports")
        streams: list[CampaignValuesReportStream] = []

        for report in named_reports:
            report_name = report.get("name")
            if not isinstance(report_name, str) or not report_name:
                msg = (
                    "Each 'campaign_values_reports' entry must include a non-empty 'name'."
                )
                raise ValueError(msg)
            streams.append(cls(tap, report_config=report, report_name=report_name))

        if streams:
            return streams

        legacy_config = _get_report_config_value(tap.config, "campaign_values_report_config")
        if legacy_config:
            return [cls(tap, report_config=legacy_config, report_name=cls.name)]

        return []

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
        "report_name",
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
    _schema_path = resources.files(schemas).joinpath("flow_values_report.json")

    def __init__(
        self,
        tap: Any,
        *,
        report_config: dict[str, Any] | None = None,
        report_name: str | None = None,
    ) -> None:
        self._report_config = report_config
        super().__init__(tap=tap, name=report_name or self.name)

    @property
    def schema(self) -> dict[str, Any]:
        """Return the shared schema for all named flow value streams."""
        return json.loads(self._schema_path.read_text(encoding="utf-8"))

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
                                "report_name": self.name,
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
                            "report_name": self.name,
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
        # describing details such as the statistics to compute, timeframe,
        # and optionally a conversion metric id.
        #
        # These can be configured in the config under
        # "flow_values_report_config" or use defaults for: statistics
        # and timeframe. conversion_metric_id is intentionally not defaulted
        # because it is account-specific.
        config = self._report_config or _get_report_config_value(
            self.config,
            "flow_values_report_config",
        )
        attributes = {
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
            "timeframe": config.get("timeframe", {"key": "last_7_days"}),
        }
        if config.get("conversion_metric_id"):
            attributes["conversion_metric_id"] = config["conversion_metric_id"]

        return {
            "data": {
                "type": "flow-values-report",
                "attributes": attributes,
            }
        }

    @classmethod
    def from_config(cls, tap: Any) -> list["FlowValuesReportStream"]:
        """Build zero or more flow values report streams from tap config."""
        named_reports = _get_report_config_list_value(tap.config, "flow_values_reports")
        streams: list[FlowValuesReportStream] = []

        for report in named_reports:
            report_name = report.get("name")
            if not isinstance(report_name, str) or not report_name:
                msg = (
                    "Each 'flow_values_reports' entry must include a non-empty 'name'."
                )
                raise ValueError(msg)
            streams.append(cls(tap, report_config=report, report_name=report_name))

        if streams:
            return streams

        legacy_config = _get_report_config_value(tap.config, "flow_values_report_config")
        if legacy_config:
            return [cls(tap, report_config=legacy_config, report_name=cls.name)]

        return []

    @property
    def is_sorted(self) -> bool:
        return True


class QueryMetricAggregatesStream(KlaviyoStream):
    """Define custom stream for Klaviyo query metric aggregates."""

    name = "query_metric_aggregates"
    path = "/metric-aggregates"
    primary_keys = ["report_name", "metric_id", "date", "dimensions", "measurement_name"]
    replication_key = "date"
    records_jsonpath = "$"
    http_method = "POST"
    payload_as_json = True
    _schema_path = resources.files(schemas).joinpath("query_metric_aggregates.json")

    def __init__(
        self,
        tap: Any,
        *,
        report_config: dict[str, Any] | None = None,
        report_name: str | None = None,
    ) -> None:
        self._report_config = report_config
        super().__init__(tap=tap, name=report_name or self.name)

    @property
    def schema(self) -> dict[str, Any]:
        """Return the shared schema for all named metric aggregate streams."""
        return json.loads(self._schema_path.read_text(encoding="utf-8"))

    @override
    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        """Get records by flattening metric aggregate response data."""
        for record in super().get_records(context):
            yield from self._flatten_response_record(record)

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, Any]:
        """Return query params without the default replication filter.

        This endpoint expects datetime filtering in the POST body. The base
        stream implementation would append a query-string filter using the
        replication key (`date`), but Klaviyo does not allow filtering this
        resource on `date`.
        """
        params: dict[str, Any] = {}
        if next_page_token:
            params.update(parse_qsl(next_page_token.query))
        return params

    def _flatten_response_record(
        self,
        response_record: dict[str, Any],
    ) -> Iterable[dict[str, Any]]:
        """Flatten a metric aggregate response into Singer records."""
        if "data" not in response_record or not isinstance(response_record.get("data"), dict):
            yield response_record
            return

        data = response_record["data"]
        attributes = data.get("attributes", {}) or {}
        dates = attributes.get("dates", []) or []
        rows = attributes.get("data", []) or []

        metric_id = self._metric_aggregate_config().get("metric_id")
        aggregate_id = data.get("id")

        for row in rows:
            dimensions = row.get("dimensions", []) or []
            measurements = row.get("measurements", {}) or {}

            for measurement_name, measurement_value in measurements.items():
                if dates and isinstance(measurement_value, list):
                    for date_idx, date in enumerate(dates):
                        yield {
                            "report_name": self.name,
                            "metric_aggregate_id": aggregate_id,
                            "metric_id": metric_id,
                            "date": date,
                            "dimensions": dimensions,
                            "measurement_name": measurement_name,
                            "measurement_value": (
                                measurement_value[date_idx]
                                if date_idx < len(measurement_value)
                                else None
                            ),
                        }
                else:
                    yield {
                        "report_name": self.name,
                        "metric_aggregate_id": aggregate_id,
                        "metric_id": metric_id,
                        "date": dates[0] if len(dates) == 1 else None,
                        "dimensions": dimensions,
                        "measurement_name": measurement_name,
                        "measurement_value": measurement_value,
                    }

    def _metric_aggregate_config(self) -> dict[str, Any]:
        """Return validated config for the query metric aggregates stream."""
        config = self._report_config or _get_report_config_value(
            self.config,
            "query_metric_aggregates_config",
        )

        if not config.get("metric_id"):
            msg = (
                "'query_metric_aggregates_config.metric_id' is required for the "
                "query_metric_aggregates stream."
            )
            raise ValueError(msg)

        return config

    @classmethod
    def from_config(cls, tap: Any) -> list["QueryMetricAggregatesStream"]:
        """Build zero or more metric aggregate streams from tap config."""
        named_reports = _get_report_config_list_value(tap.config, "query_metric_aggregates_reports")
        streams: list[QueryMetricAggregatesStream] = []

        for report in named_reports:
            report_name = report.get("name")
            if not isinstance(report_name, str) or not report_name:
                msg = (
                    "Each 'query_metric_aggregates_reports' entry must include a "
                    "non-empty 'name'."
                )
                raise ValueError(msg)
            streams.append(cls(tap, report_config=report, report_name=report_name))

        if streams:
            return streams

        legacy_config = _get_report_config_value(tap.config, "query_metric_aggregates_config")
        if legacy_config:
            return [cls(tap, report_config=legacy_config, report_name=cls.name)]

        return []

    def _get_filter_start(self, context: Context | None) -> str:
        """Return the lower datetime bound from state, config, or default."""
        if start_timestamp := self.get_starting_timestamp(context):
            return start_timestamp.isoformat()

        if start_date := self.config.get("start_date"):
            return _isodate_from_date_string(start_date)

        return DEFAULT_START_DATE

    def _get_filter_end(self, timezone_name: str | None) -> str:
        """Return the exclusive upper datetime bound at the end of today."""
        tzinfo = timezone.utc
        if timezone_name:
            tzinfo = ZoneInfo(timezone_name)

        now = datetime.now(tzinfo)
        tomorrow = now.date() + timedelta(days=1)
        return datetime.combine(tomorrow, time.min, tzinfo=tzinfo).isoformat()

    def _build_filters(self, context: Context | None, config: dict[str, Any]) -> list[str]:
        """Merge configured filters with state-driven datetime bounds."""
        configured_filters = config.get("filter", [])
        if not isinstance(configured_filters, list):
            msg = "'query_metric_aggregates_config.filter' must be an array when provided."
            raise TypeError(msg)

        datetime_prefixes = (
            "greater-than(datetime,",
            "greater-or-equal(datetime,",
            "less-than(datetime,",
            "less-or-equal(datetime,",
        )
        filters = [
            filter_value
            for filter_value in configured_filters
            if not (
                isinstance(filter_value, str)
                and filter_value.startswith(datetime_prefixes)
            )
        ]
        filters.extend(
            [
                f"greater-or-equal(datetime,{self._get_filter_start(context)})",
                f"less-than(datetime,{self._get_filter_end(config.get('timezone'))})",
            ],
        )
        return filters

    @override
    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, Any] | None:
        """Prepare the JSON body for the metric aggregates query."""
        config = self._metric_aggregate_config()
        attributes = {
            "metric_id": config["metric_id"],
            "measurements": config.get("measurements", ["count"]),
            "interval": config.get("interval", "day"),
            "page_size": config.get("page_size", 500),
            "filter": self._build_filters(context, config),
        }

        optional_keys = ("page_cursor", "by", "return_fields", "timezone", "sort")
        for key in optional_keys:
            if key in config and config[key] is not None:
                attributes[key] = config[key]

        return {
            "data": {
                "type": "metric-aggregate",
                "attributes": attributes,
            },
        }
