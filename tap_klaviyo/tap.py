"""Klaviyo tap class."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_klaviyo import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


def _report_config_type(*, include_interval: bool = False) -> th.AnyOf:
    object_type = th.ObjectType(
        th.Property("statistics", th.ArrayType(th.StringType)),
        th.Property(
            "timeframe",
            th.ObjectType(
                th.Property("key", th.StringType),
            ),
        ),
    )
    if include_interval:
        object_type = th.ObjectType(
            th.Property("statistics", th.ArrayType(th.StringType)),
            th.Property("interval", th.StringType),
            th.Property(
                "timeframe",
                th.ObjectType(
                    th.Property("key", th.StringType),
                ),
            ),
        )
    else:
        object_type = th.ObjectType(
            th.Property("statistics", th.ArrayType(th.StringType)),
            th.Property("conversion_metric_id", th.StringType),
            th.Property(
                "timeframe",
                th.ObjectType(
                    th.Property("key", th.StringType),
                ),
            ),
        )

    return th.AnyOf(object_type, th.StringType, th.NullType)


def _query_metric_aggregates_config_type() -> th.AnyOf:
    return th.AnyOf(
        th.ObjectType(
            th.Property("metric_id", th.StringType),
            th.Property("page_cursor", th.StringType),
            th.Property("measurements", th.ArrayType(th.StringType)),
            th.Property("interval", th.StringType),
            th.Property("page_size", th.IntegerType),
            th.Property("by", th.ArrayType(th.StringType)),
            th.Property("return_fields", th.ArrayType(th.StringType)),
            th.Property("filter", th.ArrayType(th.StringType)),
            th.Property("timezone", th.StringType),
            th.Property("sort", th.StringType),
        ),
        th.StringType,
        th.NullType,
    )


def _named_query_metric_aggregates_config_type() -> th.AnyOf:
    report_object = th.ObjectType(
        th.Property("name", th.StringType),
        th.Property("metric_id", th.StringType),
        th.Property("page_cursor", th.StringType),
        th.Property("measurements", th.ArrayType(th.StringType)),
        th.Property("interval", th.StringType),
        th.Property("page_size", th.IntegerType),
        th.Property("by", th.ArrayType(th.StringType)),
        th.Property("return_fields", th.ArrayType(th.StringType)),
        th.Property("filter", th.ArrayType(th.StringType)),
        th.Property("timezone", th.StringType),
        th.Property("sort", th.StringType),
    )
    return th.AnyOf(th.ArrayType(report_object), th.StringType, th.NullType)


def _named_report_config_type() -> th.AnyOf:
    report_object = th.ObjectType(
        th.Property("name", th.StringType),
        th.Property("statistics", th.ArrayType(th.StringType)),
        th.Property("conversion_metric_id", th.StringType),
        th.Property(
            "timeframe",
            th.ObjectType(
                th.Property("key", th.StringType),
            ),
        ),
    )
    return th.AnyOf(th.ArrayType(report_object), th.StringType, th.NullType)


def _named_interval_report_config_type() -> th.AnyOf:
    report_object = th.ObjectType(
        th.Property("name", th.StringType),
        th.Property("statistics", th.ArrayType(th.StringType)),
        th.Property("interval", th.StringType),
        th.Property(
            "timeframe",
            th.ObjectType(
                th.Property("key", th.StringType),
            ),
        ),
    )
    return th.AnyOf(th.ArrayType(report_object), th.StringType, th.NullType)


class TapKlaviyo(Tap):
    """Klaviyo tap class."""

    name = "tap-klaviyo"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "revision",
            th.StringType,
            required=True,
            description="Klaviyo API endpoint revision. https://developers.klaviyo.com/en/docs/api_versioning_and_deprecation_policy#versioning",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "segment_series_report_config",
            _report_config_type(include_interval=True),
            description="Optional payload override for the segment series report stream.",
        ),
        th.Property(
            "segment_series_reports",
            _named_interval_report_config_type(),
            description="Optional list of named segment series report stream definitions.",
        ),
        th.Property(
            "campaign_values_report_config",
            _report_config_type(),
            description="Optional payload override for the campaign values report stream.",
        ),
        th.Property(
            "campaign_values_reports",
            _named_report_config_type(),
            description="Optional list of named campaign values report stream definitions.",
        ),
        th.Property(
            "flow_values_report_config",
            _report_config_type(),
            description="Optional payload override for the flow values report stream.",
        ),
        th.Property(
            "flow_values_reports",
            _named_report_config_type(),
            description="Optional list of named flow values report stream definitions.",
        ),
        th.Property(
            "query_metric_aggregates_config",
            _query_metric_aggregates_config_type(),
            description="Optional payload override for the legacy single query metric aggregates stream.",
        ),
        th.Property(
            "query_metric_aggregates_reports",
            _named_query_metric_aggregates_config_type(),
            description="Optional list of named query metric aggregates stream definitions.",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.KlaviyoStream]:
        discovered_streams: list[streams.KlaviyoStream] = [
            streams.EventsStream(self),
            streams.CampaignsStream(self),
            streams.MetricsStream(self),
            streams.ProfilesStream(self),
            streams.ListsStream(self),
            streams.ListPersonStream(self),
            streams.FlowsStream(self),
            streams.SegmentsStream(self),
            streams.TemplatesStream(self),
        ]
        discovered_streams.extend(streams.SegmentSeriesReportStream.from_config(self))
        discovered_streams.extend(streams.CampaignValuesReportStream.from_config(self))
        discovered_streams.extend(streams.FlowValuesReportStream.from_config(self))
        discovered_streams.extend(streams.QueryMetricAggregatesStream.from_config(self))
        return discovered_streams


if __name__ == "__main__":
    TapKlaviyo.cli()
