"""Klaviyo tap class."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_klaviyo import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from tap_klaviyo.client import KlaviyoStream


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
            "segment_series_reports_config",
            th.ObjectType(
                th.Property("statistics", th.ArrayType(th.StringType)),
                th.Property("interval", th.StringType),
                th.Property(
                    "timeframe",
                    th.ObjectType(
                        th.Property("key", th.StringType),
                    ),
                ),
            ),
            description="Optional payload override for the segment series report stream.",
        ),
        th.Property(
            "campaign_values_report_config",
            th.ObjectType(
                th.Property("statistics", th.ArrayType(th.StringType)),
                th.Property("conversion_metric_id", th.StringType),
                th.Property(
                    "timeframe",
                    th.ObjectType(
                        th.Property("key", th.StringType),
                    ),
                ),
            ),
            description="Optional payload override for the campaign values report stream.",
        ),
        th.Property(
            "flow_values_report_config",
            th.ObjectType(
                th.Property("statistics", th.ArrayType(th.StringType)),
                th.Property("conversion_metric_id", th.StringType),
                th.Property(
                    "timeframe",
                    th.ObjectType(
                        th.Property("key", th.StringType),
                    ),
                ),
            ),
            description="Optional payload override for the flow values report stream.",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[KlaviyoStream]:
        return [
            streams.EventsStream(self),
            streams.CampaignsStream(self),
            streams.MetricsStream(self),
            streams.ProfilesStream(self),
            streams.ListsStream(self),
            streams.ListPersonStream(self),
            streams.FlowsStream(self),
            streams.SegmentsStream(self),
            streams.TemplatesStream(self),
            streams.SegmentSeriesReportStream(self),
            streams.CampaignValuesReportStream(self),
            streams.FlowValuesReportStream(self),
        ]


if __name__ == "__main__":
    TapKlaviyo.cli()
