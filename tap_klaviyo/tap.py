"""Klaviyo tap class."""

from __future__ import annotations

from datetime import datetime, timezone

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_klaviyo import streams


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
            default=datetime(2000, 1, 1, tzinfo=timezone.utc).isoformat()
        ),
        th.Property(
            "metrics_log_level",
            th.StringType,
            description="The log level for the metrics stream",
            default="INFO"
        ),
        th.Property(
            "reports_attributes",
            th.ObjectType(
                th.Property('campaign_values_reports', th.ObjectType(
                    th.Property('statistics', th.ArrayType(th.StringType)),
                    th.Property('timeframe',
                                th.ObjectType(
                                    th.Property('key', th.StringType), additional_properties=False)),
                    th.Property('conversion_metric_id', th.StringType), additional_properties=False)),
                th.Property('flow_values_reports', th.ObjectType(
                    th.Property('statistics', th.ArrayType(th.StringType)),
                    th.Property('conversion_metric_id', th.StringType), additional_properties=False)
            ), additional_properties=False),
            description="Redefine report attributes (e.g. {'campaign_values_reports': {'statistics: ['click_rate']}} )",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.KlaviyoStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.EventsStream(self),
            streams.CampaignsStream(self),
            streams.CampaignValuesReportsStream(self),
            streams.MetricsStream(self),
            streams.ProfilesStream(self),
            streams.ListsStream(self),
            streams.ListPersonIncrementalStream(self),
            streams.ListPersonStream(self),
            streams.FlowsStream(self),
            streams.FlowActionsStream(self),
            streams.FlowMessagesStream(self),
            streams.FlowValuesReportsStream(self),
            streams.TemplatesStream(self),
            streams.SegmentsStream(self)
        ]


if __name__ == "__main__":
    TapKlaviyo.cli()
