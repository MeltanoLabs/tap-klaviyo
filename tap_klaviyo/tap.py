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
            "end_date",
            th.DateTimeType,
            description="The latest record date to sync (exclusive). Used for chunked backfill.",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.KlaviyoStream]:
        return [
            streams.EventsStream(self),
            streams.CampaignsStream(self),
            streams.MetricsStream(self),
            streams.ProfilesStream(self),
            streams.ListsStream(self),
            streams.ListPersonStream(self),
            streams.FlowsStream(self),
            streams.TemplatesStream(self),
        ]


if __name__ == "__main__":
    TapKlaviyo.cli()
