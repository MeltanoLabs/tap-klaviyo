"""Stream type classes for tap-klaviyo."""

from __future__ import annotations

from pathlib import Path

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_klaviyo.client import KlaviyoStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class EventsStream(KlaviyoStream):
    """Define custom stream."""

    name = "events"
    path = "/events"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property(
            "id",
            th.StringType,
            description="The event ID",
        ),
        th.Property(
            "type",
            th.StringType,
            description="The event type",
        ),
        th.Property(
            "attributes",
            th.ObjectType(
                th.Property(
                    "metric_id",
                    th.StringType,
                    description="The metric ID"
                ),
                th.Property(
                    "timestamp",
                    th.IntegerType,
                    description="Event timestamp in seconds"
                ),
                th.Property(
                    "datetime",
                    th.DateTimeType,
                    description="Event timestamp in ISO 8601 format (YYYY-MM-DDTHH:MM:SS.mmmmmm)"
                ),
            ),
        ),
    ).to_dict()

# TODO: Change to CampaignsStream
class GroupsStream(KlaviyoStream):
    """Define custom stream."""

    name = "groups"
    path = "/groups"
    primary_keys = ["id"]
    replication_key = "modified"
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("id", th.StringType),
        th.Property("modified", th.DateTimeType),
    ).to_dict()
