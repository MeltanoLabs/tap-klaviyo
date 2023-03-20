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
    # TODO: Finish building out all properties
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

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {
            **self.base_url_params,
        }

        if next_page_token:
            params["page[cursor]"] = next_page_token

        # TODO: Convert this to a config var
        params["filter"] = "greater-than(datetime,2023-03-15T00:00:00Z)"

        return params

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
