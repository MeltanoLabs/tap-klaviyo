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

class CampaignsStream(KlaviyoStream):
    """Define custom stream."""

    name = "campaigns"
    path = "/campaigns"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("name",
            th.StringType,
            description="The campaign name"),
        th.Property(
            "id",
            th.StringType,
            description="The campaign ID"),
        th.Property(
            "type",
            th.StringType,
            description="The campaign type"),
        th.Property(
            "status",
            th.StringType,
            description="The campaign status"),
        th.Property(
            "archived",
            th.BooleanType,
            description="Whether the campaign has been archived or not"),
        th.Property(
            "channel",
            th.StringType,
            description="The campaign channel"),
        th.Property(
            "message",
            th.StringType,
            description="The campaign message (id?)"), #TODO: validate
        th.Property(
            "created_at",
            th.DateTimeType,
            description="Timestamp when the campaign was created"),
        th.Property(
            "scheduled_at",
            th.DateTimeType,
            description="Timestamp when the campaign was scheduled"),
        th.Property(
            "updated_at",
            th.DateTimeType,
            description="Timestamp when the campaign was updated"),
        th.Property(
            "send_time",
            th.StringType,
            description="The campaign time when the campaign was sent"),
        th.Property(
            "audiences",
            th.ObjectType(
                th.Property(
                    "included",
                    th.StringType,
                    description="Included metadata"
                ),
                th.Property(
                    "excluded",
                    th.StringType,
                    description="Excluded metadata"
                ),
                th.Property(
                    "use_smart_sending",
                    th.BooleanType,
                    description="Event timestamp in ISO 8601 format (YYYY-MM-DDTHH:MM:SS.mmmmmm)"
                ),
                th.Property(
                    "ignore_unsubscribes",
                    th.BooleanType,
                    description="Event timestamp in ISO 8601 format (YYYY-MM-DDTHH:MM:SS.mmmmmm)"
                ),
            ),
        ),
        th.Property(
            "tracking_options",
            th.ObjectType(
                th.Property(
                    "is_tracking_opens",
                    th.BooleanType,
                    description="Whether the campaign is tracking opens"
                ),
                th.Property(
                    "is_tracking_clicks",
                    th.BooleanType,
                    description="Whether the campaign is tracking clicks"
                ),
                th.Property(
                    "is_add_utm",
                    th.BooleanType,
                    description="Whether the campaign is added UTM"
                ),
                th.Property(
                    "utm_params",
                    th.StringType,
                    description="Campaign UTM parameters"
                ),
            ),
        ),
        th.Property(
            "send_strategy",
            th.ObjectType(
                th.Property(
                    "method",
                    th.StringType,
                    description="Campaign send strategy"
                ),
                th.Property(
                    "options_static",
                    th.StringType,
                    description="Static option"
                ),
                th.Property(
                    "options_throttled",
                    th.StringType,
                    description="Throttled option"
                ),
                th.Property(
                    "options_sto",
                    th.StringType,
                    description="STO option" #TODO: validate
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
        #params["filter"] = "greater-than(datetime,2023-02-22T00:00:00Z)"

        return params