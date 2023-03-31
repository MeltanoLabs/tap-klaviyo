"""Stream type classes for tap-klaviyo."""

from __future__ import annotations

from pathlib import Path
from datetime import datetime

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_klaviyo.client import KlaviyoStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class EventsStream(KlaviyoStream):
    """Define custom stream."""

    name = "events"
    path = "/events"
    primary_keys = ["id"]
    replication_key = "datetime"
    schema_filepath = SCHEMAS_DIR / "event.json"

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

        if self.replication_key:
            filter_timestamp = self.get_starting_timestamp(context)
            params["filter"] = f"greater-than(datetime,{filter_timestamp})"

        return params

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        row["datetime"] = row["attributes"]["datetime"]
        return row

class CampaignsStream(KlaviyoStream):
    """Define custom stream."""

    name = "campaigns"
    path = "/campaigns"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "campaigns.json"

    # schema = th.PropertiesList(
    #     th.Property(
    #         "id",
    #         th.StringType,
    #         description="The campaign ID"),
    #     th.Property(
    #         "attributes",
    #         th.ObjectType(
    #             th.Property("name",
    #                 th.StringType,
    #                 description="The campaign name"
    #             ),
    #             th.Property(
    #                 "type",
    #                 th.StringType,
    #                 description="The campaign type"
    #             ),
    #             th.Property(
    #                 "status",
    #                 th.StringType,
    #                 description="The campaign status"
    #             ),
    #             th.Property(
    #                 "archived",
    #                 th.BooleanType,
    #                 description="Whether the campaign has been archived or not"
    #             ),
    #             th.Property(
    #                 "channel",
    #                 th.StringType,
    #                 description="The campaign channel"
    #             ),
    #             th.Property(
    #                 "message",
    #                 th.StringType,
    #                 description="The campaign message (id?)"
    #             ), #TODO: validate
    #             th.Property(
    #                 "created_at",
    #                 th.DateTimeType,
    #                 description="Timestamp when the campaign was created"
    #             ),
    #             th.Property(
    #                 "scheduled_at",
    #                 th.DateTimeType,
    #                 description="Timestamp when the campaign was scheduled"
    #             ),
    #             th.Property(
    #                 "updated_at",
    #                 th.DateTimeType,
    #                 description="Timestamp when the campaign was updated"
    #             ),
    #             th.Property(
    #                 "send_time",
    #                 th.StringType,
    #                 description="The campaign time when the campaign was sent"
    #             ),
    #         ),
    #     ),
    # ).to_dict()

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

        if self.replication_key:
            if self.get_starting_timestamp(context):
                filter_timestamp = self.get_starting_timestamp(context)
            elif self.config.get("start_date"):
                filter_timestamp = datetime.strptime(self.config("start_date"), "%Y-%m-%d").isoformat()
            else:
                filter_timestamp = datetime(2000,1,1).isoformat()

            params["filter"] = f"greater-than({self.replication_key},{filter_timestamp})"

        return params

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        row["updated_at"] = row["attributes"]["updated_at"]
        return row

class MetricsStream(KlaviyoStream):
    """Define custom stream."""

    name = "metrics"
    path = "/metrics"
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "metrics.json"

    # schema = th.PropertiesList(
    #     th.Property(
    #         "id",
    #         th.StringType,
    #         description="The metric ID"),
    #     th.Property(
    #         "type",
    #         th.StringType,
    #         description="The metric type"),
    #     th.Property(
    #         "attributes",
    #         th.ObjectType(
    #             th.Property(
    #                 "name",
    #                 th.StringType,
    #                 description="The metric name"
    #             ),
    #             th.Property(
    #                 "created",
    #                 th.StringType,
    #                 description="Timestamp when the metric was created"
    #             ),
    #             th.Property(
    #                 "integration",
    #                 th.ObjectType(
    #                     th.Property(
    #                         "id",
    #                         th.StringType,
    #                         description="The name of the integration"
    #                     )
    #                 ),
    #             ),
    #         ),
    #     ),
    # ).to_dict()

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

        if self.replication_key:
            if self.get_starting_timestamp(context):
                filter_timestamp = self.get_starting_timestamp(context)
            elif self.config.get("start_date"):
                filter_timestamp = datetime.strptime(self.config("start_date"), "%Y-%m-%d").isoformat()
            else:
                filter_timestamp = datetime(2000,1,1).isoformat()

            params["filter"] = f"greater-than({self.replication_key},{filter_timestamp})"

        return params

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        row["updated_at"] = row["attributes"]["updated_at"]
        return row