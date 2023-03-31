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
            if self.get_starting_timestamp(context):
                filter_timestamp = self.get_starting_timestamp(context)
            elif self.config.get("start_date"):
                filter_timestamp = datetime.strptime(self.config("start_date"), "%Y-%m-%d").isoformat()
            else:
                filter_timestamp = datetime(2000,1,1).isoformat()

            params["filter"] = f"greater-than({self.replication_key},{filter_timestamp})"

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


class ProfilesStream(KlaviyoStream):
    """Define custom stream."""

    name = "profiles"
    path = "/profiles"
    primary_keys = ["id"]
    replication_key = "updated"
    schema_filepath = SCHEMAS_DIR / "profiles.json"

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
    replication_key = "updated"
    schema_filepath = SCHEMAS_DIR / "metrics.json"

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

        return params

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row