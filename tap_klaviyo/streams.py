"""Stream type classes for tap-klaviyo."""

from __future__ import annotations

from pathlib import Path

from tap_klaviyo.client import KlaviyoStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class EventsStream(KlaviyoStream):
    """Define custom stream."""

    name = "events"
    path = "/events"
    primary_keys = ["id"]
    replication_key = "datetime"
    schema_filepath = SCHEMAS_DIR / "event.json"

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["datetime"] = row["attributes"]["datetime"]
        return row


class CampaignsStream(KlaviyoStream):
    """Define custom stream."""

    name = "campaigns"
    path = "/campaigns"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "campaigns.json"

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated_at"] = row["attributes"]["updated_at"]
        return row


class ProfilesStream(KlaviyoStream):
    """Define custom stream."""

    name = "profiles"
    path = "/profiles"
    primary_keys = ["id"]
    replication_key = "updated"
    schema_filepath = SCHEMAS_DIR / "profiles.json"

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row


class MetricsStream(KlaviyoStream):
    """Define custom stream."""

    name = "metrics"
    path = "/metrics"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "metrics.json"

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row


class ListsStream(KlaviyoStream):
    """Define custom stream."""

    name = "lists"
    path = "/lists"
    primary_keys = ["id"]
    replication_key = "updated"
    schema_filepath = SCHEMAS_DIR / "lists.json"

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        context = context or {}
        context["list_id"] = record["id"]

        return super().get_child_context(record, context)

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row


class ListPersonStream(KlaviyoStream):
    """Define custom stream."""

    name = "listperson"
    path = "/lists/{list_id}/relationships/profiles/"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = ListsStream
    schema_filepath = SCHEMAS_DIR / "listperson.json"

    def post_process(self, row: dict, context: dict) -> dict | None:
        row["list_id"] = context["list_id"]
        return row


class FlowsStream(KlaviyoStream):
    """Define custom stream."""

    name = "flows"
    path = "/flows"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "flows.json"


class TemplatesStream(KlaviyoStream):
    """Define custom stream."""

    name = "templates"
    path = "/templates"
    primary_keys = ["id"]
    replication_key = "updated"
    schema_filepath = SCHEMAS_DIR / "templates.json"

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row
