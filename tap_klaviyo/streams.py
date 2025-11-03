"""Stream type classes for tap-klaviyo."""

from __future__ import annotations

import typing as t
from pathlib import Path

from tap_klaviyo.client import KlaviyoStream

if t.TYPE_CHECKING:
    from urllib.parse import ParseResult

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

    @property
    def is_sorted(self) -> bool:
        return True


class CampaignsStream(KlaviyoStream):
    """Define custom stream."""

    name = "campaigns"
    path = "/campaigns"
    primary_keys = ["id"]
    replication_key = "updated_at"
    schema_filepath = SCHEMAS_DIR / "campaigns.json"

    @property
    def partitions(self) -> list[dict] | None:
        return [
            {
                "filter": "equals(messages.channel,'email')",
            },
            {
                "filter": "equals(messages.channel,'sms')",
            },
        ]

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, t.Any]:
        url_params = super().get_url_params(context, next_page_token)

        # Apply channel filters
        if context:
            parent_filter = url_params["filter"]
            url_params["filter"] = f"and({parent_filter},{context['filter']})"

        return url_params

    def get_child_context(self, record: dict, context: dict) -> dict:
        """Pass branchId to child stream"""
        return {"campaign_id": record["id"]}

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated_at"] = row["attributes"]["updated_at"]
        return row

    @property
    def is_sorted(self) -> bool:
        return False


class CampaignMessagesStream(KlaviyoStream):
    """Define custom stream."""

    name = "campaign_messages"
    path = "/campaigns/{campaign_id}/campaign-messages"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_messages.json"

    parent_stream_type = CampaignsStream

    def get_child_context(self, record, context):
        return super().get_child_context(record, context)

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        return row


class ProfilesStream(KlaviyoStream):
    """Define custom stream."""

    name = "profiles"
    path = "/profiles"
    primary_keys = ["id"]
    replication_key = "updated"
    schema_filepath = SCHEMAS_DIR / "profiles.json"
    max_page_size = 100

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row

    @property
    def is_sorted(self) -> bool:
        return True


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

    def get_child_context(self, record: dict, context: dict) -> dict:
        """Pass branchId to child stream"""
        return {"metric_id": record["id"]}


class CampaignValuesStream(KlaviyoStream):
    """Stream for fetching Campaign Values Reports."""

    name = "campaign_values"
    path = "/campaign-values-reports"
    primary_keys = ["metric_id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "campaign_values.json"

    parent_stream_type = MetricsStream

    def get_next_page_token(self, response, previous_token):
        """No pagination for Campaign Values API - return None."""
        return None

    def get_url(self, context):
        """Construct the full URL."""
        return f"{self.url_base}{self.path}"

    def request_body_json(self, context):
        """Construct request body for campaign-values-reports."""
        metric_id = context.get("metric_id") if context else None
        return {
            "data": {
                "type": "campaign-values-report",
                "attributes": {
                    "statistics": ["opens"],
                    "timeframe": {
                        "key": "last_7_days",
                    },
                    "conversion_metric_id": metric_id,
                },
            }
        }

    def request_records(self, context):
        """Send POST request with correct authentication headers."""
        url = self.get_url(context)
        body = self.request_body_json(context)

        # Use the stream's http_headers property which includes auth
        headers = self.http_headers.copy()
        headers["Content-Type"] = "application/json"

        self.logger.info(
            f"Requesting campaign values for metric_id: {context.get('metric_id')}"
        )

        try:
            response = self.requests_session.request(
                "POST",
                url,
                headers=headers,
                json=body,
            )
            response.raise_for_status()
        except Exception as e:
            self.logger.error(f"Request failed: {e}")
            # Log response body for debugging
            if hasattr(e, "response") and e.response is not None:
                self.logger.error(f"Response status: {e.response.status_code}")
                self.logger.error(f"Response body: {e.response.text}")
            raise

        yield from self.parse_response(response, context)

    def parse_response(self, response, context=None):
        """Transform the response data into a flat record."""
        data = response.json()

        # Get the metric_id from context
        metric_id = context.get("metric_id") if context else None

        # The API response format is something like:
        # {"data": {"type": "campaign-values-report", "attributes": {...}}}
        response_data = data.get("data", {})

        # Yield a single record with metric_id and flattened data
        yield {
            "metric_id": metric_id,
            **response_data,
        }


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

        return super().get_child_context(record, context)  # type: ignore[no-any-return]

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
    max_page_size = 1000

    def post_process(self, row: dict, context: dict) -> dict | None:
        row["list_id"] = context["list_id"]
        return row


class FlowsStream(KlaviyoStream):
    """Define custom stream."""

    name = "flows"
    path = "/flows"
    primary_keys = ["id"]
    replication_key = "updated"
    schema_filepath = SCHEMAS_DIR / "flows.json"
    is_sorted = True

    def get_child_context(
        self, record: dict, context: dict | None = None
    ) -> dict | None:
        """Pass branchId to child stream"""
        return {"flow_id": record["id"]}

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["updated"] = row["attributes"]["updated"]
        return row


class FlowActionsStream(KlaviyoStream):
    """Stream: flow actions for each flow (parent => provides flow_action_id)."""

    name = "flow_actions"
    path = "/flows/{flow_id}/flow-actions"
    primary_keys = ["id"]
    replication_key = None  # flows/actions expose created/updated attributes
    parent_stream_type = FlowsStream  # your existing FlowsStream
    schema_filepath = SCHEMAS_DIR / "flow_actions.json"

    def get_child_context(self, record: dict, context: dict | None = None) -> dict:
        return {
            "flow_id": context.get("flow_id") if context else None,
            "flow_action_id": record["id"],
        }

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        return row


class FlowMessagesStream(KlaviyoStream):
    """Stream: messages belonging to a flow action (child of FlowActionsStream)."""

    name = "flow_messages"
    path = "/flow-actions/{flow_action_id}/flow-messages"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = FlowActionsStream
    schema_filepath = SCHEMAS_DIR / "flow_messages.json"

    def get_child_context(self, record, context):
        # If you ever chain more levels later
        return super().get_child_context(record, context)

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """
        Add flow_action_id and flow_id to the row
        (values come from parent and grandparent contexts).
        """
        if context:
            if "flow_action_id" in context:
                row["flow_action_id"] = context["flow_action_id"]
            if "flow_id" in context:
                row["flow_id"] = context["flow_id"]
        return row


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

    @property
    def is_sorted(self) -> bool:
        return True
