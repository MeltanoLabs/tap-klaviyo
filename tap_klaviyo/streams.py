"""Stream type classes for tap-klaviyo."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from tap_klaviyo.client import KlaviyoStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from urllib.parse import ParseResult

    from singer_sdk.helpers.types import Context, Record


class EventsStream(KlaviyoStream):
    """Define custom stream."""

    name = "events"
    path = "/events"
    primary_keys = ["id"]
    replication_key = "datetime"

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        row["datetime"] = row["attributes"]["datetime"]
        return row

    @override
    @property
    def is_sorted(self) -> bool:
        return True


class CampaignsStream(KlaviyoStream):
    """Define custom stream."""

    name = "campaigns"
    path = "/campaigns"
    primary_keys = ["id"]
    replication_key = "updated_at"

    @override
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

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, Any]:
        url_params = super().get_url_params(context, next_page_token)

        # Apply channel filters
        if context:
            parent_filter = url_params["filter"]
            url_params["filter"] = f"and({parent_filter},{context['filter']})"

        return url_params

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        row["updated_at"] = row["attributes"]["updated_at"]
        return row

    @override
    @property
    def is_sorted(self) -> bool:
        return True


class ProfilesStream(KlaviyoStream):
    """Define custom stream."""

    name = "profiles"
    path = "/profiles"
    primary_keys = ["id"]
    replication_key = "updated"
    max_page_size = 100

    @property
    def _declared_attrs(self) -> frozenset[str]:
        """Attribute keys declared in profiles.json - computed once, cached."""
        if not hasattr(self, "__declared_attrs"):
            self.__declared_attrs = frozenset(
                self.schema.get("properties", {})
                .get("attributes", {})
                .get("properties", {})
                .keys()
            )
        return self.__declared_attrs

    def _stringify_undeclared_complex_attrs(self, attrs: dict) -> None:
        """JSON-stringify any undeclared complex attribute values so BQ
        doesn't choke on inconsistent nested structures."""
        import json as _json

        for key, val in attrs.items():
            if key not in self._declared_attrs and isinstance(val, (dict, list)):
                attrs[key] = _json.dumps(val)

    @property
    def _number_field_paths(self) -> list[tuple[list[str], str]]:
        """Paths to number-only fields under attributes - computed once.

        Returns (container_keys, field_name) tuples where container_keys
        is the list of keys to traverse from ``attributes`` to the parent
        dict.  E.g. attributes.location.latitude -> (["location"], "latitude")
        """
        if not hasattr(self, "__number_field_paths"):
            paths: list[tuple[list[str], str]] = []

            def _walk(node: dict, crumbs: list[str]) -> None:
                for key, fs in node.get("properties", {}).items():
                    types = fs.get("type", [])
                    if isinstance(types, str):
                        types = [types]
                    if "number" in types and "string" not in types:
                        paths.append((list(crumbs), key))
                    if "object" in types:
                        _walk(fs, [*crumbs, key])

            _walk(
                self.schema.get("properties", {}).get("attributes", {}),
                [],
            )
            self.__number_field_paths = paths
        return self.__number_field_paths

    def _coerce_number_fields(self, row: Record, attrs: dict) -> None:
        """Ensure every schema-declared number field is actually numeric.

        If a value cannot be parsed as a float, log a warning with the
        profile id/email and set it to zero.
        """
        for container_keys, field_name in self._number_field_paths:
            obj = attrs
            for step in container_keys:
                obj = obj.get(step) or {}
                if not isinstance(obj, dict):
                    break
            else:
                val = obj.get(field_name)
                if val is not None:
                    try:
                        obj[field_name] = float(val)
                    except (ValueError, TypeError):
                        dotted = ".".join(
                            ["attributes", *container_keys, field_name]
                        )
                        self.logger.warning(
                            "Bad %s value: %r - setting to 0  "
                            "profile_id=%s  email=%s",
                            dotted,
                            val,
                            row.get("id"),
                            attrs.get("email"),
                        )
                        obj[field_name] = 0

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        row["updated"] = row["attributes"]["updated"]

        attrs = row.get("attributes", {})
        self._stringify_undeclared_complex_attrs(attrs)
        self._coerce_number_fields(row, attrs)

        return row

    @override
    @property
    def is_sorted(self) -> bool:
        return True


class MetricsStream(KlaviyoStream):
    """Define custom stream."""

    name = "metrics"
    path = "/metrics"
    primary_keys = ["id"]
    replication_key = None

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        row["updated"] = row["attributes"]["updated"]
        return row


class ListsStream(KlaviyoStream):
    """Define custom stream."""

    name = "lists"
    path = "/lists"
    primary_keys = ["id"]
    replication_key = "updated"

    @override
    def get_child_context(self, record: Record, context: Context | None) -> Context | None:
        context = dict(context) if context else {}
        context["list_id"] = record["id"]

        return super().get_child_context(record, context)

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        row["updated"] = row["attributes"]["updated"]
        return row


class ListPersonStream(KlaviyoStream):
    """Define custom stream."""

    name = "listperson"
    path = "/lists/{list_id}/relationships/profiles/"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = ListsStream
    max_page_size = 1000

    @override
    def post_process(self, row: Record, context: Context | None = None) -> Record | None:
        row["list_id"] = context["list_id"] if context else None
        return row


class FlowsStream(KlaviyoStream):
    """Define custom stream."""

    name = "flows"
    path = "/flows"
    primary_keys = ["id"]
    replication_key = "updated"
    is_sorted = True

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        row["updated"] = row["attributes"]["updated"]
        return row


class TemplatesStream(KlaviyoStream):
    """Define custom stream."""

    name = "templates"
    path = "/templates"
    primary_keys = ["id"]
    replication_key = "updated"

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        row["updated"] = row["attributes"]["updated"]
        return row

    @property
    def is_sorted(self) -> bool:
        return True
