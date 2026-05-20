"""Tests for CampaignsStream pagination filter behavior."""

from __future__ import annotations

from urllib.parse import quote, urlparse

import pytest

from tap_klaviyo.streams import CampaignsStream
from tap_klaviyo.tap import TapKlaviyo

SAMPLE_CONFIG = {
    "auth_token": "test-token",
    "revision": "2026-01-15",
}


@pytest.fixture
def campaigns_stream() -> CampaignsStream:
    tap = TapKlaviyo(config=SAMPLE_CONFIG)
    return CampaignsStream(tap=tap)


def test_first_page_filter_combines_replication_and_channel(
    campaigns_stream: CampaignsStream,
) -> None:
    context = {"filter": "equals(messages.channel,'email')"}
    params = campaigns_stream.get_url_params(context, next_page_token=None)
    assert params["filter"].startswith("and(")
    assert "equals(messages.channel,'email')" in params["filter"]
    assert "greater-than(updated_at," in params["filter"]


def test_paginated_request_does_not_corrupt_filter(
    campaigns_stream: CampaignsStream,
) -> None:
    """Regression test for https://github.com/MeltanoLabs/tap-klaviyo/issues/179.

    When a next_page_token is present the filter is already encoded in Klaviyo's
    HATEOAS next-page URL. Applying the channel filter a second time produced a
    malformed filter like and(["and(...)"],equals(...)) because parse_qs returns
    list values.
    """
    combined_filter = (
        "and(greater-than(updated_at,2000-01-01T00:00:00+00:00),equals(messages.channel,'email'))"
    )
    next_url = (
        "https://a.klaviyo.com/api/campaigns"
        f"?filter={quote(combined_filter, safe='')}"
        "&page%5Bcursor%5D=bmV4dDo6LXVwZGF0ZWQ"
    )
    next_page_token = urlparse(next_url)
    context = {"filter": "equals(messages.channel,'email')"}

    params = campaigns_stream.get_url_params(context, next_page_token=next_page_token)

    filter_value = params["filter"]
    # parse_qs returns lists; the filter should come through as-is (a list with one element)
    if isinstance(filter_value, list):
        filter_value = filter_value[0]

    assert filter_value == combined_filter, (
        f"Filter was corrupted on paginated request: {filter_value!r}"
    )
    assert "and([" not in filter_value, "Filter contains stringified list — double-wrapping bug"
