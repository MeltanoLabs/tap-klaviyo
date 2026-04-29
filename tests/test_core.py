"""Tests standard tap features using the built-in SDK tests library."""

from unittest import TestCase

from singer_sdk.testing import get_tap_test_class

from tap_klaviyo import streams
from tap_klaviyo.tap import TapKlaviyo

SAMPLE_CONFIG = {
    "revision": "2026-01-15",
}


# Run standard built-in tap tests from the SDK:
TestTapKlaviyo = get_tap_test_class(
    tap_class=TapKlaviyo,
    config=SAMPLE_CONFIG,
)


class TestPrimaryKeys(TestCase):
    """Validate primary keys that prevent merge collisions."""

    def test_relationship_and_series_report_primary_keys(self) -> None:
        self.assertEqual(streams.ListPersonStream.primary_keys, ["list_id", "id"])
        self.assertEqual(
            streams.SegmentSeriesReportStream.primary_keys,
            [
                "report_name",
                "segment_id",
                "date",
                "statistic_name",
            ],
        )
        self.assertEqual(
            streams.FlowSeriesReportStream.primary_keys,
            [
                "report_name",
                "flow_id",
                "flow_message_id",
                "send_channel",
                "date",
                "statistic_name",
            ],
        )
