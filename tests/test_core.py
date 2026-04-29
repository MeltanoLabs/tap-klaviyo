"""Tests standard tap features using the built-in SDK tests library."""

from singer_sdk.testing import get_tap_test_class

from tap_klaviyo.tap import TapKlaviyo

SAMPLE_CONFIG = {
    "revision": "2026-01-15",
}


# Run standard built-in tap tests from the SDK:
TestTapKlaviyo = get_tap_test_class(
    tap_class=TapKlaviyo,
    config=SAMPLE_CONFIG,
)
