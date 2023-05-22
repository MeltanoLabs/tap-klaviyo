"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_tap_test_class

from tap_klaviyo.tap import TapKlaviyo

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
}


# Run standard built-in tap tests from the SDK:
TestTapKlaviyo = get_tap_test_class(
    tap_class=TapKlaviyo,
    config=SAMPLE_CONFIG,
)
