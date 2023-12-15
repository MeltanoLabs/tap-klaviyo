"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import SuiteConfig, get_tap_test_class

from tap_klaviyo.tap import TapKlaviyo

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
}


# Run standard built-in tap tests from the SDK:
TestTapKlaviyo = get_tap_test_class(
    tap_class=TapKlaviyo,
    config=SAMPLE_CONFIG,
    suite_config=SuiteConfig(
        # TODO(edgarrmondragon): seed test account with sample data
        # https://github.com/MeltanoLabs/tap-klaviyo/pull/34
        ignore_no_records_for_streams=[
            "campaigns",
            "events",
            "flows",
            "listperson",
            "lists",
            "profiles",
            "templates",
        ],
    ),
)
