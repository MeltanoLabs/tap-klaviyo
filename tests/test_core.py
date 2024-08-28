"""Tests standard tap features using the built-in SDK tests library."""
import json
from pathlib import Path

from singer_sdk.testing import SuiteConfig, get_tap_test_class

from tap_klaviyo.tap import TapKlaviyo

current_path = Path(__file__).resolve().parent
config_path = current_path / ".." / "config.json"
# create a config object to run the core tests
SAMPLE_CONFIG = json.loads(config_path.read_text())

# Run standard built-in tap tests from the SDK:
TestTapKlaviyo = get_tap_test_class(
    tap_class=TapKlaviyo,
    config=SAMPLE_CONFIG,
    suite_config=SuiteConfig(
        # TODO(edgarrmondragon): seed test account with sample data
        # https://github.com/MeltanoLabs/tap-klaviyo/pull/34
        ignore_no_records_for_streams=[
            "campaigns",
            "campaign_values_reports"
            "events",
            "flows",
            "listperson",
            "listperson-incremental",
            "lists",
            "profiles",
            "templates",
        ],
    ),
)
