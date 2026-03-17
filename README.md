# tap-klaviyo

`tap-klaviyo` is a Singer tap for Klaviyo.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from GitHub:

```bash
uv tool install git+https://github.com/ORG_NAME/tap-klaviyo.git@main
```

-->

## Configuration

### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the tap.

This section can be created by copy-pasting the CLI output from:

```
tap-klaviyo --about --format=markdown
```
-->

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-klaviyo --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your tap requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `tap-klaviyo` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-klaviyo --version
tap-klaviyo --help
tap-klaviyo --config CONFIG --discover > ./catalog.json
```

## Reports

This tap includes four report-style streams backed by Klaviyo POST endpoints. These streams issue `POST` requests and flatten the API response into row-oriented Singer records.

### Available Report Streams

- `segment_series_report`: emits one row per `segment_id`, `date`, and `statistic_name`.
- `campaign_values_report`: emits one row per `campaign_id`, `campaign_message_id`, `send_channel`, and `statistic_name`. A `date` field is included when Klaviyo returns time-series values.
- `flow_values_report`: emits one row per `flow_id`, `flow_message_id`, `send_channel`, and `statistic_name`.
- `flow_series_report`: emits one row per `flow_id`, `flow_message_id`, `send_channel`, `date`, and `statistic_name`.
- `query_metric_aggregates`: emits one row per `metric_id`, `date`, `dimensions`, and `measurement_name`.

### Report Configuration

Each report stream can be configured independently in the tap config:

- `segment_series_reports`
- `campaign_values_reports`
- `flow_values_reports`
- `flow_series_reports`
- `query_metric_aggregates_reports`

#### `segment_series_report`

Configure this stream with `segment_series_reports`, one named stream per config entry.

Default request payload:

```json
{
  "statistics": [
    "members_added",
    "total_members",
    "members_removed",
    "net_members_changed"
  ],
  "interval": "daily",
  "timeframe": {
    "key": "last_7_days"
  }
}
```

Supported override fields:

- `name`
- `statistics`
- `conversion_metric_id`
- `interval`
- `timeframe`

`conversion_metric_id` is client-specific. Set it when your selected statistics require a conversion metric.

Flattened output fields:

- `report_name`
- `date`
- `segment_id`
- `statistic_name`
- `statistic_value`

Example multi-report config:

```json
{
  "segment_series_reports": [
    {
      "name": "segment_growth_report",
      "statistics": ["members_added", "total_members"],
      "interval": "weekly",
      "timeframe": {
        "key": "last_30_days"
      }
    },
    {
      "name": "segment_churn_report",
      "statistics": ["members_removed", "net_members_changed"],
      "interval": "daily",
      "timeframe": {
        "key": "last_30_days"
      }
    }
  ]
}
```

#### `campaign_values_report`

Configure this stream with `campaign_values_reports`, one named stream per config entry.

Default request payload:

```json
{
  "statistics": [
    "opens_unique",
    "open_rate",
    "delivered",
    "clicks_unique",
    "click_to_open_rate",
    "revenue_per_recipient"
  ],
  "timeframe": {
    "key": "last_7_days"
  }
}
```

Supported override fields:

- `name`
- `statistics`
- `conversion_metric_id`
- `timeframe`

`conversion_metric_id` is client-specific. Set it explicitly when your selected statistics require a conversion metric.

Flattened output fields:

- `report_name`
- `date` when present in the Klaviyo response
- `campaign_id`
- `campaign_message_id`
- `send_channel`
- `statistic_name`
- `statistic_value`

Example multi-report config:

```json
{
  "campaign_values_reports": [
    {
      "name": "campaign_opens_report",
      "statistics": ["opens_unique", "open_rate"],
      "conversion_metric_id": "YOUR_CONVERSION_METRIC_ID",
      "timeframe": {
        "key": "last_30_days"
      }
    },
    {
      "name": "campaign_revenue_report",
      "statistics": ["delivered", "revenue_per_recipient"],
      "conversion_metric_id": "YOUR_CONVERSION_METRIC_ID",
      "timeframe": {
        "key": "last_30_days"
      }
    }
  ]
}
```

#### `flow_values_report`

Configure this stream with `flow_values_reports`, one named stream per config entry.

Default request payload:

```json
{
  "statistics": [
    "opens",
    "open_rate",
    "delivered",
    "clicks",
    "click_rate",
    "click_to_open_rate",
    "unsubscribe_rate",
    "conversion_rate",
    "revenue_per_recipient"
  ],
  "timeframe": {
    "key": "last_7_days"
  }
}
```

#### `flow_series_report`

Configure this stream with `flow_series_reports`, one named stream per config entry.

Default request payload:

```json
{
  "statistics": [
    "opens",
    "open_rate",
    "delivered",
    "clicks",
    "click_rate",
    "click_to_open_rate",
    "unsubscribe_rate",
    "conversion_rate",
    "revenue_per_recipient"
  ],
  "interval": "daily",
  "timeframe": {
    "key": "last_7_days"
  }
}
```

Supported override fields:

- `name`
- `statistics`
- `conversion_metric_id`
- `interval`
- `timeframe`

`conversion_metric_id` is client-specific. Set it when your selected statistics require a conversion metric.

Flattened output fields:

- `report_name`
- `date`
- `flow_id`
- `flow_message_id`
- `send_channel`
- `statistic_name`
- `statistic_value`

Example multi-report config:

```json
{
  "flow_series_reports": [
    {
      "name": "flow_series_daily",
      "statistics": ["opens", "open_rate"],
      "conversion_metric_id": "YOUR_CONVERSION_METRIC_ID",
      "interval": "daily",
      "timeframe": {
        "key": "last_12_months"
      }
    }
  ]
}
```

#### `query_metric_aggregates`

This stream wraps Klaviyo's `POST /api/metric-aggregates` endpoint.

Configure this stream with `query_metric_aggregates_reports`, one named stream per config entry.

Default request payload additions:

```json
{
  "measurements": ["count"],
  "interval": "day",
  "page_size": 500
}
```

Required config fields:

- `metric_id`

Supported override fields:

- `name`
- `metric_id`
- `page_cursor`
- `measurements`
- `interval`
- `page_size`
- `by`
- `return_fields`
- `filter`
- `timezone`
- `sort`

Date window behavior:

- lower bound comes from Singer state when present
- otherwise lower bound falls back to `start_date`
- upper bound is always computed as the exclusive start of tomorrow in the configured timezone, so daily syncs include today
- any configured datetime predicates in `filter` are replaced by these generated bounds; keep `filter` for additional non-datetime predicates

Flattened output fields:

- `report_name`
- `metric_aggregate_id`
- `metric_id`
- `date` when present in the Klaviyo response
- `dimensions`
- `measurement_name`
- `measurement_value`

Example multi-report config:

```json
{
  "query_metric_aggregates_reports": [
    {
      "name": "email_opens_daily",
      "metric_id": "SCiiZ2",
      "measurements": ["unique"],
      "interval": "day",
      "timezone": "America/New_York"
    },
    {
      "name": "email_clicks_daily",
      "metric_id": "AbCd12",
      "measurements": ["count"],
      "interval": "day",
      "filter": ["equals(attributed_channel,email)"],
      "timezone": "America/New_York"
    }
  ]
}
```

### Example Config

```json
{
  "auth_token": "YOUR_KLAVIYO_PRIVATE_KEY",
  "revision": "2024-10-15",
  "segment_series_reports": [
    {
      "name": "segment_growth_report",
      "statistics": ["members_added", "total_members"],
      "interval": "weekly",
      "timeframe": {
        "key": "last_30_days"
      }
    },
    {
      "name": "segment_churn_report",
      "statistics": ["members_removed", "net_members_changed"],
      "interval": "daily",
      "timeframe": {
        "key": "last_30_days"
      }
    }
  ],
  "campaign_values_reports": [
    {
      "name": "campaign_opens_report",
      "statistics": ["opens_unique", "open_rate"],
      "conversion_metric_id": "YOUR_CONVERSION_METRIC_ID",
      "timeframe": {
        "key": "last_30_days"
      }
    },
    {
      "name": "campaign_revenue_report",
      "statistics": ["delivered", "revenue_per_recipient"],
      "conversion_metric_id": "YOUR_CONVERSION_METRIC_ID",
      "timeframe": {
        "key": "last_30_days"
      }
    }
  ],
  "flow_values_reports": [
    {
      "name": "flow_clicks_report",
      "statistics": ["clicks", "click_rate"],
      "conversion_metric_id": "YOUR_CONVERSION_METRIC_ID",
      "timeframe": {
        "key": "last_30_days"
      }
    },
    {
      "name": "flow_revenue_report",
      "statistics": ["conversion_rate", "revenue_per_recipient"],
      "conversion_metric_id": "YOUR_CONVERSION_METRIC_ID",
      "timeframe": {
        "key": "last_30_days"
      }
    }
  ],
  "flow_series_reports": [
    {
      "name": "flow_series_daily",
      "statistics": ["opens", "open_rate"],
      "interval": "daily",
      "timeframe": {
        "key": "last_12_months"
      }
    }
  ],
  "query_metric_aggregates_reports": [
    {
      "name": "email_opens_daily",
      "metric_id": "SCiiZ2",
      "measurements": ["unique"],
      "interval": "day",
      "timezone": "America/New_York"
    },
    {
      "name": "email_clicks_daily",
      "metric_id": "AbCd12",
      "measurements": ["count"],
      "interval": "day",
      "filter": ["equals(attributed_channel,email)"],
      "timezone": "America/New_York"
    }
  ]
}
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh | sh # see https://docs.astral.sh/uv/getting-started/installation/ for more details
uv sync
```

### Create and Run Tests

Create tests within the `tap_klaviyo/tests` subfolder and
  then run:

```bash
uv run pytest
```

You can also test the `tap-klaviyo` CLI interface directly using `uv run`:

```bash
uv run tap-klaviyo --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
uv tool install meltano
# Initialize meltano within this directory
cd tap-klaviyo
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-klaviyo --version
# OR run a test `elt` pipeline:
meltano elt tap-klaviyo target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
