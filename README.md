# tap-klaviyo

`tap-klaviyo` is a Singer tap for Klaviyo.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Configuration

### Accepted Config Options

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

This tap authenticates with a Klaviyo private API key supplied as `auth_token`.

You must also provide a Klaviyo API `revision`, for example `2024-10-15`.

## Usage

You can easily run `tap-klaviyo` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-klaviyo --version
tap-klaviyo --help
tap-klaviyo --config CONFIG --discover > ./catalog.json
```

## Streams

The tap implements nine resource streams and five configurable report stream families.

Current default discovery in [tap.py](/home/mathewpabraham/taps/tap-klaviyo/tap_klaviyo/tap.py) only enables:

- `campaigns`
- named streams created from `segment_series_reports`

The remaining streams below are implemented in [streams.py](/home/mathewpabraham/taps/tap-klaviyo/tap_klaviyo/streams.py), but are currently commented out in `discover_streams()`.

### Resource Streams

#### `events`

Path: `GET /events`

Fields returned:

- `id`
- `type`
- `datetime`
- `relationships`
- `links`
- `attributes.timestamp`
- `attributes.event_properties`
- `attributes.datetime`
- `attributes.uuid`

Notes:

- `datetime` is promoted from `attributes.datetime` and used as the replication key.
- `attributes.event_properties` is an open object and can contain client-specific keys from Klaviyo.

#### `campaigns`

Path: `GET /campaigns`

Fields returned:

- `id`
- `updated_at`
- `relationships`
- `links`
- `attributes.name`
- `attributes.type`
- `attributes.audiences`
- `attributes.send_options`
- `attributes.tracking_options`
- `attributes.send_strategy`
- `attributes.status`
- `attributes.archived`
- `attributes.channel`
- `attributes.message`
- `attributes.created_at`
- `attributes.scheduled_at`
- `attributes.updated_at`
- `attributes.send_time`

Notes:

- `updated_at` is promoted from `attributes.updated_at` and used as the replication key.
- The stream syncs campaigns in two partitions using Klaviyo channel filters for `email` and `sms`.

#### `profiles`

Path: `GET /profiles`

Fields returned:

- `type`
- `id`
- `updated`
- `relationships`
- `links`
- `attributes.email`
- `attributes.phone_number`
- `attributes.external_id`
- `attributes.first_name`
- `attributes.last_name`
- `attributes.organization`
- `attributes.title`
- `attributes.image`
- `attributes.created`
- `attributes.updated`
- `attributes.last_event_date`
- `attributes.location.address1`
- `attributes.location.address2`
- `attributes.location.city`
- `attributes.location.country`
- `attributes.location.latitude`
- `attributes.location.longitude`
- `attributes.location.region`
- `attributes.location.zip`
- `attributes.location.timezone`
- `attributes.location.ip`
- `attributes.properties`
- `attributes.subscriptions.email.marketing.consent`
- `attributes.subscriptions.email.marketing.custom_method_detail`
- `attributes.subscriptions.email.marketing.double_optin`
- `attributes.subscriptions.email.marketing.list_suppressions`
- `attributes.subscriptions.email.marketing.method`
- `attributes.subscriptions.email.marketing.method_detail`
- `attributes.subscriptions.email.marketing.suppressions`
- `attributes.subscriptions.email.marketing.timestamp`
- `attributes.subscriptions.sms.marketing.consent`
- `attributes.subscriptions.sms.marketing.method`
- `attributes.subscriptions.sms.marketing.method_detail`
- `attributes.subscriptions.sms.marketing.timestamp`

Notes:

- `updated` is promoted from `attributes.updated` and used as the replication key.
- `attributes.properties` is an open object for custom Klaviyo profile properties.

#### `metrics`

Path: `GET /metrics`

Fields returned:

- `id`
- `updated`
- `relationships`
- `links`
- `attributes.created`
- `attributes.integration.id`
- `attributes.integration.name`
- `attributes.integration.category`
- `attributes.links`
- `attributes.name`
- `attributes.updated`

Notes:

- `updated` is promoted from `attributes.updated`.
- `attributes.integration.category` is normalized to a string during post-processing.

#### `lists`

Path: `GET /lists`

Fields returned:

- `id`
- `updated`
- `relationships`
- `links`
- `attributes.created`
- `attributes.name`
- `attributes.opt_in_process`
- `attributes.updated`

Notes:

- `updated` is promoted from `attributes.updated` and used as the replication key.
- This stream is the parent for `listperson`.

#### `listperson`

Path: `GET /lists/{list_id}/relationships/profiles/`

Fields returned:

- `id`
- `type`
- `list_id`

Notes:

- `list_id` is injected from the parent `lists` stream context.

#### `flows`

Path: `GET /flows`

Fields returned:

- `id`
- `type`
- `updated`
- `relationships`
- `links`
- `attributes.archived`
- `attributes.created`
- `attributes.name`
- `attributes.status`
- `attributes.trigger_type`
- `attributes.updated`

Notes:

- `updated` is promoted from `attributes.updated` and used as the replication key.

#### `segments`

Path: `GET /segments`

Fields returned:

- `id`
- `type`
- `updated`
- `relationships`
- `links`
- `attributes.created`
- `attributes.definition`
- `attributes.is_active`
- `attributes.is_processing`
- `attributes.is_starred`
- `attributes.name`
- `attributes.updated`

Notes:

- `updated` is promoted from `attributes.updated` and used as the replication key.

#### `templates`

Path: `GET /templates`

Fields returned:

- `id`
- `type`
- `updated`
- `links`
- `attributes.company_id`
- `attributes.created`
- `attributes.editor_type`
- `attributes.html`
- `attributes.name`
- `attributes.text`
- `attributes.updated`

Notes:

- `updated` is promoted from `attributes.updated` and used as the replication key.

## Reports

This tap includes five report-style streams backed by Klaviyo POST endpoints. These streams issue `POST` requests and flatten the API response into row-oriented Singer records.

### Available Report Streams

- `segment_series_report`: emits one row per `segment_id`, `date`, and `statistic_name`.
- `campaign_values_report`: emits one row per `campaign_id`, `campaign_message_id`, `send_channel`, and `statistic_name`. A `date` field is included when Klaviyo returns time-series values.
- `flow_values_report`: emits one row per `flow_id`, `flow_message_id`, `send_channel`, and `statistic_name`.
- `flow_series_report`: emits one row per `flow_id`, `flow_message_id`, `send_channel`, `date`, and `statistic_name`.
- `query_metric_aggregates`: emits one row per `metric_id`, `date`, `dimensions`, and `measurement_name`.

All named report streams share the same fields within each report family:

- `segment_series_report`: `report_name`, `date`, `segment_id`, `statistic_name`, `statistic_value`
- `campaign_values_report`: `report_name`, `date`, `campaign_id`, `campaign_message_id`, `send_channel`, `statistic_name`, `statistic_value`
- `flow_values_report`: `report_name`, `date`, `flow_id`, `flow_message_id`, `send_channel`, `statistic_name`, `statistic_value`
- `flow_series_report`: `report_name`, `date`, `flow_id`, `flow_message_id`, `send_channel`, `statistic_name`, `statistic_value`
- `query_metric_aggregates`: `report_name`, `metric_aggregate_id`, `metric_id`, `date`, `dimensions`, `measurement_name`, `measurement_value`

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
