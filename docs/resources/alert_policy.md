---
page_title: "celerdatabyoc_alert_policy Resource - terraform-provider-celerdatabyoc"
subcategory: ""
description: |-
  Manages a CelerData alert policy in a specific region.
---

# celerdatabyoc_alert_policy

Manages a CelerData alert policy. A policy evaluates one or more metric rules
against a cluster on a fixed interval and, when a rule fires, dispatches
notifications via email and/or one or more PagerDuty integrations.

Alert policies are region-scoped: every cluster referenced by `rule.cluster_id`
must live in the same region as the policy. CelerData does not surface a
validation error if you mix regions — the rule will simply never fire because
the alert service in region X has no metrics for a cluster in region Y. The
`region` attribute is `ForceNew` — changing it recreates the resource.

## Example Usage

```terraform
resource "celerdatabyoc_pagerduty_integration" "ops" {
  name             = "ops-pagerduty"
  integration_key  = var.pagerduty_routing_key
  default_severity = "critical"
}

resource "celerdatabyoc_alert_policy" "high_cpu" {
  name               = "high-cpu-prod"
  region             = "us-west-2"
  alert_interval     = "5m"
  calculation_window = "5m"

  rule {
    cluster_id = celerdatabyoc_elastic_cluster_v2.prod.id
    metric     = "CPUUtilization"
    func       = "mean"
    operator   = ">"
    value      = 85
  }

  email_notification {
    addresses = ["oncall@example.com"]
    subject   = "[CelerData] CPU high on prod"
    cc        = ["sre@example.com"]
  }

  pagerduty_binding {
    integration_id    = celerdatabyoc_pagerduty_integration.ops.id
    severity_override = "critical"
  }
}
```

## Argument Reference

**Required**

- `name`: (String) Display name of the policy. Must be unique within the region
  and account.

- `region`: (String, Forces new resource) The region where the alert service
  evaluates this policy. Must match the region of every cluster referenced by
  `rule.cluster_id`.

- `alert_interval`: (String) How often the policy is evaluated. Duration string
  with suffix `s`, `m`, or `h` (e.g. `30s`, `5m`, `1h`). Minimum effective value
  is 15 seconds.

- `rule`: (Block List, min 1) One or more rules. The policy fires when **any**
  rule's condition holds over the `calculation_window`. Each rule has:

  - `cluster_id`: (String) Target cluster ID. Must belong to a cluster in the
    same region as the policy.
  - `metric`: (String) Metric name. Must be one of the names recognized by the
    alert service — currently `CPUUtilization`, `MemUtilization`,
    `DiskUtilization`, `ClusterDatasize`, `FEQueryLatencyP99`,
    `FEQueryErrorPercentage`, `NetworkReceiveThroughput`,
    `NetworkTransmitThroughput`. Unknown names are accepted by the API but the
    rule will never fire.
  - `func`: (String) Aggregation over the window. One of `max`, `min`, `mean`.
    Note: the proto definition mentions `avg`, but the real value is `mean` —
    `avg` produces an empty PromQL aggregator and the rule never fires.
  - `operator`: (String) PromQL comparison operator. One of `>`, `<`, `>=`,
    `<=`, `!=`.
  - `value`: (Float) Threshold value the aggregated metric is compared against.

**Optional**

- `calculation_window`: (String) Time window the `func` aggregation is computed
  over. Same format as `alert_interval`. Defaults to `alert_interval` when
  omitted.

- `email_notification`: (Block, max 1) Email recipients for fired alerts.

  - `addresses`: (List of strings, min 1) Primary recipients.
  - `subject`: (String) Custom subject line. Stored verbatim and used as-is in
    the outgoing email.
  - `cc`: (List of strings) Optional CC recipients.

- `pagerduty_binding`: (Block List, max 10) Zero or more PagerDuty integrations
  to notify. Each binding has:

  - `integration_id`: (String) ID from a
    [`celerdatabyoc_pagerduty_integration`](./pagerduty_integration.md)
    resource.
  - `severity_override`: (String) Optional per-policy severity that overrides
    the integration's `default_severity`. One of `critical`, `error`,
    `warning`, `info`.

## Attribute Reference

In addition to the arguments above:

- `id`: (String) Internal Terraform ID, encoded as `<region>:<policy_id>`. Use
  this for `terraform import`.
- `policy_id`: (String) Bare policy ID assigned by the alert service.
- `state`: (String) Current alert state — `firing` or `inactive`.
- `create_time`: (Integer) Unix timestamp (seconds) the policy was created.
- `creator`: (String) Username of the operator that created the policy.

## Import

Alert policies are region-scoped, so the import ID must include the region:

```shell
terraform import celerdatabyoc_alert_policy.high_cpu us-west-2:<policy_id>
```
