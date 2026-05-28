---
page_title: "celerdatabyoc_pagerduty_integration Resource - terraform-provider-celerdatabyoc"
subcategory: ""
description: |-
  Manages a PagerDuty integration in your CelerData Cloud BYOC account.
---

# celerdatabyoc_pagerduty_integration

Manages a PagerDuty integration in your CelerData Cloud BYOC account. A PagerDuty
integration stores the Events API v2 routing (integration) key that CelerData
uses to deliver alert events into a PagerDuty service. After creating an
integration, you can reference it from one or more alert policies via the
[`celerdatabyoc_alert_policy`](./alert_policy.md) resource.

The integration is account-scoped: a single integration can be reused across
regions and across multiple alert policies.

## Example Usage

```terraform
resource "celerdatabyoc_pagerduty_integration" "ops" {
  name             = "ops-pagerduty"
  integration_key  = var.pagerduty_routing_key
  default_severity = "critical"
}
```

## Argument Reference

**Required**

- `name`: (String) The display name shown in the CelerData console and used by
  alert policies to refer to the integration. Must be unique within the account.

- `integration_key`: (String, Sensitive) The PagerDuty Events API v2 routing
  key. Treat this like a secret — store it in a variable, not inline. The
  CelerData backend returns a masked version on read; Terraform keeps the
  plaintext you supplied in state.

**Optional**

- `default_severity`: (String) Default severity used when an alert policy does
  not override it. Must be one of `critical`, `error`, `warning`, `info`.
  Defaults to `critical`.

## Attribute Reference

In addition to the arguments above, this resource exports:

- `id`: (String) The integration ID assigned by CelerData.
- `created_at`: (Integer) Unix timestamp (seconds) the integration was created.
- `updated_at`: (Integer) Unix timestamp (seconds) the integration was last updated.

## Import

PagerDuty integrations can be imported by their integration ID:

```shell
terraform import celerdatabyoc_pagerduty_integration.ops <integration_id>
```

The backend never returns the plaintext routing key — only a masked form — so
after import `integration_key` in state is empty. You must supply the actual
routing key in your HCL (the field is required). Terraform will always show a
diff on the first plan after import, because the state value is empty and the
config value is not. Apply once to reconcile; the backend stores whatever you
provide, so as long as you supply the correct key the reconcile is a no-op
semantically.
