package celerdatabyoc

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"

	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/alert"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

// Policy id in Terraform state is "<region>:<policy_id>" because the alert
// service is region-scoped and the backend needs the region to route any
// follow-up call. SetId encodes; getRegionAndPolicyID decodes.
const alertPolicyIDSep = ":"

var durationPattern = regexp.MustCompile(`^[1-9][0-9]*[smh]$`)

func resourceAlertPolicy() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceAlertPolicyCreate,
		ReadContext:   resourceAlertPolicyRead,
		UpdateContext: resourceAlertPolicyUpdate,
		DeleteContext: resourceAlertPolicyDelete,
		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"region": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"alert_interval": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringMatch(durationPattern, "must be a duration like 30s, 5m, 1h"),
			},
			"calculation_window": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringMatch(durationPattern, "must be a duration like 30s, 5m, 1h"),
			},
			"rule": {
				Type:     schema.TypeList,
				Required: true,
				MinItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"cluster_id": {
							Type:     schema.TypeString,
							Required: true,
						},
						"metric": {
							Type:     schema.TypeString,
							Required: true,
						},
						"func": {
							Type:     schema.TypeString,
							Required: true,
							// Valid values come from region/alert AggrOverTimeFuncMap.
							// Note: proto comment says "avg" but the real key is "mean";
							// "avg" silently produces an empty PromQL aggregator and the
							// rule never fires.
							ValidateFunc: validation.StringInSlice(
								[]string{"max", "min", "mean"}, false),
						},
						"operator": {
							Type:     schema.TypeString,
							Required: true,
							// PromQL comparison operators only. The proto comment lists
							// "=" but PromQL has no single-equals comparator — it would
							// produce invalid PromQL.
							ValidateFunc: validation.StringInSlice(
								[]string{">", "<", ">=", "<=", "!="}, false),
						},
						"value": {
							Type:     schema.TypeFloat,
							Required: true,
						},
					},
				},
			},
			"email_notification": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"addresses": {
							Type:     schema.TypeList,
							Required: true,
							MinItems: 1,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},
						"subject": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"cc": {
							Type:     schema.TypeList,
							Optional: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},
					},
				},
			},
			"pagerduty_binding": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 10,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"integration_id": {
							Type:     schema.TypeString,
							Required: true,
						},
						"severity_override": {
							Type:     schema.TypeString,
							Optional: true,
							ValidateFunc: validation.StringInSlice(
								[]string{"critical", "error", "warning", "info"}, false),
						},
					},
				},
			},
			"policy_id": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"state": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"create_time": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"creator": {
				Type:     schema.TypeString,
				Computed: true,
			},
		},
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
	}
}

func resourceAlertPolicyCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)
	api := alert.NewAlertAPI(c)

	region := d.Get("region").(string)
	policy := buildAlertPolicyFromState(d)

	resp, err := api.CreateAlertPolicy(ctx, &alert.UpsertAlertPolicyReq{
		Region: region,
		Policy: policy,
	})
	if err != nil {
		return diag.FromErr(err)
	}

	log.Printf("[DEBUG] create alert policy succeeded, id[%s] region[%s]", resp.PolicyId, region)
	d.SetId(encodeAlertPolicyID(region, resp.PolicyId))
	d.Set("policy_id", resp.PolicyId)

	return resourceAlertPolicyRead(ctx, d, m)
}

func resourceAlertPolicyRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)
	api := alert.NewAlertAPI(c)

	region, policyID, err := decodeAlertPolicyID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	resp, err := api.GetAlertPolicy(ctx, region, policyID)
	if err != nil {
		if isNotFoundError(err) {
			d.SetId("")
			return nil
		}
		return diag.FromErr(err)
	}
	if resp.Policy == nil {
		d.SetId("")
		return nil
	}
	writeAlertPolicyToState(d, resp.Policy, region)
	return nil
}

func resourceAlertPolicyUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)
	api := alert.NewAlertAPI(c)

	region, policyID, err := decodeAlertPolicyID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	policy := buildAlertPolicyFromState(d)
	policy.PolicyId = policyID
	if err := api.UpdateAlertPolicy(ctx, policyID, &alert.UpsertAlertPolicyReq{
		Region: region,
		Policy: policy,
	}); err != nil {
		return diag.FromErr(err)
	}
	return resourceAlertPolicyRead(ctx, d, m)
}

func resourceAlertPolicyDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)
	api := alert.NewAlertAPI(c)

	region, policyID, err := decodeAlertPolicyID(d.Id())
	if err != nil {
		return diag.FromErr(err)
	}

	if err := api.DeleteAlertPolicy(ctx, region, policyID); err != nil {
		if isNotFoundError(err) {
			d.SetId("")
			return nil
		}
		return diag.FromErr(err)
	}
	d.SetId("")
	return nil
}

func encodeAlertPolicyID(region, policyID string) string {
	return region + alertPolicyIDSep + policyID
}

func decodeAlertPolicyID(id string) (string, string, error) {
	parts := strings.SplitN(id, alertPolicyIDSep, 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid alert policy id %q: expected <region>:<policy_id>", id)
	}
	return parts[0], parts[1], nil
}

func buildAlertPolicyFromState(d *schema.ResourceData) *alert.AlertPolicy {
	policy := &alert.AlertPolicy{
		Name:              d.Get("name").(string),
		Region:            d.Get("region").(string),
		Interval:          d.Get("alert_interval").(string),
		CalculationWindow: d.Get("calculation_window").(string),
	}
	if policy.CalculationWindow == "" {
		policy.CalculationWindow = policy.Interval
	}

	for _, raw := range d.Get("rule").([]interface{}) {
		r := raw.(map[string]interface{})
		policy.Exprs = append(policy.Exprs, &alert.AlertExpr{
			Func:      r["func"].(string),
			ClusterId: r["cluster_id"].(string),
			Metric:    r["metric"].(string),
			Operator:  r["operator"].(string),
			Value:     float32(r["value"].(float64)),
		})
	}

	if email := d.Get("email_notification").([]interface{}); len(email) > 0 {
		m := email[0].(map[string]interface{})
		policy.Method = "EMAIL"
		policy.EmailConfig = &alert.EmailConfig{
			EmailAddr: toStringSlice(m["addresses"]),
			Subject:   m["subject"].(string),
			EmailCc:   toStringSlice(m["cc"]),
		}
	}

	if bindings := d.Get("pagerduty_binding").([]interface{}); len(bindings) > 0 {
		policy.PagerDutyEnabled = true
		for _, raw := range bindings {
			b := raw.(map[string]interface{})
			policy.PagerDutyBindings = append(policy.PagerDutyBindings, &alert.PolicyPagerDutyBinding{
				IntegrationId:    b["integration_id"].(string),
				SeverityOverride: b["severity_override"].(string),
			})
		}
	}

	return policy
}

func writeAlertPolicyToState(d *schema.ResourceData, p *alert.AlertPolicy, region string) {
	d.Set("name", p.Name)
	d.Set("region", region)
	d.Set("alert_interval", p.Interval)
	d.Set("calculation_window", p.CalculationWindow)
	d.Set("policy_id", p.PolicyId)
	d.Set("state", p.AlertState)
	d.Set("create_time", p.CreateTime)
	d.Set("creator", p.Creator)

	rules := make([]map[string]interface{}, 0, len(p.Exprs))
	for _, e := range p.Exprs {
		rules = append(rules, map[string]interface{}{
			"cluster_id": e.ClusterId,
			"metric":     e.Metric,
			"func":       e.Func,
			"operator":   e.Operator,
			"value":      float64(e.Value),
		})
	}
	d.Set("rule", rules)

	if p.EmailConfig != nil && len(p.EmailConfig.EmailAddr) > 0 {
		d.Set("email_notification", []map[string]interface{}{{
			"addresses": p.EmailConfig.EmailAddr,
			"subject":   p.EmailConfig.Subject,
			"cc":        p.EmailConfig.EmailCc,
		}})
	} else {
		d.Set("email_notification", []map[string]interface{}{})
	}

	bindings := make([]map[string]interface{}, 0, len(p.PagerDutyBindings))
	for _, b := range p.PagerDutyBindings {
		bindings = append(bindings, map[string]interface{}{
			"integration_id":    b.IntegrationId,
			"severity_override": b.SeverityOverride,
		})
	}
	d.Set("pagerduty_binding", bindings)
}

