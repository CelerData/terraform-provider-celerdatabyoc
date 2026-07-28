package celerdatabyoc

import (
	"context"
	"log"
	"strings"

	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/alert"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func resourcePagerDutyIntegration() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourcePagerDutyIntegrationCreate,
		ReadContext:   resourcePagerDutyIntegrationRead,
		UpdateContext: resourcePagerDutyIntegrationUpdate,
		DeleteContext: resourcePagerDutyIntegrationDelete,
		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"integration_key": {
				Type:      schema.TypeString,
				Required:  true,
				Sensitive: true,
			},
			"default_severity": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "critical",
				ValidateFunc: validation.StringInSlice(
					[]string{"critical", "error", "warning", "info"}, false),
			},
			"created_at": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"updated_at": {
				Type:     schema.TypeInt,
				Computed: true,
			},
		},
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
	}
}

func resourcePagerDutyIntegrationCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)
	api := alert.NewAlertAPI(c)

	req := &alert.CreatePagerDutyIntegrationReq{
		Name:            d.Get("name").(string),
		RoutingKey:      d.Get("integration_key").(string),
		DefaultSeverity: d.Get("default_severity").(string),
	}

	resp, err := api.CreatePagerDutyIntegration(ctx, req)
	if err != nil {
		return diag.FromErr(err)
	}

	log.Printf("[DEBUG] create pagerduty integration succeeded, id[%s]", resp.IntegrationId)
	d.SetId(resp.IntegrationId)

	return resourcePagerDutyIntegrationRead(ctx, d, m)
}

func resourcePagerDutyIntegrationRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)
	api := alert.NewAlertAPI(c)

	resp, err := api.GetPagerDutyIntegration(ctx, d.Id())
	if err != nil {
		if isNotFoundError(err) {
			d.SetId("")
			return nil
		}
		return diag.FromErr(err)
	}
	if resp.Integration == nil {
		d.SetId("")
		return nil
	}

	pd := resp.Integration
	d.Set("name", pd.Name)
	d.Set("default_severity", pd.DefaultSeverity)
	d.Set("created_at", pd.CreatedAt)
	d.Set("updated_at", pd.UpdatedAt)
	// The backend returns only a masked routing key. Don't clobber the plaintext
	// the user has in state — leave the state value untouched if what we got back
	// is masked. Detect by the leading "***" prefix the backend applies.
	if !strings.HasPrefix(pd.RoutingKeyMasked, "***") && pd.RoutingKeyMasked != "" {
		d.Set("integration_key", pd.RoutingKeyMasked)
	}

	return nil
}

func resourcePagerDutyIntegrationUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)
	api := alert.NewAlertAPI(c)

	req := &alert.UpdatePagerDutyIntegrationReq{
		Name:            d.Get("name").(string),
		DefaultSeverity: d.Get("default_severity").(string),
	}
	// Only send the key if the user actually changed it — otherwise the backend
	// would receive an empty string and overwrite the stored secret.
	if d.HasChange("integration_key") {
		req.RoutingKey = d.Get("integration_key").(string)
	}

	if err := api.UpdatePagerDutyIntegration(ctx, d.Id(), req); err != nil {
		return diag.FromErr(err)
	}
	return resourcePagerDutyIntegrationRead(ctx, d, m)
}

func resourcePagerDutyIntegrationDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)
	api := alert.NewAlertAPI(c)

	if err := api.DeletePagerDutyIntegration(ctx, d.Id()); err != nil {
		return diag.FromErr(err)
	}
	d.SetId("")
	return nil
}

// isNotFoundError detects both the SDK's gRPC-style NotFound status (the
// envelope client maps code 40004 to codes.NotFound) and the generic "not found"
// substring used by other endpoints.
func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if status.Code(err) == codes.NotFound {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "not found")
}
