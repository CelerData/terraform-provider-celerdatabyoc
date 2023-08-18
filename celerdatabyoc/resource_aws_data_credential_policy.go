package celerdatabyoc

import (
	"context"
	"strings"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/csp"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func resourceAwsDataCredentialPolicy() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceAwsDataCredentialPolicyCreate,
		ReadContext:   resourceAwsDataCredentialPolicyRead,
		DeleteContext: resourceAwsDataCredentialPolicyDelete,
		Schema: map[string]*schema.Schema{
			"bucket": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"json": {
				Type:     schema.TypeString,
				Computed: true,
				ForceNew: true,
			},
			"version": {
				Type:     schema.TypeString,
				Computed: true,
				ForceNew: true,
			},
		},
	}
}

func resourceAwsDataCredentialPolicyCreate(ctx context.Context, d *schema.ResourceData, m any) diag.Diagnostics {
	bucket := d.Get("bucket").(string)
	c := m.(*client.CelerdataClient)
	cspAPI := csp.NewCspAPI(c)
	resp, err := cspAPI.Get(ctx, &csp.GetReq{
		CspName: "aws",
	})
	if err != nil {
		return diag.FromErr(err)
	}

	policyJSON := strings.Replace(resp.Csp.DataCredPolicy, "<s3-bucket-name>", bucket, -1)
	d.SetId("data_credential")
	// nolint
	d.Set("json", string(policyJSON))
	d.Set("version", resp.Csp.DataCredPolicyVersion)
	return nil
}

func resourceAwsDataCredentialPolicyRead(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	return diags
}

func resourceAwsDataCredentialPolicyDelete(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {

	// d.SetId("") is automatically called assuming delete returns no errors, but
	// it is added here for explicitness.
	d.SetId("")
	return diags
}
