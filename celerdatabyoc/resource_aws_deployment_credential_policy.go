package celerdatabyoc

import (
	"context"
	"strings"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/csp"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func resourceAwsDeploymentCredentialPolicy() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceAwsDeploymentCredentialPolicyCreate,
		ReadContext:   resourceAwsDeploymentCredentialPolicyRead,
		DeleteContext: resourceAwsDeploymentCredentialPolicyDelete,
		Schema: map[string]*schema.Schema{
			"data_role_arn": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"bucket": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"json": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"version": {
				Type:     schema.TypeString,
				Computed: true,
			},
		},
	}
}

func resourceAwsDeploymentCredentialPolicyCreate(ctx context.Context, d *schema.ResourceData, m any) diag.Diagnostics {
	return resourceAwsDeploymentCredentialPolicyRead(ctx, d, m)
}

func resourceAwsDeploymentCredentialPolicyRead(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	dataRoleARN := d.Get("data_role_arn").(string)
	bucket := d.Get("bucket").(string)
	c := m.(*client.CelerdataClient)
	cspAPI := csp.NewCspAPI(c)
	resp, err := cspAPI.Get(ctx, &csp.GetReq{
		CspName: "aws",
	})
	if err != nil {
		return diag.FromErr(err)
	}
	policy := strings.Replace(resp.Csp.DeployCredPolicy, "<Storage Role ARN>", dataRoleARN, -1)
	policyJSON := strings.Replace(policy, "<s3-bucket-name>", bucket, -1)
	d.SetId("deployment_credential")
	// nolint
	d.Set("json", string(policyJSON))
	d.Set("version", resp.Csp.DeployCredPolicyVersion)

	return diags
}

func resourceAwsDeploymentCredentialPolicyDelete(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {

	// d.SetId("") is automatically called assuming delete returns no errors, but
	// it is added here for explicitness.
	d.SetId("")
	return diags
}
