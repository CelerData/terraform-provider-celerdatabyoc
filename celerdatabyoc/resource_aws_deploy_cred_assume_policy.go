package celerdatabyoc

import (
	"context"
	"encoding/json"
	"fmt"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/csp"

	"github.com/google/uuid"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

type awsIamPolicy struct {
	Version    string                   `json:"Version,omitempty"`
	ID         string                   `json:"Id,omitempty"`
	Statements []*awsIamPolicyStatement `json:"Statement"`
}

type awsIamPolicyStatement struct {
	Sid          string                       `json:"Sid,omitempty"`
	Effect       string                       `json:"Effect,omitempty"`
	Actions      any                          `json:"Action,omitempty"`
	NotActions   any                          `json:"NotAction,omitempty"`
	Resources    any                          `json:"Resource,omitempty"`
	NotResources any                          `json:"NotResource,omitempty"`
	Principal    map[string]string            `json:"Principal,omitempty"`
	Condition    map[string]map[string]string `json:"Condition,omitempty"`
}

func resourceAwsDeployCredAssumePolicy() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceAwsDeployCredAssumePolicyCreate,
		ReadContext:   resourceAwsDeployCredAssumePolicyRead,
		DeleteContext: resourceAwsDeployCredAssumePolicyDelete,
		Schema: map[string]*schema.Schema{
			"external_id": {
				Type:     schema.TypeString,
				Computed: true,
				ForceNew: true,
			},
			"json": {
				Type:     schema.TypeString,
				Computed: true,
				ForceNew: true,
			},
		},
	}
}

func resourceAwsDeployCredAssumePolicyCreate(ctx context.Context, d *schema.ResourceData, m any) diag.Diagnostics {
	c := m.(*client.CelerdataClient)
	cspAPI := csp.NewCspAPI(c)

	resp, err := cspAPI.Get(ctx, &csp.GetReq{
		CspName: "aws",
	})
	if err != nil {
		return diag.FromErr(err)
	}

	externalID := uuid.NewString()
	policy := awsIamPolicy{
		Version: "2012-10-17",
		Statements: []*awsIamPolicyStatement{
			{
				Effect:  "Allow",
				Actions: "sts:AssumeRole",
				Condition: map[string]map[string]string{
					"StringEquals": {
						"sts:ExternalId": externalID,
					},
				},
				Principal: map[string]string{
					"AWS": fmt.Sprintf("arn:aws:iam::%s:root", resp.Csp.TrustAccountId),
				},
			},
		},
	}
	policyJSON, err := json.MarshalIndent(policy, "", "  ")
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(externalID)
	// nolint
	d.Set("json", string(policyJSON))
	d.Set("external_id", externalID)
	return nil
}

func resourceAwsDeployCredAssumePolicyRead(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	return diags
}

func resourceAwsDeployCredAssumePolicyDelete(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {

	// d.SetId("") is automatically called assuming delete returns no errors, but
	// it is added here for explicitness.
	d.SetId("")
	return diags
}
