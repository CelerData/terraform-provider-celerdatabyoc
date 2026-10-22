package celerdatabyoc

import (
	"context"
	"encoding/json"
	"fmt"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/credential"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/csp"

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

	// CelerData only accepts deployment credentials whose ExternalId it issued to the
	// organization, so the id comes from the API instead of being generated here. It is kept in
	// state from then on: issuing again after a credential used the id would return a new one.
	eidResp, err := credential.NewCredentialAPI(c).IssueExternalId(ctx)
	if err != nil {
		return diag.FromErr(err)
	}
	externalID := eidResp.ExternalId

	policyJSON, err := buildDeployCredAssumePolicy(resp.Csp.TrustAccountId, externalID)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(externalID)
	// nolint
	d.Set("json", policyJSON)
	d.Set("external_id", externalID)
	return nil
}

// buildDeployCredAssumePolicy is the trust policy that lets CelerData's AWS account assume the
// deployment role, and only with externalID.
func buildDeployCredAssumePolicy(trustAccountID, externalID string) (string, error) {
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
					"AWS": fmt.Sprintf("arn:aws:iam::%s:root", trustAccountID),
				},
			},
		},
	}
	policyJSON, err := json.MarshalIndent(policy, "", "  ")
	if err != nil {
		return "", err
	}
	return string(policyJSON), nil
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
