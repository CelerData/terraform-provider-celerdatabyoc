package celerdatabyoc

import (
	"context"
	"encoding/json"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func dataAwsDataCredentialAssumeRolePolicy() *schema.Resource {
	return &schema.Resource{
		ReadContext: func(ctx context.Context, d *schema.ResourceData, m any) diag.Diagnostics {
			policy := awsIamPolicy{
				Version: "2012-10-17",
				Statements: []*awsIamPolicyStatement{
					{
						Effect:  "Allow",
						Actions: "sts:AssumeRole",
						Principal: map[string]string{
							"Service": "ec2.amazonaws.com",
						},
					},
				},
			}
			policyJSON, err := json.MarshalIndent(policy, "", "  ")
			if err != nil {
				return diag.FromErr(err)
			}
			d.SetId("data_credential_assume_role_policy")
			d.Set("json", string(policyJSON))
			return nil
		},
		Schema: map[string]*schema.Schema{
			"json": {
				Type:     schema.TypeString,
				Computed: true,
				ForceNew: true,
			},
		},
	}
}
