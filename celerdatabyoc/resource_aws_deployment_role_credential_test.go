package celerdatabyoc

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccCelerdataAwsDeploymentCredentialBasic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testCheckAwsDeploymentRoleCredentialDestroy,
		Steps: []resource.TestStep{
			{
				Config: testCheckAwsDeploymentRoleCredentialConfigBasic(),
				Check: resource.ComposeTestCheckFunc(
					testCheckCelerdataAwsDeploymentRoleCredentialExists("celerdatabyoc_aws_deployment_role_credential.new"),
				),
			},
		},
	})
}

func testCheckAwsDeploymentRoleCredentialDestroy(s *terraform.State) error {
	for _, rs := range s.RootModule().Resources {
		if rs.Type != "celerdatabyoc_aws_deployment_role_credential" {
			continue
		}

		fmt.Println(rs.Primary.ID)
	}

	return nil
}

func testCheckAwsDeploymentRoleCredentialConfigBasic() string {
	return `
	resource "celerdatabyoc_aws_deployment_role_credential" "new" {
		name = "test-deployment-role-credential"
		role_arn = "arn:aws:iam::081976408565:role/celerdata-stack-v36pza-CelerdataDeploymentRole-AKVIT05897ML"
		external_id = "ed6b3030-1f00-11ee-a769-0ab2a5ac006f"
		policy_version = "20230322062733"
	}
	`
}

func testCheckCelerdataAwsDeploymentRoleCredentialExists(n string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[n]

		if !ok {
			return fmt.Errorf("Not found: %s", n)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("No credential set")
		}

		return nil
	}
}
