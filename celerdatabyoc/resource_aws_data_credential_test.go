package celerdatabyoc

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccCelerdataAwsDataCredentialBasic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testCheckAwsDataCredentialDestroy,
		Steps: []resource.TestStep{
			{
				Config: testCheckAwsDataCredentialConfigBasic(),
				Check: resource.ComposeTestCheckFunc(
					testCheckCelerdataAwsDataCredentialExists("celerdatabyoc_aws_data_credential.new"),
				),
			},
		},
	})
}

func testCheckAwsDataCredentialDestroy(s *terraform.State) error {
	for _, rs := range s.RootModule().Resources {
		if rs.Type != "celerdatabyoc_aws_data_credential" {
			continue
		}

		fmt.Println(rs.Primary.ID)
	}

	return nil
}

func testCheckAwsDataCredentialConfigBasic() string {
	return `
	resource "celerdatabyoc_aws_data_credential" "new" {
		name = "test-data-credential"
		role_arn = "arn:aws:iam::081976408565:role/saas-dev-storage-role"
		instance_profile_arn = "arn:aws:iam::081976408565:instance-profile/saas-dev-storage-role"
		bucket_name = "saas-us-west-2"
		policy_version = "20221121025439"
	}
	`
}

func testCheckCelerdataAwsDataCredentialExists(n string) resource.TestCheckFunc {
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
