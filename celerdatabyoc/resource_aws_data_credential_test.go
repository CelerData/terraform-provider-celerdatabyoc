package celerdatabyoc

import (
	"fmt"
	"os"
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
	return fmt.Sprintf(`
	resource "celerdatabyoc_aws_data_credential" "new" {
		name = "test-data-credential"
		role_arn = "%s"
		instance_profile_arn = "%s"
		bucket_name = "%s"
		policy_version = "%s"
	}
	`, os.Getenv("CELERDATA_DATA_ROLE_ARN"),
		os.Getenv("CELERDATA_INSTANCE_PROFILE_ARN"),
		os.Getenv("CELERDATA_BUCKET_NAME"),
		os.Getenv("CELERDATA_DATA_POLICY_VERSION"))
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
