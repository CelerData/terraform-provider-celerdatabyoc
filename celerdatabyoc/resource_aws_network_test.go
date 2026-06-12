package celerdatabyoc

import (
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccCelerdataAwsNetworkBasic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testCheckAwsNetworkDestroy,
		Steps: []resource.TestStep{
			{
				Config: testCheckAwsNetworkConfigBasic(),
				Check: resource.ComposeTestCheckFunc(
					testCheckCelerdataAwsNetworkExists("celerdatabyoc_aws_network.new"),
				),
			},
		},
	})
}

func testCheckAwsNetworkDestroy(s *terraform.State) error {
	for _, rs := range s.RootModule().Resources {
		if rs.Type != "celerdatabyoc_aws_network" {
			continue
		}

		fmt.Println(rs.Primary.ID)
	}

	return nil
}

func testCheckAwsNetworkConfigBasic() string {
	return fmt.Sprintf(`
	resource "celerdatabyoc_aws_network" "new" {
		name = "test-network"
		subnet_id = "%s"
		security_group_id = "%s"
		deployment_credential_id = "%s"
	}
	`, os.Getenv("CELERDATA_SUBNET_ID"),
		os.Getenv("CELERDATA_SECURITY_GROUP_ID"),
		os.Getenv("CELERDATA_DEPLOYMENT_CREDENTIAL_ID"))
}

func testCheckCelerdataAwsNetworkExists(n string) resource.TestCheckFunc {
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
