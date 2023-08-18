package celerdatabyoc

import (
	"fmt"
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
	return `
	resource "celerdatabyoc_aws_network" "new" {
		name = "test-network"
		subnet_id = "subnet-05344e8c20ceb9ad4"
		security_group_id = "sg-09a84dfe8fc1d8deb"
		deployment_credential_id = "e52bbbbd-9944-4b69-895f-8542482f2ef4" 
	}
	`
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
