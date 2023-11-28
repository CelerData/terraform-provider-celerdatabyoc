package celerdatabyoc

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccCelerdataClusterEndpointsBasic(t *testing.T) {
	clusterId := "2fec59aa-4bf6-4932-bd33-15af32066669"
	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testPreCheck(t) },
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: testCheckCelerdataClusterEndpointsConfigBasic(clusterId),
				Check: resource.ComposeTestCheckFunc(
					testCheckCelerdataClusterEndpointsExists("celerdatabyoc_cluster_endpoints.new"),
				),
			},
			{
				Config: testCheckCelerdataClusterConfigEndpointsConfigUpdate(clusterId),
				Check: resource.ComposeTestCheckFunc(
					testCheckCelerdataClusterEndpointsExists("celerdatabyoc_cluster_endpoints.new"),
				),
			},
		},
	})
}

func testCheckCelerdataClusterEndpointsConfigBasic(clusterId string) string {
	return fmt.Sprintf(`
	resource "celerdatabyoc_cluster_endpoints" "new" {
  cluster_id = "%s"
}
	`, clusterId)
}

func testCheckCelerdataClusterConfigEndpointsConfigUpdate(clusterId string) string {
	return fmt.Sprintf(`
	resource "celerdatabyoc_cluster_endpoints" "new" {
  cluster_id = "%s"
}
	`, clusterId)
}
func testCheckCelerdataClusterEndpointsExists(n string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[n]

		if !ok {
			return fmt.Errorf("Not found: %s", n)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("No cluster endpoints ID set")
		}

		return nil
	}
}
