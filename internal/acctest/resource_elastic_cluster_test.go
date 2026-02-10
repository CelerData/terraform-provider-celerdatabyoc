package acctest

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccElasticCluster_basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                  func() { PreCheck(t) },
		ProviderFactories:         TestAccProviderFactories,
		PreventPostDestroyRefresh: true,
		CheckDestroy:              nil,
		Steps: []resource.TestStep{
			{
				Config: ReadFile("./elastic-cluster/step1.tf"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("celerdatabyoc_elastic_cluster.test-elastic-cluster", "csp", "aws"),
					resource.TestCheckResourceAttr("celerdatabyoc_elastic_cluster.test-elastic-cluster", "region", "us-west-2"),
					testAccCheckElasticClusterExists("celerdatabyoc_elastic_cluster.test-elastic-cluster"),
				),
			},
		},
	})
}

func testAccCheckElasticClusterExists(n string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[n]
		if !ok {
			return fmt.Errorf("Not found: %s", n)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("No ID is set")
		}

		return nil
	}
}
