package acctest

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccClassicCluster_basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                  func() { PreCheck(t) },
		ProviderFactories:         TestAccProviderFactories,
		PreventPostDestroyRefresh: true,
		CheckDestroy:              nil,
		Steps: []resource.TestStep{
			{
				Config: ReadFile("./classic-cluster/step1.tf"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("celerdatabyoc_classic_cluster.test-classic-cluster", "csp", "aws"),
					resource.TestCheckResourceAttr("celerdatabyoc_classic_cluster.test-classic-cluster", "region", "us-west-2"),
					testAccCheckClassicClusterExists("celerdatabyoc_classic_cluster.test-classic-cluster"),
				),
			},
			{
				Config: ReadFile("./classic-cluster/step2.tf"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("celerdatabyoc_classic_cluster.test-classic-cluster", "csp", "aws"),
					resource.TestCheckResourceAttr("celerdatabyoc_classic_cluster.test-classic-cluster", "region", "us-west-2"),
					testAccCheckClassicClusterExists("celerdatabyoc_classic_cluster.test-classic-cluster"),
				),
			},
			{
				Config: ReadFile("./classic-cluster/step3.tf"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("celerdatabyoc_classic_cluster.test-classic-cluster", "csp", "aws"),
					resource.TestCheckResourceAttr("celerdatabyoc_classic_cluster.test-classic-cluster", "region", "us-west-2"),
					testAccCheckClassicClusterExists("celerdatabyoc_classic_cluster.test-classic-cluster"),
				),
			},
		},
	})
}

func testAccCheckClassicClusterExists(n string) resource.TestCheckFunc {
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
