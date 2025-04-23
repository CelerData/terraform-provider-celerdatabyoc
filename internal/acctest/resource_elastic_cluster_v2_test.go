package acctest

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccElasticClusterV2_basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                  func() { PreCheck(t) },
		ProviderFactories:         TestAccProviderFactories,
		PreventPostDestroyRefresh: true,
		//CheckDestroy:      checkDestroy,
		Steps: []resource.TestStep{
			{
				Config: ReadFile("./elastic-cluster-v2/step1.tf"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("celerdatabyoc_elastic_cluster_v2.multi_warehouse", "csp", "aws"),
					resource.TestCheckResourceAttr("celerdatabyoc_elastic_cluster_v2.multi_warehouse", "region", "us-west-2"),
					resource.TestCheckResourceAttr("celerdatabyoc_elastic_cluster_v2.multi_warehouse", "cluster_name", "test-multi-warehouse"),
					testAccCheckElasticClusterV2Exists("celerdatabyoc_elastic_cluster_v2.multi_warehouse"),
				),
			},
		},
	})
}

func testAccCheckElasticClusterV2Exists(n string) resource.TestCheckFunc {
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

func checkDestroy(s *terraform.State) error {
	return nil
}
