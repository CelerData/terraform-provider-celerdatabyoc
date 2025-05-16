package acctest

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func TestAccElasticClusterV2_Basic(t *testing.T) {

	resourceName := "celerdatabyoc_elastic_cluster_v2.multi_warehouse"
	clusterName := "tf-test-multi-warehouse"

	resource.Test(t, resource.TestCase{
		PreCheck:          func() { PreCheck(t) },
		ProviderFactories: TestAccProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: ReadFile("./elastic-cluster-v2/create.tf"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttrSet(resourceName, "id"),
					resource.TestCheckResourceAttr(resourceName, "cluster_name", clusterName),
					resource.TestCheckResourceAttr(resourceName, "csp", "aws"),
					resource.TestCheckResourceAttr(resourceName, "region", "us-west-2---stage"),
					resource.TestCheckResourceAttr(resourceName, "coordinator_node_size", "m6i.xlarge"),
					resource.TestCheckResourceAttr(resourceName, "coordinator_node_count", "1"),
					resource.TestCheckResourceAttr(resourceName, "default_warehouse.0.compute_node_size", "m5.xlarge"),
					resource.TestCheckResourceAttr(resourceName, "default_warehouse.0.compute_node_count", "1"),
					resource.TestCheckResourceAttr(resourceName, "default_warehouse.0.compute_node_volume_config.0.vol_size", "140"),
					resource.TestCheckResourceAttr(resourceName, "warehouse_external_info.%", "2"),
				),
			},
			{
				Config: ReadFile("./elastic-cluster-v2/update.tf"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(resourceName, "coordinator_node_count", "1"),
					resource.TestCheckResourceAttr(resourceName, "default_warehouse.0.compute_node_count", "1"),
					resource.TestCheckResourceAttr(resourceName, "default_warehouse.0.compute_node_volume_config.0.vol_size", "140"),
				),
			},
			{
				ResourceName:      resourceName,
				ImportState:       true,
				ImportStateVerify: true,
				ImportStateVerifyIgnore: []string{
					"default_admin_password",
				},
			},
		},
	})
}
