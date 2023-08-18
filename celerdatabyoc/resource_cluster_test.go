package celerdatabyoc

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccCelerdataClusterBasic(t *testing.T) {
	clusterName := "test_cluster"
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testCheckCelerdataClusterDestroy,
		Steps: []resource.TestStep{
			{
				Config: testCheckCelerdataClusterConfigBasic(clusterName),
				Check: resource.ComposeTestCheckFunc(
					testCheckCelerdataClusterExists("celerdatabyoc_cluster.new"),
				),
			},
			{
				Config: testCheckCelerdataClusterConfigUpdate(clusterName),
				Check: resource.ComposeTestCheckFunc(
					testCheckCelerdataClusterExists("celerdatabyoc_cluster.new"),
				),
			},
		},
	})
}

func testCheckCelerdataClusterDestroy(s *terraform.State) error {
	for _, rs := range s.RootModule().Resources {
		if rs.Type != "celerdatabyoc_cluster" {
			continue
		}

		fmt.Println(rs.Primary.ID)
	}

	return nil
}

func testCheckCelerdataClusterConfigBasic(cluster_name string) string {
	return fmt.Sprintf(`
	resource "celerdatabyoc_cluster" "new" {
		cluster_type = "CLASSIC"
		cluster_name = "%s"
		fe_instance_type = "t2.small"
		deployment_credential_id = "e52bbbbd-9944-4b69-895f-8542482f2ef4"
		data_credential_id = "f4273e04-8b00-4c70-866b-44b0cdb7d7fa"
		network_id = "526068e9-6257-43d4-8bac-8a9f773badb8"
		be_instance_type = "m6i.large"
		be_node_count = 1
		be_storage_size_gb = 100
		admin_password = "test"
	}
	`, cluster_name)
}

func testCheckCelerdataClusterConfigUpdate(cluster_name string) string {
	return fmt.Sprintf(`
	resource "celerdatabyoc_cluster" "new" {
		cluster_type = "CLASSIC"
		cluster_name = "%s"
		fe_instance_type = "m6i.large"
		fe_node_count = 3
		deployment_credential_id = "e52bbbbd-9944-4b69-895f-8542482f2ef4"
		data_credential_id = "f4273e04-8b00-4c70-866b-44b0cdb7d7fa"
		network_id = "526068e9-6257-43d4-8bac-8a9f773badb8"
		be_instance_type = "m5.xlarge"
		be_node_count = 3
		be_storage_size_gb = 200
		admin_password = "test"
	}
	`, cluster_name)
}
func testCheckCelerdataClusterExists(n string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[n]

		if !ok {
			return fmt.Errorf("Not found: %s", n)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("No ClusterID set")
		}

		return nil
	}
}
