package celerdatabyoc

import (
	"fmt"
	"os"
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
					testCheckCelerdataClusterExists("celerdatabyoc_classic_cluster.new"),
				),
			},
			{
				Config: testCheckCelerdataClusterConfigUpdate(clusterName),
				Check: resource.ComposeTestCheckFunc(
					testCheckCelerdataClusterExists("celerdatabyoc_classic_cluster.new"),
				),
			},
		},
	})
}

func testCheckCelerdataClusterDestroy(s *terraform.State) error {
	for _, rs := range s.RootModule().Resources {
		if rs.Type != "celerdatabyoc_classic_cluster" {
			continue
		}

		fmt.Println(rs.Primary.ID)
	}

	return nil
}

func testCheckCelerdataClusterConfigBasic(cluster_name string) string {
	return fmt.Sprintf(`
	resource "celerdatabyoc_classic_cluster" "new" {
		cluster_name = "%s"
		csp = "%s"
		region = "%s"
		fe_instance_type = "m6i.xlarge"
		fe_node_count = 1
		be_instance_type = "m6i.large"
		be_node_count = 1
		deployment_credential_id = "%s"
		data_credential_id = "%s"
		network_id = "%s"
		default_admin_password = "Test_123456"
	}
	`, cluster_name,
		os.Getenv("CELERDATA_CSP"),
		os.Getenv("CELERDATA_REGION"),
		os.Getenv("CELERDATA_DEPLOYMENT_CREDENTIAL_ID"),
		os.Getenv("CELERDATA_DATA_CREDENTIAL_ID"),
		os.Getenv("CELERDATA_NETWORK_ID"))
}

func testCheckCelerdataClusterConfigUpdate(cluster_name string) string {
	return fmt.Sprintf(`
	resource "celerdatabyoc_classic_cluster" "new" {
		cluster_name = "%s"
		csp = "%s"
		region = "%s"
		fe_instance_type = "m6i.xlarge"
		fe_node_count = 3
		be_instance_type = "m6i.large"
		be_node_count = 3
		deployment_credential_id = "%s"
		data_credential_id = "%s"
		network_id = "%s"
		default_admin_password = "Test_123456"
	}
	`, cluster_name,
		os.Getenv("CELERDATA_CSP"),
		os.Getenv("CELERDATA_REGION"),
		os.Getenv("CELERDATA_DEPLOYMENT_CREDENTIAL_ID"),
		os.Getenv("CELERDATA_DATA_CREDENTIAL_ID"),
		os.Getenv("CELERDATA_NETWORK_ID"))
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
