package celerdatabyoc

import (
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"log"
	"testing"
)

const (
	deployment_credential_id = "b6d06e66-fbaf-43fd-8260-97f17633c7c2"
	storage_credential_id    = "6848f9bf-70ea-4302-b0ed-a1e1d871308f"
	network_credential_id    = "efce534e-3988-4a64-8347-1982319d83ae"
)

func TestAccClassicCluster(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck: func() { testPreCheck(t) },
		ProviderFactories: map[string]func() (*schema.Provider, error){
			"celerdatabyoc": func() (*schema.Provider, error) {
				return Provider(), nil
			},
		},
		CheckDestroy: testCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testCreateCluster(),
				Check: resource.ComposeTestCheckFunc(
					testResourceExists("celerdatabyoc_classic_cluster.classic_cluster_1"),
				),
			},
			{
				Config: testRerunScripts(),
				Check: resource.ComposeTestCheckFunc(
					testResourceExists("celerdatabyoc_classic_cluster.classic_cluster_1"),
				),
			},
			{
				Config: testRerunScripts(),
				Check: resource.ComposeTestCheckFunc(
					testResourceExists("celerdatabyoc_classic_cluster.classic_cluster_1"),
				),
			},
			{
				Config: testRerunScripts(),
				Check: resource.ComposeTestCheckFunc(
					testResourceExists("celerdatabyoc_classic_cluster.classic_cluster_1"),
				),
			},
			{
				Config: testRerunScripts(),
				Check: resource.ComposeTestCheckFunc(
					testResourceExists("celerdatabyoc_classic_cluster.classic_cluster_1"),
				),
			},
			{
				Config: testScripts(),
				Check: resource.ComposeTestCheckFunc(
					testResourceExists("celerdatabyoc_classic_cluster.classic_cluster_1"),
				),
			},
			{
				Config: testScripts(),
				Check: resource.ComposeTestCheckFunc(
					testResourceExists("celerdatabyoc_classic_cluster.classic_cluster_1"),
				),
			},
		},
	})
}

func testCreateCluster() string {
	return fmt.Sprintf(`
resource "celerdatabyoc_classic_cluster" "classic_cluster_1" {
  deployment_credential_id = "%s"
  data_credential_id       = "%s"
  network_id               = "%s"

  cluster_name     = "test_cluster_02"
  fe_instance_type = "m6i.large"
  fe_node_count    = 1


  be_instance_type = "m6i.large"
  be_node_count    = 1
  be_volume_config {
    vol_number = 2
    vol_size = 100
 }

 scripts {
   script_path = "s3://qa-auto-test-saas-us-west-2/scripts/check_fe_leader.sh"
   logs_dir = "s3://qa-auto-test-saas-us-west-2/scripts/check_fe_leader_logs/"
 }

  default_admin_password = "saasTest@123"

  expected_cluster_state = "Running"

  csp    = "aws"
  region = "us-west-2---stage"
  idle_suspend_interval = 30
}
`, deployment_credential_id, storage_credential_id, network_credential_id)
}

func testRerunScripts() string {
	return fmt.Sprintf(`
resource "celerdatabyoc_classic_cluster" "classic_cluster_1" {
  deployment_credential_id = "%s"
  data_credential_id       = "%s"
  network_id               = "%s"

  cluster_name     = "test_cluster_02"
  fe_instance_type = "m6i.large"
  fe_node_count    = 1


  be_instance_type = "m6i.large"
  be_node_count    = 1
  be_volume_config {
    vol_number = 2
    vol_size = 100
 }

 scripts {
   script_path = "s3://qa-auto-test-saas-us-west-2/scripts/check_fe_leader.sh"
   logs_dir = "s3://qa-auto-test-saas-us-west-2/scripts/check_fe_leader_logs/"
   re_run = true
 }

  default_admin_password = "saasTest@123"

  expected_cluster_state = "Running"

  csp    = "aws"
  region = "us-west-2---stage"
  idle_suspend_interval = 30
}
`, deployment_credential_id, storage_credential_id, network_credential_id)
}

func testScripts() string {
	return fmt.Sprintf(`
resource "celerdatabyoc_classic_cluster" "classic_cluster_1" {
  deployment_credential_id = "%s"
  data_credential_id       = "%s"
  network_id               = "%s"

  cluster_name     = "test_cluster_02"
  fe_instance_type = "m6i.large"
  fe_node_count    = 1


  be_instance_type = "m6i.large"
  be_node_count    = 1
  be_volume_config {
    vol_number = 2
    vol_size = 100
 }

 scripts {
   script_path = "s3://qa-auto-test-saas-us-west-2/scripts/check_fe_leader.sh"
   logs_dir = "s3://qa-auto-test-saas-us-west-2/scripts/check_fe_leader_logs/"
 }

  default_admin_password = "saasTest@123"

  expected_cluster_state = "Running"

  csp    = "aws"
  region = "us-west-2---stage"
  idle_suspend_interval = 30
}
`, deployment_credential_id, storage_credential_id, network_credential_id)
}

func testResourceExists(n string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[n]

		if !ok {
			return fmt.Errorf("Not found: %s", n)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("Not exists")
		}

		return nil
	}
}

func testCheckDestroy(s *terraform.State) error {
	for _, rs := range s.RootModule().Resources {
		if rs.Type != "celerdatabyoc_classic_cluster" {
			continue
		}
		fmt.Println(rs.Primary.ID)
		if rs.Primary.ID != "" {
			log.Printf("[INFO] Destroy cluster failed, clusterId: %s", rs.Primary.ID)
		}
	}

	return nil
}
