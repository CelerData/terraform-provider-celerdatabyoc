
locals {
  config = {
    stage = {
      region                   = "us-west-2"
      deployment_credential_id = "15dcf515-c34a-42de-8518-7da19fd72619",
      data_credential_id       = "5a90fba6-9588-4c7c-841b-56ede903966e",
      network_id               = "f2062934-1ae7-4244-a9cf-fb1e1bf5e7c9"
    }
  }
}

resource "celerdatabyoc_classic_cluster" "classic_cluster" {
  deployment_credential_id = lookup(local.config["stage"], "deployment_credential_id")
  data_credential_id       = lookup(local.config["stage"], "data_credential_id")
  network_id               = lookup(local.config["stage"], "network_id")

  cluster_name = "test-classic-cluster-02"

  fe_instance_type = "m6i.xlarge"
  fe_node_count = 1
  fe_volume_config {
    vol_size = 140
    iops = 5100
    throughput = 130
  }

  be_instance_type = "c6i.xlarge"
  be_node_count = 1
  be_volume_config {
    vol_number = 3
    vol_size = 140
    iops = 5100
    throughput = 130
  }

  fe_configs = {
    test1=1
  }
  be_configs = {
    test2=2
  }

  default_admin_password = "admin@123"
  expected_cluster_state = "Running"
  csp = "aws"
  region = "us-west-2"
  run_scripts_parallel = false
  query_port = 9030
  idle_suspend_interval = 60
}