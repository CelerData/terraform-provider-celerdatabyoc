resource "celerdatabyoc_elastic_cluster_v2" "multi_warehouse" {
  cluster_name = "test-multi-warehouse"
  coordinator_node_size    = "m6i.xlarge"
  coordinator_node_count   = 3
  deployment_credential_id = "15dcf515-c34a-42de-8518-7da19fd72619"
  data_credential_id       = "5a90fba6-9588-4c7c-841b-56ede903966e"
  network_id               = "f2062934-1ae7-4244-a9cf-fb1e1bf5e7c9"

  coordinator_node_configs = {
    test1=1
  }

  default_warehouse {
    compute_node_size        = "m5d.xlarge"
    compute_node_count       = 1
    compute_node_configs = {
       test3=3
    }
  }

  warehouse {
    name = "wh01"
    compute_node_size        = "c6i.xlarge"
    compute_node_count       = 1
    compute_node_volume_config {
      vol_number = 1
      vol_size = 140
      iops = 5100
      throughput = 150
    }
  }

  warehouse {
    name = "wh02"
    compute_node_size        = "m5d.2xlarge"
    compute_node_count       = 1
    compute_node_configs = {
       test7=7
       test8=8
    }
  }

  default_admin_password = "admin@123"
  expected_cluster_state = "Running"
  csp = "aws"
  region = "us-west-2"
  run_scripts_parallel = false
  query_port = 9030
  idle_suspend_interval = 60
}