resource "celerdatabyoc_elastic_cluster_v2" "multi_warehouse" {

  deployment_credential_id = "b978dc74-d230-414d-9f50-d5f0cebd7531"
  data_credential_id       = "f204494e-e2ca-4f8f-a8f0-fa22dffb0bf4"
  network_id               = "18bd841f-0324-466d-85fe-9fa8077bf7af"

  cluster_name = "tf-test-multi-warehouse"
  coordinator_node_size    = "m6i.xlarge"
  coordinator_node_count   = 1

  coordinator_node_configs = {
    test1=1
  }

  default_warehouse {
    compute_node_size        = "m5.xlarge"
    compute_node_count       = 1
    compute_node_volume_config {
      vol_number = 1
      vol_size = 140
      iops = 5000
      throughput = 150
    }
    compute_node_configs = {
       sys_log_level="WARN"
    }
  }

  warehouse {
    name = "wh02"
    compute_node_size        = "m5.xlarge"
    compute_node_count       = 1
    compute_node_volume_config {
      vol_number = 1
      vol_size = 140
      iops = 5000
      throughput = 150
    }
    compute_node_configs = {
       sys_log_level="INFO"
    }
    idle_suspend_interval = 15
  }

  default_admin_password = "admin@123"
  expected_cluster_state = "Running"
  csp = "aws"
  region = "us-west-2---stage"
  run_scripts_parallel = false
  query_port = 9030
  idle_suspend_interval = 15
}