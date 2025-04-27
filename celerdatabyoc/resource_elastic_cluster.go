package celerdatabyoc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/network"
	"terraform-provider-celerdatabyoc/common"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Old vesrion
func resourceElasticCluster() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceElasticClusterCreate,
		ReadContext:   resourceElasticClusterRead,
		DeleteContext: resourceElasticClusterDelete,
		UpdateContext: resourceElasticClusterUpdate,
		Schema: map[string]*schema.Schema{
			"id": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"csp": {
				Type:     schema.TypeString,
				Required: true,
			},
			"region": {
				Type:     schema.TypeString,
				Required: true,
			},
			"cluster_state": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"cluster_name": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringMatch(regexp.MustCompile(`^[0-9a-zA-Z_-]{1,32}$`), "The cluster name is restricted to a maximum length of 32 characters and can only consist of alphanumeric characters (a-z, A-Z, 0-9), hyphens (-), and underscores (_)."),
			},
			"coordinator_node_size": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},
			"coordinator_node_count": {
				Type:         schema.TypeInt,
				Optional:     true,
				Default:      1,
				ValidateFunc: validation.IntInSlice([]int{1, 3, 5}),
			},
			"coordinator_node_volume_config": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"vol_size": {
							Type:         schema.TypeInt,
							Required:     true,
							ValidateFunc: validation.IntAtLeast(1),
						},
						"iops": {
							Type:         schema.TypeInt,
							Required:     true,
							ValidateFunc: validation.IntAtLeast(0),
						},
						"throughput": {
							Type:         schema.TypeInt,
							Required:     true,
							ValidateFunc: validation.IntAtLeast(0),
						},
					},
				},
			},
			"compute_node_size": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},
			"compute_node_count": {
				Type:         schema.TypeInt,
				Optional:     true,
				Default:      3,
				ValidateFunc: validation.IntAtLeast(1),
			},
			"compute_node_is_instance_store": {
				Type:     schema.TypeBool,
				Computed: true,
			},
			"compute_node_volume_config": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"vol_number": {
							Description: "Specifies the number of disk. The default value is 2.",
							Type:        schema.TypeInt,
							Optional:    true,
							ValidateFunc: func(i interface{}, k string) (warnings []string, errors []error) {
								v, ok := i.(int)
								if !ok {
									errors = append(errors, fmt.Errorf("expected type of %s to be int", k))
									return warnings, errors
								}

								if v < 1 || v > 24 {
									errors = append(errors, fmt.Errorf("%s`s value is invalid. The range of values is: [1,24]", k))
								}
								return warnings, errors
							},
						},
						"vol_size": {
							Description: "Specifies the size of a single disk in GB. The default size for per disk is 100GB.",
							Type:        schema.TypeInt,
							Required:    true,
							ValidateFunc: func(i interface{}, k string) (warnings []string, errors []error) {
								v, ok := i.(int)
								if !ok {
									errors = append(errors, fmt.Errorf("expected type of %s to be int", k))
									return warnings, errors
								}

								m := 16 * 1000
								if v <= 0 {
									errors = append(errors, fmt.Errorf("%s`s value is invalid", k))
								} else if v > m {
									errors = append(errors, fmt.Errorf("%s`s value is invalid. The range of values is: [1,%d]", k, m))
								}

								return warnings, errors
							},
						},
						"iops": {
							Type:         schema.TypeInt,
							Required:     true,
							ValidateFunc: validation.IntAtLeast(0),
						},
						"throughput": {
							Type:         schema.TypeInt,
							Required:     true,
							ValidateFunc: validation.IntAtLeast(0),
						},
					},
				},
			},
			"resource_tags": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"default_admin_password": {
				Type:             schema.TypeString,
				Required:         true,
				Sensitive:        true,
				ValidateDiagFunc: common.ValidatePassword(),
			},
			"data_credential_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"deployment_credential_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"network_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"expected_cluster_state": {
				Type:         schema.TypeString,
				Optional:     true,
				Default:      string(cluster.ClusterStateRunning),
				ValidateFunc: validation.StringInSlice([]string{string(cluster.ClusterStateSuspended), string(cluster.ClusterStateRunning)}, false),
			},
			"free_tier": {
				Type:     schema.TypeBool,
				Computed: true,
			},
			"init_scripts": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"script_path": {
							Type:     schema.TypeString,
							Required: true,
						},
						"logs_dir": {
							Type:     schema.TypeString,
							Required: true,
						},
					},
				},
			},
			"run_scripts_parallel": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
			"query_port": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  9030,
				ValidateFunc: func(i interface{}, k string) (warnings []string, errors []error) {
					v, ok := i.(int)
					if !ok {
						errors = append(errors, fmt.Errorf("expected type of %s to be int", k))
						return warnings, errors
					}
					if v < 1 || v > 65535 {
						errors = append(errors, fmt.Errorf("the %s range should be 1-65535", k))
						return warnings, errors
					}
					if v == 443 {
						errors = append(errors, fmt.Errorf("%s : duplicate port 443 definitions", k))
						return warnings, errors
					}
					return warnings, errors
				},
			},
			"idle_suspend_interval": {
				Type:        schema.TypeInt,
				Description: "Specifies the amount of time (in minutes) during which a cluster can stay idle. After the specified time period elapses, the cluster will be automatically suspended.",
				Optional:    true,
				Default:     0,
				ValidateFunc: func(i interface{}, k string) (warnings []string, errors []error) {
					v, ok := i.(int)
					if !ok {
						errors = append(errors, fmt.Errorf("expected type of %s to be int", k))
						return warnings, errors
					}

					if v != 0 {
						if v < 15 || v > 999999 {
							errors = append(errors, fmt.Errorf("the %s range should be [15,999999]", k))
							return warnings, errors
						}
					}
					return warnings, errors
				},
			},
			"ldap_ssl_certs": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
					ValidateFunc: func(i interface{}, k string) (warnings []string, errors []error) {
						value, ok := i.(string)
						if !ok {
							errors = append(errors, fmt.Errorf("expected type of %s to be string", k))
							return warnings, errors
						}

						if len(value) > 0 {
							if !CheckS3Path(value) {
								errors = append(errors, fmt.Errorf("for %s invalid s3 path:%s", k, value))
							}
						} else {
							errors = append(errors, fmt.Errorf("%s`s value cann`t be empty", k))
						}
						return warnings, errors
					},
				},
			},
			"run_scripts_timeout": {
				Type:         schema.TypeInt,
				Optional:     true,
				Default:      3600,
				ValidateFunc: validation.IntAtMost(int(common.DeployOrScaleClusterTimeout.Seconds())),
			},
			"coordinator_node_configs": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"compute_node_configs": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"ranger_certs_dir": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringIsNotWhiteSpace,
			},
		},
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(common.DeployOrScaleClusterTimeout),
			Update: schema.DefaultTimeout(common.DeployOrScaleClusterTimeout),
		},
		CustomizeDiff: customizeElDiff,
	}
}

func customizeElDiff(ctx context.Context, d *schema.ResourceDiff, m interface{}) error {
	c := m.(*client.CelerdataClient)
	clusterAPI := cluster.NewClustersAPI(c)
	networkAPI := network.NewNetworkAPI(c)

	clusterId := d.Id()
	csp := d.Get("csp").(string)
	region := d.Get("region").(string)
	isNewResource := d.Id() == ""

	n := d.Get("coordinator_node_size")
	newVmInfoResp, err := clusterAPI.GetVmInfo(ctx, &cluster.GetVmInfoReq{
		Csp:         csp,
		Region:      region,
		ProcessType: string(cluster.ClusterModuleTypeFE),
		VmCate:      n.(string),
	})
	if err != nil {
		log.Printf("[ERROR] query vm info failed, csp:%s region:%s vmCate:%s err:%+v", csp, region, n.(string), err)
		return fmt.Errorf("query vm info failed, csp:%s region:%s vmCate:%s errMsg:%s", csp, region, n.(string), err.Error())
	}
	if newVmInfoResp.VmInfo == nil {
		return fmt.Errorf("vm info not exists, csp:%s region:%s vmCate:%s", csp, region, n.(string))
	}

	feArch := newVmInfoResp.VmInfo.Arch

	if len(d.Get("network_id").(string)) > 0 {
		netResp, err := networkAPI.GetNetwork(ctx, d.Get("network_id").(string))
		if err != nil {
			return err
		}

		if netResp.Network.MultiAz {
			return errors.New("'celerdatabyoc_elastic_cluster' does not support multi-az deployment, please use 'celerdatabyoc_elastic_cluster_v2'")
		}
	}

	if d.HasChange("coordinator_node_size") {
		if len(clusterId) > 0 {
			o, _ := d.GetChange("coordinator_node_size")
			oldVmInfoResp, err := clusterAPI.GetVmInfo(ctx, &cluster.GetVmInfoReq{
				Csp:         csp,
				Region:      region,
				ProcessType: string(cluster.ClusterModuleTypeFE),
				VmCate:      o.(string),
			})
			if err != nil {
				log.Printf("[ERROR] query vm info failed, csp:%s region:%s vmCate:%s err:%+v", csp, region, o.(string), err)
				return fmt.Errorf("query vm info failed, csp:%s region:%s vmCate:%s errMsg:%s", csp, region, o.(string), err.Error())
			}
			if oldVmInfoResp.VmInfo == nil {
				return fmt.Errorf("vm info not exists, csp:%s region:%s vmCate:%s", csp, region, o.(string))
			}
			if feArch != oldVmInfoResp.VmInfo.Arch {
				return fmt.Errorf("vm architecture can not be changed, csp:%s region:%s oldVmCate:%s  newVmCate:%s", csp, region, o.(string), n.(string))
			}
		}
	}

	if d.HasChange("coordinator_node_volume_config") && !isNewResource {
		o, n := d.GetChange("coordinator_node_volume_config")

		oldVolumeConfig := cluster.DefaultFeVolumeMap()
		newVolumeConfig := cluster.DefaultFeVolumeMap()

		if len(o.([]interface{})) > 0 {
			oldVolumeConfig = o.([]interface{})[0].(map[string]interface{})
		}
		if len(n.([]interface{})) > 0 {
			newVolumeConfig = n.([]interface{})[0].(map[string]interface{})
		}

		oldVolumeSize, newVolumeSize := oldVolumeConfig["vol_size"].(int), newVolumeConfig["vol_size"].(int)

		if newVolumeSize < oldVolumeSize {
			return fmt.Errorf("coordinator node `vol_size` does not support decrease")
		}
	}

	if d.HasChange("compute_node_size") {
		cn := d.Get("compute_node_size")
		cnVmInfoResp, err := clusterAPI.GetVmInfo(ctx, &cluster.GetVmInfoReq{
			Csp:         csp,
			Region:      region,
			ProcessType: string(cluster.ClusterModuleTypeBE),
			VmCate:      cn.(string),
		})
		if err != nil {
			log.Printf("[ERROR] query vm info failed, csp:%s region:%s vmCate:%s err:%+v", csp, region, cn.(string), err)
			return fmt.Errorf("query vm info failed, csp:%s region:%s vmCate:%s errMsg:%s", csp, region, cn.(string), err.Error())
		}
		if cnVmInfoResp.VmInfo == nil {
			return fmt.Errorf("vm info not exists, csp:%s region:%s vmCate:%s", csp, region, cn.(string))
		}

		if feArch != cnVmInfoResp.VmInfo.Arch {
			return fmt.Errorf("compute node architecture should be same with coordinator node, expect:%s but found:%s", feArch, cnVmInfoResp.VmInfo.Arch)
		}

		if len(clusterId) > 0 {
			isInstanceStore := d.Get("compute_node_is_instance_store").(bool)
			expectStr := "local disk vm instance type"
			if !isInstanceStore {
				expectStr = "nonlocal disk vm instance type"
			}
			if cnVmInfoResp.VmInfo.IsInstanceStore != isInstanceStore {
				return fmt.Errorf("the disk type of the compute node must be the same as the previous disk type, expect:%s", expectStr)
			}
		}
	}

	if d.HasChange("compute_node_volume_config") && !isNewResource {
		o, n := d.GetChange("compute_node_volume_config")

		oldVolumeConfig := cluster.DefaultFeVolumeMap()
		newVolumeConfig := cluster.DefaultFeVolumeMap()

		if len(o.([]interface{})) > 0 {
			oldVolumeConfig = o.([]interface{})[0].(map[string]interface{})
		}
		if len(n.([]interface{})) > 0 {
			newVolumeConfig = n.([]interface{})[0].(map[string]interface{})
		}

		oldVolumeNumber, newVolumeNumber := oldVolumeConfig["vol_number"].(int), newVolumeConfig["vol_number"].(int)
		if newVolumeNumber != oldVolumeNumber {
			return fmt.Errorf("compute node `vol_number` field is not allowed to be modified")
		}

		oldVolumeSize, newVolumeSize := oldVolumeConfig["vol_size"].(int), newVolumeConfig["vol_size"].(int)

		if newVolumeSize < oldVolumeSize {
			return fmt.Errorf("compute node `vol_size` does not support decrease")
		}
	}

	return nil
}

func resourceElasticClusterCreate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	clusterAPI := cluster.NewClustersAPI(c)
	networkAPI := network.NewNetworkAPI(c)
	clusterName := d.Get("cluster_name").(string)

	clusterConf := &cluster.ClusterConf{
		ClusterName:        clusterName,
		Csp:                d.Get("csp").(string),
		Region:             d.Get("region").(string),
		ClusterType:        cluster.ClusterTypeElasic,
		Password:           d.Get("default_admin_password").(string),
		SslConnEnable:      true,
		NetIfaceId:         d.Get("network_id").(string),
		DeployCredlId:      d.Get("deployment_credential_id").(string),
		DataCredId:         d.Get("data_credential_id").(string),
		RunScriptsParallel: d.Get("run_scripts_parallel").(bool),
		QueryPort:          int32(d.Get("query_port").(int)),
		RunScriptsTimeout:  int32(d.Get("run_scripts_timeout").(int)),
	}

	netResp, err := networkAPI.GetNetwork(ctx, clusterConf.NetIfaceId)
	if err != nil {
		return diag.FromErr(err)
	}

	if netResp.Network.MultiAz {
		return diag.FromErr(errors.New("'celerdatabyoc_elastic_cluster' does not support multi-az deployment, please use 'celerdatabyoc_elastic_cluster_v2'"))
	}

	if v, ok := d.GetOk("resource_tags"); ok {
		rTags := v.(map[string]interface{})
		tags := make([]*cluster.Kv, 0, len(rTags))
		for k, v := range rTags {
			tags = append(tags, &cluster.Kv{Key: k, Value: v.(string)})
		}
		clusterConf.Tags = tags
	}

	if v, ok := d.GetOk("init_scripts"); ok {
		vL := v.(*schema.Set).List()
		scripts := make([]*cluster.Script, 0, len(vL))
		for _, v := range vL {
			s := v.(map[string]interface{})
			scripts = append(scripts, &cluster.Script{
				ScriptPath: s["script_path"].(string),
				LogsDir:    s["logs_dir"].(string),
			})
		}

		clusterConf.Scripts = scripts
	}

	coordinatorItem := &cluster.ClusterItem{
		Type:         cluster.ClusterModuleTypeFE,
		Name:         "FE",
		Num:          uint32(d.Get("coordinator_node_count").(int)),
		InstanceType: d.Get("coordinator_node_size").(string),
		DiskInfo: &cluster.DiskInfo{
			Number:  1,
			PerSize: 150,
		},
	}
	if v, ok := d.GetOk("coordinator_node_volume_config"); ok {
		volumeConfig := v.([]interface{})[0].(map[string]interface{})
		diskInfo := coordinatorItem.DiskInfo
		if v, ok := volumeConfig["vol_size"]; ok {
			diskInfo.PerSize = uint64(v.(int))
		}
		if v, ok := volumeConfig["iops"]; ok {
			diskInfo.Iops = uint64(v.(int))
		}
		if v, ok := volumeConfig["throughput"]; ok {
			diskInfo.Throughput = uint64(v.(int))
		}
	}

	computeItem := &cluster.ClusterItem{
		Type:         cluster.ClusterModuleTypeBE,
		Name:         "BE",
		Num:          uint32(d.Get("compute_node_count").(int)),
		InstanceType: d.Get("compute_node_size").(string),
		DiskInfo: &cluster.DiskInfo{
			Number:  uint32(2),
			PerSize: uint64(100),
		},
	}

	if v, ok := d.GetOk("compute_node_volume_config"); ok {
		volumeConfig := v.([]interface{})[0].(map[string]interface{})
		diskInfo := computeItem.DiskInfo
		if v, ok := volumeConfig["vol_number"]; ok {
			diskInfo.Number = uint32(v.(int))
		}
		if v, ok := volumeConfig["vol_size"]; ok {
			diskInfo.PerSize = uint64(v.(int))
		}
		if v, ok := volumeConfig["iops"]; ok {
			diskInfo.Iops = uint64(v.(int))
		}
		if v, ok := volumeConfig["throughput"]; ok {
			diskInfo.Throughput = uint64(v.(int))
		}
	}

	clusterConf.ClusterItems = append(clusterConf.ClusterItems, coordinatorItem, computeItem)

	resp, err := clusterAPI.Deploy(ctx, &cluster.DeployReq{
		RequestId:   uuid.NewString(),
		ClusterConf: clusterConf,
	})
	if err != nil {
		return diag.FromErr(err)
	}

	stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
		clusterAPI: clusterAPI,
		clusterID:  resp.ClusterID,
		actionID:   resp.ActionID,
		timeout:    common.DeployOrScaleClusterTimeout,
		pendingStates: []string{
			string(cluster.ClusterStateDeploying),
			string(cluster.ClusterStateScaling),
			string(cluster.ClusterStateResuming),
			string(cluster.ClusterStateSuspending),
			string(cluster.ClusterStateReleasing),
			string(cluster.ClusterStateUpdating),
		},
		targetStates: []string{
			string(cluster.ClusterStateRunning),
			string(cluster.ClusterStateAbnormal),
		},
	})
	if err != nil {
		return diag.FromErr(fmt.Errorf("waiting for cluster (%s) change complete: %s", d.Id(), err))
	}

	if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
		d.SetId("")
		return diag.FromErr(errors.New(stateResp.AbnormalReason))
	}

	d.SetId(resp.ClusterID)
	log.Printf("[DEBUG] deploy succeeded, action id:%s cluster id:%s]", resp.ActionID, resp.ClusterID)

	if v, ok := d.GetOk("coordinator_node_configs"); ok && len(d.Get("coordinator_node_configs").(map[string]interface{})) > 0 {
		configMap := v.(map[string]interface{})
		configs := make(map[string]string, 0)
		for k, v := range configMap {
			configs[k] = v.(string)
		}
		UpsertClusterConfig(ctx, clusterAPI, &cluster.UpsertClusterConfigReq{
			ClusterID:  resp.ClusterID,
			ConfigType: cluster.CustomConfigTypeFE,
			Configs:    configs,
		})
	}

	if v, ok := d.GetOk("compute_node_configs"); ok && len(d.Get("compute_node_configs").(map[string]interface{})) > 0 {
		configMap := v.(map[string]interface{})
		configs := make(map[string]string, 0)
		for k, v := range configMap {
			configs[k] = v.(string)
		}
		UpsertClusterConfig(ctx, clusterAPI, &cluster.UpsertClusterConfigReq{
			ClusterID:  resp.ClusterID,
			ConfigType: cluster.CustomConfigTypeBE,
			Configs:    configs,
		})
	}

	if v, ok := d.GetOk("ldap_ssl_certs"); ok {

		arr := v.(*schema.Set).List()
		sslCerts := make([]string, 0)
		for _, v := range arr {
			value := v.(string)
			sslCerts = append(sslCerts, value)
		}

		if len(sslCerts) > 0 {
			warningDiag := UpsertClusterLdapSslCert(ctx, clusterAPI, d.Id(), sslCerts, false)
			if warningDiag != nil {
				return warningDiag
			}
		}
	}

	if v, ok := d.GetOk("ranger_certs_dir"); ok {
		rangerCertsDirPath := v.(string)
		warningDiag := UpsertClusterRangerCert(ctx, clusterAPI, d.Id(), rangerCertsDirPath, false)
		if warningDiag != nil {
			return warningDiag
		}
	}

	if d.Get("expected_cluster_state").(string) == string(cluster.ClusterStateSuspended) {
		errDiag := UpdateClusterState(ctx, clusterAPI, d.Get("id").(string), string(cluster.ClusterStateRunning), string(cluster.ClusterStateSuspended))
		if errDiag != nil {
			return errDiag
		}
	}

	if d.Get("idle_suspend_interval").(int) > 0 {
		enable := true
		clusterId := resp.ClusterID
		intervalTimeMills := uint64(d.Get("idle_suspend_interval").(int) * 60 * 1000)
		warningDiag := UpdateClusterIdleConfig(ctx, clusterAPI, clusterId, intervalTimeMills, enable)
		if warningDiag != nil {
			return warningDiag
		}
	}

	return resourceElasticClusterRead(ctx, d, m)
}

func resourceElasticClusterRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)

	clusterID := d.Id()
	clusterAPI := cluster.NewClustersAPI(c)
	log.Printf("[DEBUG] resourceElasticClusterRead cluster id:%s", clusterID)
	var diags diag.Diagnostics
	stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
		clusterAPI: clusterAPI,
		clusterID:  clusterID,
		timeout:    30 * time.Minute,
		pendingStates: []string{
			string(cluster.ClusterStateDeploying),
			string(cluster.ClusterStateScaling),
			string(cluster.ClusterStateResuming),
			string(cluster.ClusterStateSuspending),
			string(cluster.ClusterStateReleasing),
			string(cluster.ClusterStateUpdating),
		},
		targetStates: []string{
			string(cluster.ClusterStateRunning),
			string(cluster.ClusterStateSuspended),
			string(cluster.ClusterStateAbnormal),
			string(cluster.ClusterStateReleased),
		},
	})
	if err != nil {
		return diag.FromErr(fmt.Errorf("waiting for cluster (%s) change complete: %s", d.Id(), err))
	}

	if stateResp.ClusterState == string(cluster.ClusterStateReleased) {
		log.Printf("[WARN] Cluster (%s) not found, removing from state", d.Id())
		d.SetId("")
		return diags
	}

	log.Printf("[DEBUG] get cluster, cluster[%s]", clusterID)
	resp, err := clusterAPI.Get(ctx, &cluster.GetReq{ClusterID: clusterID})
	if err != nil {
		if !d.IsNewResource() && status.Code(err) == codes.NotFound {
			log.Printf("[WARN] Cluster (%s) not found, removing from state", d.Id())
			d.SetId("")
			return diags
		}
		return diag.FromErr(err)
	}

	coordinatorNodeConfigsResp, err := clusterAPI.GetCustomConfig(ctx, &cluster.ListCustomConfigReq{
		ClusterID:  clusterID,
		ConfigType: cluster.CustomConfigTypeFE,
	})
	if err != nil {
		log.Printf("[ERROR] query cluster coordinator node config failed, err:%+v", err)
		return diag.FromErr(err)
	}

	computeNodeConfigsResp, err := clusterAPI.GetCustomConfig(ctx, &cluster.ListCustomConfigReq{
		ClusterID:  clusterID,
		ConfigType: cluster.CustomConfigTypeBE,
	})
	if err != nil {
		log.Printf("[ERROR] query cluster comput node config failed, err:%+v", err)
		return diag.FromErr(err)
	}

	log.Printf("[DEBUG] get cluster, resp:%+v", resp.Cluster)
	d.Set("cluster_state", string(resp.Cluster.ClusterState))
	d.Set("expected_cluster_state", string(resp.Cluster.ClusterState))
	d.Set("cluster_name", resp.Cluster.ClusterName)
	d.Set("data_credential_id", resp.Cluster.DataCredID)
	d.Set("network_id", resp.Cluster.NetIfaceID)
	d.Set("deployment_credential_id", resp.Cluster.DeployCredID)
	d.Set("compute_node_size", resp.Cluster.BeModule.InstanceType)
	d.Set("compute_node_count", int(resp.Cluster.BeModule.Num))
	d.Set("coordinator_node_size", resp.Cluster.FeModule.InstanceType)
	d.Set("coordinator_node_count", int(resp.Cluster.FeModule.Num))
	d.Set("free_tier", resp.Cluster.FreeTier)
	d.Set("query_port", resp.Cluster.QueryPort)
	d.Set("idle_suspend_interval", resp.Cluster.IdleSuspendInterval)
	tags := make(map[string]string)
	for k, v := range resp.Cluster.Tags {
		if !InternalTagKeys[k] {
			tags[k] = v
		}
	}
	d.Set("resource_tags", tags)
	if len(resp.Cluster.LdapSslCerts) > 0 {
		d.Set("ldap_ssl_certs", resp.Cluster.LdapSslCerts)
	}
	if len(resp.Cluster.RangerCertsDirPath) > 0 {
		d.Set("ranger_certs_dir", resp.Cluster.RangerCertsDirPath)
	}

	if len(coordinatorNodeConfigsResp.Configs) > 0 {
		d.Set("coordinator_node_configs", coordinatorNodeConfigsResp.Configs)
	}

	if len(computeNodeConfigsResp.Configs) > 0 {
		d.Set("compute_node_configs", computeNodeConfigsResp.Configs)
	}

	feModule := resp.Cluster.FeModule
	feVolumeConfig := make(map[string]interface{}, 0)
	feVolumeConfig["vol_size"] = feModule.VmVolSizeGB
	feVolumeConfig["iops"] = feModule.Iops
	feVolumeConfig["throughput"] = feModule.Throughput
	d.Set("coordinator_node_volume_config", []interface{}{feVolumeConfig})

	d.Set("compute_node_is_instance_store", resp.Cluster.BeModule.IsInstanceStore)
	beModule := resp.Cluster.BeModule
	beVolumeConfig := make(map[string]interface{}, 0)
	beVolumeConfig["vol_number"] = beModule.VmVolNum
	beVolumeConfig["vol_size"] = beModule.VmVolSizeGB
	beVolumeConfig["iops"] = beModule.Iops
	beVolumeConfig["throughput"] = beModule.Throughput
	d.Set("compute_node_volume_config", []interface{}{beVolumeConfig})

	return diags
}

func resourceElasticClusterDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)

	clusterID := d.Id()
	clusterAPI := cluster.NewClustersAPI(c)
	log.Printf("[DEBUG] resourceElasticClusterDelete cluster id:%s", clusterID)
	var diags diag.Diagnostics

	_, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
		clusterAPI: clusterAPI,
		clusterID:  clusterID,
		timeout:    30 * time.Minute,
		pendingStates: []string{
			string(cluster.ClusterStateDeploying),
			string(cluster.ClusterStateScaling),
			string(cluster.ClusterStateResuming),
			string(cluster.ClusterStateSuspending),
			string(cluster.ClusterStateReleasing),
			string(cluster.ClusterStateUpdating),
		},
		targetStates: []string{
			string(cluster.ClusterStateRunning),
			string(cluster.ClusterStateSuspended),
			string(cluster.ClusterStateAbnormal),
			string(cluster.ClusterStateReleased),
		},
	})
	if err != nil {
		return diag.FromErr(fmt.Errorf("waiting for Cluster (%s) delete: %s", d.Id(), err))
	}

	log.Printf("[DEBUG] release cluster, cluster id:%s", clusterID)
	resp, err := clusterAPI.Release(ctx, &cluster.ReleaseReq{ClusterID: clusterID})
	if err != nil {
		return diag.FromErr(err)
	}

	log.Printf("[DEBUG] wait release cluster, cluster id:%s action id:%s", clusterID, resp.ActionID)
	stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
		clusterAPI: clusterAPI,
		actionID:   resp.ActionID,
		clusterID:  clusterID,
		timeout:    30 * time.Minute,
		pendingStates: []string{
			string(cluster.ClusterStateReleasing),
			string(cluster.ClusterStateRunning),
			string(cluster.ClusterStateSuspended),
			string(cluster.ClusterStateAbnormal),
			string(cluster.ClusterStateUpdating),
		},
		targetStates: []string{string(cluster.ClusterStateReleased), string(cluster.ClusterStateAbnormal)},
	})
	if err != nil {
		return diag.FromErr(fmt.Errorf("waiting for Cluster (%s) delete: %s", d.Id(), err))
	}

	if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
		d.SetId("")
		return diag.FromErr(fmt.Errorf("release cluster failed: %s, we have successfully released your cluster, but cloud resources may not be released. Please release cloud resources manually according to the email", stateResp.AbnormalReason))
	}

	// d.SetId("") is automatically called assuming delete returns no errors, but
	// it is added here for explicitness.
	d.SetId("")
	return diags
}

func elasticClusterNeedUnlock(d *schema.ResourceData) bool {
	return !d.IsNewResource() && d.Get("free_tier").(bool) &&
		(d.HasChange("coordinator_node_size") ||
			d.HasChange("coordinator_node_count") ||
			d.HasChange("compute_node_size") ||
			d.HasChange("compute_node_count"))
}

func IsInstanceStore(d *schema.ResourceData) bool {
	v := d.Get("compute_node_is_instance_store")
	return v.(bool)
}

func resourceElasticClusterUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	var immutableFields = []string{"csp", "region", "cluster_name", "default_admin_password", "data_credential_id", "deployment_credential_id", "network_id", "init_scripts", "query_port"}
	for _, f := range immutableFields {
		if d.HasChange(f) && !d.IsNewResource() {
			return diag.FromErr(fmt.Errorf("the `%s` field is not allowed to be modified", f))
		}
	}

	c := m.(*client.CelerdataClient)

	// Warning or errors can be collected in a slice type
	clusterID := d.Id()
	clusterAPI := cluster.NewClustersAPI(c)
	log.Printf("[DEBUG] resourceElasticClusterUpdate cluster id:%s", clusterID)
	stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
		clusterAPI: clusterAPI,
		clusterID:  clusterID,
		timeout:    30 * time.Minute,
		pendingStates: []string{
			string(cluster.ClusterStateDeploying),
			string(cluster.ClusterStateScaling),
			string(cluster.ClusterStateResuming),
			string(cluster.ClusterStateSuspending),
			string(cluster.ClusterStateReleasing),
			string(cluster.ClusterStateUpdating),
		},
		targetStates: []string{
			string(cluster.ClusterStateRunning),
			string(cluster.ClusterStateSuspended),
			string(cluster.ClusterStateAbnormal),
			string(cluster.ClusterStateReleased),
		},
	})
	if err != nil {
		return diag.FromErr(fmt.Errorf("waiting for cluster (%s) change complete: %s", d.Id(), err))
	}

	if stateResp.ClusterState == string(cluster.ClusterStateReleased) {
		log.Printf("[WARN] cluster (%s) not found", clusterID)
		d.SetId("")
		return diag.FromErr(fmt.Errorf("cluster (%s) not found", clusterID))
	}

	if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
		return diag.FromErr(errors.New(stateResp.AbnormalReason))
	}

	if d.HasChange("idle_suspend_interval") && !d.IsNewResource() {
		o, n := d.GetChange("idle_suspend_interval")

		v := n.(int)
		enable := n.(int) > 0
		if !enable {
			v = o.(int)
		}
		intervalTimeMills := uint64(v * 60 * 1000)
		errDiag := UpdateClusterIdleConfig(ctx, clusterAPI, clusterID, intervalTimeMills, enable)
		if errDiag != nil {
			return errDiag
		}
	}

	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics
	if needResume(d) {
		o, n := d.GetChange("expected_cluster_state")
		errDiag := UpdateClusterState(ctx, clusterAPI, d.Get("id").(string), o.(string), n.(string))
		if errDiag != nil {
			return errDiag
		}
	}

	if d.HasChange("ldap_ssl_certs") && !d.IsNewResource() {
		sslCerts := make([]string, 0)
		if v, ok := d.GetOk("ldap_ssl_certs"); ok {
			arr := v.(*schema.Set).List()
			for _, v := range arr {
				value := v.(string)
				sslCerts = append(sslCerts, value)
			}
		}
		warningDiag := UpsertClusterLdapSslCert(ctx, clusterAPI, d.Id(), sslCerts, true)
		if warningDiag != nil {
			return warningDiag
		}
	}

	if d.HasChange("ranger_certs_dir") && !d.IsNewResource() {
		rangerCertsDirPath := d.Get("ranger_certs_dir").(string)
		warningDiag := UpsertClusterRangerCert(ctx, clusterAPI, d.Id(), rangerCertsDirPath, true)
		if warningDiag != nil {
			return warningDiag
		}
	}

	if elasticClusterNeedUnlock(d) {
		err := clusterAPI.UnlockFreeTier(ctx, clusterID)
		if err != nil {
			return diag.FromErr(fmt.Errorf("cluster (%s) failed to unlock free tier: %s", d.Id(), err.Error()))
		}
	}

	if d.HasChange("coordinator_node_size") && !d.IsNewResource() {
		_, n := d.GetChange("coordinator_node_size")
		resp, err := clusterAPI.ScaleUp(ctx, &cluster.ScaleUpReq{
			RequestId:  uuid.NewString(),
			ClusterId:  clusterID,
			ModuleType: cluster.ClusterModuleTypeFE,
			VmCategory: n.(string),
		})
		if err != nil {
			return diag.FromErr(fmt.Errorf("cluster (%s) failed to scale up fe nodes: %s", d.Id(), err))
		}

		stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
			clusterAPI:    clusterAPI,
			actionID:      resp.ActionId,
			clusterID:     clusterID,
			timeout:       common.DeployOrScaleClusterTimeout,
			pendingStates: []string{string(cluster.ClusterStateScaling)},
			targetStates:  []string{string(cluster.ClusterStateRunning), string(cluster.ClusterStateAbnormal)},
		})
		if err != nil {
			return diag.FromErr(fmt.Errorf("waiting for cluster (%s) running %s", d.Id(), err))
		}

		if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
			return diag.FromErr(errors.New(stateResp.AbnormalReason))
		}
	}

	if d.HasChange("coordinator_node_count") && !d.IsNewResource() {
		o, n := d.GetChange("coordinator_node_count")
		var actionID string
		if n.(int) > o.(int) {
			resp, err := clusterAPI.ScaleOut(ctx, &cluster.ScaleOutReq{
				RequestId:  uuid.NewString(),
				ClusterId:  clusterID,
				ModuleType: cluster.ClusterModuleTypeFE,
				ExpectNum:  int32(n.(int)),
			})
			if err != nil {
				return diag.FromErr(fmt.Errorf("cluster (%s) failed to scale out fe nodes: %s", d.Id(), err))
			}

			actionID = resp.ActionId
		} else if n.(int) < o.(int) {
			resp, err := clusterAPI.ScaleIn(ctx, &cluster.ScaleInReq{
				RequestId:  uuid.NewString(),
				ClusterId:  clusterID,
				ModuleType: cluster.ClusterModuleTypeFE,
				ExpectNum:  int32(n.(int)),
			})
			if err != nil {
				return diag.FromErr(fmt.Errorf("cluster (%s) failed to scale in fe nodes: %s", d.Id(), err))
			}

			actionID = resp.ActionId
		}

		stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
			clusterAPI:    clusterAPI,
			actionID:      actionID,
			clusterID:     clusterID,
			timeout:       common.DeployOrScaleClusterTimeout,
			pendingStates: []string{string(cluster.ClusterStateScaling)},
			targetStates:  []string{string(cluster.ClusterStateRunning), string(cluster.ClusterStateAbnormal)},
		})
		if err != nil {
			return diag.FromErr(fmt.Errorf("waiting for cluster (%s) running: %s", d.Id(), err))
		}

		if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
			return diag.FromErr(errors.New(stateResp.AbnormalReason))
		}
	}

	if d.HasChange("coordinator_node_volume_config") {
		o, n := d.GetChange("coordinator_node_volume_config")
		oldVolumeConfig := o.([]interface{})[0].(map[string]interface{})
		newVolumeConfig := n.([]interface{})[0].(map[string]interface{})

		nodeType := cluster.ClusterModuleTypeFE
		req := &cluster.ModifyClusterVolumeReq{
			ClusterId: clusterID,
			Type:      nodeType,
		}

		if v, ok := newVolumeConfig["vol_size"]; ok && v != oldVolumeConfig["vol_size"] {
			req.VmVolSize = int64(v.(int))
		}
		if v, ok := newVolumeConfig["iops"]; ok && v != oldVolumeConfig["iops"] {
			req.Iops = int64(v.(int))
		}
		if v, ok := newVolumeConfig["throughput"]; ok && v != oldVolumeConfig["throughput"] {
			req.Throughput = int64(v.(int))
		}

		log.Printf("[DEBUG] modify cluster volume detail, req:%+v", req)
		resp, err := clusterAPI.ModifyClusterVolume(ctx, req)
		if err != nil {
			log.Printf("[ERROR] modify cluster volume detail failed, err:%+v", err)
			return diag.FromErr(err)
		}

		infraActionId := resp.ActionID
		if len(infraActionId) > 0 {
			infraActionResp, err := WaitClusterInfraActionStateChangeComplete(ctx, &waitStateReq{
				clusterAPI: clusterAPI,
				clusterID:  clusterID,
				actionID:   infraActionId,
				timeout:    30 * time.Minute,
				pendingStates: []string{
					string(cluster.ClusterInfraActionStatePending),
					string(cluster.ClusterInfraActionStateOngoing),
				},
				targetStates: []string{
					string(cluster.ClusterInfraActionStateSucceeded),
					string(cluster.ClusterInfraActionStateCompleted),
					string(cluster.ClusterInfraActionStateFailed),
				},
			})

			summary := fmt.Sprintf("Modify %s node volume detail of the cluster[%s] failed", nodeType, clusterID)
			if err != nil {
				return diag.Diagnostics{
					diag.Diagnostic{
						Severity: diag.Error,
						Summary:  summary,
						Detail:   err.Error(),
					},
				}
			}

			if infraActionResp.InfraActionState == string(cluster.ClusterInfraActionStateFailed) {
				return diag.Diagnostics{
					diag.Diagnostic{
						Severity: diag.Error,
						Summary:  summary,
						Detail:   infraActionResp.ErrMsg,
					},
				}
			}
		}
	}

	if d.HasChange("coordinator_node_configs") {
		configMap := d.Get("coordinator_node_configs").(map[string]interface{})
		configs := make(map[string]string, 0)
		for k, v := range configMap {
			configs[k] = v.(string)
		}
		UpsertClusterConfig(ctx, clusterAPI, &cluster.UpsertClusterConfigReq{
			ClusterID:  clusterID,
			ConfigType: cluster.CustomConfigTypeFE,
			Configs:    configs,
		})
	}

	if d.HasChange("compute_node_size") && !d.IsNewResource() {
		_, n := d.GetChange("compute_node_size")
		resp, err := clusterAPI.ScaleUp(ctx, &cluster.ScaleUpReq{
			RequestId:  uuid.NewString(),
			ClusterId:  clusterID,
			ModuleType: cluster.ClusterModuleTypeBE,
			VmCategory: n.(string),
		})
		if err != nil {
			return diag.FromErr(fmt.Errorf("cluster (%s) failed to scale up be nodes: %s", d.Id(), err))
		}

		stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
			clusterAPI:    clusterAPI,
			actionID:      resp.ActionId,
			clusterID:     clusterID,
			timeout:       common.DeployOrScaleClusterTimeout,
			pendingStates: []string{string(cluster.ClusterStateScaling)},
			targetStates:  []string{string(cluster.ClusterStateRunning), string(cluster.ClusterStateAbnormal)},
		})
		if err != nil {
			return diag.FromErr(fmt.Errorf("waiting for cluster (%s) running: %s", d.Id(), err))
		}

		if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
			return diag.FromErr(errors.New(stateResp.AbnormalReason))
		}
	}

	if d.HasChange("compute_node_count") && !d.IsNewResource() {
		o, n := d.GetChange("compute_node_count")
		var actionID string
		if n.(int) > o.(int) {
			resp, err := clusterAPI.ScaleOut(ctx, &cluster.ScaleOutReq{
				RequestId:  uuid.NewString(),
				ClusterId:  clusterID,
				ModuleType: cluster.ClusterModuleTypeBE,
				ExpectNum:  int32(n.(int)),
			})
			if err != nil {
				return diag.FromErr(fmt.Errorf("cluster (%s) failed to scale out be nodes: %s", d.Id(), err))
			}

			actionID = resp.ActionId
		} else if n.(int) < o.(int) {
			resp, err := clusterAPI.ScaleIn(ctx, &cluster.ScaleInReq{
				RequestId:  uuid.NewString(),
				ClusterId:  clusterID,
				ModuleType: cluster.ClusterModuleTypeBE,
				ExpectNum:  int32(n.(int)),
			})
			if err != nil {
				return diag.FromErr(fmt.Errorf("cluster (%s) failed to scale in be nodes: %s", d.Id(), err))
			}

			actionID = resp.ActionId
		}

		stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
			clusterAPI:    clusterAPI,
			actionID:      actionID,
			clusterID:     clusterID,
			timeout:       common.DeployOrScaleClusterTimeout,
			pendingStates: []string{string(cluster.ClusterStateScaling)},
			targetStates:  []string{string(cluster.ClusterStateRunning), string(cluster.ClusterStateAbnormal)},
		})
		if err != nil {
			return diag.FromErr(fmt.Errorf("waiting for cluster (%s) running: %s", d.Id(), err))
		}

		if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
			return diag.FromErr(errors.New(stateResp.AbnormalReason))
		}
	}

	if d.HasChange("compute_node_volume_config") {
		o, n := d.GetChange("compute_node_volume_config")
		oldVolumeConfig := o.([]interface{})[0].(map[string]interface{})
		newVolumeConfig := n.([]interface{})[0].(map[string]interface{})

		nodeType := cluster.ClusterModuleTypeBE
		req := &cluster.ModifyClusterVolumeReq{
			ClusterId: clusterID,
			Type:      nodeType,
		}

		if v, ok := newVolumeConfig["vol_size"]; ok && v != oldVolumeConfig["vol_size"] {
			req.VmVolSize = int64(v.(int))
		}
		if v, ok := newVolumeConfig["iops"]; ok && v != oldVolumeConfig["iops"] {
			req.Iops = int64(v.(int))
		}
		if v, ok := newVolumeConfig["throughput"]; ok && v != oldVolumeConfig["throughput"] {
			req.Throughput = int64(v.(int))
		}

		log.Printf("[DEBUG] modify cluster volume detail, req:%+v", req)
		resp, err := clusterAPI.ModifyClusterVolume(ctx, req)
		if err != nil {
			log.Printf("[ERROR] modify cluster volume detail failed, err:%+v", err)
			return diag.FromErr(err)
		}

		infraActionId := resp.ActionID
		if len(infraActionId) > 0 {
			infraActionResp, err := WaitClusterInfraActionStateChangeComplete(ctx, &waitStateReq{
				clusterAPI: clusterAPI,
				clusterID:  clusterID,
				actionID:   infraActionId,
				timeout:    30 * time.Minute,
				pendingStates: []string{
					string(cluster.ClusterInfraActionStatePending),
					string(cluster.ClusterInfraActionStateOngoing),
				},
				targetStates: []string{
					string(cluster.ClusterInfraActionStateSucceeded),
					string(cluster.ClusterInfraActionStateCompleted),
					string(cluster.ClusterInfraActionStateFailed),
				},
			})

			summary := fmt.Sprintf("Modify %s node volume detail of the cluster[%s] failed", nodeType, clusterID)
			if err != nil {
				return diag.Diagnostics{
					diag.Diagnostic{
						Severity: diag.Error,
						Summary:  summary,
						Detail:   err.Error(),
					},
				}
			}

			if infraActionResp.InfraActionState == string(cluster.ClusterInfraActionStateFailed) {
				return diag.Diagnostics{
					diag.Diagnostic{
						Severity: diag.Error,
						Summary:  summary,
						Detail:   infraActionResp.ErrMsg,
					},
				}
			}
		}
	}

	if d.HasChange("compute_node_configs") {
		configMap := d.Get("compute_node_configs").(map[string]interface{})
		configs := make(map[string]string, 0)
		for k, v := range configMap {
			configs[k] = v.(string)
		}
		UpsertClusterConfig(ctx, clusterAPI, &cluster.UpsertClusterConfigReq{
			ClusterID:  clusterID,
			ConfigType: cluster.CustomConfigTypeBE,
			Configs:    configs,
		})
	}

	if needSuspend(d) {
		o, n := d.GetChange("expected_cluster_state")
		errDiag := UpdateClusterState(ctx, clusterAPI, d.Get("id").(string), o.(string), n.(string))
		if errDiag != nil {
			return errDiag
		}
	}

	if d.HasChange("resource_tags") && !d.IsNewResource() {
		_, n := d.GetChange("resource_tags")

		nTags := n.(map[string]interface{})
		tags := make(map[string]string, len(nTags))
		for k, v := range nTags {
			tags[k] = v.(string)
		}
		err := clusterAPI.UpdateResourceTags(ctx, &cluster.UpdateResourceTagsReq{
			ClusterId: clusterID,
			Tags:      tags,
		})
		if err != nil {
			return diag.FromErr(fmt.Errorf("cluster (%s) failed to update resource tags: %s", d.Id(), err.Error()))
		}
	}

	return diags
}
