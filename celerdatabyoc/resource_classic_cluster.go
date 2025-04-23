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
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/retry"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func resourceClassicCluster() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceClusterCreate,
		ReadContext:   resourceClusterRead,
		DeleteContext: resourceClusterDelete,
		UpdateContext: resourceClusterUpdate,
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
			"fe_instance_type": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},
			"fe_node_count": {
				Type:         schema.TypeInt,
				Optional:     true,
				Default:      1,
				ValidateFunc: validation.IntInSlice([]int{1, 3, 5}),
			},
			"fe_volume_config": {
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
							ValidateFunc: validation.IntAtLeast(1),
						},
						"throughput": {
							Type:         schema.TypeInt,
							Required:     true,
							ValidateFunc: validation.IntAtLeast(1),
						},
					},
				},
			},
			"be_instance_type": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},
			"be_node_count": {
				Type:         schema.TypeInt,
				Optional:     true,
				Default:      3,
				ValidateFunc: validation.IntAtLeast(1),
			},
			"be_volume_config": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"vol_number": {
							Description: "Specifies the number of disk. The default value is 2.",
							Type:        schema.TypeInt,
							Optional:    true,
							Default:     2,
							ValidateFunc: func(i interface{}, k string) (warnings []string, errors []error) {
								v, ok := i.(int)
								if !ok {
									errors = append(errors, fmt.Errorf("expected type of %s to be int", k))
									return warnings, errors
								}

								if v <= 0 {
									errors = append(errors, fmt.Errorf("%s`s value is invalid", k))
								} else if v > 24 {
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
							ValidateFunc: validation.IntAtLeast(1),
						},
						"throughput": {
							Type:         schema.TypeInt,
							Required:     true,
							ValidateFunc: validation.IntAtLeast(1),
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
				ForceNew:         true,
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
			"fe_configs": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"be_configs": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"ranger_certs_dir_path": {
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
		CustomizeDiff: classicCustomizeElDiff,
	}
}

func classicCustomizeElDiff(ctx context.Context, d *schema.ResourceDiff, m interface{}) error {
	c := m.(*client.CelerdataClient)
	clusterAPI := cluster.NewClustersAPI(c)
	networkAPI := network.NewNetworkAPI(c)

	csp := d.Get("csp").(string)
	region := d.Get("region").(string)
	isNewResource := d.Id() == ""

	feInstanceType := d.Get("fe_instance_type").(string)
	if d.HasChange("fe_instance_type") {
		_, n := d.GetChange("fe_instance_type")
		feInstanceType = n.(string)
	}
	feVmInfoResp, err := clusterAPI.GetVmInfo(ctx, &cluster.GetVmInfoReq{
		Csp:         csp,
		Region:      region,
		ProcessType: string(cluster.ClusterModuleTypeFE),
		VmCate:      feInstanceType,
	})
	if err != nil {
		log.Printf("[ERROR] query instance type failed, csp:%s region:%s instance type:%s err:%+v", csp, region, feInstanceType, err)
		return fmt.Errorf("query instance type failed, csp:%s region:%s instance type:%s errMsg:%s", csp, region, feInstanceType, err.Error())
	}
	if feVmInfoResp.VmInfo == nil {
		return fmt.Errorf("instance type not exists, csp:%s region:%s instance type:%s", csp, region, feInstanceType)
	}

	if d.HasChange("fe_volume_config") && !isNewResource {
		o, n := d.GetChange("fe_volume_config")

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
			return fmt.Errorf("fe `vol_size` does not support decrease, oldVolumeSize:%d, newVolumeSize:%d", oldVolumeSize, newVolumeSize)
		}
	}

	beInstanceType := d.Get("be_instance_type").(string)
	if d.HasChange("be_instance_type") {
		_, n := d.GetChange("be_instance_type")
		beInstanceType = n.(string)
	}
	beVmInfoResp, err := clusterAPI.GetVmInfo(ctx, &cluster.GetVmInfoReq{
		Csp:         csp,
		Region:      region,
		ProcessType: string(cluster.ClusterModuleTypeBE),
		VmCate:      beInstanceType,
	})
	if err != nil {
		log.Printf("[ERROR] query instance type failed, csp:%s region:%s instance type:%s err:%+v", csp, region, beInstanceType, err)
		return fmt.Errorf("query instance type failed, csp:%s region:%s instance type:%s errMsg:%s", csp, region, beInstanceType, err.Error())
	}
	if beVmInfoResp.VmInfo == nil {
		return fmt.Errorf("instance type not exists, csp:%s region:%s instance type:%s", csp, region, beInstanceType)
	}

	if d.HasChange("be_volume_config") && !isNewResource {

		oldVolumeNum, oldVolumeSize := 2, 100
		newVolumeNum, newVolumeSize := 2, 100

		o, n := d.GetChange("be_volume_config")

		oldVolumeConfig := cluster.DefaultBeVolumeMap()
		newVolumeConfig := cluster.DefaultBeVolumeMap()

		if len(o.([]interface{})) > 0 {
			oldVolumeConfig = o.([]interface{})[0].(map[string]interface{})
		}
		if len(n.([]interface{})) > 0 {
			newVolumeConfig = n.([]interface{})[0].(map[string]interface{})
		}

		oldVolumeNum, oldVolumeSize = oldVolumeConfig["vol_number"].(int), oldVolumeConfig["vol_size"].(int)
		newVolumeNum, newVolumeSize = newVolumeConfig["vol_number"].(int), newVolumeConfig["vol_size"].(int)

		if oldVolumeNum != newVolumeNum {
			return fmt.Errorf("the be `vol_number` field is not allowed to be modified")
		}

		if newVolumeSize < oldVolumeSize {
			return fmt.Errorf("be `vol_size` does not support decrease")
		}
	}

	if len(d.Get("network_id").(string)) > 0 {
		netResp, err := networkAPI.GetNetwork(ctx, d.Get("network_id").(string))
		if err != nil {
			return err
		}

		if netResp.Network.MultiAz {
			return errors.New("classic cluster does not support multi-az deployment")
		}
	}

	return nil
}

func resourceClusterCreate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	clusterAPI := cluster.NewClustersAPI(c)
	networkAPI := network.NewNetworkAPI(c)
	clusterName := d.Get("cluster_name").(string)

	clusterConf := &cluster.ClusterConf{
		ClusterName:        clusterName,
		Csp:                d.Get("csp").(string),
		Region:             d.Get("region").(string),
		ClusterType:        cluster.ClusterTypeClassic,
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
		return diag.FromErr(errors.New("classic cluster does not support multi-az deployment"))
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

	feItem := &cluster.ClusterItem{
		Type:         cluster.ClusterModuleTypeFE,
		Name:         "FE",
		Num:          uint32(d.Get("fe_node_count").(int)),
		InstanceType: d.Get("fe_instance_type").(string),
		DiskInfo: &cluster.DiskInfo{
			Number:  1,
			PerSize: 150,
		},
	}

	if v, ok := d.GetOk("fe_volume_config"); ok {
		volumeConfig := v.([]interface{})[0].(map[string]interface{})
		diskInfo := feItem.DiskInfo
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

	beItem := &cluster.ClusterItem{
		Type:         cluster.ClusterModuleTypeBE,
		Name:         "BE",
		Num:          uint32(d.Get("be_node_count").(int)),
		InstanceType: d.Get("be_instance_type").(string),
		DiskInfo: &cluster.DiskInfo{
			Number:  uint32(2),
			PerSize: uint64(100),
		},
	}

	if v, ok := d.GetOk("be_volume_config"); ok {
		volumeConfig := v.([]interface{})[0].(map[string]interface{})
		diskInfo := beItem.DiskInfo
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

	clusterConf.ClusterItems = append(clusterConf.ClusterItems, feItem, beItem)

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

	if v, ok := d.GetOk("fe_configs"); ok && len(d.Get("fe_configs").(map[string]interface{})) > 0 {
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

	if v, ok := d.GetOk("be_configs"); ok && len(d.Get("be_configs").(map[string]interface{})) > 0 {
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

	if v, ok := d.GetOk("ranger_certs_dir_path"); ok {
		rangerCertsDirPath := v.(string)
		UpsertClusterRangerCert(ctx, clusterAPI, d.Id(), rangerCertsDirPath, false)
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

	return resourceClusterRead(ctx, d, m)
}

type ClusterResp struct {
	ClusterName string
	ID          string
}

func resourceClusterRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)

	clusterID := d.Id()
	clusterAPI := cluster.NewClustersAPI(c)
	log.Printf("[DEBUG] resourceClusterRead cluster id:%s", clusterID)
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

	feConfigsResp, err := clusterAPI.GetCustomConfig(ctx, &cluster.ListCustomConfigReq{
		ClusterID:  clusterID,
		ConfigType: cluster.CustomConfigTypeFE,
	})
	if err != nil {
		log.Printf("[ERROR] query cluster fe config failed, err:%+v", err)
		return diag.FromErr(err)
	}

	beConfigsResp, err := clusterAPI.GetCustomConfig(ctx, &cluster.ListCustomConfigReq{
		ClusterID:  clusterID,
		ConfigType: cluster.CustomConfigTypeBE,
	})
	if err != nil {
		log.Printf("[ERROR] query cluster be config failed, err:%+v", err)
		return diag.FromErr(err)
	}

	log.Printf("[DEBUG] get cluster, resp:%+v", resp.Cluster)
	d.Set("cluster_state", resp.Cluster.ClusterState)
	d.Set("expected_cluster_state", resp.Cluster.ClusterState)
	d.Set("cluster_name", resp.Cluster.ClusterName)
	d.Set("data_credential_id", resp.Cluster.DataCredID)
	d.Set("network_id", resp.Cluster.NetIfaceID)
	d.Set("deployment_credential_id", resp.Cluster.DeployCredID)
	d.Set("be_instance_type", resp.Cluster.BeModule.InstanceType)
	d.Set("be_node_count", int(resp.Cluster.BeModule.Num))
	d.Set("fe_instance_type", resp.Cluster.FeModule.InstanceType)
	d.Set("fe_node_count", int(resp.Cluster.FeModule.Num))
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
		d.Set("ranger_certs_dir_path", resp.Cluster.RangerCertsDirPath)
	}

	if len(feConfigsResp.Configs) > 0 {
		d.Set("fe_configs", feConfigsResp.Configs)
	}

	if len(beConfigsResp.Configs) > 0 {
		d.Set("be_configs", beConfigsResp.Configs)
	}

	feModule := resp.Cluster.FeModule
	feVolumeConfig := make(map[string]interface{}, 0)
	feVolumeConfig["vol_size"] = feModule.VmVolSizeGB
	feVolumeConfig["iops"] = feModule.Iops
	feVolumeConfig["throughput"] = feModule.Throughput
	d.Set("fe_volume_config", []interface{}{feVolumeConfig})

	beModule := resp.Cluster.BeModule
	beVolumeConfig := make(map[string]interface{}, 0)
	beVolumeConfig["vol_number"] = beModule.VmVolNum
	beVolumeConfig["vol_size"] = beModule.VmVolSizeGB
	beVolumeConfig["iops"] = beModule.Iops
	beVolumeConfig["throughput"] = beModule.Throughput
	d.Set("be_volume_config", []interface{}{beVolumeConfig})

	return diags
}

func resourceClusterDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)

	clusterID := d.Id()
	clusterAPI := cluster.NewClustersAPI(c)
	log.Printf("[DEBUG] resourceClusterDelete cluster id:%s", clusterID)
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

func needUnlock(d *schema.ResourceData) bool {
	return !d.IsNewResource() && d.Get("free_tier").(bool) &&
		(d.HasChange("fe_instance_type") ||
			d.HasChange("fe_node_count") ||
			d.HasChange("be_instance_type") ||
			d.HasChange("be_node_count") ||
			d.HasChange("be_disk_number") ||
			d.HasChange("be_disk_per_size"))
}

func needResume(d *schema.ResourceData) bool {
	if !d.HasChange("expected_cluster_state") {
		return false
	}

	_, n := d.GetChange("expected_cluster_state")
	return n.(string) == string(cluster.ClusterStateRunning)
}

func needSuspend(d *schema.ResourceData) bool {
	if !d.HasChange("expected_cluster_state") {
		return false
	}

	_, n := d.GetChange("expected_cluster_state")
	return n.(string) == string(cluster.ClusterStateSuspended)
}

func resourceClusterUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
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
	log.Printf("[DEBUG] resourceClusterUpdate cluster id:%s", clusterID)
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

	if d.HasChange("ranger_certs_dir_path") && !d.IsNewResource() {
		rangerCertsDirPath := d.Get("ranger_certs_dir_path").(string)
		UpsertClusterRangerCert(ctx, clusterAPI, d.Id(), rangerCertsDirPath, true)
	}

	if needUnlock(d) {
		err := clusterAPI.UnlockFreeTier(ctx, clusterID)
		if err != nil {
			return diag.FromErr(fmt.Errorf("cluster (%s) failed to unlock free tier: %s", d.Id(), err.Error()))
		}
	}

	if d.HasChange("fe_instance_type") && !d.IsNewResource() {
		_, n := d.GetChange("fe_instance_type")
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

	if d.HasChange("fe_node_count") && !d.IsNewResource() {
		o, n := d.GetChange("fe_node_count")
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

	if d.HasChange("fe_volume_config") {
		o, n := d.GetChange("fe_volume_config")

		oldVolumeConfig := cluster.DefaultFeVolumeMap()
		newVolumeConfig := cluster.DefaultFeVolumeMap()

		if len(o.([]interface{})) > 0 {
			oldVolumeConfig = o.([]interface{})[0].(map[string]interface{})
		}
		if len(n.([]interface{})) > 0 {
			newVolumeConfig = n.([]interface{})[0].(map[string]interface{})
		}

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

	if d.HasChange("fe_configs") {
		configMap := d.Get("fe_configs").(map[string]interface{})
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

	if d.HasChange("be_instance_type") && !d.IsNewResource() {
		_, n := d.GetChange("be_instance_type")
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

	if d.HasChange("be_node_count") && !d.IsNewResource() {
		o, n := d.GetChange("be_node_count")
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

	if d.HasChange("be_volume_config") {
		o, n := d.GetChange("be_volume_config")

		oldVolumeConfig, newVolumeConfig := cluster.DefaultBeVolumeMap(), cluster.DefaultBeVolumeMap()

		if len(o.([]interface{})) > 0 {
			oldVolumeConfig = o.([]interface{})[0].(map[string]interface{})
		}
		if len(n.([]interface{})) > 0 {
			newVolumeConfig = n.([]interface{})[0].(map[string]interface{})
		}

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

	if d.HasChange("be_configs") {
		configMap := d.Get("be_configs").(map[string]interface{})
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

type waitStateReq struct {
	clusterAPI          cluster.IClusterAPI
	actionID, clusterID string
	timeout             time.Duration
	pendingStates       []string
	targetStates        []string
}

func WaitClusterStateChangeComplete(ctx context.Context, req *waitStateReq) (*cluster.GetStateResp, error) {
	stateConf := &retry.StateChangeConf{
		Pending:    req.pendingStates,
		Target:     req.targetStates,
		Refresh:    StatusClusterState(ctx, req.clusterAPI, req.actionID, req.clusterID),
		Timeout:    req.timeout,
		MinTimeout: 5 * time.Second,
	}

	outputRaw, err := stateConf.WaitForStateContext(ctx)
	if output, ok := outputRaw.(*cluster.GetStateResp); ok {
		time.Sleep(time.Second * 5)
		if output.ClusterState == string(cluster.ClusterStateAbnormal) {
			time.Sleep(time.Second * 10)
		}

		return output, err
	}

	return nil, err
}

func StatusClusterState(ctx context.Context, clusterAPI cluster.IClusterAPI, actionID, clusterID string) retry.StateRefreshFunc {
	return func() (interface{}, string, error) {
		log.Printf("[DEBUG] get state: action[%s] cluster[%s]", actionID, clusterID)
		resp, err := clusterAPI.GetState(ctx, &cluster.GetStateReq{
			ActionID:  actionID,
			ClusterID: clusterID,
		})
		if err != nil {
			return nil, "", err
		}

		log.Printf("[DEBUG] The current state of the cluster is : %s", resp.ClusterState)
		return resp, resp.ClusterState, nil
	}
}

func WaitClusterInfraActionStateChangeComplete(ctx context.Context, req *waitStateReq) (*cluster.GetClusterInfraActionStateResp, error) {
	stateConf := &retry.StateChangeConf{
		Pending:    req.pendingStates,
		Target:     req.targetStates,
		Refresh:    ClusterInfraActionState(ctx, req.clusterAPI, req.clusterID, req.actionID),
		Timeout:    req.timeout,
		MinTimeout: 5 * time.Second,
	}

	outputRaw, err := stateConf.WaitForStateContext(ctx)
	if output, ok := outputRaw.(*cluster.GetClusterInfraActionStateResp); ok {
		return output, err
	}

	return nil, err
}

func ClusterInfraActionState(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterId, actionId string) retry.StateRefreshFunc {
	return func() (interface{}, string, error) {
		log.Printf("[DEBUG] get cluster infra action state, clusterId[%s] actionId[%s]", clusterId, actionId)
		resp, err := clusterAPI.GetClusterInfraActionState(ctx, &cluster.GetClusterInfraActionStateReq{
			ClusterId: clusterId,
			ActionId:  actionId,
		})
		if err != nil {
			return nil, "", err
		}

		log.Printf("[DEBUG] get cluster infra action state, resp:%+v", resp)
		return resp, resp.InfraActionState, nil
	}
}

func UpdateClusterState(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterID string, currentState string, configuredState string) diag.Diagnostics {
	if currentState == configuredState {
		return nil
	}

	if configuredState == string(cluster.ClusterStateSuspended) {
		if err := SuspendWithContext(ctx, clusterAPI, clusterID); err != nil {
			return err
		}
	}

	if configuredState == string(cluster.ClusterStateRunning) {
		if err := ResumeWithContext(ctx, clusterAPI, clusterID); err != nil {
			return err
		}
	}

	return nil
}

func SuspendWithContext(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterID string) diag.Diagnostics {
	log.Printf("[DEBUG] suspend cluster: %s", clusterID)
	summary := "Suspend cluster"
	resp, err := clusterAPI.Suspend(ctx, &cluster.SuspendReq{ClusterID: clusterID})
	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  summary,
				Detail:   err.Error(),
			},
		}
	}

	log.Printf("[DEBUG] suspend succeeded, action id:%s cluster id:%s]", resp.ActionID, clusterID)
	stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
		clusterAPI:    clusterAPI,
		actionID:      resp.ActionID,
		clusterID:     clusterID,
		timeout:       20 * time.Minute,
		pendingStates: []string{string(cluster.ClusterStateSuspending)},
		targetStates:  []string{string(cluster.ClusterStateSuspended), string(cluster.ClusterStateAbnormal)},
	})
	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  summary,
				Detail:   fmt.Sprintf("waiting for cluster (%s) suspend faild: %s", clusterID, err.Error()),
			},
		}
	}

	if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  summary,
				Detail:   fmt.Sprintf("waiting for cluster (%s) suspend faild: %s", clusterID, stateResp.AbnormalReason),
			},
		}
	}

	return nil
}
func ResumeWithContext(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterID string) diag.Diagnostics {
	log.Printf("[DEBUG] resume cluster: %s", clusterID)
	summary := "resume cluster"
	resp, err := clusterAPI.Resume(ctx, &cluster.ResumeReq{ClusterID: clusterID})
	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  summary,
				Detail:   err.Error(),
			},
		}
	}

	log.Printf("[DEBUG] resume succeeded, action id:%s cluster id:%s]", resp.ActionID, clusterID)
	stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
		clusterAPI:    clusterAPI,
		actionID:      resp.ActionID,
		clusterID:     clusterID,
		timeout:       20 * time.Minute,
		pendingStates: []string{string(cluster.ClusterStateResuming)},
		targetStates:  []string{string(cluster.ClusterStateRunning), string(cluster.ClusterStateAbnormal)},
	})
	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  summary,
				Detail:   fmt.Sprintf("waiting for cluster (%s) resume failed: %s", clusterID, err.Error()),
			},
		}
	}

	if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  summary,
				Detail:   fmt.Sprintf("waiting for cluster (%s) resume failed: %s", clusterID, stateResp.AbnormalReason),
			},
		}
	}

	return nil
}

func UpdateClusterIdleConfig(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterId string, intervalTimeMills uint64, enable bool) diag.Diagnostics {
	err := clusterAPI.UpsertClusterIdleConfig(ctx, &cluster.UpsertClusterIdleConfigReq{
		ClusterId:  clusterId,
		IntervalMs: intervalTimeMills,
		Enable:     enable,
	})
	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  "Failed to set idle suspend interval, please retry again!",
				Detail:   err.Error(),
			},
		}
	}
	return nil
}

func CheckS3Path(path string) bool {
	re := regexp.MustCompile(`s3://([^/]+)/*(.*)`)
	match := re.FindStringSubmatch(path)
	return len(match) > 2 && len(match[1]) > 0 && len(match[2]) > 0
}

func UpsertClusterLdapSslCert(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterID string, sslCerts []string, removeOld bool) diag.Diagnostics {

	var err diag.Diagnostics
	if removeOld {
		err = removeClusterLdapSSLCert(ctx, clusterAPI, clusterID)
		if err != nil {
			return err
		}
	}

	if len(sslCerts) > 0 {
		err = configClusterLdapSSLCert(ctx, clusterAPI, clusterID, sslCerts)
	}
	return err
}

func removeClusterLdapSSLCert(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterId string) diag.Diagnostics {

	summary := "Failed to remove old ldap ssl certs."
	resp, err := clusterAPI.UpsertClusterLdapSSLCert(ctx, &cluster.UpsertLDAPSSLCertsReq{
		ClusterId: clusterId,
	})

	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  summary,
				Detail:   err.Error(),
			},
		}
	}

	if len(resp.InfraActionId) > 0 {
		infraActionResp, err := WaitClusterInfraActionStateChangeComplete(ctx, &waitStateReq{
			clusterAPI: clusterAPI,
			clusterID:  clusterId,
			actionID:   resp.InfraActionId,
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

		if err != nil {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  summary,
					Detail:   err.Error(),
				},
			}
		}

		if infraActionResp.InfraActionState == string(cluster.ClusterInfraActionStateFailed) {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  summary,
					Detail:   infraActionResp.ErrMsg,
				},
			}
		}
	}
	return nil
}

func configClusterLdapSSLCert(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterId string, sslCerts []string) diag.Diagnostics {

	summary := "Failed to config ldap ssl certs."
	resp, err := clusterAPI.UpsertClusterLdapSSLCert(ctx, &cluster.UpsertLDAPSSLCertsReq{
		ClusterId: clusterId,
		S3Objects: sslCerts,
	})

	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  summary,
				Detail:   err.Error(),
			},
		}
	}

	infraActionResp, err := WaitClusterInfraActionStateChangeComplete(ctx, &waitStateReq{
		clusterAPI: clusterAPI,
		clusterID:  clusterId,
		actionID:   resp.InfraActionId,
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

	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  summary,
				Detail:   err.Error(),
			},
		}
	}

	if infraActionResp.InfraActionState == string(cluster.ClusterInfraActionStateFailed) {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  summary,
				Detail:   infraActionResp.ErrMsg,
			},
		}
	}

	return nil
}

func UpsertClusterConfig(ctx context.Context, clusterAPI cluster.IClusterAPI, req *cluster.UpsertClusterConfigReq) diag.Diagnostics {

	log.Printf("[DEBUG] UpsertClusterConfig, req:%v", req)

	var err diag.Diagnostics
	if len(req.Configs) == 0 {
		err = removeClusterConfig(ctx, clusterAPI, &cluster.RemoveClusterConfigReq{
			ClusterID:   req.ClusterID,
			ConfigType:  req.ConfigType,
			WarehouseID: req.WarehouseID,
		})
		if err != nil {
			return err
		}
	}

	if len(req.Configs) > 0 {
		err = configClusterConfig(ctx, clusterAPI, req)
	}

	log.Printf("[DEBUG] UpsertClusterConfig, err:%v", err)

	return err
}

func removeClusterConfig(ctx context.Context, clusterAPI cluster.IClusterAPI, req *cluster.RemoveClusterConfigReq) diag.Diagnostics {

	summary := "Failed to remove cluster configs."
	resp, err := clusterAPI.RemoveClusterConfig(ctx, req)
	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  summary,
				Detail:   err.Error(),
			},
		}
	}

	if len(resp.InfraActionId) > 0 {
		infraActionResp, err := WaitClusterInfraActionStateChangeComplete(ctx, &waitStateReq{
			clusterAPI: clusterAPI,
			clusterID:  req.ClusterID,
			actionID:   resp.InfraActionId,
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

		if err != nil {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  summary,
					Detail:   err.Error(),
				},
			}
		}

		if infraActionResp.InfraActionState == string(cluster.ClusterInfraActionStateFailed) {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  summary,
					Detail:   infraActionResp.ErrMsg,
				},
			}
		}
	}
	return nil
}

func configClusterConfig(ctx context.Context, clusterAPI cluster.IClusterAPI, req *cluster.UpsertClusterConfigReq) diag.Diagnostics {

	summary := "Failed to config cluster configs."
	resp, err := clusterAPI.UpsertClusterConfig(ctx, req)

	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  summary,
				Detail:   err.Error(),
			},
		}
	}

	log.Printf("[DEBUG] configClusterConfig, req:%v resp:%+v", req, resp)

	infraActionResp, err := WaitClusterInfraActionStateChangeComplete(ctx, &waitStateReq{
		clusterAPI: clusterAPI,
		clusterID:  req.ClusterID,
		actionID:   resp.InfraActionId,
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

	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  summary,
				Detail:   err.Error(),
			},
		}
	}

	if infraActionResp.InfraActionState == string(cluster.ClusterInfraActionStateFailed) {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  summary,
				Detail:   infraActionResp.ErrMsg,
			},
		}
	}

	return nil
}

func UpsertClusterRangerCert(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterID string, rangerCertsDir string, removeOld bool) diag.Diagnostics {

	var err diag.Diagnostics
	if removeOld {
		err = removeClusterRangerCert(ctx, clusterAPI, clusterID)
		if err != nil {
			return err
		}
	}

	if len(rangerCertsDir) > 0 {
		err = configClusterRangerCert(ctx, clusterAPI, clusterID, rangerCertsDir)
	}
	return err
}

func removeClusterRangerCert(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterId string) diag.Diagnostics {

	summary := "Failed to remove old ranger ssl certs."
	resp, err := clusterAPI.RemoveClusterRangerCert(ctx, &cluster.RemoveRangerCertsReq{
		ClusterId: clusterId,
	})

	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  summary,
				Detail:   err.Error(),
			},
		}
	}

	if len(resp.InfraActionId) > 0 {
		infraActionResp, err := WaitClusterInfraActionStateChangeComplete(ctx, &waitStateReq{
			clusterAPI: clusterAPI,
			clusterID:  clusterId,
			actionID:   resp.InfraActionId,
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

		if err != nil {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  summary,
					Detail:   err.Error(),
				},
			}
		}

		if infraActionResp.InfraActionState == string(cluster.ClusterInfraActionStateFailed) {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  summary,
					Detail:   infraActionResp.ErrMsg,
				},
			}
		}
	}
	return nil
}

func configClusterRangerCert(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterId string, rangerCertsDir string) diag.Diagnostics {

	summary := "Failed to config ranger ssl certs."
	resp, err := clusterAPI.UpsertClusterRangerCert(ctx, &cluster.UpsertRangerCertsReq{
		ClusterId: clusterId,
		DirPath:   rangerCertsDir,
	})

	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  summary,
				Detail:   err.Error(),
			},
		}
	}

	infraActionResp, err := WaitClusterInfraActionStateChangeComplete(ctx, &waitStateReq{
		clusterAPI: clusterAPI,
		clusterID:  clusterId,
		actionID:   resp.InfraActionId,
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

	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  summary,
				Detail:   err.Error(),
			},
		}
	}

	if infraActionResp.InfraActionState == string(cluster.ClusterInfraActionStateFailed) {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  summary,
				Detail:   infraActionResp.ErrMsg,
			},
		}
	}

	return nil
}
