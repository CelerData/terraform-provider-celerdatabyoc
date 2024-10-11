package celerdatabyoc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"
	"terraform-provider-celerdatabyoc/common"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// V2 support multi-warehouse
func resourceElasticClusterV2() *schema.Resource {
	return &schema.Resource{
		ReadContext:   resourceElasticClusterV2Read,
		CreateContext: resourceElasticClusterV2Create,
		UpdateContext: resourceElasticClusterV2Update,
		DeleteContext: resourceElasticClusterV2Delete,
		Schema: map[string]*schema.Schema{
			"id": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"default_warehouse_id": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"csp": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"region": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"cluster_state": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"cluster_name": {
				Type:         schema.TypeString,
				ForceNew:     true,
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
			"warehouse": {
				Type:     schema.TypeSet,
				Required: true,
				MinItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringIsNotEmpty,
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
						"compute_node_ebs_disk_per_size": {
							Description: "Specifies the size of a single disk in GB. The default size for per disk is 100GB.",
							Type:        schema.TypeInt,
							Optional:    true,
							ValidateFunc: func(i interface{}, k string) (warnings []string, errors []error) {
								v, ok := i.(int)
								if !ok {
									errors = append(errors, fmt.Errorf("expected type of %s to be int", k))
									return warnings, errors
								}

								m := 16 * 1000
								if v > m {
									errors = append(errors, fmt.Errorf("%s`s value is invalid. The range of values is: [1,%d]", k, m))
								}

								return warnings, errors
							},
						},
						"compute_node_ebs_disk_number": {
							Description: "Specifies the number of disk. The default value is 2.",
							Type:        schema.TypeInt,
							Optional:    true,
							ValidateFunc: func(i interface{}, k string) (warnings []string, errors []error) {
								v, ok := i.(int)
								if !ok {
									errors = append(errors, fmt.Errorf("expected type of %s to be int", k))
									return warnings, errors
								}

								if v > 24 {
									errors = append(errors, fmt.Errorf("%s`s value is invalid. The range of values is: [1,24]", k))
								}

								return warnings, errors
							},
						},
						"auto_scaling_policy": {
							Type:     schema.TypeString,
							Optional: true,
							ValidateFunc: func(i interface{}, s string) ([]string, []error) {
								err := ValidateAutoScalingPolicyStr(i.(string))
								if err != nil {
									return nil, []error{err}
								}
								return nil, nil
							},
						},
					},
				},
			},
			"resource_tags": {
				Type:     schema.TypeMap,
				Optional: true,
				ForceNew: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"default_admin_password": {
				Type:             schema.TypeString,
				Required:         true,
				ForceNew:         true,
				Sensitive:        true,
				ValidateDiagFunc: common.ValidatePassword(),
			},
			"data_credential_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"deployment_credential_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"network_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
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
				ForceNew: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"script_path": {
							Type:     schema.TypeString,
							Required: true,
							ForceNew: true,
						},
						"logs_dir": {
							Type:     schema.TypeString,
							Required: true,
							ForceNew: true,
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
				ForceNew: true,
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
		},
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(common.DeployOrScaleClusterTimeout),
			Update: schema.DefaultTimeout(common.DeployOrScaleClusterTimeout),
		},
	}
}

func resourceElasticClusterV2Create(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	clusterAPI := cluster.NewClustersAPI(c)
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

	clusterConf.ClusterItems = append(clusterConf.ClusterItems, &cluster.ClusterItem{
		Type:          cluster.ClusterModuleTypeFE,
		Name:          "FE",
		Num:           uint32(d.Get("coordinator_node_count").(int)),
		StorageSizeGB: 50,
		InstanceType:  d.Get("coordinator_node_size").(string),
	})

	whInfos := d.Get("warehouse").(*schema.Set).List()
	if len(whInfos) > 1 {
		return diag.FromErr(fmt.Errorf("a maximum of one built-in warehouse is allowed to be created"))
	}

	var defaultWhMap map[string]interface{}
	normalWhMaps := make([]map[string]interface{}, 0)
	for _, wh := range whInfos {
		whMap := wh.(map[string]interface{})
		if whMap["name"] == "default_warehouse" {
			defaultWhMap = whMap
		} else {
			normalWhMaps = append(normalWhMaps, whMap)
		}
	}

	if defaultWhMap == nil {
		return diag.Errorf("`default_warehouse` is required")
	}

	clusterConf.ClusterItems = append(clusterConf.ClusterItems, &cluster.ClusterItem{
		Type:         cluster.ClusterModuleTypeWarehouse,
		Name:         defaultWhMap["name"].(string),
		Num:          uint32(defaultWhMap["compute_node_count"].(int)),
		InstanceType: defaultWhMap["compute_node_size"].(string),
		DiskInfo: &cluster.DiskInfo{
			Number:  uint32(defaultWhMap["compute_node_ebs_disk_number"].(int)),
			PerSize: uint64(defaultWhMap["compute_node_ebs_disk_per_size"].(int)),
		},
	})

	resp, err := clusterAPI.Deploy(ctx, &cluster.DeployReq{
		RequestId:   uuid.NewString(),
		ClusterConf: clusterConf,
	})
	if err != nil {
		return diag.FromErr(err)
	}
	log.Printf("[DEBUG] submit deploy succeeded, action id:%s cluster id:%s]", resp.ActionID, resp.ClusterID)

	clusterId := resp.ClusterID
	defaultWarehouseId := resp.DefaultWarehouseId
	d.SetId(clusterId)
	d.Set("default_warehouse_id", defaultWarehouseId)
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
	log.Printf("[DEBUG] deploy succeeded, action id:%s cluster id:%s]", resp.ActionID, resp.ClusterID)

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

	for _, v := range whInfos {
		whMap := v.(map[string]interface{})
		if v, ok := whMap["auto_scaling_policy"]; ok {
			var autoScalingConfig *cluster.WarehouseAutoScalingConfig
			json.Unmarshal([]byte(v.(string)), autoScalingConfig)
			req := &cluster.SaveWarehouseAutoScalingConfigReq{
				ClusterId:                  clusterId,
				WarehouseId:                defaultWarehouseId,
				WarehouseAutoScalingConfig: *autoScalingConfig,
				State:                      true,
			}
			_, err := clusterAPI.SaveWarehouseAutoScalingConfig(ctx, req)
			if err != nil {
				msg := fmt.Sprintf("Add warehouse auto-scaling configuration failed, errMsg:%s", err.Error())
				log.Printf("[ERROR] %s", msg)
				return diag.Diagnostics{
					diag.Diagnostic{
						Severity: diag.Warning,
						Summary:  err.Error(),
					},
				}
			}
		}
	}

	return diags
}

func resourceElasticClusterV2Read(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)

	clusterId := d.Id()
	clusterAPI := cluster.NewClustersAPI(c)
	log.Printf("[DEBUG] resourceElasticClusterV2Read cluster id:%s", clusterId)
	var diags diag.Diagnostics
	stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
		clusterAPI: clusterAPI,
		clusterID:  clusterId,
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

	if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
		d.SetId("")
		return diag.FromErr(errors.New(stateResp.AbnormalReason))
	}

	log.Printf("[DEBUG] get cluster, cluster[%s]", clusterId)
	resp, err := clusterAPI.Get(ctx, &cluster.GetReq{ClusterID: clusterId})
	if err != nil {
		if !d.IsNewResource() && status.Code(err) == codes.NotFound {
			log.Printf("[WARN] Cluster (%s) not found, removing from state", d.Id())
			d.SetId("")
			return diags
		}
		return diag.FromErr(err)
	}

	jsonBytes, err := json.Marshal(resp.Cluster)
	if err != nil {
		log.Printf("[Error] marshaling to JSON:%s %r%n [DEBUG] get cluster, resp:%+v", resp.Cluster)
	} else {
		log.Printf("[DEBUG] get cluster, resp:%s", string(jsonBytes))
	}

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
	if len(resp.Cluster.LdapSslCerts) > 0 {
		d.Set("ldap_ssl_certs", resp.Cluster.LdapSslCerts)
	}

	dwObj := make([]map[string]interface{}, 0)
	for _, v := range resp.Cluster.Warehouses {
		dwMapping := make(map[string]interface{}, 0)
		warehouseId := v.Id
		if v.IsDefaultWarehouse {
			d.Set("default_warehouse_id", warehouseId)
		}
		dwMapping["name"] = v.Name
		dwMapping["compute_node_size"] = v.Module.InstanceType
		dwMapping["compute_node_count"] = v.Module.Num
		if !v.Module.IsInstanceStore {
			dwMapping["compute_node_ebs_disk_number"] = v.Module.VmVolNum
			dwMapping["compute_node_ebs_disk_per_size"] = v.Module.VmVolSizeGB
		}

		autoScalingConfigResp, err := clusterAPI.GetWarehouseAutoScalingConfig(ctx, &cluster.GetWarehouseAutoScalingConfigReq{
			WarehouseId: warehouseId,
		})
		if err != nil {
			log.Printf("[ERROR] Query warehouse auto scaling config failed, warehouseId:%s", warehouseId)
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  fmt.Sprintf("Failed to get warehouse auto scaling config, warehouseId:[%s] ", warehouseId),
					Detail:   err.Error(),
				},
			}
		}

		policy := autoScalingConfigResp.Policy
		if policy != nil && policy.State {
			bytes, _ := json.Marshal(policy)
			dwMapping["auto_scaling_policy"] = string(bytes)
		}
		dwObj = append(dwObj, dwMapping)
	}
	d.Set("warehouse", dwObj)

	log.Printf("[DEBUG] get cluster, warehouse:%+v", dwObj)

	return diags
}

func resourceElasticClusterV2Delete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)

	clusterId := d.Id()
	clusterAPI := cluster.NewClustersAPI(c)
	log.Printf("[DEBUG] resourceElasticClusterV2Delete cluster id:%s", clusterId)
	var diags diag.Diagnostics

	_, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
		clusterAPI: clusterAPI,
		clusterID:  clusterId,
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

	log.Printf("[DEBUG] release cluster, cluster id:%s", clusterId)
	resp, err := clusterAPI.Release(ctx, &cluster.ReleaseReq{ClusterID: clusterId})
	if err != nil {
		return diag.FromErr(err)
	}

	log.Printf("[DEBUG] wait release cluster, cluster id:%s action id:%s", clusterId, resp.ActionID)
	stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
		clusterAPI: clusterAPI,
		actionID:   resp.ActionID,
		clusterID:  clusterId,
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
		return diag.FromErr(errors.New(stateResp.AbnormalReason))
	}

	// d.SetId("") is automatically called assuming delete returns no errors, but
	// it is added here for explicitness.
	d.SetId("")
	return diags
}

func elasticClusterV2NeedUnlock(d *schema.ResourceData) bool {
	result := !d.IsNewResource() && d.Get("free_tier").(bool) &&
		(d.HasChange("coordinator_node_size") ||
			d.HasChange("coordinator_node_count") ||
			d.HasChange("compute_node_size") ||
			d.HasChange("compute_node_count"))

	if !result && d.HasChange("warehouse") {
		o, n := d.GetChange("warehouse")
		oldWhInfo := o.(*schema.Set).List()[0].(map[string]interface{})
		newWhInfo := n.(*schema.Set).List()[0].(map[string]interface{})
		changed := (newWhInfo["compute_node_size"].(string) != oldWhInfo["compute_node_size"].(string) || newWhInfo["compute_node_count"].(int) != oldWhInfo["compute_node_count"].(int))
		result = result || changed
	}

	return result
}

func resourceElasticClusterV2Update(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {

	c := m.(*client.CelerdataClient)

	// Warning or errors can be collected in a slice type
	clusterId := d.Id()
	defaultWarehouseId := d.Get("default_warehouse_id").(string)

	clusterAPI := cluster.NewClustersAPI(c)
	log.Printf("[DEBUG] resourceElasticClusterV2Update cluster id:%s", clusterId)
	if d.HasChange("query_port") {
		return diag.FromErr(fmt.Errorf("the `query_port` field is not allowed to be modified"))
	}

	if d.HasChange("idle_suspend_interval") && !d.IsNewResource() {
		o, n := d.GetChange("idle_suspend_interval")

		v := n.(int)
		enable := n.(int) > 0
		if !enable {
			v = o.(int)
		}
		intervalTimeMills := uint64(v * 60 * 1000)
		errDiag := UpdateClusterIdleConfig(ctx, clusterAPI, clusterId, intervalTimeMills, enable)
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

	if elasticClusterV2NeedUnlock(d) {
		err := clusterAPI.UnlockFreeTier(ctx, clusterId)
		if err != nil {
			return diag.FromErr(fmt.Errorf("cluster (%s) failed to unlock free tier: %s", d.Id(), err.Error()))
		}
	}

	if d.HasChange("coordinator_node_size") && !d.IsNewResource() {
		_, n := d.GetChange("coordinator_node_size")
		resp, err := clusterAPI.ScaleUp(ctx, &cluster.ScaleUpReq{
			RequestId:  uuid.NewString(),
			ClusterId:  clusterId,
			ModuleType: cluster.ClusterModuleTypeFE,
			VmCategory: n.(string),
		})
		if err != nil {
			return diag.FromErr(fmt.Errorf("cluster (%s) failed to scale up fe nodes: %s", d.Id(), err))
		}

		stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
			clusterAPI:    clusterAPI,
			actionID:      resp.ActionId,
			clusterID:     clusterId,
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
				ClusterId:  clusterId,
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
				ClusterId:  clusterId,
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
			clusterID:     clusterId,
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

	if d.HasChange("warehouse") {
		warehouseId := d.Get("default_warehouse_id").(string)
		o, n := d.GetChange("warehouse")
		oldWhInfo := o.(*schema.Set).List()[0].(map[string]interface{})
		newWhInfo := n.(*schema.Set).List()[0].(map[string]interface{})
		if newWhInfo["compute_node_size"].(string) != oldWhInfo["compute_node_size"].(string) {
			resp, err := clusterAPI.ScaleWarehouseSize(ctx, &cluster.ScaleWarehouseSizeReq{
				WarehouseId: warehouseId,
				VmCate:      newWhInfo["compute_node_size"].(string),
			})

			if err != nil {
				return diag.FromErr(fmt.Errorf("built-in warehouse failed to scale size, clusterId:%s warehouseId:%s, errMsg:%s", d.Id(), warehouseId, err))
			}

			stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
				clusterAPI:    clusterAPI,
				actionID:      resp.ActionID,
				clusterID:     clusterId,
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

		if newWhInfo["compute_node_count"].(int) != oldWhInfo["compute_node_count"].(int) {
			resp, err := clusterAPI.ScaleWarehouseNum(ctx, &cluster.ScaleWarehouseNumReq{
				WarehouseId: warehouseId,
				VmNum:       int32(newWhInfo["compute_node_count"].(int)),
			})

			if err != nil {
				return diag.FromErr(fmt.Errorf("built-in warehouse failed to scale number, clusterId:%s warehouseId:%s, errMsg:%s", d.Id(), warehouseId, err))
			}

			stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
				clusterAPI:    clusterAPI,
				actionID:      resp.ActionID,
				clusterID:     clusterId,
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

		/*
			if !newWhInfo["compute_node_is_instance_store"].(bool) && (newWhInfo["compute_node_ebs_disk_number"].(int) != oldWhInfo["compute_node_ebs_disk_number"].(int)) {
				return diag.FromErr(errors.New("modifying the number of ebs disks is not supported"))
			}
		*/

		if v, ok := newWhInfo["auto_scaling_policy"]; ok {
			var autoScalingConfig *cluster.WarehouseAutoScalingConfig
			json.Unmarshal([]byte(v.(string)), autoScalingConfig)
			req := &cluster.SaveWarehouseAutoScalingConfigReq{
				ClusterId:                  clusterId,
				WarehouseId:                defaultWarehouseId,
				WarehouseAutoScalingConfig: *autoScalingConfig,
				State:                      true,
			}
			_, err := clusterAPI.SaveWarehouseAutoScalingConfig(ctx, req)
			if err != nil {
				msg := fmt.Sprintf("Update warehouse auto-scaling configuration failed, errMsg:%s", err.Error())
				log.Printf("[ERROR] %s", msg)
				return diag.FromErr(fmt.Errorf("%s", msg))
			}
		} else {
			err := clusterAPI.DeleteWarehouseAutoScalingConfig(ctx, &cluster.DeleteWarehouseAutoScalingConfigReq{
				WarehouseId: warehouseId,
			})
			if err != nil {
				return diag.Diagnostics{
					diag.Diagnostic{
						Severity: diag.Warning,
						Summary:  "Delete warehouse auto scaling config failed",
						Detail:   err.Error(),
					},
				}
			}
		}
	}

	if needSuspend(d) {
		o, n := d.GetChange("expected_cluster_state")
		errDiag := UpdateClusterState(ctx, clusterAPI, d.Get("id").(string), o.(string), n.(string))
		if errDiag != nil {
			return errDiag
		}
	}

	return diags
}
