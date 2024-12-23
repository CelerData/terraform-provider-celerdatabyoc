package celerdatabyoc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
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

const (
	DEFAULT_WAREHOUSE_NAME = "default_warehouse"
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
								if v < 1 || v > m {
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

								if v < 1 || v > 24 {
									errors = append(errors, fmt.Errorf("%s`s value is invalid. The range of values is: [1,24]", k))
								}

								return warnings, errors
							},
						},
						"idle_suspend_interval": {
							Type:        schema.TypeInt,
							Description: "Specifies the amount of time (in minutes) during which a warehouse can stay idle. After the specified time period elapses, the warehouse will be automatically suspended.",
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
						"expected_state": {
							Type:     schema.TypeString,
							Optional: true,
							//Default:      string(cluster.ClusterStateRunning),
							ValidateFunc: validation.StringInSlice([]string{string(cluster.ClusterStateSuspended), string(cluster.ClusterStateRunning)}, false),
						},
					},
				},
			},
			"warehouse_external_info": {
				Type:     schema.TypeMap,
				Computed: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
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
		},
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(common.DeployOrScaleClusterTimeout),
			Update: schema.DefaultTimeout(common.DeployOrScaleClusterTimeout),
		},
		CustomizeDiff: customizeEl2Diff,
	}
}

func customizeEl2Diff(ctx context.Context, d *schema.ResourceDiff, m interface{}) error {
	c := m.(*client.CelerdataClient)
	clusterAPI := cluster.NewClustersAPI(c)

	clusterId := d.Id()
	csp := d.Get("csp").(string)
	region := d.Get("region").(string)

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
				return fmt.Errorf("the vm instance architecture can not be changed, csp:%s region:%s oldVmCate:%s  newVmCate:%s", csp, region, o.(string), n.(string))
			}
		}
	}

	if d.HasChange("warehouse") {

		_, n := d.GetChange("warehouse")
		// 1. pre check, must has default warehouse and warehosue name must be unique
		hasDefaultWh := false
		countMap := make(map[string]int, 0)
		for _, item := range n.(*schema.Set).List() {
			m := item.(map[string]interface{})
			whName := strings.TrimSpace(m["name"].(string))
			if whName == DEFAULT_WAREHOUSE_NAME {
				hasDefaultWh = true
				if v, ok := m["idle_suspend_interval"]; ok {
					if v.(int) != 0 {
						return fmt.Errorf("the warehouse with name '%s' does not support the `idle_suspend_interval` attribute", DEFAULT_WAREHOUSE_NAME)
					}
				}
				if v, ok := m["expected_state"]; ok && len(v.(string)) > 0 {
					return fmt.Errorf("the warehouse with name '%s' does not support change the `expected_state` attribute", DEFAULT_WAREHOUSE_NAME)
				}
			}
			if v, ok := countMap[whName]; ok {
				v++
				countMap[whName] = v
			} else {
				countMap[whName] = 1
			}
		}

		if !hasDefaultWh {
			return fmt.Errorf("warehouse with name '%s' is required", DEFAULT_WAREHOUSE_NAME)
		}

		for k, v := range countMap {
			if v > 1 {
				return fmt.Errorf("only one warehouse with name '%s' is allowed", k)
			}
		}

		// 2. check vm arch
		whVmInfoMap := make(map[string]*cluster.VMInfo)
		for _, item := range n.(*schema.Set).List() {
			m := item.(map[string]interface{})
			whName := strings.TrimSpace(m["name"].(string))
			vmCateName := m["compute_node_size"].(string)
			vmCateInfoResp, err := clusterAPI.GetVmInfo(ctx, &cluster.GetVmInfoReq{
				Csp:         csp,
				Region:      region,
				ProcessType: string(cluster.ClusterModuleTypeBE),
				VmCate:      vmCateName,
			})
			if err != nil {
				log.Printf("[ERROR] query vm info failed, csp:%s region:%s vmCate:%s err:%+v", csp, region, vmCateName, err)
				return fmt.Errorf("query vm info failed, csp:%s region:%s vmCate:%s errMsg:%s", csp, region, vmCateName, err.Error())
			}
			if vmCateInfoResp.VmInfo == nil {
				return fmt.Errorf("vm info not exists, csp:%s region:%s vmCate:%s", csp, region, vmCateName)
			}
			if vmCateInfoResp.VmInfo.Arch != feArch {
				return fmt.Errorf("the vm instance`s architecture of the warehouse[%s] must be the same as the coordinator node, expect:%s but found:%s", whName, feArch, vmCateInfoResp.VmInfo.Arch)
			}

			if vmCateInfoResp.VmInfo.IsInstanceStore {
				attrs := make([]string, 0)
				if v, ok := m["compute_node_ebs_disk_per_size"]; ok && v.(int) > 0 {
					attrs = append(attrs, "compute_node_ebs_disk_per_size")
				}
				if v, ok := m["compute_node_ebs_disk_number"]; ok && v.(int) > 0 {
					attrs = append(attrs, "compute_node_ebs_disk_number")
				}
				if len(attrs) > 0 {
					return fmt.Errorf("the vm instance type[%s] of the warehouse[%s] does not support specifying the number or size of disks, field: %+v is not supported", vmCateName, whName, strings.Join(attrs, ","))
				}
			} else {
				attrs := make([]string, 0)
				if v, ok := m["compute_node_ebs_disk_per_size"]; !ok || v.(int) <= 0 {
					attrs = append(attrs, "compute_node_ebs_disk_per_size")
				}
				if v, ok := m["compute_node_ebs_disk_number"]; !ok || v.(int) <= 0 {
					attrs = append(attrs, "compute_node_ebs_disk_number")
				}
				if len(attrs) > 0 {
					return fmt.Errorf("the vm instance type[%s] of the warehouse[%s] needs specifying the number and size of disks, field: %+v is needed", vmCateName, whName, strings.Join(attrs, ","))
				}
			}

			whVmInfoMap[whName] = vmCateInfoResp.VmInfo
		}

		if len(clusterId) > 0 {
			// 3. check is instance store
			whExternalInfoMap := d.Get("warehouse_external_info").(map[string]interface{})
			for whName, whExInfo := range whExternalInfoMap {
				if v, ok := whVmInfoMap[whName]; ok {
					whExternalInfo := &cluster.WarehouseExternalInfo{}
					json.Unmarshal([]byte(whExInfo.(string)), whExternalInfo)

					expectStr := "local disk vm instance type"
					if !whExternalInfo.IsInstanceStore {
						expectStr = "nonlocal disk vm instance type"
					}
					if whExternalInfo.IsInstanceStore != v.IsInstanceStore {
						return fmt.Errorf("the disk type of the warehouse[%s] must be the same as the previous disk type, expect:%s", whName, expectStr)
					}
				}
			}
		}
	}
	return nil
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
		StorageSizeGB: 100,
		InstanceType:  d.Get("coordinator_node_size").(string),
	})

	whInfos := d.Get("warehouse").(*schema.Set).List()
	var defaultWhMap map[string]interface{}
	normalWhMaps := make([]map[string]interface{}, 0)
	for _, wh := range whInfos {
		whMap := wh.(map[string]interface{})
		whMap["name"] = strings.TrimSpace(whMap["name"].(string))
		if whMap["name"] == DEFAULT_WAREHOUSE_NAME {
			defaultWhMap = whMap
		} else {
			normalWhMaps = append(normalWhMaps, whMap)
		}
	}

	if defaultWhMap == nil {
		return diag.Errorf("`%s` is required", DEFAULT_WAREHOUSE_NAME)
	}

	diskNumber := 0
	perDiskSize := 0
	if v, ok := defaultWhMap["compute_node_ebs_disk_number"]; ok && v.(int) > 0 {
		diskNumber = v.(int)
	}
	if v, ok := defaultWhMap["compute_node_ebs_disk_per_size"]; ok && v.(int) > 0 {
		perDiskSize = v.(int)
	}

	clusterConf.ClusterItems = append(clusterConf.ClusterItems, &cluster.ClusterItem{
		Type:         cluster.ClusterModuleTypeWarehouse,
		Name:         defaultWhMap["name"].(string),
		Num:          uint32(defaultWhMap["compute_node_count"].(int)),
		InstanceType: defaultWhMap["compute_node_size"].(string),
		DiskInfo: &cluster.DiskInfo{
			Number:  uint32(diskNumber),
			PerSize: uint64(perDiskSize),
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
		diags = append(diags, diag.Diagnostic{
			Severity: diag.Warning,
			Summary:  "Operation state not complete",
			Detail:   fmt.Sprintf("waiting for cluster (%s) change complete failed errMsg: %s", d.Id(), err.Error()),
		})
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

	if d.Get("idle_suspend_interval").(int) > 0 {
		enable := true
		clusterId := resp.ClusterID
		intervalTimeMills := uint64(d.Get("idle_suspend_interval").(int) * 60 * 1000)
		warningDiag := UpdateClusterIdleConfig(ctx, clusterAPI, clusterId, intervalTimeMills, enable)
		if warningDiag != nil {
			return warningDiag
		}
	}

	policyJson := defaultWhMap["auto_scaling_policy"].(string)
	if len(policyJson) > 0 {
		err := setWarehouseAutoScalingPolicy(ctx, clusterAPI, clusterId, defaultWarehouseId, policyJson)
		if err != nil {
			msg := fmt.Sprintf("Add warehouse auto-scaling configuration failed, errMsg:%s", err.Error())
			log.Printf("[ERROR] %s", msg)
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  fmt.Sprintf("Config warehouse[%s] auto-scaling configuration failed", DEFAULT_WAREHOUSE_NAME),
					Detail:   msg,
				},
			}
		}
	}

	// create normal warehouses
	for _, v := range normalWhMaps {
		errDiag := createWarehouse(ctx, clusterAPI, clusterId, v)
		if errDiag != nil {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  "Create warehouse",
					Detail:   errDiag[0].Detail,
				},
			}
		}
	}

	if d.Get("expected_cluster_state").(string) == string(cluster.ClusterStateSuspended) {
		warningDiag := UpdateClusterState(ctx, clusterAPI, d.Get("id").(string), string(cluster.ClusterStateRunning), string(cluster.ClusterStateSuspended))
		if warningDiag != nil {
			return warningDiag
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
		log.Printf("[Error] marshaling to JSON:%s [DEBUG] get cluster, resp:%+v", err.Error(), resp.Cluster)
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

	warehouses := make([]interface{}, 0)
	warehouseExternalInfo := make(map[string]interface{}, 0)

	for _, v := range resp.Cluster.Warehouses {
		if v.Deleted {
			continue
		}
		warehouseId := v.Id
		warehouseName := v.Name
		isDefaultWarehouse := v.IsDefaultWarehouse

		whMap := make(map[string]interface{}, 0)
		whMap["name"] = warehouseName
		whMap["compute_node_size"] = v.Module.InstanceType
		whMap["compute_node_count"] = v.Module.Num
		if !isDefaultWarehouse {
			whMap["expected_state"] = v.State
		}
		if !v.Module.IsInstanceStore {
			whMap["compute_node_ebs_disk_number"] = v.Module.VmVolNum
			whMap["compute_node_ebs_disk_per_size"] = v.Module.VmVolSizeGB
		}

		idleConfigResp, err := clusterAPI.GetWarehouseIdleConfig(ctx, &cluster.GetWarehouseIdleConfigReq{
			WarehouseId: warehouseId,
		})
		if err != nil {
			log.Printf("[ERROR] Query warehouse idle suspend config failed, warehouseId:%s", warehouseId)
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  fmt.Sprintf("Failed to get warehouse idle suspend config, warehouseId:[%s] ", warehouseId),
					Detail:   err.Error(),
				},
			}
		}
		idleConfig := idleConfigResp.Config
		if idleConfig != nil && idleConfig.State {
			whMap["idle_suspend_interval"] = idleConfig.IntervalMs / 1000 / 60
		} else {
			whMap["idle_suspend_interval"] = 0
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
			whMap["auto_scaling_policy"] = string(bytes)
		}

		warehouses = append(warehouses, whMap)

		whInfo := &cluster.WarehouseExternalInfo{
			Id:                 warehouseId,
			IsInstanceStore:    v.Module.IsInstanceStore,
			IsDefaultWarehouse: isDefaultWarehouse,
		}
		whInfoBytes, _ := json.Marshal(whInfo)
		warehouseExternalInfo[warehouseName] = string(whInfoBytes)
	}
	d.Set("warehouse", warehouses)
	d.Set("warehouse_external_info", warehouseExternalInfo)

	log.Printf("[DEBUG] get cluster, warehouses:%+v", warehouses)

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
		(d.HasChange("coordinator_node_size") || d.HasChange("coordinator_node_count"))

	if !result && d.HasChange("warehouse") {
		o, n := d.GetChange("warehouse")
		if len(n.(*schema.Set).List()) > 1 {
			result = true
		} else {
			oldWhInfo := o.(*schema.Set).List()[0].(map[string]interface{})
			newWhInfo := n.(*schema.Set).List()[0].(map[string]interface{})
			changed := (newWhInfo["compute_node_size"].(string) != oldWhInfo["compute_node_size"].(string) || newWhInfo["compute_node_count"].(int) != oldWhInfo["compute_node_count"].(int))
			result = result || changed
		}
	}

	return result
}

func resourceElasticClusterV2Update(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	var immutableFields = []string{"csp", "region", "cluster_name", "compute_node_ebs_disk_number", "default_admin_password", "data_credential_id", "deployment_credential_id", "network_id", "init_scripts", "query_port"}
	for _, f := range immutableFields {
		if d.HasChange(f) && !d.IsNewResource() {
			return diag.FromErr(fmt.Errorf("the `%s` field is not allowed to be modified", f))
		}
	}

	c := m.(*client.CelerdataClient)

	// Warning or errors can be collected in a slice type
	clusterId := d.Id()
	clusterAPI := cluster.NewClustersAPI(c)
	log.Printf("[DEBUG] resourceElasticClusterV2Update cluster id:%s", clusterId)

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
		o, n := d.GetChange("warehouse")
		old := o.(*schema.Set).List()
		new := n.(*schema.Set).List()
		whExternalInfoMap := d.Get("warehouse_external_info").(map[string]interface{})

		oldWhMap := make(map[string]map[string]interface{})
		for _, v := range old {
			whMap := v.(map[string]interface{})
			oldWhMap[whMap["name"].(string)] = whMap
		}
		newWhMap := make(map[string]map[string]interface{})
		for _, v := range new {
			whMap := v.(map[string]interface{})
			newWhMap[whMap["name"].(string)] = whMap
		}

		for _, v := range new {
			newWh := v.(map[string]interface{})
			whName := newWh["name"].(string)
			if oldWh, ok := oldWhMap[whName]; ok {
				// modified
				whExternalInfoStr := whExternalInfoMap[whName].(string)
				whExternalInfo := &cluster.WarehouseExternalInfo{}
				json.Unmarshal([]byte(whExternalInfoStr), whExternalInfo)
				diags := updateWarehouse(ctx, clusterAPI, clusterId, oldWh, newWh, whExternalInfo)
				if diags != nil {
					return diags
				}
			} else {
				// added
				diags := createWarehouse(ctx, clusterAPI, clusterId, newWh)
				if diags != nil {
					return diags
				}
			}
		}

		for _, v := range old {
			oldWh := v.(map[string]interface{})
			whName := oldWh["name"].(string)
			if _, ok := newWhMap[whName]; !ok {
				// removed
				whExternalInfoStr := whExternalInfoMap[whName].(string)
				whExternalInfo := &cluster.WarehouseExternalInfo{}
				json.Unmarshal([]byte(whExternalInfoStr), whExternalInfo)
				whId := whExternalInfo.Id
				diags := DeleteWarehouse(ctx, clusterAPI, clusterId, whId)
				if diags != nil {
					return diags
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

	if d.HasChange("resource_tags") && !d.IsNewResource() {
		_, n := d.GetChange("resource_tags")

		nTags := n.(map[string]interface{})
		tags := make(map[string]string, len(nTags))
		for k, v := range nTags {
			tags[k] = v.(string)
		}
		err := clusterAPI.UpdateResourceTags(ctx, &cluster.UpdateResourceTagsReq{
			ClusterId: clusterId,
			Tags:      tags,
		})
		if err != nil {
			return diag.FromErr(fmt.Errorf("cluster (%s) failed to update resource tags: %s", d.Id(), err.Error()))
		}
	}

	return diags
}

func setWarehouseAutoScalingPolicy(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterId, warehouseId, policyJson string) error {

	if len(policyJson) > 0 {
		autoScalingConfig := &cluster.WarehouseAutoScalingConfig{}
		json.Unmarshal([]byte(policyJson), autoScalingConfig)
		req := &cluster.SaveWarehouseAutoScalingConfigReq{
			ClusterId:                  clusterId,
			WarehouseId:                warehouseId,
			WarehouseAutoScalingConfig: *autoScalingConfig,
			State:                      true,
		}
		_, err := clusterAPI.SaveWarehouseAutoScalingConfig(ctx, req)
		return err
	}
	return nil
}

func createWarehouse(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterId string, whParamMap map[string]interface{}) diag.Diagnostics {

	warehouseName := whParamMap["name"].(string)

	diskNumber := 2
	perDiskSize := 100
	if v, ok := whParamMap["compute_node_ebs_disk_number"]; ok {
		diskNumber = v.(int)
	}
	if v, ok := whParamMap["compute_node_ebs_disk_per_size"]; ok {
		perDiskSize = v.(int)
	}
	req := &cluster.CreateWarehouseReq{
		ClusterId:    clusterId,
		Name:         warehouseName,
		VmCate:       whParamMap["compute_node_size"].(string),
		VmNum:        int32(whParamMap["compute_node_count"].(int)),
		VolumeSizeGB: int64(perDiskSize),
		VolumeNum:    int32(diskNumber),
	}

	log.Printf("[DEBUG] Create warehouse, req:%+v", req)
	resp, err := clusterAPI.CreateWarehouse(ctx, req)
	if err != nil {
		log.Printf("[ERROR] Create warehouse failed, err:%+v", err)
		return diag.FromErr(err)
	}
	log.Printf("[DEBUG] Create warehouse, resp:%+v", resp)

	warehouseId := resp.WarehouseId
	infraActionId := resp.ActionID
	if len(infraActionId) > 0 {
		stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
			clusterAPI: clusterAPI,
			clusterID:  clusterId,
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
			summary := fmt.Sprintf("create warehouse[%s] of the cluster[%s] failed, errMsg:%s", warehouseName, clusterId, err.Error())
			return diag.FromErr(fmt.Errorf("%s", summary))
		}

		if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
			return diag.FromErr(errors.New(stateResp.AbnormalReason))
		}
	}

	if v, ok := whParamMap["auto_scaling_policy"]; ok {
		policyJson := v.(string)
		err := setWarehouseAutoScalingPolicy(ctx, clusterAPI, clusterId, warehouseId, policyJson)
		if err != nil {
			msg := fmt.Sprintf("Add warehouse auto-scaling configuration failed, errMsg:%s", err.Error())
			log.Printf("[ERROR] %s", msg)
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  fmt.Sprintf("Config warehouse[%s] auto-scaling configuration failed", warehouseName),
					Detail:   msg,
				},
			}
		}
	}

	expectedState := whParamMap["expected_state"].(string)
	if expectedState == string(cluster.ClusterStateSuspended) {
		summary := fmt.Sprintf("Suspend warehouse[%s] failed", warehouseName)
		suspendWhResp, err := clusterAPI.SuspendWarehouse(ctx, &cluster.SuspendWarehouseReq{
			WarehouseId: warehouseId,
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
		infraActionId := suspendWhResp.ActionID
		if len(infraActionId) > 0 {
			stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
				clusterAPI: clusterAPI,
				clusterID:  clusterId,
				actionID:   infraActionId,
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
					string(cluster.ClusterStateSuspended),
					string(cluster.ClusterStateAbnormal),
				},
			})

			summary := fmt.Sprintf("suspend warehouse[%s] failed", warehouseName)
			if err != nil {
				return diag.Diagnostics{
					diag.Diagnostic{
						Severity: diag.Warning,
						Summary:  summary,
						Detail:   fmt.Sprintf("%s. errMsg:%s", summary, err.Error()),
					},
				}
			}

			if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
				return diag.Diagnostics{
					diag.Diagnostic{
						Severity: diag.Warning,
						Summary:  summary,
						Detail:   fmt.Sprintf("%s. errMsg:%s", summary, stateResp.AbnormalReason),
					},
				}
			}
		}
	}

	idleSuspendInterval := whParamMap["idle_suspend_interval"].(int)
	if idleSuspendInterval > 0 {
		err = clusterAPI.UpdateWarehouseIdleConfig(ctx, &cluster.UpdateWarehouseIdleConfigReq{
			WarehouseId: warehouseId,
			IntervalMs:  int64(idleSuspendInterval * 60 * 1000),
			State:       true,
		})
		if err != nil {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  fmt.Sprintf("Config warehouse[%s] idle config failed", warehouseName),
					Detail:   err.Error(),
				},
			}
		}
	}
	return nil
}

func updateWarehouse(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterId string, oldParamMap, newParamMap map[string]interface{}, whExternalInfo *cluster.WarehouseExternalInfo) diag.Diagnostics {

	warehouseId := whExternalInfo.Id
	isDefaultWarehouse := whExternalInfo.IsDefaultWarehouse
	warehouseName := newParamMap["name"].(string)
	expectedState := newParamMap["expected_state"].(string)

	computeNodeIsInstanceStore := whExternalInfo.IsInstanceStore
	expectedStateChanged := oldParamMap["expected_state"].(string) != newParamMap["expected_state"].(string)
	computeNodeSizeChanged := oldParamMap["compute_node_size"].(string) != newParamMap["compute_node_size"].(string)
	computeNodeCountChanged := oldParamMap["compute_node_count"].(int) != newParamMap["compute_node_count"].(int)
	computeNodeEbsDiskNumberChanged := oldParamMap["compute_node_ebs_disk_number"].(int) != newParamMap["compute_node_ebs_disk_number"].(int)
	computeNodeEbsDiskPerSizeChanged := oldParamMap["compute_node_ebs_disk_per_size"].(int) != newParamMap["compute_node_ebs_disk_per_size"].(int)

	idleSuspendIntervalChanged := oldParamMap["idle_suspend_interval"].(int) != newParamMap["idle_suspend_interval"].(int)
	autoScalingPolicyChanged := oldParamMap["auto_scaling_policy"].(string) != newParamMap["auto_scaling_policy"].(string)

	if !computeNodeIsInstanceStore {
		if computeNodeEbsDiskNumberChanged {
			return diag.FromErr(errors.New("modifying the number of ebs disks is not supported"))
		}
		if computeNodeEbsDiskPerSizeChanged && oldParamMap["compute_node_ebs_disk_per_size"].(int) > newParamMap["compute_node_ebs_disk_per_size"].(int) {
			return diag.FromErr(errors.New("disk capacity reduction is not supported"))
		}
	}

	if expectedStateChanged && !isDefaultWarehouse {
		if expectedState == string(cluster.ClusterStateRunning) {
			resp := ResumeWarehouse(ctx, clusterAPI, clusterId, warehouseId, warehouseName)
			if resp != nil {
				return resp
			}
		}
	}

	if computeNodeSizeChanged {
		vmCate := newParamMap["compute_node_size"].(string)
		resp, err := clusterAPI.ScaleWarehouseSize(ctx, &cluster.ScaleWarehouseSizeReq{
			WarehouseId: warehouseId,
			VmCate:      vmCate,
		})

		if err != nil {
			return diag.FromErr(fmt.Errorf("failed to scale warehouse size, clusterId:%s warehouseId:%s, errMsg:%s", clusterId, warehouseId, err))
		}

		stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
			clusterAPI: clusterAPI,
			actionID:   resp.ActionID,
			clusterID:  clusterId,
			timeout:    common.DeployOrScaleClusterTimeout,
			pendingStates: []string{
				string(cluster.ClusterStateRunning),
				string(cluster.ClusterStateScaling)},
			targetStates: []string{string(cluster.ClusterStateRunning), string(cluster.ClusterStateAbnormal)},
		})
		if err != nil {
			return diag.FromErr(fmt.Errorf("waiting for cluster (%s) running: %s", clusterId, err))
		}

		if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
			return diag.FromErr(errors.New(stateResp.AbnormalReason))
		}
	}

	if computeNodeCountChanged {
		vmNum := int32(newParamMap["compute_node_count"].(int))
		resp, err := clusterAPI.ScaleWarehouseNum(ctx, &cluster.ScaleWarehouseNumReq{
			WarehouseId: warehouseId,
			VmNum:       vmNum,
		})

		if err != nil {
			return diag.FromErr(fmt.Errorf("failed to scale warehouse number, clusterId:%s warehouseId:%s, errMsg:%s", clusterId, warehouseId, err))
		}

		stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
			clusterAPI: clusterAPI,
			actionID:   resp.ActionID,
			clusterID:  clusterId,
			timeout:    common.DeployOrScaleClusterTimeout,
			pendingStates: []string{
				string(cluster.ClusterStateRunning),
				string(cluster.ClusterStateScaling)},
			targetStates: []string{string(cluster.ClusterStateRunning), string(cluster.ClusterStateAbnormal)},
		})
		if err != nil {
			return diag.FromErr(fmt.Errorf("waiting for cluster (%s) running: %s", clusterId, err))
		}

		if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
			return diag.FromErr(errors.New(stateResp.AbnormalReason))
		}
	}

	if !computeNodeIsInstanceStore && computeNodeEbsDiskPerSizeChanged {

		perDiskSize := newParamMap["compute_node_ebs_disk_per_size"].(int)

		req := &cluster.ModifyClusterVolumeReq{
			ClusterId:   clusterId,
			WarehouseID: warehouseId,
			Type:        cluster.ClusterModuleTypeWarehouse,
			VmVolSize:   int64(perDiskSize),
		}

		log.Printf("[DEBUG] modify warehouse[%s] volume config, req:%+v", warehouseName, req)
		modifyVolumeResp, err := clusterAPI.ModifyClusterVolume(ctx, req)
		if err != nil {
			log.Printf("[ERROR] modify warehouse[%s] volume config failed, err:%+v", warehouseName, err)
			return diag.FromErr(err)
		}
		infraActionId := modifyVolumeResp.ActionID
		if len(infraActionId) > 0 {
			infraActionResp, err := WaitClusterInfraActionStateChangeComplete(ctx, &waitStateReq{
				clusterAPI: clusterAPI,
				clusterID:  clusterId,
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

			summary := fmt.Sprintf("Modify warehouse[%s] volume config failed", warehouseName)

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

	if idleSuspendIntervalChanged {
		idleSuspendInterval := newParamMap["idle_suspend_interval"].(int)
		err := clusterAPI.UpdateWarehouseIdleConfig(ctx, &cluster.UpdateWarehouseIdleConfigReq{
			WarehouseId: warehouseId,
			IntervalMs:  int64(idleSuspendInterval * 60 * 1000),
			State:       idleSuspendInterval > 0,
		})
		if err != nil {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  "Config warehouse idle config failed",
					Detail:   err.Error(),
				},
			}
		}
	}

	if expectedStateChanged && !isDefaultWarehouse {
		if expectedState == string(cluster.ClusterStateSuspended) {
			resp := SuspendWarehouse(ctx, clusterAPI, clusterId, warehouseId, warehouseName)
			if resp != nil {
				return resp
			}
		}
	}

	if autoScalingPolicyChanged {
		policyJson := ""
		if v, ok := newParamMap["auto_scaling_policy"]; ok {
			policyJson = v.(string)
		}

		if len(policyJson) > 0 {
			autoScalingConfig := &cluster.WarehouseAutoScalingConfig{}
			json.Unmarshal([]byte(policyJson), autoScalingConfig)
			req := &cluster.SaveWarehouseAutoScalingConfigReq{
				ClusterId:                  clusterId,
				WarehouseId:                warehouseId,
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

	return nil
}

func DeleteWarehouse(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterId, warehouseId string) (diags diag.Diagnostics) {

	resp, err := clusterAPI.ReleaseWarehouse(ctx, &cluster.ReleaseWarehouseReq{
		WarehouseId: warehouseId,
	})

	if err != nil {
		log.Printf("[ERROR] release warehouse failed, err:%+v", err)
		return diag.FromErr(err)
	}

	infraActionId := resp.ActionID
	if len(infraActionId) > 0 {
		stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
			clusterAPI: clusterAPI,
			clusterID:  clusterId,
			actionID:   resp.ActionID,
			timeout:    common.DeployOrScaleClusterTimeout,
			pendingStates: []string{
				string(cluster.ClusterStateDeploying),
				string(cluster.ClusterStateRunning),
				string(cluster.ClusterStateScaling),
				string(cluster.ClusterStateResuming),
				string(cluster.ClusterStateSuspending),
				string(cluster.ClusterStateReleasing),
				string(cluster.ClusterStateUpdating),
			},
			targetStates: []string{
				string(cluster.ClusterStateReleased),
				string(cluster.ClusterStateAbnormal),
			},
		})

		if err != nil {
			summary := fmt.Sprintf("release warehouse[%s] of the cluster[%s] failed, errMsg:%s", warehouseId, clusterId, err.Error())
			return diag.FromErr(fmt.Errorf(summary))
		}

		if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
			return diag.FromErr(errors.New(stateResp.AbnormalReason))
		}
	}
	return diags
}

func SuspendWarehouse(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterId, warehouseId, warehouseName string) (diags diag.Diagnostics) {
	suspendWhResp, err := clusterAPI.SuspendWarehouse(ctx, &cluster.SuspendWarehouseReq{
		WarehouseId: warehouseId,
	})
	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  "Suspend warehouse failed",
				Detail:   err.Error(),
			},
		}
	}
	infraActionId := suspendWhResp.ActionID
	if len(infraActionId) > 0 {
		stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
			clusterAPI: clusterAPI,
			clusterID:  clusterId,
			actionID:   infraActionId,
			timeout:    common.DeployOrScaleClusterTimeout,
			pendingStates: []string{
				string(cluster.ClusterStateDeploying),
				string(cluster.ClusterStateRunning),
				string(cluster.ClusterStateScaling),
				string(cluster.ClusterStateResuming),
				string(cluster.ClusterStateSuspending),
				string(cluster.ClusterStateReleasing),
				string(cluster.ClusterStateUpdating),
			},
			targetStates: []string{
				string(cluster.ClusterStateSuspended),
				string(cluster.ClusterStateAbnormal),
			},
		})

		if err != nil {
			msg := fmt.Sprintf("suspend warehouse[%s] of the cluster[%s] failed, errMsg:%s", warehouseName, clusterId, err.Error())
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  "Suspend warehouse",
					Detail:   msg,
				},
			}
		}

		if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  "Suspend warehouse",
					Detail:   stateResp.AbnormalReason,
				},
			}
		}
	}
	return diags
}

func ResumeWarehouse(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterId, warehouseId, warehouseName string) (diags diag.Diagnostics) {
	resumeWhResp, err := clusterAPI.ResumeWarehouse(ctx, &cluster.ResumeWarehouseReq{
		WarehouseId: warehouseId,
	})
	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "Resume warehouse failed",
				Detail:   err.Error(),
			},
		}
	}
	infraActionId := resumeWhResp.ActionID
	if len(infraActionId) > 0 {
		stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
			clusterAPI: clusterAPI,
			clusterID:  clusterId,
			actionID:   infraActionId,
			timeout:    common.DeployOrScaleClusterTimeout,
			pendingStates: []string{
				string(cluster.ClusterStateDeploying),
				string(cluster.ClusterStateScaling),
				string(cluster.ClusterStateResuming),
				string(cluster.ClusterStateSuspending),
				string(cluster.ClusterStateSuspended),
				string(cluster.ClusterStateReleasing),
				string(cluster.ClusterStateUpdating),
			},
			targetStates: []string{
				string(cluster.ClusterStateRunning),
				string(cluster.ClusterStateAbnormal),
			},
		})

		if err != nil {
			summary := fmt.Sprintf("resume warehouse[%s] of the cluster[%s] failed, errMsg:%s", warehouseName, clusterId, err.Error())
			return diag.FromErr(fmt.Errorf(summary))
		}

		if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
			return diag.FromErr(errors.New(stateResp.AbnormalReason))
		}
	}
	return diags
}

var InternalTagKeys = map[string]bool{
	"Vendor":             true,
	"Creator":            true,
	"ClusterName":        true,
	"ServiceAccountName": true,
	"ServiceAccountID":   true,
}
