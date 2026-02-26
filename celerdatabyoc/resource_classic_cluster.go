package celerdatabyoc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"
	"time"

	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/network"
	"terraform-provider-celerdatabyoc/common"

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
		DeprecationMessage: "This resource is about to be deprecated. For create new clusters, it is recommended to use `celerdatabyoc_elastic_cluster_v2`",
		CreateContext:      resourceClusterCreate,
		ReadContext:        resourceClusterRead,
		DeleteContext:      resourceClusterDelete,
		UpdateContext:      resourceClusterUpdate,
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
							Type:             schema.TypeInt,
							Optional:         true,
							Default:          150,
							ValidateDiagFunc: common.ValidateVolumeSize(),
						},
						"iops": {
							Type:         schema.TypeInt,
							Optional:     true,
							ValidateFunc: validation.IntAtLeast(0),
							Computed:     true,
						},
						"throughput": {
							Type:         schema.TypeInt,
							Optional:     true,
							ValidateFunc: validation.IntAtLeast(0),
							Computed:     true,
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
								} else if v > 16 {
									errors = append(errors, fmt.Errorf("%s`s value is invalid. The range of values is: [1,16]", k))
								}

								return warnings, errors
							},
						},
						"vol_size": {
							Description:      "Specifies the size of a single disk in GB. The default size for per disk is 100GB.",
							Type:             schema.TypeInt,
							Optional:         true,
							Default:          100,
							ValidateDiagFunc: common.ValidateVolumeSize(),
						},
						"iops": {
							Type:         schema.TypeInt,
							Optional:     true,
							ValidateFunc: validation.IntAtLeast(0),
							Computed:     true,
						},
						"throughput": {
							Type:         schema.TypeInt,
							Optional:     true,
							ValidateFunc: validation.IntAtLeast(0),
							Computed:     true,
						},
					},
				},
			},
			"resource_tags": {
				Type:        schema.TypeMap,
				Optional:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Description: "A map of tags to assign to the resource. For AWS, these are tags; for GCP, these are labels.",
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
			"scripts": {
				Type:     schema.TypeSet,
				Optional: true,
				Computed: true,
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
						"rerun": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
					},
				},
			},
			"run_scripts_parallel": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
			"enabled_termination_protection": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
			"table_name_case_insensitive": {
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
			"global_session_variables": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem: &schema.Schema{
					Type:         schema.TypeString,
					ValidateFunc: validation.StringIsNotWhiteSpace,
				},
			},
			"ranger_certs_dir": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringIsNotWhiteSpace,
			},
			"scheduling_policy": {
				Type:     schema.TypeList,
				Optional: true,
				MaxItems: 5,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"policy_name": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringIsNotWhiteSpace,
						},
						"description": {
							Type:         schema.TypeString,
							Optional:     true,
							ValidateFunc: validation.StringIsNotWhiteSpace,
						},
						"time_zone": {
							Type:         schema.TypeString,
							Description:  "IANA Time-Zone",
							Optional:     true,
							Default:      "UTC",
							ValidateFunc: common.ValidateSchedulingPolicyTimeZone,
						},
						"active_days": {
							Type:     schema.TypeSet,
							Required: true,
							MinItems: 1,
							MaxItems: 7,
							Elem: &schema.Schema{
								Type:         schema.TypeString,
								ValidateFunc: validation.StringInSlice(cluster.WeekDays, false),
							},
						},
						"resume_at": {
							Type:         schema.TypeString,
							Optional:     true,
							ValidateFunc: common.ValidateSchedulingPolicyDateTime,
						},
						"suspend_at": {
							Type:         schema.TypeString,
							Optional:     true,
							ValidateFunc: common.ValidateSchedulingPolicyDateTime,
						},
						"enable": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  true,
						},
					},
				},
			},
			"scheduling_policy_extra_info": {
				Type:     schema.TypeMap,
				Computed: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"ranger_config_id": {
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
			return fmt.Errorf("the fe `vol_size` does not support decrease")
		}
	}
	if !feVmInfoResp.VmInfo.IsInstanceStore {
		if v, ok := d.GetOk("fe_volume_config"); ok {
			nodeType := cluster.ClusterModuleTypeFE
			volumeCate := feVmInfoResp.VmInfo.VmVolumeInfos[0].VolumeCate
			volumeConfig := v.([]interface{})[0].(map[string]interface{})
			err = VolumeParamVerify(ctx, &VolumeParamVerifyReq{
				ClusterAPI:   clusterAPI,
				VolumeCate:   volumeCate,
				VolumeConfig: volumeConfig,
			})
			if err != nil {
				log.Printf("[ERROR] verify %s volume params failed, volumeCate:%s volumeConfig:%+v err:%+v", nodeType, volumeCate, volumeConfig, err)
				return fmt.Errorf("verify %s volume params failed, volumeCate:%s volumeConfig:%+v err:%+v", nodeType, volumeCate, volumeConfig, err)
			}
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

		if newVolumeNum < oldVolumeNum {
			return fmt.Errorf("the be `vol_number` is not allowed to be decreased for classic clusters")
		}

		if newVolumeNum > int(beVmInfoResp.VmInfo.MaxDataDiskCount) {
			return fmt.Errorf("the maximum allowed `vol_number` for this VM type is: %d", beVmInfoResp.VmInfo.MaxDataDiskCount)
		}

		if newVolumeNum < 1 {
			return fmt.Errorf("the minimum allowed `vol_number` is: 1")
		}

		if newVolumeSize < oldVolumeSize {
			return fmt.Errorf("the be `vol_size` does not support decrease")
		}
	}

	if !beVmInfoResp.VmInfo.IsInstanceStore {
		if v, ok := d.GetOk("be_volume_config"); ok {
			nodeType := cluster.ClusterModuleTypeBE
			volumeCate := feVmInfoResp.VmInfo.VmVolumeInfos[0].VolumeCate
			volumeConfig := v.([]interface{})[0].(map[string]interface{})
			err = VolumeParamVerify(ctx, &VolumeParamVerifyReq{
				ClusterAPI:   clusterAPI,
				VolumeCate:   volumeCate,
				VolumeConfig: volumeConfig,
			})
			if err != nil {
				log.Printf("[ERROR] verify %s volume params failed, volumeCate:%s volumeConfig:%+v err:%+v", nodeType, volumeCate, volumeConfig, err)
				return fmt.Errorf("verify %s volume params failed, volumeCate:%s volumeConfig:%+v err:%+v", nodeType, volumeCate, volumeConfig, err)
			}
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

	if d.HasChange("global_session_variables") && d.Get("expected_cluster_state") != string(cluster.ClusterStateRunning) {
		o, n := d.GetChange("global_session_variables")
		return fmt.Errorf("when modify `global_session_variables` [from %s to %s], field `expected_cluster_state` should change to:%s", o, n, cluster.ClusterStateRunning)
	}

	err = MarkScriptReRun(d)
	if err != nil {
		return err
	}

	err2 := SchedulingPolicyParamCheck(d)
	return err2
}

func MarkScriptReRun(d *schema.ResourceDiff) error {
	if d.HasChange("scripts") {
		return nil
	}

	scripts := d.Get("scripts").(*schema.Set).List()
	for _, s := range scripts {
		script := s.(map[string]interface{})
		if reRun, ok := script["rerun"].(bool); ok && reRun {
			if err := d.SetNewComputed("scripts"); err != nil {
				return fmt.Errorf("failed to force diff for scripts: %v", err)
			}
			break
		}
	}
	return nil
}

func SchedulingPolicyParamCheck(d *schema.ResourceDiff) error {
	if v, ok := d.GetOk("scheduling_policy"); ok {
		policies := v.([]interface{})
		policyNameMap := make(map[string]bool)
		for _, item := range policies {
			m := item.(map[string]interface{})
			if _, ok := policyNameMap[m["policy_name"].(string)]; ok {
				return fmt.Errorf("Duplicate scheduling policy name `%s`", m["policy_name"].(string))
			}

			if m["resume_at"].(string) == "" && m["suspend_at"].(string) == "" {
				return fmt.Errorf("For scheduling policy [`%s`], field `resume_at` and `suspend_at` cannot be empty at the same time.", m["policy_name"].(string))
			}

			if m["resume_at"].(string) == m["suspend_at"].(string) {
				return fmt.Errorf("For scheduling policy [`%s`], field `resume_at` and `suspend_at` cannot be the same", m["policy_name"].(string))
			}

			policyNameMap[m["policy_name"].(string)] = true
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
		ClusterName:                  clusterName,
		Csp:                          d.Get("csp").(string),
		Region:                       d.Get("region").(string),
		ClusterType:                  cluster.ClusterTypeClassic,
		Password:                     d.Get("default_admin_password").(string),
		SslConnEnable:                true,
		NetIfaceId:                   d.Get("network_id").(string),
		DeployCredlId:                d.Get("deployment_credential_id").(string),
		DataCredId:                   d.Get("data_credential_id").(string),
		RunScriptsParallel:           d.Get("run_scripts_parallel").(bool),
		QueryPort:                    int32(d.Get("query_port").(int)),
		RunScriptsTimeout:            int32(d.Get("run_scripts_timeout").(int)),
		EnabledTerminationProtection: d.Get("enabled_termination_protection").(bool),
		TableNameCaseInsensitive:     d.Get("table_name_case_insensitive").(bool),
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

	clusterId := resp.ClusterID
	d.SetId(clusterId)
	log.Printf("[DEBUG] deploy succeeded, action id:%s cluster id:%s]", resp.ActionID, clusterId)

	if v, ok := d.GetOk("fe_configs"); ok && len(d.Get("fe_configs").(map[string]interface{})) > 0 {
		configMap := v.(map[string]interface{})
		configs := make(map[string]string, 0)
		for k, v := range configMap {
			configs[k] = v.(string)
		}
		warnDiag := UpsertClusterConfig(ctx, clusterAPI, &cluster.UpsertClusterConfigReq{
			ClusterID:  clusterId,
			ConfigType: cluster.CustomConfigTypeFE,
			Configs:    configs,
		})
		if warnDiag != nil {
			return warnDiag
		}
	}

	if v, ok := d.GetOk("be_configs"); ok && len(d.Get("be_configs").(map[string]interface{})) > 0 {
		configMap := v.(map[string]interface{})
		configs := make(map[string]string, 0)
		for k, v := range configMap {
			configs[k] = v.(string)
		}
		warnDiag := UpsertClusterConfig(ctx, clusterAPI, &cluster.UpsertClusterConfigReq{
			ClusterID:  resp.ClusterID,
			ConfigType: cluster.CustomConfigTypeBE,
			Configs:    configs,
		})

		if warnDiag != nil {
			return warnDiag
		}
	}

	if v, ok := d.GetOk("global_session_variables"); ok && len(d.Get("global_session_variables").(map[string]interface{})) > 0 {
		diagnostics := SetGlobalSqlSessionVariables(ctx, v, clusterAPI, clusterId)
		if diagnostics != nil {
			return diagnostics
		}
	}

	if v, ok := d.GetOk("ldap_ssl_certs"); ok {

		arr := v.(*schema.Set).List()
		sslCerts := make([]string, 0)
		for _, v := range arr {
			value := v.(string)
			sslCerts = append(sslCerts, value)
		}

		if len(sslCerts) > 0 {
			warningDiag := UpsertClusterLdapSslCert(ctx, clusterAPI, clusterId, sslCerts, false)
			if warningDiag != nil {
				return warningDiag
			}
		}
	}

	if v, ok := d.GetOk("ranger_certs_dir"); ok {
		rangerCertsDirPath := v.(string)
		warningDiag := UpsertClusterRangerCert(ctx, clusterAPI, clusterId, rangerCertsDirPath, false)
		if warningDiag != nil {
			return warningDiag
		}
	}

	RunScripts(ctx, RunScriptsReq{
		ResourceData:       d,
		ClusterAPI:         clusterAPI,
		ClusterID:          clusterId,
		RunScriptsParallel: d.Get("run_scripts_parallel").(bool),
		IsCreate:           true,
	})

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

	if v, ok := d.GetOk("scheduling_policy"); ok {
		policies := v.([]interface{})
		for _, item := range policies {
			m := item.(map[string]interface{})
			err := SaveClusterSchedulingPolicy(ctx, clusterAPI, clusterId, m)
			if err != nil {
				return diag.Diagnostics{
					diag.Diagnostic{
						Severity: diag.Warning,
						Summary:  fmt.Sprintf("Failed to save scheduling policy[%s], please retry again!", m["policy_name"].(string)),
						Detail:   err.Error(),
					},
				}
			}
		}
	}

	if v, ok := d.GetOk("ranger_config_id"); ok {
		rangerConfigID := v.(string)
		warningDiag := ApplyRangerV2(ctx, clusterAPI, clusterId, rangerConfigID)
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

	globalSessionVariables := make(map[string]string)
	if v, ok := d.GetOk("global_session_variables"); ok && len(v.(map[string]interface{})) > 0 {
		for k, v := range v.(map[string]interface{}) {
			globalSessionVariables[k] = v.(string)
		}
		if stateResp.ClusterState == string(cluster.ClusterStateRunning) {
			sessionVariablesResp, diagnostics := GetGlobalSqlSessionVariables(ctx, clusterAPI, clusterID, v)
			if diagnostics != nil {
				return diagnostics
			}
			globalSessionVariables = sessionVariablesResp.Variables
		}
	}

	policies, policyExtraInfo, err := ListClusterSchedulingPolicy(ctx, clusterAPI, clusterID)
	if err != nil {
		log.Printf("[ERROR] list cluster schedule policy failed,clusterId:%s err:%+v", clusterID, err)
		return diag.FromErr(err)
	}

	terminationProtection, err := clusterAPI.GetClusterTerminationProtection(ctx, &cluster.GetClusterTerminationProtectionReq{ClusterId: clusterID})
	if err != nil {
		log.Printf("[ERROR] get cluster termination protection failed, clusterId:%s err:%+v", clusterID, err)
		return diag.FromErr(err)
	}

	rangerConfigResp, err := clusterAPI.GetCustomConfig(ctx, &cluster.ListCustomConfigReq{
		ClusterID:  clusterID,
		ConfigType: cluster.CustomConfigTypeRangerV2,
	})
	if err != nil {
		log.Printf("[ERROR] query cluster ranger config failed, err:%+v", err)
	}

	tableNameCaseInsensitive, err := clusterAPI.GetClusterTableNameCaseInsensitive(ctx, &cluster.GetClusterTableNameCaseInsensitiveReq{ClusterId: clusterID})
	if err != nil {
		log.Printf("[ERROR] get cluster config[table_name_case_insensitive] failed, clusterId:%s err:%+v", clusterID, err)
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
	d.Set("csp", resp.Cluster.Csp)
	d.Set("region", resp.Cluster.Region)

	csp := d.Get("csp").(string)
	tags := make(map[string]interface{})
	for k, v := range resp.Cluster.Tags {
		if !IsInternalTagKeys(csp, k) {
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

	if len(feConfigsResp.Configs) > 0 {
		d.Set("fe_configs", feConfigsResp.Configs)
	}

	if len(beConfigsResp.Configs) > 0 {
		d.Set("be_configs", beConfigsResp.Configs)
	}

	if len(globalSessionVariables) > 0 {
		d.Set("global_session_variables", globalSessionVariables)
	}

	feModule := resp.Cluster.FeModule
	if !feModule.IsInstanceStore {
		feVolumeConfig := make(map[string]interface{}, 0)
		feVolumeConfig["vol_size"] = feModule.VmVolSizeGB
		feVolumeConfig["iops"] = feModule.Iops
		feVolumeConfig["throughput"] = feModule.Throughput
		if v, ok := d.GetOk("fe_volume_config"); ok && v != nil {
			d.Set("fe_volume_config", []interface{}{feVolumeConfig})
		}
	}

	beModule := resp.Cluster.BeModule
	if !beModule.IsInstanceStore {
		beVolumeConfig := make(map[string]interface{}, 0)
		beVolumeConfig["vol_number"] = beModule.VmVolNum
		beVolumeConfig["vol_size"] = beModule.VmVolSizeGB
		beVolumeConfig["iops"] = beModule.Iops
		beVolumeConfig["throughput"] = beModule.Throughput
		if v, ok := d.GetOk("be_volume_config"); ok && v != nil {
			d.Set("be_volume_config", []interface{}{beVolumeConfig})
		}
	}

	d.Set("scheduling_policy", policies)
	d.Set("scheduling_policy_extra_info", policyExtraInfo)
	d.Set("enabled_termination_protection", terminationProtection.Enabled)
	d.Set("table_name_case_insensitive", tableNameCaseInsensitive.Enabled)

	if len(rangerConfigResp.Configs) > 0 {
		d.Set("ranger_config_id", rangerConfigResp.Configs["biz_id"])
	}

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
	var immutableFields = []string{"csp", "region", "cluster_name", "default_admin_password", "data_credential_id", "deployment_credential_id", "network_id", "query_port"}
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

	if d.HasChange("scheduling_policy") && !d.IsNewResource() {
		diagError := HandleChangedClusterSchedulingPolicy(ctx, clusterAPI, d)
		if diagError != nil {
			return diagError
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

	if d.HasChange("global_session_variables") && !d.IsNewResource() {
		errDiag := HandleChangedGlobalSqlSessionVariables(ctx, clusterAPI, d)
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

	if d.HasChange("init_scripts") && !d.IsNewResource() {
		_, n := d.GetChange("init_scripts")
		vL := n.(*schema.Set).List()
		scripts := make([]*cluster.Script, 0, len(vL))
		for _, v := range vL {
			s := v.(map[string]interface{})
			scripts = append(scripts, &cluster.Script{
				ScriptPath: s["script_path"].(string),
				LogsDir:    s["logs_dir"].(string),
			})
		}
		err := clusterAPI.UpdateDeploymentScripts(ctx, &cluster.UpdateDeploymentScriptsReq{
			ClusterId: clusterID,
			Scripts:   scripts,
			Parallel:  d.Get("run_scripts_parallel").(bool),
			Timeout:   int32(d.Get("run_scripts_timeout").(int)),
		})
		if err != nil {
			return diag.FromErr(fmt.Errorf("failed to update cluster(%s) init-scripts: %s", d.Id(), err.Error()))
		}
	}

	if d.HasChange("ranger_certs_dir") && !d.IsNewResource() {
		rangerCertsDirPath := d.Get("ranger_certs_dir").(string)
		warningDiag := UpsertClusterRangerCert(ctx, clusterAPI, d.Id(), rangerCertsDirPath, true)
		if warningDiag != nil {
			return warningDiag
		}
	}

	if d.HasChange("enabled_termination_protection") && !d.IsNewResource() {
		enabled := d.Get("enabled_termination_protection").(bool)
		err := clusterAPI.SetClusterTerminationProtection(ctx, clusterID, &cluster.SetClusterTerminationProtectionReq{
			Enabled: enabled,
		})
		if err != nil {
			return diag.FromErr(fmt.Errorf("cluster (%s) failed to set termination protection: %s", d.Id(), err.Error()))
		}
	}

	if d.HasChange("table_name_case_insensitive") && !d.IsNewResource() {
		return diag.FromErr(fmt.Errorf("`table_name_case_insensitive` of cluster (%s) cannot be modifeid after the cluster is created", d.Id()))
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
		warnDiag := UpsertClusterConfig(ctx, clusterAPI, &cluster.UpsertClusterConfigReq{
			ClusterID:  clusterID,
			ConfigType: cluster.CustomConfigTypeFE,
			Configs:    configs,
		})

		if warnDiag != nil {
			return warnDiag
		}

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
		if v, ok := newVolumeConfig["vol_number"]; ok && v != oldVolumeConfig["vol_number"] {
			req.VmVolNum = int32(v.(int))
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
		warnDiag := UpsertClusterConfig(ctx, clusterAPI, &cluster.UpsertClusterConfigReq{
			ClusterID:  clusterID,
			ConfigType: cluster.CustomConfigTypeBE,
			Configs:    configs,
		})
		if warnDiag != nil {
			return warnDiag
		}
	}

	if d.HasChange("ranger_config_id") {
		rangerConfigID := d.Get("ranger_config_id").(string)
		var warningDiag diag.Diagnostics
		if rangerConfigID == "" {
			warningDiag = ClearRangerV2(ctx, clusterAPI, clusterID)
		} else {
			warningDiag = ApplyRangerV2(ctx, clusterAPI, clusterID, rangerConfigID)
		}
		if warningDiag != nil {
			return warningDiag
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

func HandleChangedClusterSchedulingPolicy(ctx context.Context, api cluster.IClusterAPI, d *schema.ResourceData) diag.Diagnostics {

	clusterId := d.Id()
	if clusterId == "" {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  "Resource id not found",
				Detail:   "Handle changed cluster scheduling policy, cluster id can`t be empty",
			},
		}
	}
	policyExtraInfo := make(map[string]string)
	if v, ok := d.GetOk("scheduling_policy_extra_info"); ok {
		for k, v := range v.(map[string]interface{}) {
			policyExtraInfo[k] = v.(string)
		}
	}
	o, n := d.GetChange("scheduling_policy")
	opMap := make(map[string]map[string]interface{})
	npMap := make(map[string]map[string]interface{})
	if o != nil && len(o.([]interface{})) > 0 {
		for _, item := range o.([]interface{}) {
			m := item.(map[string]interface{})
			opMap[m["policy_name"].(string)] = m
		}
	}
	if n != nil && len(n.([]interface{})) > 0 {
		for _, item := range n.([]interface{}) {
			m := item.(map[string]interface{})
			npMap[m["policy_name"].(string)] = m
		}
	}

	newPolicies := make([]map[string]interface{}, 0)
	updatedPolicies := make([]map[string]interface{}, 0)
	deletedPolicies := make([]map[string]interface{}, 0)

	keys := []string{"policy_name", "description", "time_zone", "resume_at", "suspend_at"}
	for k, nv := range npMap {
		if opMap[k] == nil {
			newPolicies = append(newPolicies, nv)
		} else {
			ov := opMap[k]

			changed := false
			if ov["enable"].(bool) != nv["enable"].(bool) {
				updatedPolicies = append(updatedPolicies, nv)
				changed = true
			}
			if !changed {
				for _, k2 := range keys {
					if nv[k2].(string) != ov[k2].(string) {
						updatedPolicies = append(updatedPolicies, nv)
						changed = true
						break
					}
				}
			}

			if !changed {
				ovdays := make([]string, 0)
				for _, item := range ov["active_days"].(*schema.Set).List() {
					ovdays = append(ovdays, item.(string))
				}
				sort.Strings(ovdays)

				nvdays := make([]string, 0)
				for _, item := range nv["active_days"].(*schema.Set).List() {
					nvdays = append(nvdays, item.(string))
				}
				sort.Strings(nvdays)

				ov_active_days_str := strings.Join(ovdays, ",")
				nv_active_days_str := strings.Join(nvdays, ",")

				if ov_active_days_str != nv_active_days_str {
					updatedPolicies = append(updatedPolicies, nv)
				}
			}
		}
	}

	for k, ov := range opMap {
		if npMap[k] == nil {
			deletedPolicies = append(deletedPolicies, ov)
		}
	}

	for _, item := range deletedPolicies {
		policyId := policyExtraInfo[item["policy_name"].(string)]
		err := DeleteClusterSchedulingPolicy(ctx, api, clusterId, policyId)
		if err != nil {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  "Failed to delete cluster scheduling policy",
					Detail:   fmt.Sprintf("Failed to delete cluster scheduling policy, clusterId:%s policyItem:%+v err:%s", clusterId, item, err.Error()),
				},
			}
		}
	}

	for _, item := range updatedPolicies {
		policyId := policyExtraInfo[item["policy_name"].(string)]
		err := ModifyClusterSchedulingPolicy(ctx, api, clusterId, policyId, item)
		if err != nil {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  "Failed to modify cluster scheduling policy",
					Detail:   fmt.Sprintf("Failed to modify cluster scheduling policy, clusterId:%s policyItem:%+v err:%s", clusterId, item, err.Error()),
				},
			}
		}
	}

	for _, item := range newPolicies {
		err := SaveClusterSchedulingPolicy(ctx, api, clusterId, item)
		if err != nil {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  "Failed to add cluster scheduling policy",
					Detail:   fmt.Sprintf("Failed to add cluster scheduling policy, clusterId:%s policyItem:%+v err:%s", clusterId, item, err.Error()),
				},
			}
		}
	}
	return nil
}

func ListClusterSchedulingPolicy(ctx context.Context, api cluster.IClusterAPI, clusterId string) ([]map[string]interface{}, map[string]string, error) {

	policyResp, err := api.ListClusterSchedulePolicy(ctx, &cluster.ListClusterSchedulePolicyReq{
		ClusterId: clusterId,
	})
	if err != nil {
		log.Printf("[ERROR] list cluster scheduling policy failed, cluster[%s] err:%+v", clusterId, err)
		return nil, nil, err
	}

	policyList := make([]map[string]interface{}, 0)
	policyExtraInfo := make(map[string]string)

	if len(policyResp.SchedulePolicies) > 0 {
		for _, item := range policyResp.SchedulePolicies {
			m := map[string]interface{}{
				"policy_name": item.PolicyName,
				"description": item.Description,
				"time_zone":   item.TimeZone,
				"active_days": strings.Split(item.ActiveDateValue, ","),
				"enable":      item.State == int32(1),
			}
			if item.ResumeAt != "" {
				m["resume_at"] = item.ResumeAt
			}
			if item.SuspendAt != "" {
				m["suspend_at"] = item.SuspendAt
			}

			policyList = append(policyList, m)
			policyExtraInfo[item.PolicyName] = item.PolicyId
		}
	}
	return policyList, policyExtraInfo, nil
}

func SaveClusterSchedulingPolicy(ctx context.Context, api cluster.IClusterAPI, clusterId string, m map[string]interface{}) error {

	activeDays := m["active_days"].(*schema.Set).List()
	dayArr := make([]string, 0)
	for _, item := range activeDays {
		dayArr = append(dayArr, item.(string))
	}

	resp, err := api.SaveClusterSchedulePolicy(ctx, &cluster.SaveClusterSchedulePolicyReq{
		ClusterId:   clusterId,
		PolicyName:  m["policy_name"].(string),
		Description: m["description"].(string),
		TimeZone:    m["time_zone"].(string),
		ActiveDays:  strings.Join(dayArr, ","),
		ResumeAt:    m["resume_at"].(string),
		SuspendAt:   m["suspend_at"].(string),
		State:       m["enable"].(bool),
	})
	if err != nil {
		log.Printf("[ERROR] save cluster scheduling policy failed,cluster[%s] paramMap:%+v  err:%+v", clusterId, m, err)
		return err
	}
	log.Printf("[DEBUG] save cluster scheduling policy, cluster[%s] paramMap:%+v resp:%+v", clusterId, m, resp)
	return nil
}

func ModifyClusterSchedulingPolicy(ctx context.Context, api cluster.IClusterAPI, clusterId string, policyId string, m map[string]interface{}) error {

	activeDays := m["active_days"].(*schema.Set).List()
	dayArr := make([]string, 0)
	for _, item := range activeDays {
		dayArr = append(dayArr, item.(string))
	}

	err := api.ModifyClusterSchedulePolicy(ctx, &cluster.ModifyClusterSchedulePolicyReq{
		ClusterId:   clusterId,
		PolicyId:    policyId,
		PolicyName:  m["policy_name"].(string),
		Description: m["description"].(string),
		TimeZone:    m["time_zone"].(string),
		ActiveDays:  strings.Join(dayArr, ","),
		ResumeAt:    m["resume_at"].(string),
		SuspendAt:   m["suspend_at"].(string),
		State:       m["enable"].(bool),
	})
	if err != nil {
		log.Printf("[ERROR] modify cluster scheduling policy failed,cluster[%s] paramMap:%+v  err:%+v", clusterId, m, err)
		return err
	}
	log.Printf("[DEBUG] modify cluster scheduling policy, cluster[%s] paramMap:%+v", clusterId, m)
	return nil
}

func DeleteClusterSchedulingPolicy(ctx context.Context, api cluster.IClusterAPI, clusterId string, policyId string) error {

	err := api.DeleteClusterSchedulePolicy(ctx, &cluster.DeleteClusterSchedulePolicyReq{
		ClusterId: clusterId,
		PolicyId:  policyId,
	})
	if err != nil {
		log.Printf("[ERROR] delete cluster scheduling policy failed,cluster[%s] policyId:%s err:%+v", clusterId, policyId, err)
		return err
	}
	log.Printf("[DEBUG] delete cluster scheduling policy, cluster[%s] policyId:%s", clusterId, policyId)
	return nil
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

	if err != nil {
		log.Printf("[WARN] Update cluster config failed, e:%v", err)
	}
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
	err := clusterAPI.CheckRangerCert(ctx, &cluster.CheckRangerCertsReq{
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

type VolumeParamVerifyReq struct {
	ClusterAPI   cluster.IClusterAPI
	VolumeCate   string
	VolumeConfig map[string]interface{}
}

func VolumeParamVerify(ctx context.Context, req *VolumeParamVerifyReq) error {
	clusterAPI := req.ClusterAPI
	volumeCate := req.VolumeCate
	volumeConfig := req.VolumeConfig
	volumeSize := int64(volumeConfig["vol_size"].(int))
	volumeNum := int32(1)
	iops := int64(volumeConfig["iops"].(int))
	throughput := int64(volumeConfig["throughput"].(int))
	err := clusterAPI.VolumeParamVerification(ctx, &cluster.ModifyClusterVolumeReq{
		VmVolCate:  volumeCate,
		VmVolSize:  volumeSize,
		VmVolNum:   volumeNum,
		Iops:       iops,
		Throughput: throughput,
	})
	return err
}

func IsInternalTagKeys(csp, key string) bool {
	switch csp {
	case cluster.CSP_AWS:
		return AwsInternalTagKeys[key]
	case cluster.CSP_AZURE:
		return AzureInternalTagKeys[key]
	case cluster.CSP_GOOGLE:
		return GcpInternalTagKeys[key]
	default:
		return AwsInternalTagKeys[key] || AzureInternalTagKeys[key] || GcpInternalTagKeys[key]
	}
}

var AwsInternalTagKeys = map[string]bool{
	"Vendor":                  true,
	"Creator":                 true,
	"ClusterName":             true,
	"WarehouseName":           true,
	"CelerdataManaged":        true,
	"Name":                    true,
	"ProcessType":             true,
	"CelerDataCloudAccountID": true,
	"ActionID":                true,
	"Workspace":               true,
	"ServiceAccountName":      true,
	"ServiceAccountID":        true,
}

var AzureInternalTagKeys = map[string]bool{
	"Vendor":                  true,
	"Creator":                 true,
	"ClusterName":             true,
	"WarehouseName":           true,
	"CelerdataManaged":        true,
	"ProcessType":             true,
	"CelerDataCloudAccountID": true,
	"ServiceAccountName":      true,
	"ServiceAccountID":        true,
}

var GcpInternalTagKeys = map[string]bool{
	"vendor":                         true,
	"creator":                        true,
	"cluster-id":                     true,
	"cluster-name":                   true,
	"warehouse-name":                 true,
	"celerdata-managed":              true,
	"name":                           true,
	"process-type":                   true,
	"celerdata-cloud-account-id":     true,
	"celerdata-action-id":            true,
	"celerdata-workspace":            true,
	"celerdata-service-account-name": true,
	"celerdata-service-account-id":   true,
}

func HandleChangedGlobalSqlSessionVariables(ctx context.Context, api cluster.IClusterAPI, d *schema.ResourceData) diag.Diagnostics {

	clusterId := d.Id()
	if clusterId == "" {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  "Resource id not found",
				Detail:   "Handle changed cluster scheduling policy, cluster id can`t be empty",
			},
		}
	}

	o, n := d.GetChange("global_session_variables")
	oMap := make(map[string]string)
	nMap := make(map[string]string)
	if o != nil && len(o.(map[string]interface{})) > 0 {
		for k, v := range o.(map[string]interface{}) {
			oMap[k] = v.(string)
		}
	}
	if n != nil && len(n.(map[string]interface{})) > 0 {
		for k, v := range n.(map[string]interface{}) {
			nMap[k] = v.(string)
		}
	}

	updatedVariables := make(map[string]string)
	removedVariables := make([]string, 0)

	for k, nv := range nMap {
		if ov, ok := oMap[k]; !ok || nv != ov {
			updatedVariables[k] = nv
		}
	}

	for k, _ := range oMap {
		if _, ok := nMap[k]; !ok {
			removedVariables = append(removedVariables, k)
		}
	}

	if len(updatedVariables) > 0 {
		resp, err := api.SetGlobalSqlSessionVariables(ctx, &cluster.SetGlobalSqlSessionVariablesReq{
			ClusterId: clusterId,
			Variables: updatedVariables,
		})
		if err != nil {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  "Failed to update global session variables",
					Detail:   fmt.Sprintf("ErrMsg:%s", err.Error()),
				},
			}
		}
		if resp.HasFailed {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  "Failed to update global session variables",
					Detail:   fmt.Sprintf("ErrMsg:%s", strings.Join(resp.ErrMsgArr, "\n")),
				},
			}
		}
	}

	if len(removedVariables) > 0 {
		resp, err := api.ResetGlobalSqlSessionVariables(ctx, &cluster.ResetGlobalSqlSessionVariablesReq{
			ClusterId: clusterId,
			Variables: removedVariables,
		})
		if err != nil {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  "Failed to reset global session variables",
					Detail:   fmt.Sprintf("ErrMsg:%s", err.Error()),
				},
			}
		}
		if resp.HasFailed {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  "Failed to reset global session variables",
					Detail:   fmt.Sprintf("ErrMsg:%s", strings.Join(resp.ErrMsgArr, "\n")),
				},
			}
		}
	}
	return nil
}

func SetGlobalSqlSessionVariables(ctx context.Context, v interface{}, clusterAPI cluster.IClusterAPI, clusterId string) diag.Diagnostics {
	configMap := v.(map[string]interface{})
	configs := make(map[string]string, 0)
	for k, v := range configMap {
		configs[k] = v.(string)
	}

	setVariablesResp, err := clusterAPI.SetGlobalSqlSessionVariables(ctx, &cluster.SetGlobalSqlSessionVariablesReq{
		ClusterId: clusterId,
		Variables: configs,
	})
	if err != nil {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  "Set global sql session variables error",
				Detail:   err.Error(),
			},
		}
	}

	if setVariablesResp.HasFailed {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  "Set global sql session variables failed",
				Detail:   strings.Join(setVariablesResp.ErrMsgArr, "\n"),
			},
		}
	}
	return nil
}

func GetGlobalSqlSessionVariables(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterID string, v interface{}) (*cluster.GetGlobalSqlSessionVariablesResp, diag.Diagnostics) {
	variables := make([]string, 0)
	for k, _ := range v.(map[string]interface{}) {
		variables = append(variables, k)
	}

	sessionVariablesResp, err := clusterAPI.GetGlobalSqlSessionVariables(ctx, &cluster.GetGlobalSqlSessionVariablesReq{
		ClusterId: clusterID,
		Variables: variables,
	})
	if err != nil {
		log.Printf("[ERROR] query cluster global session variables failed, err:%+v", err)
		return nil, diag.FromErr(err)
	}
	return sessionVariablesResp, nil
}

type RunScriptsReq struct {
	ResourceData       *schema.ResourceData
	ClusterAPI         cluster.IClusterAPI
	ClusterID          string
	RunScriptsParallel bool
	IsCreate           bool
}

func RunScripts(ctx context.Context, req RunScriptsReq) diag.Diagnostics {

	d := req.ResourceData
	scripts := make([]*cluster.Script, 0)
	var scriptList []interface{}
	if req.IsCreate {
		if v, ok := d.GetOk("scripts"); ok {
			scriptList = v.(*schema.Set).List()
		}
	} else {
		_, n := d.GetChange("scripts")
		for _, item := range n.(*schema.Set).List() {
			m := item.(map[string]interface{})
			if m["rerun"].(bool) {
				scriptList = append(scriptList, item)
			}
		}
	}

	for _, v := range scriptList {
		scriptMap := v.(map[string]interface{})
		scriptPath := scriptMap["script_path"].(string)
		logsDir := scriptMap["logs_dir"].(string)
		if len(scriptPath) == 0 {
			continue
		}
		scripts = append(scripts, &cluster.Script{
			ScriptPath: scriptPath,
			LogsDir:    logsDir,
		})
	}

	clusterAPI := req.ClusterAPI
	clusterID := req.ClusterID
	runScriptsParallel := req.RunScriptsParallel
	if len(scripts) > 0 {
		err := clusterAPI.RunScripts(ctx, &cluster.RunScriptsReq{
			ClusterId:          clusterID,
			Scripts:            scripts,
			RunScriptsParallel: runScriptsParallel,
		})
		if err != nil {
			log.Printf("[ERROR] run script failed, err:%+v", err)
			return diag.FromErr(fmt.Errorf("run script failed, err:%s", err.Error()))
		}
	}
	return nil
}

func ApplyRangerV2(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterID, rangerConfID string) diag.Diagnostics {
	summary := "Failed to apply ranger config."
	resp, err := clusterAPI.ApplyRangerConfigV2(ctx, &cluster.ApplyRangerConfigV2Req{
		ClusterID: clusterID,
		BizID:     rangerConfID,
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

	log.Printf("[DEBUG] ApplyRangerConfigV2 succeeded, action id:%s, cluster id:%s, rangerConfID:%s", resp.InfraActionID, clusterID, rangerConfID)

	if len(resp.InfraActionID) > 0 {
		infraActionResp, err := WaitClusterInfraActionStateChangeComplete(ctx, &waitStateReq{
			clusterAPI: clusterAPI,
			clusterID:  clusterID,
			actionID:   resp.InfraActionID,
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

func ClearRangerV2(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterID string) diag.Diagnostics {
	summary := "Failed to clear ranger config."
	resp, err := clusterAPI.CleanRangerConfigV2(ctx, &cluster.CleanRangerConfigV2Req{
		ClusterID: clusterID,
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

	if len(resp.InfraActionID) > 0 {
		infraActionResp, err := WaitClusterInfraActionStateChangeComplete(ctx, &waitStateReq{
			clusterAPI: clusterAPI,
			clusterID:  clusterID,
			actionID:   resp.InfraActionID,
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
