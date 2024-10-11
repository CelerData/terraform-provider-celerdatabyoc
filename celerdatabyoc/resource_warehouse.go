package celerdatabyoc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"
	"terraform-provider-celerdatabyoc/common"

	"github.com/hashicorp/go-cty/cty"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func resourceWarehouse() *schema.Resource {
	return &schema.Resource{
		ReadContext:   resourceWarehouseRead,
		CreateContext: resourceWarehouseCreate,
		UpdateContext: resourceWarehouseUpdate,
		DeleteContext: resourceWarehouseDelete,
		Schema: map[string]*schema.Schema{
			"cluster_id": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},
			"warehouse_name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Warehouse name",
				ValidateDiagFunc: func(v interface{}, p cty.Path) diag.Diagnostics {
					reg, _ := regexp.Compile("^[a-zA-Z][a-zA-Z0-9_]{0,31}$")

					var diags diag.Diagnostics
					if !reg.MatchString(v.(string)) {
						diags = append(diags, diag.Diagnostic{
							Severity: diag.Error,
							Summary:  "The name must start with a letter, can only contain letters, numbers and underscores. The maximum length is 32.",
						})
					}
					return diags
				},
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
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
				Type:     schema.TypeInt,
				Optional: true,
				Default:  100,
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
				Description: "Specifies the size of a single disk in GB. The default size for per disk is 100GB.",
			},
			"compute_node_ebs_disk_number": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  2,
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
				Description: "Specifies the number of disk. The default value is 2.",
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
				Type:         schema.TypeString,
				Optional:     true,
				Default:      string(cluster.ClusterStateRunning),
				ValidateFunc: validation.StringInSlice([]string{string(cluster.ClusterStateSuspended), string(cluster.ClusterStateRunning)}, false),
			},
			"compute_node_is_instance_store": {
				Type:     schema.TypeBool,
				Computed: true,
			},
			"state": {
				Type:     schema.TypeString,
				Computed: true,
			},
		},
	}
}

func resourceWarehouseRead(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	warehouseId := d.Id()
	clusterAPI := cluster.NewClustersAPI(c)
	resp, err := clusterAPI.GetWarehouse(ctx, &cluster.GetWarehouseReq{
		WarehouseId: warehouseId,
	})

	if err != nil {
		log.Printf("[ERROR] Query warehouse info failed, warehouseId:%s", warehouseId)
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  fmt.Sprintf("Failed to get warehouse info, warehouseId:[%s] ", warehouseId),
				Detail:   err.Error(),
			},
		}
	}

	if resp.Info == nil {
		d.SetId("")
		return diags
	}

	idleConfigResp, err := clusterAPI.GetWarehouseIdleConfig(ctx, &cluster.GetWarehouseIdleConfigReq{
		WarehouseId: warehouseId,
	})
	if err != nil {
		log.Printf("[ERROR] Query warehouse idle config failed, warehouseId:%s", warehouseId)
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  fmt.Sprintf("Failed to get warehouse idle config, warehouseId:[%s] ", warehouseId),
				Detail:   err.Error(),
			},
		}
	}

	idleConfig := idleConfigResp.Config
	if idleConfig != nil && idleConfig.State {
		d.Set("idle_suspend_interval", idleConfig.IntervalMs)
	} else {
		d.Set("idle_suspend_interval", 0)
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
	if policy != nil {
		bytes, _ := json.Marshal(policy)
		d.Set("auto_scaling_policy", string(bytes))
	}

	warehouseInfo := resp.Info

	d.Set("warehouse_name", warehouseInfo.WarehouseName)
	d.Set("compute_node_size", warehouseInfo.VmCate)
	d.Set("compute_node_count", warehouseInfo.NodeCount)
	d.Set("compute_node_is_instance_store", warehouseInfo.IsInstanceStore)
	if !warehouseInfo.IsInstanceStore {
		d.Set("compute_node_ebs_disk_number", warehouseInfo.VmVolNum)
		d.Set("compute_node_ebs_disk_per_size", warehouseInfo.VmVolSizeGB)
	}
	d.Set("expected_state", warehouseInfo.State)
	d.Set("state", warehouseInfo.State)
	return diags
}

func resourceWarehouseCreate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {

	c := m.(*client.CelerdataClient)
	clusterAPI := cluster.NewClustersAPI(c)

	clusterId := d.Get("cluster_id").(string)
	warehouseName := d.Get("warehouse_name").(string)
	req := &cluster.CreateWarehouseReq{
		ClusterId:    clusterId,
		Name:         warehouseName,
		Description:  d.Get("description").(string),
		VmCate:       d.Get("compute_node_size").(string),
		VmNum:        int32(d.Get("compute_node_count").(int)),
		VolumeSizeGB: int64(d.Get("compute_node_ebs_disk_per_size").(int)),
		VolumeNum:    int32(d.Get("compute_node_ebs_disk_number").(int)),
	}

	log.Printf("[DEBUG] Create warehouse, req:%+v", req)
	resp, err := clusterAPI.CreateWarehouse(ctx, req)
	if err != nil {
		log.Printf("[ERROR] Create warehouse failed, err:%+v", err)
		return diag.FromErr(err)
	}
	log.Printf("[DEBUG] Create warehouse, resp:%+v", resp)

	warehouseId := resp.WarehouseId
	d.SetId(warehouseId)
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
			return diag.FromErr(fmt.Errorf(summary))
		}

		if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
			d.SetId("")
			return diag.FromErr(errors.New(stateResp.AbnormalReason))
		}
	}

	if v, ok := d.GetOk("auto_scaling_policy"); ok {
		var autoScalingConfig cluster.WarehouseAutoScalingConfig
		json.Unmarshal([]byte(v.(string)), autoScalingConfig)
		req := &cluster.SaveWarehouseAutoScalingConfigReq{
			ClusterId:                  clusterId,
			WarehouseId:                warehouseId,
			WarehouseAutoScalingConfig: autoScalingConfig,
			State:                      true,
		}
		_, err := clusterAPI.SaveWarehouseAutoScalingConfig(ctx, req)
		if err != nil {
			msg := fmt.Sprintf("Add warehouse auto-scaling configuration failed, errMsg:%s", err.Error())
			log.Printf("[ERROR] %s", msg)
			return diag.FromErr(fmt.Errorf("%s", msg))
		}
	}

	expectedState := d.Get("expected_state").(string)
	if expectedState == string(cluster.ClusterStateSuspended) {
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

			summary := fmt.Sprintf("suspend warehouse[%s] of the cluster[%s] failed", warehouseName, clusterId)
			if err != nil {
				return diag.Diagnostics{
					diag.Diagnostic{
						Severity: diag.Warning,
						Summary:  "Suspend warehouse failed",
						Detail:   fmt.Sprintf("%s. errMsg:%s", summary, err.Error()),
					},
				}
			}

			if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
				return diag.Diagnostics{
					diag.Diagnostic{
						Severity: diag.Warning,
						Summary:  "Suspend warehouse failed",
						Detail:   fmt.Sprintf("%s. errMsg:%s", summary, stateResp.AbnormalReason),
					},
				}
			}
		}
	}

	idleSuspendInterval := d.Get("idle_suspend_interval").(int)
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
					Summary:  "Config warehouse idle config failed",
					Detail:   err.Error(),
				},
			}
		}
	}

	return diags
}

func resourceWarehouseUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {

	c := m.(*client.CelerdataClient)
	clusterAPI := cluster.NewClustersAPI(c)
	warehouseId := d.Id()
	clusterId := d.Get("cluster_id").(string)
	warehouseName := d.Get("warehouse_name").(string)
	expectedState := d.Get("expected_state").(string)

	if d.HasChange("expect_state") {
		if expectedState == string(cluster.ClusterStateRunning) {
			resp := ResumeWarehouse(ctx, clusterAPI, clusterId, warehouseId, warehouseName)
			if resp != nil {
				return resp
			}
		}
	}

	if d.HasChange("compute_node_size") {

		vmCate := d.Get("compute_node_size").(string)
		resp, err := clusterAPI.ScaleWarehouseSize(ctx, &cluster.ScaleWarehouseSizeReq{
			WarehouseId: warehouseId,
			VmCate:      vmCate,
		})

		if err != nil {
			return diag.FromErr(fmt.Errorf("failed to scale warehouse size, clusterId:%s warehouseId:%s, errMsg:%s", clusterId, warehouseId, err))
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

	if d.HasChange("compute_node_count") {

		vmNum := int32(d.Get("compute_node_count").(int))
		resp, err := clusterAPI.ScaleWarehouseNum(ctx, &cluster.ScaleWarehouseNumReq{
			WarehouseId: warehouseId,
			VmNum:       vmNum,
		})

		if err != nil {
			return diag.FromErr(fmt.Errorf("failed to scale warehouse number, clusterId:%s warehouseId:%s, errMsg:%s", clusterId, warehouseId, err))
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

	if !d.Get("compute_node_is_instance_store").(bool) && d.HasChange("compute_node_ebs_disk_number") {
		return diag.FromErr(errors.New("modifying the number of ebs disks is not supported"))
	}

	if d.HasChange("idle_suspend_interval") {
		o, n := d.GetChange("idle_suspend_interval")
		idleSuspendInterval := n.(int)
		if n.(int) == 0 {
			idleSuspendInterval = o.(int)
		}
		err := clusterAPI.UpdateWarehouseIdleConfig(ctx, &cluster.UpdateWarehouseIdleConfigReq{
			WarehouseId: warehouseId,
			IntervalMs:  int64(idleSuspendInterval * 60 * 1000),
			State:       n.(int) > 0,
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

	if d.HasChange("expect_state") {
		if expectedState == string(cluster.ClusterStateSuspended) {
			resp := SuspendWarehouse(ctx, clusterAPI, clusterId, warehouseId, warehouseName)
			if resp != nil {
				return resp
			}
		}
	}

	if d.HasChange("auto_scaling_policy") {
		if v, ok := d.GetOk("auto_scaling_policy"); ok {
			var autoScalingConfig *cluster.WarehouseAutoScalingConfig
			json.Unmarshal([]byte(v.(string)), autoScalingConfig)
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

	return diags
}

func resourceWarehouseDelete(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {

	c := m.(*client.CelerdataClient)
	clusterAPI := cluster.NewClustersAPI(c)
	warehouseId := d.Id()
	clusterId := d.Get("cluster_id").(string)
	warehouseName := d.Get("warehouse_name").(string)

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
			summary := fmt.Sprintf("release warehouse[%s] of the cluster[%s] failed, errMsg:%s", warehouseName, clusterId, err.Error())
			return diag.FromErr(fmt.Errorf(summary))
		}

		if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
			return diag.FromErr(errors.New(stateResp.AbnormalReason))
		}
	}
	d.SetId("")
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
			summary := fmt.Sprintf("suspend warehouse[%s] of the cluster[%s] failed, errMsg:%s", warehouseName, clusterId, err.Error())
			return diag.FromErr(fmt.Errorf(summary))
		}

		if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
			return diag.FromErr(errors.New(stateResp.AbnormalReason))
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
				Severity: diag.Warning,
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

func FromAutoScalingConfigStruct(scalingPolicy *cluster.WarehouseAutoScalingConfig) map[string]interface{} {

	if scalingPolicy == nil || !scalingPolicy.State {
		return nil
	}

	policyItemsMap := make([]map[string]interface{}, 0)

	for _, v := range scalingPolicy.PolicyItem {

		conditionMap := make([]map[string]interface{}, 0)
		for _, c := range v.Conditions {
			floatValue, err := strconv.ParseFloat(c.Value, 64)
			if err != nil {
				log.Printf("[ERROR] parse float failed, v:%s err:%+v", c.Value, err)
			}
			conditionMap = append(conditionMap, map[string]interface{}{
				"type":             "AVERAGE_CPU_UTILIZATION",
				"duration_seconds": c.DurationSeconds,
				"value":            floatValue,
			})
		}

		policyType := "SCALE_OUT"
		if v.Type == int32(cluster.WearhouseScalingType_SCALE_IN) {
			policyType = "SCALE_IN"
		}
		policyItemsMap = append(policyItemsMap, map[string]interface{}{
			"type":      policyType,
			"step_size": v.StepSize,
			"condition": conditionMap,
		})
	}

	return map[string]interface{}{
		"min_size":    scalingPolicy.MinSize,
		"max_size":    scalingPolicy.MaxSize,
		"policy_item": policyItemsMap,
	}
}
