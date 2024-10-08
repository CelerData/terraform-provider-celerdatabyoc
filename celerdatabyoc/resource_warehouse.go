package celerdatabyoc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"
	"terraform-provider-celerdatabyoc/common"
	"time"

	"github.com/hashicorp/go-cty/cty"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/mitchellh/mapstructure"
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
				ForceNew: true,
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
			"auto_scaling_policies": {
				Type:     schema.TypeSet,
				Optional: true,
				ValidateDiagFunc: func(i interface{}, p cty.Path) diag.Diagnostics {
					var diags diag.Diagnostics
					v := i.([]interface{})
					if len(v) > 1 {
						diags = append(diags, diag.Diagnostic{
							Severity: diag.Error,
							Summary:  fmt.Sprintf("`auto_scaling_policies` cannot have more than one item, got %d", len(v)),
						})
					}
					return diags
				},
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"biz_id": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"min_size": {
							Type:         schema.TypeInt,
							Required:     true,
							ValidateFunc: validation.IntAtLeast(1),
						},
						"max_size": {
							Type:         schema.TypeInt,
							Required:     true,
							ValidateFunc: validation.IntAtLeast(1),
						},
						"policy_item": {
							Type:     schema.TypeSet,
							Required: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"type": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.StringInSlice([]string{"SCALE_OUT", "SCALE_IN"}, false),
									},
									"step_size": {
										Type:         schema.TypeInt,
										Required:     true,
										ValidateFunc: validation.IntAtLeast(1),
									},
									"conditions": {
										Type:     schema.TypeSet,
										Optional: true,
										Elem: &schema.Resource{
											Schema: map[string]*schema.Schema{
												"type": {
													Type:         schema.TypeString,
													Required:     true,
													ValidateFunc: validation.StringInSlice([]string{"AVERAGE_CPU_UTILIZATION"}, false),
												},
												"duration_seconds": {
													Type:         schema.TypeInt,
													Required:     true,
													ValidateFunc: validation.IntAtLeast(300),
												},
												"value": {
													Type:         schema.TypeFloat,
													Required:     true,
													ValidateFunc: validation.FloatAtLeast(0.00),
												},
											},
										},
									},
								},
							},
						},
					},
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
	var result map[string]interface{}
	if policy != nil {
		err := mapstructure.Decode(policy, &result)
		if err != nil {
			log.Printf("[ERROR] Decode warehouse auto scaling config failed, warehouseId:%s", warehouseId)
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  fmt.Sprintf("Failed to decode warehouse auto scaling config, warehouseId:[%s] ", warehouseId),
					Detail:   err.Error(),
				},
			}
		} else {
			d.Set("auto_scaling_policies", result)
		}
	} else {
		d.Set("auto_scaling_policies", result)
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

	warehouseId := resp.WarehouseId

	d.SetId(warehouseId)
	infraActionId := resp.ActionID
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

		summary := fmt.Sprintf("Create warehouse[%s] of the cluster[%s] failed", warehouseName, clusterId)

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

			summary := fmt.Sprintf("Suspend warehouse[%s] of the cluster[%s] failed", warehouseName, clusterId)

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
			return diag.FromErr(fmt.Errorf("Failed to scale warehouse size, clusterId:%s warehouseId:%s, errMsg:%s", clusterId, warehouseId, err))
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
			return diag.FromErr(fmt.Errorf("Waiting for cluster (%s) running: %s", d.Id(), err))
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
			return diag.FromErr(fmt.Errorf("Failed to scale warehouse number, clusterId:%s warehouseId:%s, errMsg:%s", clusterId, warehouseId, err))
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
			return diag.FromErr(fmt.Errorf("Waiting for cluster (%s) running: %s", d.Id(), err))
		}

		if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
			return diag.FromErr(errors.New(stateResp.AbnormalReason))
		}
	}

	if d.Get("compute_node_is_instance_store").(bool) && (d.HasChange("compute_node_ebs_disk_number") || d.HasChange("compute_node_ebs_disk_per_size")) {
		return diag.FromErr(errors.New("local storage model does not support ebs disk"))
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

	if d.HasChange("auto_scaling_policies") {
		if _, ok := d.GetOk("auto_scaling_policies"); ok {
			// -----todo
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

		summary := fmt.Sprintf("Release warehouse[%s] of the cluster[%s] failed", warehouseName, clusterId)

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

		summary := fmt.Sprintf("Suspend warehouse[%s] of the cluster[%s] failed", warehouseName, clusterId)

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

		summary := fmt.Sprintf("Resume warehouse[%s] of the cluster[%s] failed", warehouseName, clusterId)

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
	return diags
}
