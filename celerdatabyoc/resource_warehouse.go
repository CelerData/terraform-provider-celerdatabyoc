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
			"compute_node_is_instance_store": {
				Type:     schema.TypeBool,
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
		log.Printf("[ERROR] query warehouse info failed, warehouseId:%s", warehouseId)
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Warning,
				Summary:  fmt.Sprintf("Failed to get warehouse info, warehouseId:[%s] ", warehouseId),
				Detail:   err.Error(),
			},
		}
	}

	if resp.Info == nil {
		d.SetId("")
		return diags
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

	log.Printf("[DEBUG] create warehouse, req:%+v", req)
	resp, err := clusterAPI.CreateWarehouse(ctx, req)
	if err != nil {
		log.Printf("[ERROR] create warehouse failed, err:%+v", err)
		return diag.FromErr(err)
	}

	d.SetId(resp.WarehouseId)
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

		d.Set("result", infraActionResp.InfraActionState)
	}

	return diags
}

func resourceWarehouseUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {

	c := m.(*client.CelerdataClient)
	clusterAPI := cluster.NewClustersAPI(c)
	warehouseId := d.Id()
	clusterId := d.Get("cluster_id").(string)

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

	if d.Get("compute_node_is_instance_store").(bool) && (d.HasChange("compute_node_ebs_disk_number") || d.HasChange("compute_node_ebs_disk_per_size")) {
		return diag.FromErr(errors.New("local storage model does not support ebs disk"))
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
