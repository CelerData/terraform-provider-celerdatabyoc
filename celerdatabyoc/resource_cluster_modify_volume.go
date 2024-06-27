package celerdatabyoc

import (
	"context"
	"fmt"
	"log"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func resourceClusterModifyVolume() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceClusterModifyVolumeCreate,
		ReadContext:   resourceClusterModifyVolumeRead,
		DeleteContext: resourceClusterModifyVolumeDelete,
		Schema: map[string]*schema.Schema{
			"cluster_id": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},
			"node_type": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringInSlice(cluster.SupportedConfigType, false),
			},
			"vol_cate": {
				Type:         schema.TypeString,
				ForceNew:     true,
				Optional:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},
			"vol_size": {
				Type:         schema.TypeInt,
				ForceNew:     true,
				Optional:     true,
				ValidateFunc: validation.IntAtLeast(1),
			},
			"iops": {
				Type:         schema.TypeInt,
				ForceNew:     true,
				Optional:     true,
				ValidateFunc: validation.IntAtLeast(1),
			},
			"throughput": {
				Type:         schema.TypeInt,
				ForceNew:     true,
				Optional:     true,
				ValidateFunc: validation.IntAtLeast(1),
			},
			"result": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Modify volume detail execution result",
			},
		},
	}
}

func resourceClusterModifyVolumeCreate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {

	c := m.(*client.CelerdataClient)
	clusterAPI := cluster.NewClustersAPI(c)

	clusterId := d.Get("cluster_id").(string)
	nodeTypeStr := d.Get("node_type").(string)
	nodeType := cluster.ConvertStrToClusterModuleType(nodeTypeStr)

	if nodeType == cluster.ClusterModuleTypeUnknown {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "Unsupported node type",
				Detail:   fmt.Sprintf("nodeType:%s is invalid", nodeTypeStr),
			},
		}
	}

	req := &cluster.ModifyClusterVolumeReq{
		ClusterId: clusterId,
		Type:      nodeType,
	}

	if v, ok := d.GetOk("vol_cate"); ok {
		req.VmVolCate = v.(string)
	}
	if v, ok := d.GetOk("vol_size"); ok {
		req.VmVolSize = int64(v.(int))
	}
	if v, ok := d.GetOk("iops"); ok {
		req.Iops = int64(v.(int))
	}
	if v, ok := d.GetOk("throughput"); ok {
		req.Throughput = int64(v.(int))
	}

	log.Printf("[DEBUG] modify cluster volume detail, req:%+v", req)
	resp, err := clusterAPI.ModifyClusterVolume(ctx, req)
	if err != nil {
		log.Printf("[ERROR] modify cluster volume detail failed, err:%+v", err)
		return diag.FromErr(err)
	}

	infraActionId := resp.ActionID

	d.SetId(infraActionId)

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

		summary := fmt.Sprintf("Modify %s node volume detail of the cluster[%s] failed", nodeType, clusterId)

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

func resourceClusterModifyVolumeRead(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	clusterAPI := cluster.NewClustersAPI(c)
	infraActionId := d.Id()
	result := ""
	if len(infraActionId) > 0 {

		clusterId := d.Get("cluster_id").(string)
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

		if err != nil {
			log.Printf("[ERROR] query modify cluster volume detail infra action failed, infraActionId:%s", infraActionId)
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Warning,
					Summary:  fmt.Sprintf("Failed to get cluster infra action info, actionId:[%s] ", infraActionId),
					Detail:   err.Error(),
				},
			}
		}

		if infraActionResp.InfraActionState == string(cluster.ClusterInfraActionStateFailed) {
			log.Printf("[INFO] clean failed infra action, infraActionId:%s", infraActionId)
			d.SetId("")
			return diags
		}
		result = infraActionResp.InfraActionState
		d.Set("result", result)
	}
	return diags
}

func resourceClusterModifyVolumeDelete(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	d.SetId("")
	return diags
}
