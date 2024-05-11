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
			"be_disk_per_size": {
				Description: "Specifies the size of a single disk in GB. The default size for per disk is 100GB.",
				Type:        schema.TypeInt,
				Optional:    true,
				Default:     100,
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
			"be_disk_number": {
				Description: "Specifies the number of disk. The default value is 2.",
				Type:        schema.TypeInt,
				ForceNew:    true,
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
						if v < 60 || v > 999999 {
							errors = append(errors, fmt.Errorf("the %s range should be [60,999999]", k))
							return warnings, errors
						}
					}
					return warnings, errors
				},
			},
		},
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
	}
}

func resourceClusterCreate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	clusterAPI := cluster.NewClustersAPI(c)
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
		Num:           uint32(d.Get("fe_node_count").(int)),
		StorageSizeGB: 50,
		InstanceType:  d.Get("fe_instance_type").(string),
	})

	clusterConf.ClusterItems = append(clusterConf.ClusterItems, &cluster.ClusterItem{
		Type:         cluster.ClusterModuleTypeBE,
		Name:         "BE",
		Num:          uint32(d.Get("be_node_count").(int)),
		InstanceType: d.Get("be_instance_type").(string),
		// StorageSizeGB: uint64(d.Get("be_storage_size_gb").(int)),
		DiskInfo: &cluster.DiskInfo{
			Number:  uint32(d.Get("be_disk_number").(int)),
			PerSize: uint64(d.Get("be_disk_per_size").(int)),
		},
	})

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
		timeout:    30 * time.Minute,
		pendingStates: []string{
			string(cluster.ClusterStateDeploying),
			string(cluster.ClusterStateScaling),
			string(cluster.ClusterStateResuming),
			string(cluster.ClusterStateSuspending),
			string(cluster.ClusterStateReleasing),
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

	if d.Get("idle_suspend_interval").(int) > 0 {
		enable := true
		clusterId := resp.ClusterID
		intervalTimeMills := uint64(d.Get("idle_suspend_interval").(int) * 60 * 1000)
		errDiag := UpdateClusterIdleConfig(ctx, clusterAPI, clusterId, intervalTimeMills, enable)
		if errDiag != nil {
			return errDiag
		}
	}

	if d.Get("expected_cluster_state").(string) == string(cluster.ClusterStateSuspended) {
		errDiag := UpdateClusterState(ctx, clusterAPI, d.Get("id").(string), string(cluster.ClusterStateRunning), string(cluster.ClusterStateSuspended))
		if errDiag != nil {
			return errDiag
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

	log.Printf("[DEBUG] get cluster, resp:%+v", resp.Cluster)
	d.Set("cluster_state", resp.Cluster.ClusterState)
	d.Set("expected_cluster_state", resp.Cluster.ClusterState)
	d.Set("cluster_name", resp.Cluster.ClusterName)
	d.Set("data_credential_id", resp.Cluster.DataCredID)
	d.Set("network_id", resp.Cluster.NetIfaceID)
	d.Set("deployment_credential_id", resp.Cluster.DeployCredID)
	d.Set("be_instance_type", resp.Cluster.BeModule.InstanceType)
	d.Set("be_node_count", int(resp.Cluster.BeModule.Num))
	d.Set("be_disk_number", int(resp.Cluster.BeModule.VmVolNum))
	d.Set("be_disk_per_size", int(resp.Cluster.BeModule.VmVolSizeGB))
	d.Set("fe_instance_type", resp.Cluster.FeModule.InstanceType)
	d.Set("fe_node_count", int(resp.Cluster.FeModule.Num))
	d.Set("free_tier", resp.Cluster.FreeTier)
	d.Set("query_port", resp.Cluster.QueryPort)
	d.Set("idle_suspend_interval", resp.Cluster.IdleSuspendInterval)
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

	c := m.(*client.CelerdataClient)

	// Warning or errors can be collected in a slice type
	clusterID := d.Id()
	clusterAPI := cluster.NewClustersAPI(c)
	log.Printf("[DEBUG] resourceClusterUpdate cluster id:%s", clusterID)
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
			timeout:       30 * time.Minute,
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
			timeout:       30 * time.Minute,
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
			timeout:       30 * time.Minute,
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
			timeout:       30 * time.Minute,
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

	if (d.HasChange("be_disk_number") || d.HasChange("be_disk_per_size")) && !d.IsNewResource() {
		o1, n1 := d.GetChange("be_disk_number")
		o2, n2 := d.GetChange("be_disk_per_size")

		if (n1.(int) * n2.(int)) < (o1.(int) * o2.(int)) {
			return diag.FromErr(fmt.Errorf("total be storage size: %dGB => %dGB, be storage size does not support decrease", o1.(int)*o2.(int), n1.(int)*n2.(int)))
		}

		resp, err := clusterAPI.IncrStorageSize(ctx, &cluster.IncrStorageSizeReq{
			RequestId:  uuid.NewString(),
			ClusterId:  clusterID,
			ModuleType: cluster.ClusterModuleTypeBE,
			// StorageSizeGB: int64(n.(int)),
			DiskInfo: &cluster.DiskInfo{
				Number:  uint32(n1.(int)),
				PerSize: uint64(n2.(int)),
			},
		})
		if err != nil {
			return diag.FromErr(fmt.Errorf("cluster (%s) failed to increase be storage size: %s", d.Id(), err))
		}

		stateResp, err := WaitClusterStateChangeComplete(ctx, &waitStateReq{
			clusterAPI:    clusterAPI,
			actionID:      resp.ActionId,
			clusterID:     clusterID,
			timeout:       30 * time.Minute,
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

	if needSuspend(d) {
		o, n := d.GetChange("expected_cluster_state")
		errDiag := UpdateClusterState(ctx, clusterAPI, d.Get("id").(string), o.(string), n.(string))
		if errDiag != nil {
			return errDiag
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
	resp, err := clusterAPI.Suspend(ctx, &cluster.SuspendReq{ClusterID: clusterID})
	if err != nil {
		return diag.FromErr(err)
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
		return diag.FromErr(fmt.Errorf("waiting for cluster (%s) suspend: %s", clusterID, err.Error()))
	}

	if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
		return diag.FromErr(errors.New(stateResp.AbnormalReason))
	}

	return nil
}
func ResumeWithContext(ctx context.Context, clusterAPI cluster.IClusterAPI, clusterID string) diag.Diagnostics {
	log.Printf("[DEBUG] resume cluster: %s", clusterID)
	resp, err := clusterAPI.Resume(ctx, &cluster.ResumeReq{ClusterID: clusterID})
	if err != nil {
		return diag.FromErr(err)
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
		return diag.FromErr(fmt.Errorf("waiting for cluster (%s) resume: %s", clusterID, err.Error()))
	}

	if stateResp.ClusterState == string(cluster.ClusterStateAbnormal) {
		return diag.FromErr(errors.New(stateResp.AbnormalReason))
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
		return diag.FromErr(fmt.Errorf("set cluster idle suspend interval failed, errMsg:%s", err.Error()))
	}
	return nil
}
