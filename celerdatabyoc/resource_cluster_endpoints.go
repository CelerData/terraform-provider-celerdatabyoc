package celerdatabyoc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/retry"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func resourceClusterEndpoints() *schema.Resource {

	return &schema.Resource{
		CreateContext: resourceClusterEndpointsCreate,
		ReadContext:   resourceClusterEndpointsRead,
		DeleteContext: resourceClusterEndpointsDelete,
		Schema: map[string]*schema.Schema{
			"cluster_id": {
				Type:     schema.TypeString,
				ForceNew: true,
				Required: true,
			},
			"endpoints": {
				Type:        schema.TypeList,
				Computed:    true,
				Description: "Endpoints of the cluster",
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"network_method": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"host": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"port": {
							Type:     schema.TypeInt,
							Computed: true,
						},
						"nlb_endpoint": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"nlb_endpoint_type": {
							Type:     schema.TypeString,
							Computed: true,
						},
					},
				},
			},
		},
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
	}
}

func resourceClusterEndpointsCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)
	clusterAPI := cluster.NewClustersAPI(c)
	clusterId := d.Get("cluster_id").(string)

	log.Printf("[DEBUG] get cluster, cluster[%s]", clusterId)
	resp, err := clusterAPI.Get(ctx, &cluster.GetReq{ClusterID: clusterId})
	if err != nil {
		log.Printf("[ERROR] get cluster, error:%s", err.Error())
		return diag.FromErr(err)
	}

	if resp.Cluster == nil || resp.Cluster.ClusterState == cluster.ClusterStateReleased {
		d.SetId("")
		return diag.FromErr(fmt.Errorf("cluster %s not found", clusterId))
	}

	err = clusterAPI.AllocateClusterEndpoints(ctx, &cluster.AllocateClusterEndpointsReq{
		ClusterId: clusterId,
	})

	if err != nil {
		log.Printf("[ERROR] allocate cluster endpoints failed, error:%s", err.Error())
		return diag.FromErr(err)
	}

	stateResp, err := WaitClusterEndpointsStateChangeComplete(ctx, &waitEndpointsStateReq{
		clusterAPI: clusterAPI,
		clusterId:  clusterId,
		timeout:    30 * time.Minute,
		pendingStates: []string{
			strconv.Itoa(int(cluster.DomainAllocateStateUnknown)),
			strconv.Itoa(int(cluster.DomainAllocateStateOngoing)),
		},
		targetStates: []string{
			strconv.Itoa(int(cluster.DomainAllocateStateSucceeded)),
			strconv.Itoa(int(cluster.DomainAllocateStateFailed)),
		},
	})
	if err != nil {
		return diag.FromErr(fmt.Errorf("waiting for cluster (%s) change complete: %s", d.Id(), err))
	}

	if stateResp.State != cluster.DomainAllocateStateSucceeded {
		d.SetId("")
		return diag.FromErr(errors.New("failed to get cluster endpoints"))
	}

	d.SetId(clusterId)
	return resourceClusterEndpointsRead(ctx, d, m)
}

func resourceClusterEndpointsRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)
	clusterAPI := cluster.NewClustersAPI(c)
	clusterId := d.Id()

	log.Printf("[DEBUG] resourceClusterEndpointsRead, cluster id:%s", d.Id())
	var diags diag.Diagnostics
	stateResp, err := WaitClusterEndpointsStateChangeComplete(ctx, &waitEndpointsStateReq{
		clusterAPI: clusterAPI,
		clusterId:  clusterId,
		timeout:    30 * time.Minute,
		pendingStates: []string{
			strconv.Itoa(int(cluster.DomainAllocateStateUnknown)),
			strconv.Itoa(int(cluster.DomainAllocateStateOngoing)),
		},
		targetStates: []string{
			strconv.Itoa(int(cluster.DomainAllocateStateSucceeded)),
			strconv.Itoa(int(cluster.DomainAllocateStateFailed)),
		},
	})
	if err != nil {
		return diag.FromErr(fmt.Errorf("waiting for cluster (%s) change complete: %s", d.Id(), err))
	}

	d.Set("endpoints", stateResp.List)
	return diags
}

func resourceClusterEndpointsDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	return resourceClusterEndpointsRead(ctx, d, m)
}

type waitEndpointsStateReq struct {
	clusterAPI    cluster.IClusterAPI
	clusterId     string
	timeout       time.Duration
	pendingStates []string
	targetStates  []string
}

func WaitClusterEndpointsStateChangeComplete(ctx context.Context, req *waitEndpointsStateReq) (*cluster.GetClusterEndpointsResp, error) {
	stateConf := &retry.StateChangeConf{
		Pending:    req.pendingStates,
		Target:     req.targetStates,
		Refresh:    StatusClusterEndpointsState(ctx, req.clusterAPI, req.clusterId),
		Timeout:    req.timeout,
		MinTimeout: 3 * time.Second,
	}

	outputRaw, err := stateConf.WaitForStateContext(ctx)
	if output, ok := outputRaw.(*cluster.GetClusterEndpointsResp); ok {
		return output, err
	}

	return nil, err
}

func StatusClusterEndpointsState(ctx context.Context, clusterAPI cluster.IClusterAPI,
	clusterId string) retry.StateRefreshFunc {
	return func() (interface{}, string, error) {
		log.Printf("[DEBUG] get cluster endpoints state: cluster[%s]", clusterId)
		resp, err := clusterAPI.GetClusterEndpoints(ctx, &cluster.GetClusterEndpointsReq{
			ClusterId: clusterId,
		})
		if err != nil {
			return nil, "", err
		}

		log.Printf("[DEBUG] The current state of the cluster endpoints is : %d", resp.State)
		return resp, strconv.Itoa(int(resp.State)), nil
	}
}
