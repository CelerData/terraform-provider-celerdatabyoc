package celerdatabyoc

import (
	"context"
	"fmt"
	"log"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func dataSourceClusterVolumeDetail() *schema.Resource {
	return &schema.Resource{
		ReadContext: dataSourceClusterVolumeRead,
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
				Type:     schema.TypeString,
				Computed: true,
			},
			"vol_size": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"vol_num": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"iops": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"throughput": {
				Type:     schema.TypeInt,
				Computed: true,
			},
		},
	}
}

func dataSourceClusterVolumeRead(ctx context.Context, d *schema.ResourceData, meta any) diag.Diagnostics {

	c := meta.(*client.CelerdataClient)

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

	req := &cluster.GetClusterVolumeDetailReq{
		ClusterId: clusterId,
		Type:      nodeType,
	}
	log.Printf("[DEBUG] retrive cluster volume detail, req:%+v", req)
	resp, err := clusterAPI.GetClusterVolumeDetail(ctx, req)
	if err != nil {
		log.Printf("[ERROR] retrive cluster volume detail failed, err: %v", err)
		return diag.FromErr(err)
	}

	d.Set("cluster_id", clusterId)
	d.Set("node_type", nodeTypeStr)
	d.Set("vol_cate", resp.VmVolCate)
	d.Set("vol_size", resp.VmVolSize)
	d.Set("vol_num", resp.VmVolNum)
	d.Set("iops", resp.Iops)
	d.Set("throughput", resp.Throughput)
	d.SetId(fmt.Sprintf("%s:%s", clusterId, nodeTypeStr))
	return nil
}
