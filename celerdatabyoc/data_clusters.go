package celerdatabyoc

import (
	"context"
	"log"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func dataSourceClusters() *schema.Resource {
	return &schema.Resource{
		ReadContext: dataSourceClustersRead,
		Schema: map[string]*schema.Schema{
			"ids": {
				Type:     schema.TypeSet,
				Computed: true,
			},
		},
	}
}

func dataSourceClustersRead(ctx context.Context, d *schema.ResourceData, meta any) diag.Diagnostics {

	c := meta.(*client.CelerdataClient)

	clusterAPI := cluster.NewClustersAPI(c)
	ids, err := clusterAPI.ListAccountClusterIds(ctx)
	if err != nil {
		log.Printf("[ERROR] list account cluster ids failed, err: %v", err)
		return diag.FromErr(err)
	}

	d.Set("ids", ids)
	d.SetId("_")
	return nil
}
