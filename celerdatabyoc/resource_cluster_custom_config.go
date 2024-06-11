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

func resourceClusterCustomConfig() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceClusterCustomConfigCreate,
		UpdateContext: resourceClusterCustomConfigCreate,
		ReadContext:   resourceClusterCustomConfigRead,
		DeleteContext: resourceClusterCustomConfigDelete,
		Schema: map[string]*schema.Schema{
			"cluster_id": {
				Type:         schema.TypeString,
				ForceNew:     true,
				Required:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},
			"config_type": {
				Type:         schema.TypeInt,
				ForceNew:     true,
				Required:     true,
				ValidateFunc: validation.IntInSlice([]int{1}),
			},
			"warehouse_id": {
				Type:     schema.TypeString,
				ForceNew: true,
				Optional: true,
				Default:  "",
			},
			"configs": {
				Type:     schema.TypeMap,
				Required: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			"last_edit_at": {
				Type:     schema.TypeInt,
				Computed: true,
			},
			"last_apply_at": {
				Type:     schema.TypeInt,
				Computed: true,
			},
		},
	}
}

func resourceClusterCustomConfigCreate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	clusterAPI := cluster.NewClustersAPI(c)

	clusterID := d.Get("cluster_id").(string)
	warehouseID := d.Get("warehouse_id").(string)
	configType := cluster.CustomConfigType(d.Get("config_type").(int))
	configs := make(map[string]string, 0)

	for k, v := range d.Get("configs").(map[string]interface{}) {
		configs[k] = v.(string)
	}

	req := &cluster.SaveCustomConfigReq{
		ClusterID:   clusterID,
		WarehouseID: warehouseID,
		ConfigType:  configType,
		Configs:     configs,
	}

	err := clusterAPI.UpdateCustomConfig(ctx, req)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(genConfigID(clusterID, warehouseID, int(configType)))
	return diags
}

func resourceClusterCustomConfigRead(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	clusterAPI := cluster.NewClustersAPI(c)

	clusterID := d.Get("cluster_id").(string)
	warehouseID := d.Get("warehouse_id").(string)
	configType := cluster.CustomConfigType(d.Get("config_type").(int))

	req := &cluster.ListCustomConfigReq{
		ClusterID:   clusterID,
		WarehouseID: warehouseID,
		ConfigType:  configType,
	}
	log.Printf("[DEBUG] resourceClusterCustomConfigRead, req:%+v", req)
	resp, err := clusterAPI.GetCustomConfig(ctx, req)
	if err != nil {
		log.Printf("[DEBUG] resourceClusterCustomConfigRead, err:%v", err)

		return diag.FromErr(err)
	}

	d.Set("cluster_id", clusterID)
	d.Set("warehouse_id", warehouseID)
	d.Set("config_type", configType)
	d.Set("configs", resp.Configs)
	d.Set("last_apply_at", resp.LastApplyAt)
	d.Set("last_edit_at", resp.LastEditAt)

	d.SetId(genConfigID(clusterID, warehouseID, int(configType)))

	return diags
}

func resourceClusterCustomConfigDelete(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	return diags
}

func genConfigID(clusterID, warehouseID string, configType int) string {
	return fmt.Sprintf("config-id-%v-%v-%v", clusterID, warehouseID, configType)
}
