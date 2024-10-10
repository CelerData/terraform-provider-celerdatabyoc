package celerdatabyoc

import (
	"context"
	"fmt"
	"log"
	"strings"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"
	"time"

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
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringInSlice(cluster.SupportedConfigType, false),
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
	configTypeStr := d.Get("config_type").(string)
	configType := cluster.ConvertStrToCustomConfigType(configTypeStr)
	configs := make(map[string]string, 0)

	for k, v := range d.Get("configs").(map[string]interface{}) {
		configs[k] = v.(string)
	}

	if configType == cluster.CustomConfigTypeRanger {
		if value, ok := configs[cluster.RANGER_CONFIG_KEY]; ok {
			if !CheckS3Path(value) {
				return diag.FromErr(fmt.Errorf("invalid s3 path:%s", value))
			}
		} else {
			return diag.FromErr(fmt.Errorf("custom ranger config, config key[`s3_path`] cann`t be empty"))
		}
	}

	req := &cluster.SaveCustomConfigReq{
		ClusterID:   clusterID,
		WarehouseID: warehouseID,
		ConfigType:  configType,
		Configs:     configs,
	}

	log.Printf("[DEBUG] save cluster custom config, req:%+v", req)
	err := clusterAPI.UpdateCustomConfig(ctx, req)
	if err != nil {
		log.Printf("[ERROR] save cluster custom config failed, err:%+v", err)
		return diag.FromErr(err)
	}

	id := genID(configTypeStr, clusterID, warehouseID)
	d.SetId(id)
	return diags
}

func resourceClusterCustomConfigRead(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	clusterAPI := cluster.NewClustersAPI(c)
	customConfigId := d.Id()
	log.Printf("[DEBUG] query cluster custom config, customConfigId:%s", customConfigId)
	arr := strings.Split(customConfigId, ":")
	if len(arr) < 3 {
		d.SetId("")
		return diags
	}

	clusterID := arr[1]
	warehouseID := arr[2]
	configTypeStr := arr[0]
	configType := cluster.ConvertStrToCustomConfigType(configTypeStr)
	req := &cluster.ListCustomConfigReq{
		ClusterID:   clusterID,
		WarehouseID: warehouseID,
		ConfigType:  configType,
	}
	log.Printf("[DEBUG] query cluster custom config, req:%+v", req)
	resp, err := clusterAPI.GetCustomConfig(ctx, req)
	if err != nil {
		log.Printf("[ERROR] query cluster custom config failed, err:%+v", err)
		return diag.FromErr(err)
	}
	log.Printf("[DEBUG] query cluster custom config, resp:%+v", resp)

	if resp.Configs == nil || len(resp.Configs) == 0 {
		d.SetId("")
		return diags
	}

	configMap := make(map[string]string)
	if configType == cluster.CustomConfigTypeRanger {
		rangConfigPath := ""
		if v1, ok := resp.Configs["prefix"]; ok {
			rangConfigPath = v1
		} else if v2, ok := resp.Configs[cluster.RANGER_CONFIG_KEY]; ok {
			rangConfigPath = v2
		}
		configMap[cluster.RANGER_CONFIG_KEY] = rangConfigPath
	} else {
		configMap = resp.Configs
	}

	d.Set("cluster_id", clusterID)
	d.Set("warehouse_id", warehouseID)
	d.Set("config_type", configTypeStr)
	d.Set("configs", configMap)
	d.Set("last_apply_at", resp.LastApplyAt)
	d.Set("last_edit_at", resp.LastEditAt)
	return diags
}

func resourceClusterCustomConfigDelete(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	clusterAPI := cluster.NewClustersAPI(c)
	customConfigId := d.Id()
	log.Printf("[DEBUG] remove cluster custom config, customConfigId:%s", customConfigId)
	arr := strings.Split(customConfigId, ":")
	if len(arr) < 3 {
		d.SetId("")
		return diags
	}

	clusterID := arr[1]
	warehouseID := arr[2]
	configTypeStr := arr[0]
	configType := cluster.ConvertStrToCustomConfigType(configTypeStr)
	req := &cluster.SaveCustomConfigReq{
		ClusterID:   clusterID,
		WarehouseID: warehouseID,
		ConfigType:  configType,
		Configs:     make(map[string]string, 0),
	}

	log.Printf("[DEBUG] remove cluster custom config, req:%+v", req)
	var err error
	resp, err := clusterAPI.CleanCustomConfig(ctx, &cluster.CleanCustomConfigReq{
		ClusterID:   clusterID,
		WarehouseID: warehouseID,
		ConfigType:  configType,
	})

	if err != nil {
		log.Printf("[ERROR] remove cluster custom config failed, err:%+v", err)
		return diag.FromErr(err)
	}

	infraActionId := resp.InfraActionId
	if len(infraActionId) > 0 {
		_, err = WaitClusterInfraActionStateChangeComplete(ctx, &waitStateReq{
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
	}

	if err != nil {
		log.Printf("[ERROR] remove cluster custom config failed, err:%+v", err)
		return diag.FromErr(err)
	}

	d.SetId("")
	return diags
}

func genID(configType, clusterID, warehouseID string) string {
	return fmt.Sprintf("%s:%s:%s", configType, clusterID, warehouseID)
}
