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

const (
	CustomConfigIdInvalid = "Invalid parameter `custom_config_id`"
)

func resourceClusterApplyCustomConfig() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceClusterApplyCustomConfigCreate,
		ReadContext:   resourceClusterApplyCustomConfigRead,
		DeleteContext: resourceClusterApplyCustomConfigDelete,
		Schema: map[string]*schema.Schema{
			"custom_config_id": {
				Type:         schema.TypeString,
				ForceNew:     true,
				Required:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},
			"result": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Custom config execution results",
			},
		},
	}
}

func resourceClusterApplyCustomConfigCreate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	clusterAPI := cluster.NewClustersAPI(c)

	customConfigId := d.Get("custom_config_id").(string)
	log.Printf("[DEBUG] apply cluster custom config, customConfigId:%s", customConfigId)
	arr := strings.Split(customConfigId, ":")
	if len(arr) < 3 {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  CustomConfigIdInvalid,
				Detail:   fmt.Sprintf("value:%s is invalid", customConfigId),
			},
		}
	}

	clusterID := arr[1]
	configTypeStr := arr[0]
	configType := cluster.ConvertStrToCustomConfigType(configTypeStr)

	if configType == cluster.CustomConfigTypeUnknown {
		return diag.Diagnostics{
			diag.Diagnostic{
				Severity: diag.Error,
				Summary:  CustomConfigIdInvalid,
				Detail:   fmt.Sprintf("value:%s is invalid, unknown config type [%s]", customConfigId, configTypeStr),
			},
		}
	}

	req := &cluster.ApplyCustomConfigReq{
		ClusterID:  clusterID,
		ConfigType: configType,
	}

	log.Printf("[DEBUG] apply cluster custom config, req:%+v", req)
	resp, err := clusterAPI.ApplyCustomConfig(ctx, req)
	if err != nil {
		log.Printf("[ERROR] apply cluster custom config failed, err:%+v", err)
		return diag.FromErr(err)
	}

	infraActionId := resp.InfraActionId

	d.SetId(infraActionId)

	if len(infraActionId) > 0 {
		infraActionResp, err := WaitClusterInfraActionStateChangeComplete(ctx, &waitStateReq{
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

		summary := fmt.Sprintf("Apply custom %s configuration of the cluster[%s] failed", configTypeStr, clusterID)

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

func resourceClusterApplyCustomConfigRead(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	clusterAPI := cluster.NewClustersAPI(c)
	infraActionId := d.Id()
	result := ""
	if len(infraActionId) > 0 {

		customConfigId := d.Get("custom_config_id").(string)
		log.Printf("[DEBUG] query cluster custom config, customConfigId:%s", customConfigId)
		arr := strings.Split(customConfigId, ":")
		if len(arr) < 3 {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Error,
					Summary:  CustomConfigIdInvalid,
					Detail:   fmt.Sprintf("value:%s is invalid", customConfigId),
				},
			}
		}
		clusterID := arr[1]

		infraActionResp, err := WaitClusterInfraActionStateChangeComplete(ctx, &waitStateReq{
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

		if err != nil {
			log.Printf("[ERROR] query cluster infra action failed, infraActionId:%s", infraActionId)
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

func resourceClusterApplyCustomConfigDelete(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	d.SetId("")
	return diags
}
