package celerdatabyoc

import (
	"context"
	"fmt"
	"log"

	"github.com/hashicorp/go-cty/cty"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"
)

const skipInitScriptFailureKey = "skip_init_script_failure"

const skipInitScriptFailureDescription = "Whether to skip user script failures (the \"Skip user script failures\" setting in the CelerData Cloud console). " +
	"User scripts (the scripts configured in `init_scripts`) run when the cluster is deployed and during scale-out, AMI upgrades, blue-green upgrades and node replacement. " +
	"When `true`, a failed user script is skipped and the operation continues; the affected nodes are reported through a console banner on the cluster, the notification centre and an email. " +
	"When `false`, a user script failure fails the operation and the cluster becomes `Abnormal`, except a failure of the post-deployment script during scale-out, which is always skipped and reported because the new nodes are already serving. " +
	"If this argument is omitted when the cluster is created, it defaults to `true`. Clusters created before this setting existed report `false` until it is changed. " +
	"This argument can be modified in place after the cluster is created."

// skipInitScriptFailureSchema is shared by every cluster resource that supports init_scripts.
// It is Optional+Computed with no Default: the backend picks the default for new clusters (true)
// while legacy clusters read back false, so a static Default would produce a spurious diff.
func skipInitScriptFailureSchema() *schema.Schema {
	return &schema.Schema{
		Type:        schema.TypeBool,
		Optional:    true,
		Computed:    true,
		Description: skipInitScriptFailureDescription,
	}
}

// expandSkipInitScriptFailure returns the value explicitly written in the configuration, or nil
// when the attribute is absent (so the server applies its own default). The raw config is used
// instead of d.Get because d.Get cannot tell an explicit `false` from "not set".
func expandSkipInitScriptFailure(rawConfig cty.Value) *bool {
	if rawConfig.IsNull() || !rawConfig.IsKnown() || !rawConfig.Type().IsObjectType() ||
		!rawConfig.Type().HasAttribute(skipInitScriptFailureKey) {
		return nil
	}
	v := rawConfig.GetAttr(skipInitScriptFailureKey)
	if v.IsNull() || !v.IsKnown() || !v.Type().Equals(cty.Bool) {
		return nil
	}
	b := v.True()
	return &b
}

// readSkipInitScriptFailure fetches the cluster setting and stores it in state.
func readSkipInitScriptFailure(ctx context.Context, clusterAPI cluster.IClusterAPI, d *schema.ResourceData, clusterID string) diag.Diagnostics {
	resp, err := clusterAPI.GetClusterSkipInitScriptFailure(ctx, &cluster.GetClusterSkipInitScriptFailureReq{ClusterId: clusterID})
	if err != nil {
		log.Printf("[ERROR] get cluster config[%s] failed, clusterId:%s err:%+v", skipInitScriptFailureKey, clusterID, err)
		return diag.FromErr(err)
	}
	if err := d.Set(skipInitScriptFailureKey, resp.Enabled); err != nil {
		return diag.FromErr(err)
	}
	return nil
}

// updateSkipInitScriptFailure pushes an in-place change of the setting to the backend.
func updateSkipInitScriptFailure(ctx context.Context, clusterAPI cluster.IClusterAPI, d *schema.ResourceData, clusterID string) diag.Diagnostics {
	if !d.HasChange(skipInitScriptFailureKey) || d.IsNewResource() {
		return nil
	}
	enabled := d.Get(skipInitScriptFailureKey).(bool)
	err := clusterAPI.SetClusterSkipInitScriptFailure(ctx, clusterID, &cluster.SetClusterSkipInitScriptFailureReq{Enabled: enabled})
	if err != nil {
		return diag.FromErr(fmt.Errorf("cluster (%s) failed to set %s: %s", d.Id(), skipInitScriptFailureKey, err.Error()))
	}
	return nil
}
