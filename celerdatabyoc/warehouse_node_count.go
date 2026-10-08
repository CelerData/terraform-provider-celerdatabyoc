package celerdatabyoc

import (
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// A warehouse's compute_node_count has two writers: the user, and the backend
// when auto scaling or scheduled scaling resizes the warehouse. While the
// backend owns the live count, state keeps the user's declared value in
// compute_node_count and reports the live count in effective_compute_node_count,
// so the backend's moves never surface as a plan diff.

// isAutoScalingActive reports whether a warehouse map (state or plan) carries an
// auto scaling policy. Read only populates auto_scaling_policy when the backend
// policy is enabled, so a non-empty value means the policy is in force.
func isAutoScalingActive(wh map[string]interface{}) bool {
	if wh == nil {
		return false
	}
	policy, _ := wh["auto_scaling_policy"].(string)
	return len(policy) > 0
}

// autoScalingOwnsNodeCount reports whether auto scaling is in force both before
// and after the apply. Only then is a compute_node_count change left to the
// autoscaler instead of being sent to ScaleWarehouseNum: enabling or disabling
// the policy in the same apply still honors the declared count.
func autoScalingOwnsNodeCount(oldWh, newWh map[string]interface{}) bool {
	return isAutoScalingActive(oldWh) && isAutoScalingActive(newWh)
}

// hasEnabledSchedulePolicy reports whether any scheduled scaling policy is
// enabled. policies is either Read's []map[string]interface{} or the
// []interface{} that ResourceData returns for the block.
func hasEnabledSchedulePolicy(policies interface{}) bool {
	var items []map[string]interface{}
	switch v := policies.(type) {
	case []map[string]interface{}:
		items = v
	case []interface{}:
		for _, item := range v {
			if m, ok := item.(map[string]interface{}); ok {
				items = append(items, m)
			}
		}
	}
	for _, p := range items {
		if enabled, _ := p["enable"].(bool); enabled {
			return true
		}
	}
	return false
}

// liveNodeCount returns the warehouse's live node count from a state map:
// effective_compute_node_count when present, else compute_node_count (state
// written by provider versions that predate effective_compute_node_count, where
// compute_node_count always held the live value).
func liveNodeCount(wh map[string]interface{}) int {
	if wh == nil {
		return 0
	}
	if v, _ := wh["effective_compute_node_count"].(int); v > 0 {
		return v
	}
	v, _ := wh["compute_node_count"].(int)
	return v
}

// priorDeclaredNodeCount returns the compute_node_count held in d for the named
// warehouse: prior state during refresh, the applied plan after Create/Update.
// ok is false when there is nothing to keep (import, or a warehouse not yet in
// state), in which case Read falls back to the live count.
func priorDeclaredNodeCount(d *schema.ResourceData, isDefaultWarehouse bool, whName string) (int, bool) {
	var wh map[string]interface{}
	if isDefaultWarehouse {
		wh = firstBlock(d.Get("default_warehouse"))
	} else {
		wh = findWarehouseByName(d.Get("warehouse").([]interface{}), whName)
	}
	if wh == nil {
		return 0, false
	}
	count, _ := wh["compute_node_count"].(int)
	return count, count > 0
}

// firstBlock returns the first element of a list block value as a map, or nil
// when the list is empty.
func firstBlock(v interface{}) map[string]interface{} {
	items, _ := v.([]interface{})
	if len(items) == 0 {
		return nil
	}
	m, _ := items[0].(map[string]interface{})
	return m
}

// findWarehouseByName returns the warehouse map with the given name, or nil.
func findWarehouseByName(whs []interface{}, whName string) map[string]interface{} {
	for _, item := range whs {
		wh, ok := item.(map[string]interface{})
		if ok && wh["name"].(string) == whName {
			return wh
		}
	}
	return nil
}

// autoScalingSkippedScaleWarning explains why a compute_node_count change was
// recorded in state without resizing the warehouse.
func autoScalingSkippedScaleWarning(whName string, declared, live int) diag.Diagnostic {
	return diag.Diagnostic{
		Severity: diag.Warning,
		Summary:  fmt.Sprintf("compute_node_count change for warehouse %q was recorded but not applied", whName),
		Detail: fmt.Sprintf("Auto scaling is active on this warehouse and manages its node count within the policy's min_size/max_size "+
			"(declared: %d, current: %d). To change the baseline, adjust min_size/max_size instead.", declared, live),
	}
}
