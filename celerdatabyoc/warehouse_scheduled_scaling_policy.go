package celerdatabyoc

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"
	"terraform-provider-celerdatabyoc/common"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

// The backend caps a warehouse at 5 scheduled scaling policies, counting the
// disabled ones.
const maxWarehouseScheduledScalingPolicies = 5

func warehouseScheduledScalingPolicySchema() *schema.Schema {
	return &schema.Schema{
		Type:     schema.TypeList,
		Optional: true,
		MaxItems: maxWarehouseScheduledScalingPolicies,
		Description: "Scheduled scaling policies for this warehouse. At the scheduled start time the warehouse " +
			"is resized to `size`, and at the end time it returns to its previous size. " +
			"Requires cluster version 3.5.5 or above.",
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"policy_name": {
					Type:         schema.TypeString,
					Required:     true,
					ValidateFunc: validation.StringIsNotWhiteSpace,
				},
				"description": {
					Type:     schema.TypeString,
					Optional: true,
				},
				"time_zone": {
					Type:         schema.TypeString,
					Description:  "IANA Time-Zone",
					Optional:     true,
					Default:      "UTC",
					ValidateFunc: common.ValidateSchedulingPolicyTimeZone,
				},
				"enable": {
					Type:     schema.TypeBool,
					Optional: true,
					Default:  true,
				},
				"size": {
					Type: schema.TypeInt,
					Description: "Number of compute nodes per cngroup to scale to. The resulting total node count " +
						"is `size` * `cngroup_count`.",
					Required:     true,
					ValidateFunc: validation.IntAtLeast(1),
				},
				"schedule_type": {
					Type:         schema.TypeString,
					Required:     true,
					ValidateFunc: validation.StringInSlice(cluster.WarehouseSchedulePolicyTypes, false),
				},
				"start_time": {
					Type:         schema.TypeString,
					Description:  "Time of day the policy takes effect, in `HH:mm`.",
					Required:     true,
					ValidateFunc: validateWarehouseSchedulePolicyTime,
				},
				"end_time": {
					Type:         schema.TypeString,
					Description:  "Time of day the policy stops, in `HH:mm`. Must be later than `start_time`.",
					Required:     true,
					ValidateFunc: validateWarehouseSchedulePolicyTime,
				},
				"week_days": {
					Type:        schema.TypeSet,
					Description: "Days the policy runs on. Required when `schedule_type` is `WEEKLY`.",
					Optional:    true,
					MaxItems:    7,
					Elem: &schema.Schema{
						Type:         schema.TypeString,
						ValidateFunc: validation.StringInSlice(cluster.WeekDays, false),
					},
				},
				"month_days": {
					Type:        schema.TypeSet,
					Description: "Days of the month the policy runs on. Required when `schedule_type` is `MONTHLY`.",
					Optional:    true,
					MaxItems:    31,
					Elem: &schema.Schema{
						Type:         schema.TypeInt,
						ValidateFunc: validation.IntBetween(1, 31),
					},
				},
				"start_date": {
					Type:         schema.TypeString,
					Description:  "First day the policy applies, in `yyyy-MM-dd`. Required when `schedule_type` is `DATE_RANGE`.",
					Optional:     true,
					ValidateFunc: validateWarehouseSchedulePolicyDate,
				},
				"end_date": {
					Type:         schema.TypeString,
					Description:  "Last day the policy applies, in `yyyy-MM-dd`. Required when `schedule_type` is `DATE_RANGE`.",
					Optional:     true,
					ValidateFunc: validateWarehouseSchedulePolicyDate,
				},
			},
		},
	}
}

func warehouseScheduledScalingPolicyExtraInfoSchema() *schema.Schema {
	return &schema.Schema{
		Type:        schema.TypeMap,
		Computed:    true,
		Description: "Maps each scheduled scaling policy name to its backend policy id. Read-only.",
		Elem:        &schema.Schema{Type: schema.TypeString},
	}
}

func validateWarehouseSchedulePolicyTime(i interface{}, k string) ([]string, []error) {
	v, ok := i.(string)
	if !ok {
		return nil, []error{fmt.Errorf("expected type of %s to be string", k)}
	}
	if _, err := parseWarehouseSchedulePolicyTime(v); err != nil {
		return nil, []error{fmt.Errorf("for param `%s`, %s", k, err.Error())}
	}
	return nil, nil
}

func validateWarehouseSchedulePolicyDate(i interface{}, k string) ([]string, []error) {
	v, ok := i.(string)
	if !ok {
		return nil, []error{fmt.Errorf("expected type of %s to be string", k)}
	}
	if _, err := time.Parse("2006-01-02", v); err != nil {
		return nil, []error{fmt.Errorf("for param `%s`, invalid date `%s`. Please use the \"yyyy-MM-dd\" format", k, v)}
	}
	return nil, nil
}

// parseWarehouseSchedulePolicyTime turns "HH:mm" into the HHmm integer the
// backend compares on, so 09:30 becomes 930 and 18:00 becomes 1800. The hour is
// required to be zero-padded: time.Parse would happily take "9:00", but the
// backend always reads back "09:00" and the mismatch would show up as a
// permanent diff.
func parseWarehouseSchedulePolicyTime(v string) (int, error) {
	invalid := fmt.Errorf("invalid time `%s`. Please use the \"HH:mm\" format", v)
	if len(v) != len("15:04") {
		return 0, invalid
	}
	t, err := time.Parse("15:04", v)
	if err != nil {
		return 0, invalid
	}
	return t.Hour()*100 + t.Minute(), nil
}

// WarehouseScheduledScalingPolicyParamCheck validates every warehouse's scheduled
// scaling policies at plan time, so the obvious mistakes surface before apply
// starts creating them one by one.
func WarehouseScheduledScalingPolicyParamCheck(d *schema.ResourceDiff) error {
	if v, ok := d.GetOk("default_warehouse"); ok {
		whs := v.([]interface{})
		if len(whs) > 0 && whs[0] != nil {
			wh := whs[0].(map[string]interface{})
			if err := checkWarehouseScheduledScalingPolicies(DEFAULT_WAREHOUSE_NAME, wh["scheduled_scaling_policy"]); err != nil {
				return err
			}
		}
	}

	if v, ok := d.GetOk("warehouse"); ok {
		for _, item := range v.([]interface{}) {
			wh := item.(map[string]interface{})
			whName := strings.TrimSpace(wh["name"].(string))
			if err := checkWarehouseScheduledScalingPolicies(whName, wh["scheduled_scaling_policy"]); err != nil {
				return err
			}
		}
	}

	return nil
}

func checkWarehouseScheduledScalingPolicies(whName string, raw interface{}) error {
	if raw == nil {
		return nil
	}
	policies, ok := raw.([]interface{})
	if !ok || len(policies) == 0 {
		return nil
	}

	if len(policies) > maxWarehouseScheduledScalingPolicies {
		return fmt.Errorf("warehouse[%s] defines %d scheduled scaling policies, but at most %d are allowed "+
			"(disabled policies count too)", whName, len(policies), maxWarehouseScheduledScalingPolicies)
	}

	seen := make(map[string]bool, len(policies))
	for _, item := range policies {
		m := item.(map[string]interface{})
		policyName := m["policy_name"].(string)
		if seen[policyName] {
			return fmt.Errorf("warehouse[%s] has duplicate scheduled scaling policy name `%s`", whName, policyName)
		}
		seen[policyName] = true

		if err := checkWarehouseScheduledScalingPolicy(whName, m); err != nil {
			return err
		}
	}
	return nil
}

func checkWarehouseScheduledScalingPolicy(whName string, m map[string]interface{}) error {
	policyName := m["policy_name"].(string)
	errPrefix := fmt.Sprintf("for warehouse[%s] scheduled scaling policy [`%s`]", whName, policyName)

	startTime, err := parseWarehouseSchedulePolicyTime(m["start_time"].(string))
	if err != nil {
		return fmt.Errorf("%s, %s", errPrefix, err.Error())
	}
	endTime, err := parseWarehouseSchedulePolicyTime(m["end_time"].(string))
	if err != nil {
		return fmt.Errorf("%s, %s", errPrefix, err.Error())
	}
	if endTime <= startTime {
		return fmt.Errorf("%s, field `end_time` must be later than `start_time`", errPrefix)
	}

	scheduleType := m["schedule_type"].(string)
	hasWeekDays := m["week_days"].(*schema.Set).Len() > 0
	hasMonthDays := m["month_days"].(*schema.Set).Len() > 0
	startDate := m["start_date"].(string)
	endDate := m["end_date"].(string)

	// Each schedule type owns exactly one group of fields; anything else set is
	// silently dropped by the backend, so reject it here instead.
	requireUnset := func(cond bool, field string) error {
		if cond {
			return fmt.Errorf("%s, field `%s` can only be set when `schedule_type` is `%s`",
				errPrefix, field, warehouseSchedulePolicyFieldOwner(field))
		}
		return nil
	}

	switch scheduleType {
	case cluster.WarehouseSchedulePolicyTypeDaily:
		for _, e := range []error{
			requireUnset(hasWeekDays, "week_days"),
			requireUnset(hasMonthDays, "month_days"),
			requireUnset(startDate != "", "start_date"),
			requireUnset(endDate != "", "end_date"),
		} {
			if e != nil {
				return e
			}
		}
	case cluster.WarehouseSchedulePolicyTypeWeekly:
		if !hasWeekDays {
			return fmt.Errorf("%s, field `week_days` is required when `schedule_type` is `WEEKLY`", errPrefix)
		}
		for _, e := range []error{
			requireUnset(hasMonthDays, "month_days"),
			requireUnset(startDate != "", "start_date"),
			requireUnset(endDate != "", "end_date"),
		} {
			if e != nil {
				return e
			}
		}
	case cluster.WarehouseSchedulePolicyTypeMonthly:
		if !hasMonthDays {
			return fmt.Errorf("%s, field `month_days` is required when `schedule_type` is `MONTHLY`", errPrefix)
		}
		for _, e := range []error{
			requireUnset(hasWeekDays, "week_days"),
			requireUnset(startDate != "", "start_date"),
			requireUnset(endDate != "", "end_date"),
		} {
			if e != nil {
				return e
			}
		}
	case cluster.WarehouseSchedulePolicyTypeDateRange:
		if startDate == "" || endDate == "" {
			return fmt.Errorf("%s, fields `start_date` and `end_date` are required when `schedule_type` is `DATE_RANGE`", errPrefix)
		}
		if endDate < startDate {
			return fmt.Errorf("%s, field `end_date` must not be earlier than `start_date`", errPrefix)
		}
		for _, e := range []error{
			requireUnset(hasWeekDays, "week_days"),
			requireUnset(hasMonthDays, "month_days"),
		} {
			if e != nil {
				return e
			}
		}
	}

	return nil
}

func warehouseSchedulePolicyFieldOwner(field string) string {
	switch field {
	case "week_days":
		return cluster.WarehouseSchedulePolicyTypeWeekly
	case "month_days":
		return cluster.WarehouseSchedulePolicyTypeMonthly
	default:
		return cluster.WarehouseSchedulePolicyTypeDateRange
	}
}

func toStringSet(raw interface{}) []string {
	out := make([]string, 0)
	for _, item := range raw.(*schema.Set).List() {
		out = append(out, item.(string))
	}
	sort.Strings(out)
	return out
}

func toInt32Set(raw interface{}) []int32 {
	values := raw.(*schema.Set).List()
	out := make([]int32, 0, len(values))
	for _, item := range values {
		out = append(out, int32(item.(int)))
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// SaveWarehouseScheduledScalingPolicy creates one policy for the given warehouse.
func SaveWarehouseScheduledScalingPolicy(ctx context.Context, api cluster.IClusterAPI,
	clusterId, warehouseId string, m map[string]interface{}) error {

	req := &cluster.SaveWarehouseSchedulePolicyReq{
		ClusterId:   clusterId,
		WarehouseId: warehouseId,
		Name:        m["policy_name"].(string),
		Description: m["description"].(string),
		TimeZone:    m["time_zone"].(string),
		Type:        cluster.WarehouseSchedulePolicyTypeNumber(m["schedule_type"].(string)),
		State:       m["enable"].(bool),
		Size:        int32(m["size"].(int)),
		StartTime:   m["start_time"].(string),
		EndTime:     m["end_time"].(string),
		WeekDays:    toStringSet(m["week_days"]),
		MonthDays:   toInt32Set(m["month_days"]),
		StartDate:   m["start_date"].(string),
		EndDate:     m["end_date"].(string),
	}

	resp, err := api.SaveWarehouseSchedulePolicy(ctx, req)
	if err != nil {
		log.Printf("[ERROR] save warehouse scheduled scaling policy failed, warehouse[%s] paramMap:%+v err:%+v",
			warehouseId, m, err)
		return err
	}
	log.Printf("[DEBUG] save warehouse scheduled scaling policy, warehouse[%s] paramMap:%+v resp:%+v",
		warehouseId, m, resp)
	return nil
}

// UpdateWarehouseScheduledScalingPolicy rewrites an existing policy in place.
func UpdateWarehouseScheduledScalingPolicy(ctx context.Context, api cluster.IClusterAPI,
	warehouseId, policyId string, m map[string]interface{}) error {

	req := &cluster.UpdateWarehouseSchedulePolicyReq{
		WarehouseId: warehouseId,
		PolicyId:    policyId,
		Name:        m["policy_name"].(string),
		Description: m["description"].(string),
		TimeZone:    m["time_zone"].(string),
		Type:        cluster.WarehouseSchedulePolicyTypeNumber(m["schedule_type"].(string)),
		State:       m["enable"].(bool),
		Size:        int32(m["size"].(int)),
		StartTime:   m["start_time"].(string),
		EndTime:     m["end_time"].(string),
		WeekDays:    toStringSet(m["week_days"]),
		MonthDays:   toInt32Set(m["month_days"]),
		StartDate:   m["start_date"].(string),
		EndDate:     m["end_date"].(string),
	}

	if err := api.UpdateWarehouseSchedulePolicy(ctx, req); err != nil {
		log.Printf("[ERROR] update warehouse scheduled scaling policy failed, warehouse[%s] policy[%s] err:%+v",
			warehouseId, policyId, err)
		return err
	}
	return nil
}

// DeleteWarehouseScheduledScalingPolicy removes one policy.
func DeleteWarehouseScheduledScalingPolicy(ctx context.Context, api cluster.IClusterAPI,
	warehouseId, policyId string) error {

	err := api.DeleteWarehouseSchedulePolicy(ctx, &cluster.DeleteWarehouseSchedulePolicyReq{
		WarehouseId: warehouseId,
		PolicyId:    policyId,
	})
	if err != nil {
		log.Printf("[ERROR] delete warehouse scheduled scaling policy failed, warehouse[%s] policy[%s] err:%+v",
			warehouseId, policyId, err)
		return err
	}
	return nil
}

// SaveWarehouseScheduledScalingPolicies creates every policy configured on a
// warehouse. Used on create, where nothing exists on the backend yet.
func SaveWarehouseScheduledScalingPolicies(ctx context.Context, api cluster.IClusterAPI,
	clusterId, warehouseId, warehouseName string, raw interface{}) diag.Diagnostics {

	if raw == nil {
		return nil
	}
	policies, ok := raw.([]interface{})
	if !ok {
		return nil
	}

	for _, item := range policies {
		m := item.(map[string]interface{})
		if err := SaveWarehouseScheduledScalingPolicy(ctx, api, clusterId, warehouseId, m); err != nil {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Error,
					Summary: fmt.Sprintf("Failed to add scheduled scaling policy[%s] for warehouse[%s]",
						m["policy_name"].(string), warehouseName),
					Detail: err.Error(),
				},
			}
		}
	}
	return nil
}

// ListWarehouseScheduledScalingPolicies reads back a warehouse's policies. The
// backend does not promise a stable order, so results are aligned to the order
// the names appear in configuredNames to avoid a perpetual diff; anything not in
// the configuration (added through the console, say) is appended afterwards.
func ListWarehouseScheduledScalingPolicies(ctx context.Context, api cluster.IClusterAPI,
	warehouseId string, configuredNames []string) ([]map[string]interface{}, map[string]string, error) {

	resp, err := api.ListWarehouseSchedulePolicy(ctx, &cluster.ListWarehouseSchedulePolicyReq{
		WarehouseId: warehouseId,
	})
	if err != nil {
		log.Printf("[ERROR] list warehouse scheduled scaling policy failed, warehouse[%s] err:%+v", warehouseId, err)
		return nil, nil, err
	}

	byName := make(map[string]*cluster.WarehouseSchedulePolicy, len(resp.SchedulePolicies))
	extraInfo := make(map[string]string, len(resp.SchedulePolicies))
	for _, p := range resp.SchedulePolicies {
		byName[p.Name] = p
		extraInfo[p.Name] = p.PolicyId
	}

	policyList := make([]map[string]interface{}, 0, len(resp.SchedulePolicies))
	emitted := make(map[string]bool, len(resp.SchedulePolicies))
	for _, name := range configuredNames {
		if p, ok := byName[name]; ok && !emitted[name] {
			policyList = append(policyList, warehouseSchedulePolicyToMap(p))
			emitted[name] = true
		}
	}
	for _, p := range resp.SchedulePolicies {
		if !emitted[p.Name] {
			policyList = append(policyList, warehouseSchedulePolicyToMap(p))
			emitted[p.Name] = true
		}
	}

	return policyList, extraInfo, nil
}

// ConfiguredWarehouseSchedulePolicyNames returns, per warehouse name, the order
// in which its scheduled scaling policies appear in the configuration. Read uses
// this to line the backend's arbitrary ordering back up with what the user wrote.
func ConfiguredWarehouseSchedulePolicyNames(d *schema.ResourceData) map[string][]string {
	out := make(map[string][]string)

	collect := func(whName string, raw interface{}) {
		if raw == nil {
			return
		}
		policies, ok := raw.([]interface{})
		if !ok || len(policies) == 0 {
			return
		}
		names := make([]string, 0, len(policies))
		for _, item := range policies {
			m, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			names = append(names, m["policy_name"].(string))
		}
		out[whName] = names
	}

	if v, ok := d.GetOk("default_warehouse"); ok {
		whs := v.([]interface{})
		if len(whs) > 0 && whs[0] != nil {
			collect(DEFAULT_WAREHOUSE_NAME, whs[0].(map[string]interface{})["scheduled_scaling_policy"])
		}
	}

	if v, ok := d.GetOk("warehouse"); ok {
		for _, item := range v.([]interface{}) {
			wh, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			collect(strings.TrimSpace(wh["name"].(string)), wh["scheduled_scaling_policy"])
		}
	}

	return out
}

func warehouseSchedulePolicyToMap(p *cluster.WarehouseSchedulePolicy) map[string]interface{} {
	monthDays := make([]int, 0, len(p.MonthDays))
	for _, d := range p.MonthDays {
		monthDays = append(monthDays, int(d))
	}

	return map[string]interface{}{
		"policy_name":   p.Name,
		"description":   p.Description,
		"time_zone":     p.TimeZone,
		"enable":        p.State,
		"size":          int(p.Size),
		"schedule_type": cluster.WarehouseSchedulePolicyTypeName(p.Type),
		"start_time":    p.StartTime,
		"end_time":      p.EndTime,
		"week_days":     p.WeekDays,
		"month_days":    monthDays,
		"start_date":    p.StartDate,
		"end_date":      p.EndDate,
	}
}

// HandleChangedWarehouseScheduledScalingPolicy reconciles one warehouse's policies.
// Policies are keyed by name, and the work is ordered delete -> update -> create so
// the 5-policy cap and the unique-name rule are never tripped by an intermediate state.
func HandleChangedWarehouseScheduledScalingPolicy(ctx context.Context, api cluster.IClusterAPI,
	clusterId, warehouseId, warehouseName string, oldRaw, newRaw interface{}, extraInfo map[string]string) diag.Diagnostics {

	opMap := warehouseSchedulePolicyMapByName(oldRaw)
	npMap := warehouseSchedulePolicyMapByName(newRaw)

	newPolicies := make([]map[string]interface{}, 0)
	updatedPolicies := make([]map[string]interface{}, 0)
	deletedPolicies := make([]map[string]interface{}, 0)

	for name, nv := range npMap {
		ov, exists := opMap[name]
		if !exists {
			newPolicies = append(newPolicies, nv)
			continue
		}
		if warehouseSchedulePolicyChanged(ov, nv) {
			updatedPolicies = append(updatedPolicies, nv)
		}
	}

	for name, ov := range opMap {
		if _, exists := npMap[name]; !exists {
			deletedPolicies = append(deletedPolicies, ov)
		}
	}

	for _, item := range deletedPolicies {
		policyName := item["policy_name"].(string)
		policyId := extraInfo[policyName]
		if policyId == "" {
			log.Printf("[WARN] no policy id known for warehouse[%s] scheduled scaling policy[%s], skipping delete",
				warehouseName, policyName)
			continue
		}
		if err := DeleteWarehouseScheduledScalingPolicy(ctx, api, warehouseId, policyId); err != nil {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Error,
					Summary:  fmt.Sprintf("Failed to delete scheduled scaling policy[%s] of warehouse[%s]", policyName, warehouseName),
					Detail:   err.Error(),
				},
			}
		}
	}

	for _, item := range updatedPolicies {
		policyName := item["policy_name"].(string)
		policyId := extraInfo[policyName]
		if policyId == "" {
			// Known to the configuration but not to the backend: create it instead.
			newPolicies = append(newPolicies, item)
			continue
		}
		if err := UpdateWarehouseScheduledScalingPolicy(ctx, api, warehouseId, policyId, item); err != nil {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Error,
					Summary:  fmt.Sprintf("Failed to modify scheduled scaling policy[%s] of warehouse[%s]", policyName, warehouseName),
					Detail:   err.Error(),
				},
			}
		}
	}

	for _, item := range newPolicies {
		if err := SaveWarehouseScheduledScalingPolicy(ctx, api, clusterId, warehouseId, item); err != nil {
			return diag.Diagnostics{
				diag.Diagnostic{
					Severity: diag.Error,
					Summary: fmt.Sprintf("Failed to add scheduled scaling policy[%s] for warehouse[%s]",
						item["policy_name"].(string), warehouseName),
					Detail: err.Error(),
				},
			}
		}
	}

	return nil
}

func warehouseSchedulePolicyMapByName(raw interface{}) map[string]map[string]interface{} {
	out := make(map[string]map[string]interface{})
	if raw == nil {
		return out
	}
	items, ok := raw.([]interface{})
	if !ok {
		return out
	}
	for _, item := range items {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		out[m["policy_name"].(string)] = m
	}
	return out
}

func warehouseSchedulePolicyChanged(ov, nv map[string]interface{}) bool {
	for _, k := range []string{"description", "time_zone", "schedule_type", "start_time", "end_time", "start_date", "end_date"} {
		if ov[k].(string) != nv[k].(string) {
			return true
		}
	}
	if ov["enable"].(bool) != nv["enable"].(bool) {
		return true
	}
	if ov["size"].(int) != nv["size"].(int) {
		return true
	}
	if strings.Join(toStringSet(ov["week_days"]), ",") != strings.Join(toStringSet(nv["week_days"]), ",") {
		return true
	}
	return !int32SliceEqual(toInt32Set(ov["month_days"]), toInt32Set(nv["month_days"]))
}

func int32SliceEqual(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
