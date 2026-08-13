package celerdatabyoc

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func stringSet(values ...string) *schema.Set {
	items := make([]interface{}, 0, len(values))
	for _, v := range values {
		items = append(items, v)
	}
	return schema.NewSet(schema.HashString, items)
}

func intSet(values ...int) *schema.Set {
	items := make([]interface{}, 0, len(values))
	for _, v := range values {
		items = append(items, v)
	}
	return schema.NewSet(schema.HashInt, items)
}

// policyMap builds a fully-populated policy map, then applies the overrides so
// each test only has to state the field it cares about.
func policyMap(overrides map[string]interface{}) map[string]interface{} {
	m := map[string]interface{}{
		"policy_name":   "peak",
		"description":   "",
		"time_zone":     "UTC",
		"enable":        true,
		"size":          4,
		"schedule_type": cluster.WarehouseSchedulePolicyTypeDaily,
		"start_time":    "09:00",
		"end_time":      "18:00",
		"week_days":     stringSet(),
		"month_days":    intSet(),
		"start_date":    "",
		"end_date":      "",
	}
	for k, v := range overrides {
		m[k] = v
	}
	return m
}

func TestCheckWarehouseScheduledScalingPolicies(t *testing.T) {
	tests := []struct {
		name     string
		policies []interface{}
		errMsg   string
	}{
		{
			name:     "no policies",
			policies: nil,
		},
		{
			name:     "valid daily",
			policies: []interface{}{policyMap(nil)},
		},
		{
			name: "valid weekly",
			policies: []interface{}{policyMap(map[string]interface{}{
				"schedule_type": cluster.WarehouseSchedulePolicyTypeWeekly,
				"week_days":     stringSet("MONDAY", "FRIDAY"),
			})},
		},
		{
			name: "valid monthly",
			policies: []interface{}{policyMap(map[string]interface{}{
				"schedule_type": cluster.WarehouseSchedulePolicyTypeMonthly,
				"month_days":    intSet(1, 15),
			})},
		},
		{
			name: "valid date range",
			policies: []interface{}{policyMap(map[string]interface{}{
				"schedule_type": cluster.WarehouseSchedulePolicyTypeDateRange,
				"start_date":    "2026-01-01",
				"end_date":      "2026-03-31",
			})},
		},
		{
			name: "date range spanning a single day",
			policies: []interface{}{policyMap(map[string]interface{}{
				"schedule_type": cluster.WarehouseSchedulePolicyTypeDateRange,
				"start_date":    "2026-01-01",
				"end_date":      "2026-01-01",
			})},
		},
		{
			name: "duplicate policy name",
			policies: []interface{}{
				policyMap(nil),
				policyMap(map[string]interface{}{"start_time": "20:00", "end_time": "22:00"}),
			},
			errMsg: "duplicate scheduled scaling policy name `peak`",
		},
		{
			name: "more than five policies",
			policies: []interface{}{
				policyMap(map[string]interface{}{"policy_name": "p1"}),
				policyMap(map[string]interface{}{"policy_name": "p2"}),
				policyMap(map[string]interface{}{"policy_name": "p3"}),
				policyMap(map[string]interface{}{"policy_name": "p4"}),
				policyMap(map[string]interface{}{"policy_name": "p5"}),
				policyMap(map[string]interface{}{"policy_name": "p6"}),
			},
			errMsg: "at most 5 are allowed",
		},
		{
			name:     "end_time equal to start_time",
			policies: []interface{}{policyMap(map[string]interface{}{"start_time": "09:00", "end_time": "09:00"})},
			errMsg:   "`end_time` must be later than `start_time`",
		},
		{
			name:     "end_time before start_time",
			policies: []interface{}{policyMap(map[string]interface{}{"start_time": "18:00", "end_time": "09:00"})},
			errMsg:   "`end_time` must be later than `start_time`",
		},
		{
			name:     "malformed start_time",
			policies: []interface{}{policyMap(map[string]interface{}{"start_time": "25:00"})},
			errMsg:   "invalid time `25:00`",
		},
		{
			name:     "daily with week_days",
			policies: []interface{}{policyMap(map[string]interface{}{"week_days": stringSet("MONDAY")})},
			errMsg:   "`week_days` can only be set when `schedule_type` is `WEEKLY`",
		},
		{
			name:     "daily with month_days",
			policies: []interface{}{policyMap(map[string]interface{}{"month_days": intSet(1)})},
			errMsg:   "`month_days` can only be set when `schedule_type` is `MONTHLY`",
		},
		{
			name:     "daily with start_date",
			policies: []interface{}{policyMap(map[string]interface{}{"start_date": "2026-01-01"})},
			errMsg:   "`start_date` can only be set when `schedule_type` is `DATE_RANGE`",
		},
		{
			name: "weekly without week_days",
			policies: []interface{}{policyMap(map[string]interface{}{
				"schedule_type": cluster.WarehouseSchedulePolicyTypeWeekly,
			})},
			errMsg: "`week_days` is required when `schedule_type` is `WEEKLY`",
		},
		{
			name: "weekly with month_days",
			policies: []interface{}{policyMap(map[string]interface{}{
				"schedule_type": cluster.WarehouseSchedulePolicyTypeWeekly,
				"week_days":     stringSet("MONDAY"),
				"month_days":    intSet(1),
			})},
			errMsg: "`month_days` can only be set when `schedule_type` is `MONTHLY`",
		},
		{
			name: "monthly without month_days",
			policies: []interface{}{policyMap(map[string]interface{}{
				"schedule_type": cluster.WarehouseSchedulePolicyTypeMonthly,
			})},
			errMsg: "`month_days` is required when `schedule_type` is `MONTHLY`",
		},
		{
			name: "date range missing end_date",
			policies: []interface{}{policyMap(map[string]interface{}{
				"schedule_type": cluster.WarehouseSchedulePolicyTypeDateRange,
				"start_date":    "2026-01-01",
			})},
			errMsg: "`start_date` and `end_date` are required",
		},
		{
			name: "date range with end_date before start_date",
			policies: []interface{}{policyMap(map[string]interface{}{
				"schedule_type": cluster.WarehouseSchedulePolicyTypeDateRange,
				"start_date":    "2026-03-31",
				"end_date":      "2026-01-01",
			})},
			errMsg: "`end_date` must not be earlier than `start_date`",
		},
		{
			name: "date range with week_days",
			policies: []interface{}{policyMap(map[string]interface{}{
				"schedule_type": cluster.WarehouseSchedulePolicyTypeDateRange,
				"start_date":    "2026-01-01",
				"end_date":      "2026-03-31",
				"week_days":     stringSet("MONDAY"),
			})},
			errMsg: "`week_days` can only be set when `schedule_type` is `WEEKLY`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkWarehouseScheduledScalingPolicies("wh1", tt.policies)
			if tt.errMsg == "" {
				if err != nil {
					t.Fatalf("expected no error, got: %s", err.Error())
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.errMsg)
			}
			if !strings.Contains(err.Error(), tt.errMsg) {
				t.Fatalf("expected error containing %q, got: %s", tt.errMsg, err.Error())
			}
		})
	}
}

// fakeSchedulePolicyAPI implements only the calls the policy helpers make; the
// embedded interface covers the rest of IClusterAPI and panics if ever reached.
type fakeSchedulePolicyAPI struct {
	cluster.IClusterAPI

	listResp []*cluster.WarehouseSchedulePolicy
	listErr  error

	saved   []*cluster.SaveWarehouseSchedulePolicyReq
	updated []*cluster.UpdateWarehouseSchedulePolicyReq
	deleted []*cluster.DeleteWarehouseSchedulePolicyReq
}

func (f *fakeSchedulePolicyAPI) ListWarehouseSchedulePolicy(_ context.Context,
	_ *cluster.ListWarehouseSchedulePolicyReq) (*cluster.ListWarehouseSchedulePolicyResp, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return &cluster.ListWarehouseSchedulePolicyResp{SchedulePolicies: f.listResp}, nil
}

func (f *fakeSchedulePolicyAPI) SaveWarehouseSchedulePolicy(_ context.Context,
	req *cluster.SaveWarehouseSchedulePolicyReq) (*cluster.SaveWarehouseSchedulePolicyResp, error) {
	f.saved = append(f.saved, req)
	return &cluster.SaveWarehouseSchedulePolicyResp{PolicyId: "new-" + req.Name}, nil
}

func (f *fakeSchedulePolicyAPI) UpdateWarehouseSchedulePolicy(_ context.Context,
	req *cluster.UpdateWarehouseSchedulePolicyReq) error {
	f.updated = append(f.updated, req)
	return nil
}

func (f *fakeSchedulePolicyAPI) DeleteWarehouseSchedulePolicy(_ context.Context,
	req *cluster.DeleteWarehouseSchedulePolicyReq) error {
	f.deleted = append(f.deleted, req)
	return nil
}

func policyNames(policies []map[string]interface{}) []string {
	names := make([]string, 0, len(policies))
	for _, p := range policies {
		names = append(names, p["policy_name"].(string))
	}
	return names
}

func TestListWarehouseScheduledScalingPoliciesOrdering(t *testing.T) {
	// The backend hands them back in an arbitrary order.
	api := &fakeSchedulePolicyAPI{listResp: []*cluster.WarehouseSchedulePolicy{
		{PolicyId: "id-c", Name: "c", Type: 1, Size: 3, StartTime: "01:00", EndTime: "02:00"},
		{PolicyId: "id-a", Name: "a", Type: 1, Size: 1, StartTime: "03:00", EndTime: "04:00"},
		{PolicyId: "id-b", Name: "b", Type: 1, Size: 2, StartTime: "05:00", EndTime: "06:00"},
	}}

	tests := []struct {
		name       string
		configured []string
		want       []string
	}{
		{name: "aligned to the configured order", configured: []string{"a", "b", "c"}, want: []string{"a", "b", "c"}},
		{name: "unconfigured policies are appended", configured: []string{"b"}, want: []string{"b", "c", "a"}},
		{name: "configured but missing names are skipped", configured: []string{"gone", "a"}, want: []string{"a", "c", "b"}},
		{name: "no configuration keeps backend order", configured: nil, want: []string{"c", "a", "b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policies, extraInfo, err := ListWarehouseScheduledScalingPolicies(context.Background(), api, "wh-1", tt.configured)
			if err != nil {
				t.Fatalf("unexpected error: %s", err.Error())
			}
			if got := policyNames(policies); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("expected order %v, got %v", tt.want, got)
			}
			wantExtra := map[string]string{"a": "id-a", "b": "id-b", "c": "id-c"}
			if !reflect.DeepEqual(extraInfo, wantExtra) {
				t.Fatalf("expected extra info %v, got %v", wantExtra, extraInfo)
			}
		})
	}
}

func TestWarehouseSchedulePolicyToMap(t *testing.T) {
	m := warehouseSchedulePolicyToMap(&cluster.WarehouseSchedulePolicy{
		PolicyId:    "id-1",
		Name:        "peak",
		Description: "business hours",
		TimeZone:    "Asia/Shanghai",
		Type:        2,
		State:       true,
		Size:        4,
		StartTime:   "09:00",
		EndTime:     "18:00",
		WeekDays:    []string{"MONDAY"},
		MonthDays:   []int32{1, 15},
	})

	if m["policy_name"] != "peak" {
		t.Errorf("expected policy_name peak, got %v", m["policy_name"])
	}
	if m["schedule_type"] != cluster.WarehouseSchedulePolicyTypeWeekly {
		t.Errorf("expected schedule_type WEEKLY, got %v", m["schedule_type"])
	}
	if m["size"] != 4 {
		t.Errorf("expected size 4, got %v", m["size"])
	}
	if m["enable"] != true {
		t.Errorf("expected enable true, got %v", m["enable"])
	}
	if !reflect.DeepEqual(m["month_days"], []int{1, 15}) {
		t.Errorf("expected month_days [1 15], got %v", m["month_days"])
	}
}

func TestHandleChangedWarehouseScheduledScalingPolicy(t *testing.T) {
	extraInfo := map[string]string{"keep": "id-keep", "change": "id-change", "drop": "id-drop"}

	oldPolicies := []interface{}{
		policyMap(map[string]interface{}{"policy_name": "keep"}),
		policyMap(map[string]interface{}{"policy_name": "change", "size": 2}),
		policyMap(map[string]interface{}{"policy_name": "drop"}),
	}
	newPolicies := []interface{}{
		policyMap(map[string]interface{}{"policy_name": "keep"}),
		policyMap(map[string]interface{}{"policy_name": "change", "size": 8}),
		policyMap(map[string]interface{}{"policy_name": "add"}),
	}

	api := &fakeSchedulePolicyAPI{}
	if diags := HandleChangedWarehouseScheduledScalingPolicy(context.Background(), api, "c-1", "wh-1", "wh1",
		oldPolicies, newPolicies, extraInfo); diags != nil {
		t.Fatalf("unexpected diagnostics: %+v", diags)
	}

	if len(api.deleted) != 1 || api.deleted[0].PolicyId != "id-drop" {
		t.Fatalf("expected one delete of id-drop, got %+v", api.deleted)
	}
	if len(api.updated) != 1 || api.updated[0].PolicyId != "id-change" || api.updated[0].Size != 8 {
		t.Fatalf("expected one update of id-change to size 8, got %+v", api.updated)
	}
	if len(api.saved) != 1 || api.saved[0].Name != "add" {
		t.Fatalf("expected one save of `add`, got %+v", api.saved)
	}
	if api.saved[0].WarehouseId != "wh-1" || api.saved[0].ClusterId != "c-1" {
		t.Errorf("expected save to carry wh-1/c-1, got %s/%s", api.saved[0].WarehouseId, api.saved[0].ClusterId)
	}
}

func TestHandleChangedWarehouseScheduledScalingPolicyCreatesWhenIdUnknown(t *testing.T) {
	// The policy is in state but the backend never gave us an id for it, so the
	// change has to become a create rather than a silent no-op.
	api := &fakeSchedulePolicyAPI{}
	if diags := HandleChangedWarehouseScheduledScalingPolicy(context.Background(), api, "c-1", "wh-1", "wh1",
		[]interface{}{policyMap(map[string]interface{}{"policy_name": "p", "size": 2})},
		[]interface{}{policyMap(map[string]interface{}{"policy_name": "p", "size": 8})},
		map[string]string{}); diags != nil {
		t.Fatalf("unexpected diagnostics: %+v", diags)
	}

	if len(api.updated) != 0 {
		t.Fatalf("expected no update, got %+v", api.updated)
	}
	if len(api.saved) != 1 || api.saved[0].Size != 8 {
		t.Fatalf("expected one save with size 8, got %+v", api.saved)
	}
}

func TestWarehouseSchedulePolicyChanged(t *testing.T) {
	base := policyMap(nil)

	tests := []struct {
		name string
		next map[string]interface{}
		want bool
	}{
		{name: "identical", next: policyMap(nil), want: false},
		{name: "size", next: policyMap(map[string]interface{}{"size": 8}), want: true},
		{name: "enable", next: policyMap(map[string]interface{}{"enable": false}), want: true},
		{name: "end_time", next: policyMap(map[string]interface{}{"end_time": "19:00"}), want: true},
		{name: "time_zone", next: policyMap(map[string]interface{}{"time_zone": "Asia/Shanghai"}), want: true},
		{name: "description", next: policyMap(map[string]interface{}{"description": "x"}), want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := warehouseSchedulePolicyChanged(base, tt.next); got != tt.want {
				t.Fatalf("expected changed=%v, got %v", tt.want, got)
			}
		})
	}

	// Set ordering must not register as a change.
	weekly := func(days ...string) map[string]interface{} {
		return policyMap(map[string]interface{}{
			"schedule_type": cluster.WarehouseSchedulePolicyTypeWeekly,
			"week_days":     stringSet(days...),
		})
	}
	if warehouseSchedulePolicyChanged(weekly("MONDAY", "FRIDAY"), weekly("FRIDAY", "MONDAY")) {
		t.Error("reordered week_days should not count as a change")
	}
	if !warehouseSchedulePolicyChanged(weekly("MONDAY"), weekly("TUESDAY")) {
		t.Error("different week_days should count as a change")
	}

	monthly := func(days ...int) map[string]interface{} {
		return policyMap(map[string]interface{}{
			"schedule_type": cluster.WarehouseSchedulePolicyTypeMonthly,
			"month_days":    intSet(days...),
		})
	}
	if warehouseSchedulePolicyChanged(monthly(1, 15), monthly(15, 1)) {
		t.Error("reordered month_days should not count as a change")
	}
	if !warehouseSchedulePolicyChanged(monthly(1, 15), monthly(1, 16)) {
		t.Error("different month_days should count as a change")
	}
}

func TestParseWarehouseSchedulePolicyTime(t *testing.T) {
	for _, tt := range []struct {
		in   string
		want int
		ok   bool
	}{
		{"00:00", 0, true},
		{"09:30", 930, true},
		{"18:00", 1800, true},
		{"23:59", 2359, true},
		{"24:00", 0, false},
		{"25:00", 0, false},
		{"9:00", 0, false},
		{"", 0, false},
		{"noon", 0, false},
	} {
		got, err := parseWarehouseSchedulePolicyTime(tt.in)
		if !tt.ok {
			if err == nil {
				t.Errorf("expected %q to be rejected", tt.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("expected %q to be accepted, got: %s", tt.in, err.Error())
			continue
		}
		if got != tt.want {
			t.Errorf("expected %q to parse to %d, got %d", tt.in, tt.want, got)
		}
	}
}
