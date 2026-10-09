package celerdatabyoc

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

const testPolicyJSON = `{"min_size":2,"max_size":6,"policyItem":[{"type":2,"step_size":1,"conditions":[{"type":1,"duration_seconds":300,"value":"80"}]}],"state":true}`

func TestAutoScalingOwnsNodeCount(t *testing.T) {
	withPolicy := map[string]interface{}{"auto_scaling_policy": testPolicyJSON}
	withoutPolicy := map[string]interface{}{"auto_scaling_policy": ""}

	tests := []struct {
		name     string
		old, new map[string]interface{}
		want     bool
	}{
		{"active before and after", withPolicy, withPolicy, true},
		{"enabled in this apply", withoutPolicy, withPolicy, false},
		{"disabled in this apply", withPolicy, withoutPolicy, false},
		{"never active", withoutPolicy, withoutPolicy, false},
		{"new warehouse", nil, withPolicy, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := autoScalingOwnsNodeCount(tt.old, tt.new); got != tt.want {
				t.Fatalf("autoScalingOwnsNodeCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHasEnabledSchedulePolicy(t *testing.T) {
	tests := []struct {
		name     string
		policies interface{}
		want     bool
	}{
		{"nil", nil, false},
		{"read shape, disabled only", []map[string]interface{}{{"enable": false}}, false},
		{"read shape, one enabled", []map[string]interface{}{{"enable": false}, {"enable": true}}, true},
		{"resource data shape, enabled", []interface{}{map[string]interface{}{"enable": true}}, true},
		{"resource data shape, empty", []interface{}{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasEnabledSchedulePolicy(tt.policies); got != tt.want {
				t.Fatalf("hasEnabledSchedulePolicy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLiveNodeCount(t *testing.T) {
	tests := []struct {
		name string
		wh   map[string]interface{}
		want int
	}{
		{"effective present", map[string]interface{}{"compute_node_count": 12, "effective_compute_node_count": 15}, 15},
		// State written before effective_compute_node_count existed held the live count.
		{"legacy state", map[string]interface{}{"compute_node_count": 15, "effective_compute_node_count": 0}, 15},
		{"nil", nil, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := liveNodeCount(tt.wh); got != tt.want {
				t.Fatalf("liveNodeCount() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestPriorDeclaredNodeCount(t *testing.T) {
	res := resourceElasticClusterV2()

	t.Run("refresh keeps the declared counts", func(t *testing.T) {
		d := res.Data(&terraform.InstanceState{ID: "c-1", Attributes: map[string]string{
			"default_warehouse.#":                              "1",
			"default_warehouse.0.compute_node_count":           "12",
			"default_warehouse.0.effective_compute_node_count": "15",
			"warehouse.#":                    "1",
			"warehouse.0.name":               "wh1",
			"warehouse.0.compute_node_count": "4",
		}})
		if got, ok := priorDeclaredNodeCount(d, true, DEFAULT_WAREHOUSE_NAME); !ok || got != 12 {
			t.Fatalf("default warehouse: got (%d, %v), want (12, true)", got, ok)
		}
		if got, ok := priorDeclaredNodeCount(d, false, "wh1"); !ok || got != 4 {
			t.Fatalf("wh1: got (%d, %v), want (4, true)", got, ok)
		}
		if _, ok := priorDeclaredNodeCount(d, false, "absent"); ok {
			t.Fatal("warehouse not in state must fall back to the live count")
		}
	})

	t.Run("import has nothing to keep", func(t *testing.T) {
		d := res.Data(&terraform.InstanceState{ID: "c-1"})
		if _, ok := priorDeclaredNodeCount(d, true, DEFAULT_WAREHOUSE_NAME); ok {
			t.Fatal("import must fall back to the live count")
		}
	})
}

var errScaleCalled = errors.New("scale called")

// fakeScaleAPI records ScaleWarehouseNum and fails it, so a test can see the call
// without going through the cluster state wait.
type fakeScaleAPI struct {
	cluster.IClusterAPI
	scaled []*cluster.ScaleWarehouseNumReq
}

func (f *fakeScaleAPI) ScaleWarehouseNum(_ context.Context, req *cluster.ScaleWarehouseNumReq) (*cluster.ScaleWarehouseNumResp, error) {
	f.scaled = append(f.scaled, req)
	return nil, errScaleCalled
}

// defaultWarehouseChange builds ResourceData whose default_warehouse moves from
// the prior state to the given config, without running CustomizeDiff.
func defaultWarehouseChange(t *testing.T, oldPolicy string, oldCount, liveCount int, newPolicy string, newCount int) *schema.ResourceData {
	t.Helper()
	res := resourceElasticClusterV2()
	res.CustomizeDiff = nil

	state := &terraform.InstanceState{ID: "c-1", Attributes: map[string]string{
		"default_warehouse.#":                               "1",
		"default_warehouse.0.name":                          DEFAULT_WAREHOUSE_NAME,
		"default_warehouse.0.compute_node_size":             "m6i.4xlarge",
		"default_warehouse.0.compute_node_count":            strconv.Itoa(oldCount),
		"default_warehouse.0.effective_compute_node_count":  strconv.Itoa(liveCount),
		"default_warehouse.0.auto_scaling_policy":           oldPolicy,
		"default_warehouse.0.distribution_policy":           "",
		"warehouse_external_info.%":                         "1",
		"warehouse_external_info." + DEFAULT_WAREHOUSE_NAME: `{"id":"wh-default","is_default_warehouse":true}`,
	}}
	cfg := terraform.NewResourceConfigRaw(map[string]interface{}{
		"default_warehouse": []interface{}{map[string]interface{}{
			"compute_node_size":   "m6i.4xlarge",
			"compute_node_count":  newCount,
			"auto_scaling_policy": newPolicy,
		}},
	})

	diff, err := res.Diff(context.Background(), state, cfg, nil)
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	d, err := schema.InternalMap(res.Schema).Data(state, diff)
	if err != nil {
		t.Fatalf("data: %v", err)
	}
	return d
}

func TestHandleScaleWarehousesUnderAutoScaling(t *testing.T) {
	tests := []struct {
		name                string
		oldPolicy           string
		oldCount, liveCount int
		newPolicy           string
		newCount            int
		isScaleOut          bool
		wantScaleTo         int32 // 0: no ScaleWarehouseNum call
		wantWarning         bool
	}{
		{
			// AppLovin: state from an older provider holds the drifted live count.
			name:      "legacy drifted state is recorded, not scaled",
			oldPolicy: testPolicyJSON, oldCount: 15, liveCount: 15,
			newPolicy: testPolicyJSON, newCount: 12,
			wantWarning: true,
		},
		{
			name:      "explicit change while auto scaling is active is recorded, not scaled",
			oldPolicy: testPolicyJSON, oldCount: 3, liveCount: 5,
			newPolicy: testPolicyJSON, newCount: 6, isScaleOut: true,
			wantWarning: true,
		},
		{
			name:     "no auto scaling scales as before",
			oldCount: 3, liveCount: 3, newCount: 2,
			wantScaleTo: 2,
		},
		{
			name:     "enabling auto scaling in the same apply still honors the count",
			oldCount: 3, liveCount: 3,
			newPolicy: testPolicyJSON, newCount: 4, isScaleOut: true,
			wantScaleTo: 4,
		},
		{
			name:      "disabling auto scaling with a count change scales",
			oldPolicy: testPolicyJSON, oldCount: 3, liveCount: 5,
			newCount:    2,
			wantScaleTo: 2,
		},
		{
			name:     "direction not handled in this pass",
			oldCount: 3, liveCount: 3, newCount: 2, isScaleOut: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := defaultWarehouseChange(t, tt.oldPolicy, tt.oldCount, tt.liveCount, tt.newPolicy, tt.newCount)
			api := &fakeScaleAPI{}

			diags := handleScaleWarehouses(context.Background(), d, api, "c-1", tt.isScaleOut)

			if tt.wantScaleTo == 0 {
				if len(api.scaled) != 0 {
					t.Fatalf("unexpected ScaleWarehouseNum(%d)", api.scaled[0].VmNum)
				}
				if diags.HasError() {
					t.Fatalf("unexpected error: %+v", diags)
				}
			} else {
				if len(api.scaled) != 1 || api.scaled[0].VmNum != tt.wantScaleTo || api.scaled[0].WarehouseId != "wh-default" {
					t.Fatalf("want one ScaleWarehouseNum(%d) on wh-default, got %+v", tt.wantScaleTo, api.scaled)
				}
			}

			gotWarning := false
			for _, dg := range diags {
				if dg.Severity == diag.Warning {
					gotWarning = true
				}
			}
			if gotWarning != tt.wantWarning {
				t.Fatalf("warning = %v, want %v (diags: %+v)", gotWarning, tt.wantWarning, diags)
			}
		})
	}
}

// With Read keeping the declared count, a warehouse the autoscaler has moved
// (live 15, declared 12) must plan clean.
func TestAutoScaledWarehousePlansClean(t *testing.T) {
	d := defaultWarehouseChange(t, testPolicyJSON, 12, 15, testPolicyJSON, 12)
	if d.HasChange("default_warehouse.0.compute_node_count") {
		o, n := d.GetChange("default_warehouse.0.compute_node_count")
		t.Fatalf("compute_node_count diff %v -> %v", o, n)
	}
	if got := d.Get("default_warehouse.0.effective_compute_node_count").(int); got != 15 {
		t.Fatalf("effective_compute_node_count = %d, want 15", got)
	}
}

func TestAutoScalingSkippedScaleWarning(t *testing.T) {
	dg := autoScalingSkippedScaleWarning("wh01", 2)
	if dg.Severity != diag.Warning {
		t.Fatalf("severity = %v, want warning", dg.Severity)
	}
	if !strings.Contains(dg.Summary, `"wh01"`) {
		t.Errorf("summary missing warehouse name: %s", dg.Summary)
	}
	for _, want := range []string{"compute_node_count (2)", "will be applied when auto_scaling_policy is removed"} {
		if !strings.Contains(dg.Detail, want) {
			t.Errorf("detail missing %q: %s", want, dg.Detail)
		}
	}
}
