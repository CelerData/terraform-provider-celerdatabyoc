package celerdatabyoc

import (
	"context"
	"reflect"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"

	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"
)

func TestSetWarehouseAZPlacement(t *testing.T) {
	tests := []struct {
		name           string
		networkMultiAz bool
		wh             cluster.Warehouse
		want           map[string]interface{}
	}{
		{
			// The backend keeps SpecifyAZ = specified_azs[0] after a switch to multi_az.
			name:           "multi_az drops the backend's leftover specify_az",
			networkMultiAz: true,
			wh: cluster.Warehouse{
				DistributionPolicyStr: MULTI_AZ,
				SpecifyAZ:             "us-central1-c",
				SpecifiedAZs:          []string{"us-central1-c", "us-central1-f"},
			},
			want: map[string]interface{}{
				"distribution_policy": MULTI_AZ,
				"specify_az":          "",
				"specified_azs":       []string{"us-central1-c", "us-central1-f"},
			},
		},
		{
			name:           "specify_az keeps its az",
			networkMultiAz: true,
			wh: cluster.Warehouse{
				DistributionPolicyStr: SPECIFY_AZ,
				SpecifyAZ:             "us-central1-c",
				SpecifiedAZs:          []string{"us-central1-c"},
			},
			want: map[string]interface{}{
				"distribution_policy": SPECIFY_AZ,
				"specify_az":          "us-central1-c",
				"specified_azs":       []string{},
			},
		},
		{
			name:           "crossing_az",
			networkMultiAz: true,
			wh:             cluster.Warehouse{DistributionPolicyStr: CROSSING_AZ},
			want: map[string]interface{}{
				"distribution_policy": CROSSING_AZ,
				"specify_az":          "",
				"specified_azs":       []string{},
			},
		},
		{
			name:           "single-AZ network clears the policy",
			networkMultiAz: false,
			wh: cluster.Warehouse{
				DistributionPolicyStr: SPECIFY_AZ,
				SpecifyAZ:             "us-central1-c",
			},
			want: map[string]interface{}{
				"distribution_policy": "",
				"specify_az":          "us-central1-c",
				"specified_azs":       []string{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := map[string]interface{}{}
			setWarehouseAZPlacement(got, tt.networkMultiAz, &tt.wh)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %#v, want %#v", got, tt.want)
			}
		})
	}
}

// planFromMultiAzState plans warehouse.0 against the state Read produces for a multi_az
// warehouse over us-central1-c/f (the backend still reports SpecifyAZ = us-central1-c).
// placement is the config's AZ settings for warehouse.0.
func planFromMultiAzState(t *testing.T, placement map[string]interface{}) *terraform.InstanceDiff {
	t.Helper()
	res := resourceElasticClusterV2()
	res.CustomizeDiff = nil

	whMap := map[string]interface{}{}
	setWarehouseAZPlacement(whMap, true, &cluster.Warehouse{
		DistributionPolicyStr: MULTI_AZ,
		SpecifyAZ:             "us-central1-c",
		SpecifiedAZs:          []string{"us-central1-c", "us-central1-f"},
	})
	azs := whMap["specified_azs"].([]string)

	prior := &terraform.InstanceState{ID: "c-1", Attributes: map[string]string{
		"default_warehouse.#":                    "1",
		"default_warehouse.0.name":               DEFAULT_WAREHOUSE_NAME,
		"default_warehouse.0.compute_node_size":  "n4-standard-64",
		"default_warehouse.0.compute_node_count": "2",
		"warehouse.#":                            "1",
		"warehouse.0.name":                       "wh0",
		"warehouse.0.compute_node_size":          "n4-standard-64",
		"warehouse.0.compute_node_count":         "2",
		"warehouse.0.distribution_policy":        whMap["distribution_policy"].(string),
		"warehouse.0.specify_az":                 whMap["specify_az"].(string),
		"warehouse.0.specified_azs.#":            "2",
		"warehouse.0.specified_azs.0":            azs[0],
		"warehouse.0.specified_azs.1":            azs[1],
	}}
	wh := map[string]interface{}{
		"name":               "wh0",
		"compute_node_size":  "n4-standard-64",
		"compute_node_count": 2,
	}
	for k, v := range placement {
		wh[k] = v
	}
	cfg := terraform.NewResourceConfigRaw(map[string]interface{}{
		"default_warehouse": []interface{}{map[string]interface{}{
			"compute_node_size":  "n4-standard-64",
			"compute_node_count": 2,
		}},
		"warehouse": []interface{}{wh},
	})

	diff, err := res.SimpleDiff(context.Background(), prior, cfg, nil)
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	if diff == nil {
		return &terraform.InstanceDiff{}
	}
	return diff
}

// A multi_az config that declares no specify_az must plan clean against Read's state.
func TestMultiAzWarehouseNoSpecifyAzDrift(t *testing.T) {
	diff := planFromMultiAzState(t, map[string]interface{}{
		"distribution_policy": MULTI_AZ,
		"specified_azs":       []interface{}{"us-central1-c", "us-central1-f"},
	})
	for _, k := range []string{"distribution_policy", "specify_az", "specified_azs.#", "specified_azs.0", "specified_azs.1"} {
		if attr, ok := diff.Attributes["warehouse.0."+k]; ok {
			t.Errorf("unexpected diff on warehouse.0.%s: %q -> %q", k, attr.Old, attr.New)
		}
	}
}

// Switching back to specify_az must still plan the AZ: with specify_az now "" in state,
// suppressPinnedSpecifyAzDiff must not absorb `"" -> "us-central1-c"`.
func TestMultiAzToSpecifyAzPlansSpecifyAz(t *testing.T) {
	diff := planFromMultiAzState(t, map[string]interface{}{
		"distribution_policy": SPECIFY_AZ,
		"specify_az":          "us-central1-c",
	})
	for k, want := range map[string]string{"distribution_policy": SPECIFY_AZ, "specify_az": "us-central1-c"} {
		attr, ok := diff.Attributes["warehouse.0."+k]
		if !ok || attr.New != want {
			t.Errorf("warehouse.0.%s: got %#v, want a diff to %q", k, attr, want)
		}
	}
}
