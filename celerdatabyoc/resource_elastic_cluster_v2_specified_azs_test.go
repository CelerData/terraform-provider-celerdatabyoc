package celerdatabyoc

import (
	"context"
	"reflect"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

// planWarehouseAZs plans a change to one warehouse's AZ settings against the resource's real
// schema, with CustomizeDiff left out, and returns the planned specified_azs. The warehouse
// at prefix ("default_warehouse.0." or "warehouse.1.") starts as multi_az over us-east-1a/b/c.
// cfgAZs is the config's specified_azs; nil leaves the attribute out of the config.
func planWarehouseAZs(t *testing.T, prefix, policy string, cfgAZs []interface{}) []string {
	t.Helper()
	res := resourceElasticClusterV2()
	res.CustomizeDiff = nil

	warehouse := func(name string) map[string]interface{} {
		wh := map[string]interface{}{"compute_node_size": "m6i.4xlarge", "compute_node_count": 3}
		if name != "" {
			wh["name"] = name
		}
		return wh
	}
	// wh0 puts the changed warehouse at index 1, so the attribute path isn't always .0.
	defaultWh, warehouses := warehouse(""), []interface{}{warehouse("wh0"), warehouse("wh1")}
	target := defaultWh
	if prefix == "warehouse.1." {
		target = warehouses[1].(map[string]interface{})
	}
	target["distribution_policy"] = policy
	if policy == SPECIFY_AZ {
		target["specify_az"] = "us-east-1a"
	}
	if cfgAZs != nil {
		target["specified_azs"] = cfgAZs
	}

	state := map[string]string{
		"default_warehouse.#":                    "1",
		"default_warehouse.0.name":               DEFAULT_WAREHOUSE_NAME,
		"default_warehouse.0.compute_node_size":  "m6i.4xlarge",
		"default_warehouse.0.compute_node_count": "3",
		"warehouse.#":                            "2",
		"warehouse.0.name":                       "wh0",
		"warehouse.0.compute_node_size":          "m6i.4xlarge",
		"warehouse.0.compute_node_count":         "3",
		"warehouse.1.name":                       "wh1",
		"warehouse.1.compute_node_size":          "m6i.4xlarge",
		"warehouse.1.compute_node_count":         "3",
		prefix + "distribution_policy":           MULTI_AZ,
		prefix + "specified_azs.#":               "3",
		prefix + "specified_azs.0":               "us-east-1a",
		prefix + "specified_azs.1":               "us-east-1b",
		prefix + "specified_azs.2":               "us-east-1c",
	}
	prior := &terraform.InstanceState{ID: "c-1", Attributes: state}
	cfg := terraform.NewResourceConfigRaw(map[string]interface{}{
		"default_warehouse": []interface{}{defaultWh},
		"warehouse":         warehouses,
	})

	diff, err := res.SimpleDiff(context.Background(), prior, cfg, nil)
	if err != nil {
		t.Fatalf("diff: %v", err)
	}
	d, err := schema.InternalMap(res.Schema).Data(prior, diff)
	if err != nil {
		t.Fatalf("data: %v", err)
	}
	return toStringSlice(d.Get(prefix + "specified_azs"))
}

func TestSpecifiedAZsPlan(t *testing.T) {
	tests := []struct {
		name    string
		policy  string
		cfgAZs  []interface{}
		wantAZs []string
	}{
		// The reported bug: the old AZs stayed in the plan, and CustomizeDiff then rejected
		// them with "specified_azs parameter only takes effect when the distribution_policy
		// value is \"multi_az\"".
		{"multi_az to specify_az with specified_azs omitted", SPECIFY_AZ, nil, []string{}},
		{"multi_az to specify_az with an explicit empty list", SPECIFY_AZ, []interface{}{}, []string{}},
		{"reordered AZs plan no change", MULTI_AZ, []interface{}{"us-east-1c", "us-east-1a", "us-east-1b"}, []string{"us-east-1a", "us-east-1b", "us-east-1c"}},
		{"dropping an AZ is a change", MULTI_AZ, []interface{}{"us-east-1a", "us-east-1b"}, []string{"us-east-1a", "us-east-1b"}},
		{"swapping an AZ is a change", MULTI_AZ, []interface{}{"us-east-1a", "us-east-1b", "us-east-1d"}, []string{"us-east-1a", "us-east-1b", "us-east-1d"}},
	}

	for _, prefix := range []string{"default_warehouse.0.", "warehouse.1."} {
		for _, tt := range tests {
			t.Run(prefix+tt.name, func(t *testing.T) {
				if got := planWarehouseAZs(t, prefix, tt.policy, tt.cfgAZs); !reflect.DeepEqual(got, tt.wantAZs) {
					t.Fatalf("planned specified_azs = %v, want %v", got, tt.wantAZs)
				}
			})
		}
	}
}
