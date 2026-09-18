package celerdatabyoc

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// Both cluster resources expose the release channel the same way, and the channel is
// stored normalised so "GA" in HCL never diffs against "ga" in state.
func TestClusterResources_ReleaseVersionSchema(t *testing.T) {
	for name, res := range map[string]*schema.Resource{
		"celerdatabyoc_elastic_cluster_v2": resourceElasticClusterV2(),
		"celerdatabyoc_classic_cluster":    resourceClassicCluster(),
	} {
		t.Run(name, func(t *testing.T) {
			if err := res.InternalValidate(nil, true); err != nil {
				t.Fatalf("schema invalid: %v", err)
			}
			rv := res.Schema["release_version"]
			if rv == nil || !rv.Optional || rv.Default != "stable" {
				t.Fatalf("release_version must be optional with default \"stable\", got %+v", rv)
			}
			if got := rv.StateFunc("GA"); got != "ga" {
				t.Errorf("StateFunc(GA) = %q, want ga", got)
			}
			if _, errs := rv.ValidateFunc("Preview", "release_version"); len(errs) != 0 {
				t.Errorf("Preview rejected: %v", errs)
			}
			cv := res.Schema["cluster_version"]
			if cv == nil || !cv.Computed || cv.Optional {
				t.Fatalf("cluster_version must be a computed-only attribute, got %+v", cv)
			}
		})
	}
}
