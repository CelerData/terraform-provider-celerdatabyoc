package celerdatabyoc

import (
	"testing"

	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"
)

func TestVolumeAutoscalingSupported(t *testing.T) {
	cases := map[string]bool{
		cluster.CSP_AWS:   true,
		"gcp":             true,
		cluster.CSP_AZURE: false,
	}
	for csp, want := range cases {
		if got := volumeAutoscalingSupported(csp); got != want {
			t.Errorf("volumeAutoscalingSupported(%q) = %v, want %v", csp, got, want)
		}
	}
}
