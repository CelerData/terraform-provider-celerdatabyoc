package celerdatabyoc

import (
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"

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

func TestVolumeAutoscalingUnsupportedWarning(t *testing.T) {
	w := volumeAutoscalingUnsupportedWarning(cluster.CSP_AZURE)
	if w.Severity != diag.Warning {
		t.Errorf("severity = %v, want Warning (must not fail the apply)", w.Severity)
	}
	if !strings.Contains(w.Summary, cluster.CSP_AZURE) || !strings.Contains(w.Detail, "enable = true") {
		t.Errorf("warning should name the cloud and the ignored setting, got %q / %q", w.Summary, w.Detail)
	}
}
