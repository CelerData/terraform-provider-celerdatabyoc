package celerdatabyoc

import (
	"strings"
	"testing"

	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"
)

// TestValidateAutoScalingPolicyMultiAZ verifies the MULTI_AZ + SINGLE
// divisibility invariant: min_size, max_size, and every policy_item.step_size
// must be positive multiples of cngroup count. Callers are expected to have
// already gated on distribution_policy == MULTI_AZ.
func TestValidateAutoScalingPolicyMultiAZ(t *testing.T) {
	mkConfig := func(min, max, step int32, unit cluster.AutoScalingUnit) *cluster.WarehouseAutoScalingConfig {
		return &cluster.WarehouseAutoScalingConfig{
			MinSize:         min,
			MaxSize:         max,
			AutoScalingUnit: unit,
			PolicyItem: []*cluster.WearhouseScalingPolicyItem{{
				Type:     int32(cluster.WhScalingType_SCALE_OUT),
				StepSize: step,
				Conditions: []*cluster.WearhouseScalingCondition{{
					Type:            int32(cluster.MetricType_AVERAGE_CPU_UTILIZATION),
					DurationSeconds: 300,
					Value:           "80",
				}},
			}},
		}
	}

	cases := []struct {
		name          string
		cfg           *cluster.WarehouseAutoScalingConfig
		cngroupCount  int
		wantErrSubstr string
	}{
		{
			name:         "multi_az_NODE_aligned_2_ok",
			cfg:          mkConfig(2, 10, 2, cluster.AutoScalingUnit_SINGLE),
			cngroupCount: 2,
		},
		{
			name:         "multi_az_NODE_aligned_3_ok",
			cfg:          mkConfig(3, 12, 3, cluster.AutoScalingUnit_SINGLE),
			cngroupCount: 3,
		},
		{
			name:          "multi_az_NODE_minSize_misaligned",
			cfg:           mkConfig(3, 10, 2, cluster.AutoScalingUnit_SINGLE),
			cngroupCount:  2,
			wantErrSubstr: "min_size (3) must be a positive multiple of cngroup count (2)",
		},
		{
			name:          "multi_az_NODE_stepSize_misaligned",
			cfg:           mkConfig(2, 10, 3, cluster.AutoScalingUnit_SINGLE),
			cngroupCount:  2,
			wantErrSubstr: "step_size (3) must be a positive multiple of cngroup count (2)",
		},
		{
			name:          "multi_az_NODE_stepSize_zero_rejected",
			cfg:           mkConfig(2, 10, 0, cluster.AutoScalingUnit_SINGLE),
			cngroupCount:  2,
			wantErrSubstr: "policyItem.step_size",
		},
		{
			name:         "multi_az_GROUP_no_az_check",
			cfg:          mkConfig(2, 10, 1, cluster.AutoScalingUnit_CN_GROUP),
			cngroupCount: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateAutoScalingPolicyMultiAZ(tc.cfg, tc.cngroupCount)
			if tc.wantErrSubstr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErrSubstr)
			}
			if !strings.Contains(err.Error(), tc.wantErrSubstr) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErrSubstr)
			}
		})
	}
}

// TestValidatePolicyJSONMultiAZ covers the JSON-string wrapper: empty input is
// a no-op, malformed JSON surfaces a parse error, and well-formed JSON delegates
// to the alignment check.
func TestValidatePolicyJSONMultiAZ(t *testing.T) {
	if err := ValidatePolicyJSONMultiAZ("", 2); err != nil {
		t.Fatalf("empty rawPolicyJSON should be no-op, got %v", err)
	}
	if err := ValidatePolicyJSONMultiAZ("{not json", 2); err == nil {
		t.Fatalf("expected JSON parse error")
	}
	aligned := `{"min_size":2,"max_size":10,"auto_scaling_unit":0,"policyItem":[{"type":2,"step_size":2,"conditions":[{"type":1,"duration_seconds":300,"value":"80"}]}]}`
	if err := ValidatePolicyJSONMultiAZ(aligned, 2); err != nil {
		t.Fatalf("aligned policy rejected: %v", err)
	}
	misaligned := `{"min_size":3,"max_size":10,"auto_scaling_unit":0,"policyItem":[{"type":2,"step_size":2,"conditions":[{"type":1,"duration_seconds":300,"value":"80"}]}]}`
	if err := ValidatePolicyJSONMultiAZ(misaligned, 2); err == nil || !strings.Contains(err.Error(), "min_size (3)") {
		t.Fatalf("expected min_size misalignment, got %v", err)
	}
}
