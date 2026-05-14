package celerdatabyoc

import (
	"strings"
	"testing"

	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"
)

// TestValidateAutoScalingPolicyForDistribution_MultiAZ_NODE verifies the
// MULTI_AZ + SINGLE divisibility invariant: min_size, max_size, and every
// policy_item.step_size must be positive multiples of cngroup count. Mirrors the
// Go web-layer helper (sr-cloud-platform validateMultiAZSingleAlignment) and
// the Java billing helper (WarehouseAutoScalingServiceImpl.validateMultiAzAlignment).
func TestValidateAutoScalingPolicyForDistribution_MultiAZ_NODE(t *testing.T) {
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
		name               string
		cfg                *cluster.WarehouseAutoScalingConfig
		distributionPolicy string
		cngroupCount       int
		wantErrSubstr      string
	}{
		{
			name:               "multi_az_NODE_aligned_2_ok",
			cfg:                mkConfig(2, 10, 2, cluster.AutoScalingUnit_SINGLE),
			distributionPolicy: MULTI_AZ,
			cngroupCount:2,
		},
		{
			name:               "multi_az_NODE_aligned_3_ok",
			cfg:                mkConfig(3, 12, 3, cluster.AutoScalingUnit_SINGLE),
			distributionPolicy: MULTI_AZ,
			cngroupCount:3,
		},
		{
			name:               "multi_az_NODE_minSize_misaligned",
			cfg:                mkConfig(3, 10, 2, cluster.AutoScalingUnit_SINGLE),
			distributionPolicy: MULTI_AZ,
			cngroupCount:2,
			wantErrSubstr:      "min_size (3) must be a positive multiple of cngroup count (2)",
		},
		{
			name:               "multi_az_NODE_stepSize_misaligned",
			cfg:                mkConfig(2, 10, 3, cluster.AutoScalingUnit_SINGLE),
			distributionPolicy: MULTI_AZ,
			cngroupCount:2,
			wantErrSubstr:      "step_size (3) must be a positive multiple of cngroup count (2)",
		},
		{
			name:               "multi_az_NODE_stepSize_zero_rejected",
			cfg:                mkConfig(2, 10, 0, cluster.AutoScalingUnit_SINGLE),
			distributionPolicy: MULTI_AZ,
			cngroupCount:2,
			// step_size=0 is already rejected by the base ValidateAutoScalingPolicy
			// (policyItem.step_size must be at least 1) - assert on that bound.
			wantErrSubstr: "policyItem.step_size",
		},
		{
			name:               "multi_az_GROUP_no_az_check",
			cfg:                mkConfig(2, 10, 1, cluster.AutoScalingUnit_CN_GROUP),
			distributionPolicy: MULTI_AZ,
			cngroupCount:2,
		},
		{
			name:               "specify_az_NODE_no_az_check",
			cfg:                mkConfig(1, 5, 1, cluster.AutoScalingUnit_SINGLE),
			distributionPolicy: "specify_az",
			cngroupCount:0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateAutoScalingPolicyForDistribution(tc.cfg, tc.distributionPolicy, tc.cngroupCount)
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
