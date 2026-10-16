package celerdatabyoc

import (
	"testing"

	"terraform-provider-celerdatabyoc/celerdata-sdk/service/network"
)

// singleAZ builds the cluster's current (single-AZ) network fixture.
func singleAZ(bizID, subnetID, sgID, region, csp string) *network.Network {
	return &network.Network{
		BizID:           bizID,
		CspId:           csp,
		RegionId:        region,
		SubnetId:        subnetID,
		SecurityGroupId: sgID,
		MultiAz:         false,
	}
}

// multiAZ builds a multi-AZ target network fixture spanning the given AZ/subnet pairs.
// primarySubnetID is the network's top-level (primary) subnet_id.
func multiAZ(bizID, primarySubnetID, sgID, region, csp string, rows [][2]string) *network.Network {
	azRows := make([]*network.AZNetWorkInterface, 0, len(rows))
	for _, r := range rows {
		azRows = append(azRows, &network.AZNetWorkInterface{Az: r[0], SubnetId: r[1]})
	}
	return &network.Network{
		BizID:               bizID,
		CspId:               csp,
		RegionId:            region,
		SubnetId:            primarySubnetID,
		SecurityGroupId:     sgID,
		MultiAz:             true,
		AZNetWorkInterfaces: azRows,
	}
}

func TestValidateMultiAzConversionTarget(t *testing.T) {
	baseOld := singleAZ("net-old", "subnet-primary", "sg-1", "us-west-2", "aws")

	validTarget := func() *network.Network {
		return multiAZ("net-new", "subnet-primary", "sg-1", "us-west-2", "aws", [][2]string{
			{"us-west-2a", "subnet-primary"},
			{"us-west-2b", "subnet-b"},
			{"us-west-2c", "subnet-c"},
		})
	}

	tests := []struct {
		name    string
		old     *network.Network
		target  *network.Network
		wantErr bool
	}{
		{
			name:    "allowed: single-AZ -> valid multi-AZ target",
			old:     baseOld,
			target:  validTarget(),
			wantErr: false,
		},
		{
			name:    "rejected: old network already multi-AZ (multi -> different multi)",
			old:     multiAZ("net-old-multi", "subnet-primary", "sg-1", "us-west-2", "aws", [][2]string{{"a", "subnet-primary"}, {"b", "s2"}, {"c", "s3"}}),
			target:  validTarget(),
			wantErr: true,
		},
		{
			name:    "rejected: target is not multi-AZ (single -> single repoint)",
			old:     baseOld,
			target:  singleAZ("net-new", "subnet-other", "sg-1", "us-west-2", "aws"),
			wantErr: true,
		},
		{
			name: "rejected: target region differs",
			old:  baseOld,
			target: multiAZ("net-new", "subnet-primary", "sg-1", "us-east-1", "aws", [][2]string{
				{"a", "subnet-primary"}, {"b", "s2"}, {"c", "s3"},
			}),
			wantErr: true,
		},
		{
			name: "rejected: target csp differs",
			old:  baseOld,
			target: multiAZ("net-new", "subnet-primary", "sg-1", "us-west-2", "azure", [][2]string{
				{"a", "subnet-primary"}, {"b", "s2"}, {"c", "s3"},
			}),
			wantErr: true,
		},
		{
			name: "rejected: only 2 AZ rows",
			old:  baseOld,
			target: multiAZ("net-new", "subnet-primary", "sg-1", "us-west-2", "aws", [][2]string{
				{"a", "subnet-primary"}, {"b", "s2"},
			}),
			wantErr: true,
		},
		{
			name: "rejected: 3 rows but only 2 distinct AZs",
			old:  baseOld,
			target: multiAZ("net-new", "subnet-primary", "sg-1", "us-west-2", "aws", [][2]string{
				{"a", "subnet-primary"}, {"a", "s2"}, {"b", "s3"},
			}),
			wantErr: true,
		},
		{
			// Containment among the AZ rows is what matters; the target's own top-level subnet_id is
			// not compared. The backend (both the web gate and the data-layer rebind) accepts this,
			// so rejecting it here would fail at plan time a config that applies fine via the console.
			name: "allowed: target's top-level subnet_id differs from old's but appears in its AZ rows",
			old:  baseOld,
			target: multiAZ("net-new", "subnet-b", "sg-1", "us-west-2", "aws", [][2]string{
				{"a", "subnet-primary"}, {"b", "subnet-b"}, {"c", "subnet-c"},
			}),
			wantErr: false,
		},
		{
			// A natively-multi-AZ config is allowed an empty top-level subnet — the shape that made
			// the removed equality check reject real targets.
			name: "allowed: target has no top-level subnet_id at all",
			old:  baseOld,
			target: multiAZ("net-new", "", "sg-1", "us-west-2", "aws", [][2]string{
				{"a", "subnet-primary"}, {"b", "subnet-b"}, {"c", "subnet-c"},
			}),
			wantErr: false,
		},
		{
			name: "rejected: old primary subnet missing from target's AZ rows",
			old:  baseOld,
			target: multiAZ("net-new", "subnet-primary", "sg-1", "us-west-2", "aws", [][2]string{
				{"a", "subnet-x"}, {"b", "subnet-y"}, {"c", "subnet-z"},
			}),
			wantErr: true,
		},
		{
			name: "rejected: security group differs",
			old:  baseOld,
			target: multiAZ("net-new", "subnet-primary", "sg-2", "us-west-2", "aws", [][2]string{
				{"a", "subnet-primary"}, {"b", "s2"}, {"c", "s3"},
			}),
			wantErr: true,
		},
		{
			// Guards the "&& old.BizID != target.BizID" clause: a network that is its
			// own conversion target (old == target, e.g. a half-finished retry where
			// the cluster's current network already equals the target) must not be
			// rejected just because old.MultiAz is true.
			name:    "allowed: old and target are the same already-multi-AZ network",
			old:     multiAZ("net-same", "subnet-primary", "sg-1", "us-west-2", "aws", [][2]string{{"a", "subnet-primary"}, {"b", "s2"}, {"c", "s3"}}),
			target:  multiAZ("net-same", "subnet-primary", "sg-1", "us-west-2", "aws", [][2]string{{"a", "subnet-primary"}, {"b", "s2"}, {"c", "s3"}}),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateMultiAzConversionTarget(tt.old, tt.target)
			if tt.wantErr && err == nil {
				t.Fatalf("expected an error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
		})
	}
}

func TestSuppressPinnedPolicyDiff(t *testing.T) {
	tests := []struct {
		name string
		old  string // state
		new  string // config
		want bool   // suppress the diff?
	}{
		{
			// The case this exists for: a conversion pinned the warehouse to SpecifyAZ and
			// the config never declared a policy. Without suppression the plan is
			// "specify_az" -> null, and the apply dies on "param distribution_policy is
			// invalid".
			name: "undeclared config over a pinned SpecifyAZ",
			old:  SPECIFY_AZ,
			new:  "",
			want: true,
		},
		{
			// MULTI_AZ only reaches state from an explicit config, so emptying it is a real
			// user-requested change and has to reach the backend.
			name: "undeclared config over MultiAZ is a real change",
			old:  MULTI_AZ,
			new:  "",
			want: false,
		},
		{
			// A multi-AZ create with no declared policy leaves the backend default
			// CrossingAZ in state; without suppression the next apply sends an empty
			// policy and fails on "param distribution_policy is invalid".
			name: "undeclared config over backend-defaulted CrossingAZ",
			old:  CROSSING_AZ,
			new:  "",
			want: true,
		},
		{
			name: "declared switch away from CrossingAZ is not suppressed",
			old:  CROSSING_AZ,
			new:  SPECIFY_AZ,
			want: false,
		},
		{
			name: "declared config is never suppressed",
			old:  SPECIFY_AZ,
			new:  MULTI_AZ,
			want: false,
		},
		{
			// Declaring the pin explicitly produces no diff of its own; nothing to suppress.
			name: "config restating the pin",
			old:  SPECIFY_AZ,
			new:  SPECIFY_AZ,
			want: false,
		},
		{
			// Single-AZ cluster: Read forces the policy to "" (see resourceElasticClusterV2Read),
			// so there is no pin to absorb and the single-AZ validation still sees "".
			name: "no policy on either side",
			old:  "",
			new:  "",
			want: false,
		},
		{
			// Create: the config asks for SpecifyAZ against empty state.
			name: "declared on create",
			old:  "",
			new:  SPECIFY_AZ,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := suppressPinnedPolicyDiff("default_warehouse.0.distribution_policy", tt.old, tt.new, nil); got != tt.want {
				t.Fatalf("suppressPinnedPolicyDiff(%q, %q) = %v, want %v", tt.old, tt.new, got, tt.want)
			}
		})
	}
}
