package celerdatabyoc

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// rule builds the map shape that schema produces for a single "rule" block;
// only cluster_id matters for region validation.
func rule(clusterID string) map[string]interface{} {
	return map[string]interface{}{"cluster_id": clusterID}
}

func TestValidateRuleClusterRegions(t *testing.T) {
	type lookupResult struct {
		region string
		found  bool
		err    error
	}

	tests := []struct {
		name         string
		policyRegion string
		rules        []interface{}
		// table of cluster_id -> what the lookup returns
		lookups     map[string]lookupResult
		wantErr     bool
		errContains []string
		// wantCalls is the expected number of lookup invocations (-1 = don't assert)
		wantCalls int
	}{
		{
			name:         "all clusters match region",
			policyRegion: "us-east-1",
			rules:        []interface{}{rule("c1"), rule("c2")},
			lookups: map[string]lookupResult{
				"c1": {region: "us-east-1", found: true},
				"c2": {region: "us-east-1", found: true},
			},
			wantErr:   false,
			wantCalls: 2,
		},
		{
			name:         "region mismatch reports cluster and both regions",
			policyRegion: "us-east-1",
			rules:        []interface{}{rule("c1")},
			lookups: map[string]lookupResult{
				"c1": {region: "us-west-2", found: true},
			},
			wantErr:     true,
			errContains: []string{"c1", "us-west-2", "us-east-1"},
			wantCalls:   1,
		},
		{
			name:         "cluster not found is an error",
			policyRegion: "us-east-1",
			rules:        []interface{}{rule("ghost")},
			lookups: map[string]lookupResult{
				"ghost": {found: false},
			},
			wantErr:     true,
			errContains: []string{"ghost", "does not exist"},
			wantCalls:   1,
		},
		{
			name:         "empty policy region skips validation entirely",
			policyRegion: "",
			rules:        []interface{}{rule("c1")},
			lookups:      map[string]lookupResult{},
			wantErr:      false,
			wantCalls:    0,
		},
		{
			name:         "empty cluster_id is skipped",
			policyRegion: "us-east-1",
			rules:        []interface{}{rule(""), rule("c1")},
			lookups: map[string]lookupResult{
				"c1": {region: "us-east-1", found: true},
			},
			wantErr:   false,
			wantCalls: 1,
		},
		{
			name:         "duplicate cluster_id is looked up only once",
			policyRegion: "us-east-1",
			rules:        []interface{}{rule("c1"), rule("c1"), rule("c1")},
			lookups: map[string]lookupResult{
				"c1": {region: "us-east-1", found: true},
			},
			wantErr:   false,
			wantCalls: 1,
		},
		{
			name:         "lookup error is propagated",
			policyRegion: "us-east-1",
			rules:        []interface{}{rule("c1")},
			lookups: map[string]lookupResult{
				"c1": {err: errors.New("boom")},
			},
			wantErr:     true,
			errContains: []string{"boom"},
			wantCalls:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			lookup := func(_ context.Context, clusterID string) (string, bool, error) {
				calls++
				r, ok := tt.lookups[clusterID]
				if !ok {
					t.Fatalf("unexpected lookup for cluster_id %q", clusterID)
				}
				return r.region, r.found, r.err
			}

			err := validateRuleClusterRegions(context.Background(), tt.policyRegion, tt.rules, lookup)

			if tt.wantErr && err == nil {
				t.Fatalf("expected an error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			for _, sub := range tt.errContains {
				if err == nil || !strings.Contains(err.Error(), sub) {
					t.Fatalf("error %v should contain %q", err, sub)
				}
			}
			if tt.wantCalls >= 0 && calls != tt.wantCalls {
				t.Fatalf("expected %d lookup calls, got %d", tt.wantCalls, calls)
			}
		})
	}
}
