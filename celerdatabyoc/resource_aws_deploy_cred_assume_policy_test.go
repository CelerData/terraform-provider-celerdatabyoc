package celerdatabyoc

import (
	"encoding/json"
	"testing"
)

// The trust policy must name CelerData's account and require exactly the issued ExternalId;
// CelerData rejects deployment roles whose trust policy accepts any id.
func TestBuildDeployCredAssumePolicy(t *testing.T) {
	policyJSON, err := buildDeployCredAssumePolicy("123456789012", "issued-eid")
	if err != nil {
		t.Fatal(err)
	}

	var policy awsIamPolicy
	if err := json.Unmarshal([]byte(policyJSON), &policy); err != nil {
		t.Fatal(err)
	}
	if len(policy.Statements) != 1 {
		t.Fatalf("got %d statements, want 1", len(policy.Statements))
	}
	stmt := policy.Statements[0]
	if got := stmt.Principal["AWS"]; got != "arn:aws:iam::123456789012:root" {
		t.Errorf("principal = %q", got)
	}
	if got := stmt.Condition["StringEquals"]["sts:ExternalId"]; got != "issued-eid" {
		t.Errorf("sts:ExternalId = %q, want the issued id", got)
	}
}
