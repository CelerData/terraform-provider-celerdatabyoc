package celerdatabyoc

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/hashicorp/go-cty/cty"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"

	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"
)

// Every cluster resource that supports init_scripts exposes skip_init_script_failure the same way:
// Optional+Computed, no Default (legacy clusters read back false, new ones default to true on the
// server), and updatable in place.
func TestClusterResources_SkipInitScriptFailureSchema(t *testing.T) {
	for name, res := range map[string]*schema.Resource{
		"celerdatabyoc_classic_cluster":    resourceClassicCluster(),
		"celerdatabyoc_elastic_cluster":    resourceElasticCluster(),
		"celerdatabyoc_elastic_cluster_v2": resourceElasticClusterV2(),
	} {
		t.Run(name, func(t *testing.T) {
			if err := res.InternalValidate(nil, true); err != nil {
				t.Fatalf("schema invalid: %v", err)
			}
			if res.Schema["init_scripts"] == nil {
				t.Fatalf("resource no longer supports init_scripts; revisit skip_init_script_failure")
			}
			s := res.Schema[skipInitScriptFailureKey]
			if s == nil {
				t.Fatalf("%s missing from schema", skipInitScriptFailureKey)
			}
			if s.Type != schema.TypeBool || !s.Optional || !s.Computed {
				t.Errorf("want optional+computed bool, got %+v", s)
			}
			if s.Default != nil || s.DefaultFunc != nil {
				t.Errorf("must not have a default (legacy clusters read back false), got %v", s.Default)
			}
			if s.ForceNew {
				t.Errorf("must be updatable in place, not ForceNew")
			}
			for _, want := range []string{"Skip user script failures", "`Abnormal`", "defaults to `true`"} {
				if !strings.Contains(s.Description, want) {
					t.Errorf("description missing %q", want)
				}
			}
		})
	}
}

func TestExpandSkipInitScriptFailure(t *testing.T) {
	objType := cty.Object(map[string]cty.Type{skipInitScriptFailureKey: cty.Bool})
	cases := map[string]struct {
		raw  cty.Value
		want *bool
	}{
		"null config":    {cty.NullVal(objType), nil},
		"unknown config": {cty.UnknownVal(objType), nil},
		"attr omitted":   {cty.ObjectVal(map[string]cty.Value{skipInitScriptFailureKey: cty.NullVal(cty.Bool)}), nil},
		"attr unknown":   {cty.ObjectVal(map[string]cty.Value{skipInitScriptFailureKey: cty.UnknownVal(cty.Bool)}), nil},
		"attr absent":    {cty.ObjectVal(map[string]cty.Value{"other": cty.True}), nil},
		"explicit true":  {cty.ObjectVal(map[string]cty.Value{skipInitScriptFailureKey: cty.True}), boolPtr(true)},
		"explicit false": {cty.ObjectVal(map[string]cty.Value{skipInitScriptFailureKey: cty.False}), boolPtr(false)},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := expandSkipInitScriptFailure(tc.raw)
			if (got == nil) != (tc.want == nil) || (got != nil && *got != *tc.want) {
				t.Fatalf("got %v, want %v", fmtBoolPtr(got), fmtBoolPtr(tc.want))
			}
		})
	}
}

// The create payload must omit the field when unset (server default applies) but still send an
// explicit false.
func TestClusterConf_SkipInitScriptFailureJSON(t *testing.T) {
	for name, tc := range map[string]struct {
		v    *bool
		want string
	}{
		"unset": {nil, ""},
		"true":  {boolPtr(true), `"skip_init_script_failure":true`},
		"false": {boolPtr(false), `"skip_init_script_failure":false`},
	} {
		t.Run(name, func(t *testing.T) {
			b, err := json.Marshal(&cluster.ClusterConf{SkipInitScriptFailure: tc.v})
			if err != nil {
				t.Fatal(err)
			}
			s := string(b)
			if tc.want == "" {
				if strings.Contains(s, skipInitScriptFailureKey) {
					t.Fatalf("unset value must be omitted, got %s", s)
				}
			} else if !strings.Contains(s, tc.want) {
				t.Fatalf("want %s in %s", tc.want, s)
			}
		})
	}
}

type fakeSkipInitScriptAPI struct {
	cluster.IClusterAPI

	getResp *cluster.GetClusterSkipInitScriptFailureResp
	getErr  error
	setErr  error

	setCalls []*cluster.SetClusterSkipInitScriptFailureReq
	setIDs   []string
}

func (f *fakeSkipInitScriptAPI) GetClusterSkipInitScriptFailure(_ context.Context,
	_ *cluster.GetClusterSkipInitScriptFailureReq) (*cluster.GetClusterSkipInitScriptFailureResp, error) {
	return f.getResp, f.getErr
}

func (f *fakeSkipInitScriptAPI) SetClusterSkipInitScriptFailure(_ context.Context, clusterID string,
	req *cluster.SetClusterSkipInitScriptFailureReq) error {
	f.setIDs = append(f.setIDs, clusterID)
	f.setCalls = append(f.setCalls, req)
	return f.setErr
}

// skipInitScriptData builds ResourceData for a cluster whose state holds `stateVal` and whose
// config holds `cfg` (nil means the attribute is omitted).
func skipInitScriptData(t *testing.T, stateVal string, cfg map[string]interface{}) *schema.ResourceData {
	t.Helper()
	res := &schema.Resource{Schema: map[string]*schema.Schema{skipInitScriptFailureKey: skipInitScriptFailureSchema()}}
	state := &terraform.InstanceState{ID: "c-1", Attributes: map[string]string{"id": "c-1", skipInitScriptFailureKey: stateVal}}
	diff, err := res.Diff(context.Background(), state, terraform.NewResourceConfigRaw(cfg), nil)
	if err != nil {
		t.Fatal(err)
	}
	d, err := schema.InternalMap(res.Schema).Data(state, diff)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestUpdateSkipInitScriptFailure(t *testing.T) {
	ctx := context.Background()

	t.Run("change calls POST", func(t *testing.T) {
		api := &fakeSkipInitScriptAPI{}
		d := skipInitScriptData(t, "true", map[string]interface{}{skipInitScriptFailureKey: false})
		if diags := updateSkipInitScriptFailure(ctx, api, d, "c-1"); diags.HasError() {
			t.Fatalf("unexpected error: %v", diags)
		}
		if len(api.setCalls) != 1 || api.setCalls[0].Enabled || api.setIDs[0] != "c-1" {
			t.Fatalf("want one POST with enabled=false for c-1, got %+v %v", api.setCalls, api.setIDs)
		}
	})

	t.Run("no change no call", func(t *testing.T) {
		api := &fakeSkipInitScriptAPI{}
		d := skipInitScriptData(t, "true", map[string]interface{}{skipInitScriptFailureKey: true})
		_ = updateSkipInitScriptFailure(ctx, api, d, "c-1")
		if len(api.setCalls) != 0 {
			t.Fatalf("unexpected POST %+v", api.setCalls)
		}
	})

	t.Run("omitted from config keeps state", func(t *testing.T) {
		api := &fakeSkipInitScriptAPI{}
		d := skipInitScriptData(t, "false", map[string]interface{}{})
		_ = updateSkipInitScriptFailure(ctx, api, d, "c-1")
		if len(api.setCalls) != 0 {
			t.Fatalf("unexpected POST %+v", api.setCalls)
		}
	})

	t.Run("new resource skipped", func(t *testing.T) {
		api := &fakeSkipInitScriptAPI{}
		d := skipInitScriptData(t, "true", map[string]interface{}{skipInitScriptFailureKey: false})
		d.MarkNewResource()
		_ = updateSkipInitScriptFailure(ctx, api, d, "c-1")
		if len(api.setCalls) != 0 {
			t.Fatalf("create must send the value in the deploy request, not POST; got %+v", api.setCalls)
		}
	})

	t.Run("error surfaced", func(t *testing.T) {
		api := &fakeSkipInitScriptAPI{setErr: errors.New("boom")}
		d := skipInitScriptData(t, "false", map[string]interface{}{skipInitScriptFailureKey: true})
		diags := updateSkipInitScriptFailure(ctx, api, d, "c-1")
		if !diags.HasError() || !strings.Contains(diags[0].Summary, "boom") {
			t.Fatalf("want error containing boom, got %v", diags)
		}
	})
}

func TestReadSkipInitScriptFailure(t *testing.T) {
	ctx := context.Background()
	for _, enabled := range []bool{true, false} {
		api := &fakeSkipInitScriptAPI{getResp: &cluster.GetClusterSkipInitScriptFailureResp{Enabled: enabled}}
		d := skipInitScriptData(t, "", map[string]interface{}{})
		if diags := readSkipInitScriptFailure(ctx, api, d, "c-1"); diags.HasError() {
			t.Fatalf("unexpected error: %v", diags)
		}
		if got := d.Get(skipInitScriptFailureKey).(bool); got != enabled {
			t.Errorf("state = %v, want %v", got, enabled)
		}
	}

	api := &fakeSkipInitScriptAPI{getErr: errors.New("boom")}
	d := skipInitScriptData(t, "", map[string]interface{}{})
	if diags := readSkipInitScriptFailure(ctx, api, d, "c-1"); !diags.HasError() {
		t.Fatal("want read error to be returned")
	}
}

func boolPtr(b bool) *bool { return &b }

func fmtBoolPtr(b *bool) string {
	if b == nil {
		return "nil"
	}
	if *b {
		return "true"
	}
	return "false"
}
