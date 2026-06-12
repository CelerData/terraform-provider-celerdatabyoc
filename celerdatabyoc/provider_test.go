package celerdatabyoc

import (
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

var testAccProviders map[string]*schema.Provider
var testAccProvider *schema.Provider

func TestMain(m *testing.M) {
	testAccProvider = Provider()
	testAccProviders = map[string]*schema.Provider{
		"celerdatabyoc": testAccProvider,
	}
	os.Exit(m.Run())
}

func TestProvider(t *testing.T) {
	if err := Provider().InternalValidate(); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func TestProvider_impl(t *testing.T) {
	var _ *schema.Provider = Provider()
}

func testPreCheck(t *testing.T) {
	for _, k := range []string{
		"CELERDATA_HOST",
		"CELERDATA_CLIENT_ID",
		"CELERDATA_CLIENT_SECRET",
		"CELERDATA_CSP",
		"CELERDATA_REGION",
	} {
		if os.Getenv(k) == "" {
			t.Fatalf("%s must be set for acceptance tests", k)
		}
	}
}
