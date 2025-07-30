package celerdatabyoc

import (
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

var testAccProviders map[string]*schema.Provider
var testAccProvider *schema.Provider

func TestMain(m *testing.M) {
	err := os.Setenv("CELERDATA_ACCOUNT_ID", "caxgqwn0a")
	if err != nil {
		panic(err)
	}

	err = os.Setenv("CELERDATA_HOST", "https://cloud-api.celerdata.com")
	// err = os.Setenv("CELERDATA_HOST", "http://localhost:18455")
	if err != nil {
		panic(err)
	}

	err = os.Setenv("CELERDATA_CSP", "aws")
	if err != nil {
		panic(err)
	}

	err = os.Setenv("CELERDATA_REGION", "us-west-2")
	if err != nil {
		panic(err)
	}

	err = os.Setenv("CELERDATA_CLIENT_ID", "1c627f4c-39b6-465f-bbc7-347928f95056")
	if err != nil {
		panic(err)
	}

	err = os.Setenv("CELERDATA_CLIENT_SECRET", "R84fPK9R0zkU8Pm8n3edURBsFSldDKS44OaUW31r")
	if err != nil {
		panic(err)
	}

	err = os.Setenv("TF_ACC", "1")
	if err != nil {
		panic(err)
	}

	err = os.Setenv("TF_LOG", "DEBUG")
	if err != nil {
		panic(err)
	}

	testAccProvider = Provider()
	testAccProviders = map[string]*schema.Provider{
		"celerdatabyoc": testAccProvider,
	}
	ec := m.Run()
	os.Exit(ec)
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
	if err := os.Getenv("CELERDATA_CSP"); err == "" {
		t.Fatal("CELERDATA_CSP must be set for acceptance tests")
	}
	if err := os.Getenv("CELERDATA_REGION"); err == "" {
		t.Fatal("CELERDATA_REGION must be set for acceptance tests")
	}
}
