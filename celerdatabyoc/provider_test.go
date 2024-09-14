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

	err = os.Setenv("CELERDATA_HOST", "https://cloud-api-sandbox.celerdata.com")
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

	err = os.Setenv("CELERDATA_CLIENT_ID", "a2735be0-4542-4c11-9871-8426fc6d4f25")
	if err != nil {
		panic(err)
	}

	err = os.Setenv("CELERDATA_CLIENT_SECRET", "s74dAo7T6IpzhRuE58UsGTluBMEBLjIxSiPKcWrr")
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
