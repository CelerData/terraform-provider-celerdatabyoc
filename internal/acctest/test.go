package acctest

import (
	"log"
	"os"
	"terraform-provider-celerdatabyoc/celerdatabyoc"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

var Provider *schema.Provider
var TestAccProviders map[string]*schema.Provider
var TestAccProviderFactories map[string]func() (*schema.Provider, error)

func PreCheck(t *testing.T) {
	if err := os.Getenv("CELERDATA_HOST"); err == "" {
		t.Fatal("CELERDATA_HOST must be set for acceptance tests")
	}
	if err := os.Getenv("CELERDATA_CLIENT_ID"); err == "" {
		t.Fatal("CELERDATA_CLIENT_ID must be set for acceptance tests")
	}
	if err := os.Getenv("CELERDATA_CLIENT_SECRET"); err == "" {
		t.Fatal("CELERDATA_CLIENT_SECRET must be set for acceptance tests")
	}
}

func init() {
	Provider := celerdatabyoc.Provider()
	TestAccProviders = map[string]*schema.Provider{
		"celerdatabyoc": Provider,
	}
	TestAccProviderFactories =
		map[string]func() (*schema.Provider, error){
			"celerdatabyoc": func() (*schema.Provider, error) {
				return Provider, nil
			},
		}
}

func ReadFile(filePath string) string {
	if filePath == "" {
		log.Fatalf("filePath is invalid")
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Failed to read file:%s err: %v", filePath, err)
	}
	return string(data)
}
