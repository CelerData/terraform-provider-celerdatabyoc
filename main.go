package main

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/plugin"

	"terraform-provider-celerdatabyoc/celerdatabyoc"
)

func main() {
	//dadfadafadfa
	plugin.Serve(&plugin.ServeOpts{
		ProviderFunc: func() *schema.Provider {
			return celerdatabyoc.Provider()
		},
	})
}
