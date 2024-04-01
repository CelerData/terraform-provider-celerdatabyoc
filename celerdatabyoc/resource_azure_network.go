package celerdatabyoc

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/network"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func azureResourceNetwork() *schema.Resource {
	return &schema.Resource{
		CreateContext: azureResourceNetworkCreate,
		ReadContext:   azureResourceNetworkRead,
		DeleteContext: azureResourceNetworkDelete,
		Schema: map[string]*schema.Schema{
			"id": {
				Type:     schema.TypeString,
				Optional: true,
				Computed: true,
			},
			"name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringMatch(regexp.MustCompile(`^[0-9a-zA-Z_-]{1,128}$`), "The name is restricted to a maximum length of 128 characters and can only consist of alphanumeric characters (a-z, A-Z, 0-9), hyphens (-), and underscores (_)."),
				DefaultFunc: func() (any, error) {
					currDate := time.Now().Format("20060102")
					randomStr := uuid.NewString()
					return fmt.Sprintf("azure-network-credential-%s-%s", currDate, randomStr[:6]), nil
				},
			},
			"virtual_network_resource_id": {
				Type:        schema.TypeString,
				Description: "The resource ID of the Azure virtual network that you want to deploy cluster in.",
				Required:    true,
				ForceNew:    true,
			},
			"subnet_name": {
				Type:        schema.TypeString,
				Description: "The subnet name.",
				Required:    true,
				ForceNew:    true,
			},
			"public_accessible": {
				Type:        schema.TypeBool,
				Description: "You can optionally specify whether the cluster can be accessed from public networks by selecting or clearing the check box next to Public accessible. If you select Public accessible, CelerData Cloud will attach a load balancer to the cluster to distribute incoming queries, and will assign a public domain name to the cluster so you can access the cluster over a public network. If you do not select Public accessible, the cluster is accessible only through a private domain name.",
				Required:    false,
				Default:     false,
				ForceNew:    true,
			},
		},
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
	}
}

func azureResourceNetworkCreate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	networkCli := network.NewNetworkAPI(c)
	req := &network.CreateNetworkReq{
		Csp:             "azure",
		Name:            d.Get("name").(string),
		PublicAccess:    d.Get("public_accessible").(bool),
		SubnetId:        fmt.Sprintf("%s/subnets/%s", d.Get("virtual_network_resource_id").(string), d.Get("subnet_name").(string)),
		SecurityGroupId: "temp",
	}

	resp, err := networkCli.CreateNetwork(ctx, req)
	if err != nil {
		return diag.FromErr(err)
	}

	log.Printf("[DEBUG] create network succeeded, id:%s]", resp.NetworkID)
	d.SetId(resp.NetworkID)
	return diags
}

func azureResourceNetworkRead(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	netID := d.Id()
	networkCli := network.NewNetworkAPI(c)
	// // Warning or errors can be collected in a slice type
	log.Printf("[DEBUG] get network, id[%s]", netID)
	resp, err := networkCli.GetNetwork(ctx, netID)
	if err != nil {
		return diag.FromErr(err)
	}
	if resp.Network == nil || len(resp.Network.BizID) == 0 {
		d.SetId("")
	}

	log.Printf("[DEBUG] get Network, resp:%+v", resp)
	return diags
}

func azureResourceNetworkDelete(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	netID := d.Id()
	networkCli := network.NewNetworkAPI(c)
	// // Warning or errors can be collected in a slice type
	log.Printf("[DEBUG] delete network, id[%s]", netID)
	err := networkCli.DeleteNetwork(ctx, netID)
	if err != nil {
		return diag.FromErr(err)
	}

	// d.SetId("") is automatically called assuming delete returns no errors, but
	// it is added here for explicitness.
	d.SetId("")
	return diags
}
