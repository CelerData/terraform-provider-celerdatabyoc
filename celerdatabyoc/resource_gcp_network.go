package celerdatabyoc

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/network"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func gcpResourceNetwork() *schema.Resource {
	return &schema.Resource{
		CreateContext: gcpResourceNetworkCreate,
		ReadContext:   gcpResourceNetworkRead,
		DeleteContext: gcpResourceNetworkDelete,
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
			},
			"region": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"subnet_name": {
				Type:       schema.TypeString,
				Optional:   true,
				ForceNew:   true,
				Deprecated: "This field has been deprecated. Please use the 'subnet' field.",
			},
			"subnet": {
				Type:          schema.TypeString,
				Optional:      true,
				ForceNew:      true,
				ConflictsWith: []string{"subnet_name"},
			},
			"network_tag": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"psc_connection_id": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"deployment_credential_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
		},
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		CustomizeDiff: func(ctx context.Context, diff *schema.ResourceDiff, i interface{}) error {
			subnetName := diff.Get("subnet_name").(string)
			if subnetName != "" {
				log.Printf("[WARN] 'subnet_name' is deprecated. Please use 'subnet' instead.")
			}
			return nil
		},
	}
}

func gcpResourceNetworkCreate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)
	networkCli := network.NewNetworkAPI(c)

	subnet := d.Get("subnet").(string)
	if subnet == "" {
		subnet = d.Get("subnet_name").(string)
	}
	if subnet == "" {
		return diag.FromErr(fmt.Errorf("attribute 'subnet' is required"))
	}

	req := &network.CreateGcpNetworkReq{
		DeploymentCredentialID: d.Get("deployment_credential_id").(string),
		Name:                   d.Get("name").(string),
		Region:                 d.Get("region").(string),
		NetworkTag:             d.Get("network_tag").(string),
		Subnet:                 subnet,
		PscConnectionId:        d.Get("psc_connection_id").(string),
	}

	resp, err := networkCli.CreateGcpNetwork(ctx, req)
	if err != nil {
		return diag.FromErr(err)
	}

	log.Printf("[DEBUG] create network succeeded, id:%s]", resp.NetworkID)
	d.SetId(resp.NetworkID)
	return diags
}

func gcpResourceNetworkRead(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	netID := d.Id()
	networkCli := network.NewNetworkAPI(c)
	log.Printf("[DEBUG] get network, networkCredential ID: %s", netID)
	resp, err := networkCli.GetNetwork(ctx, netID)
	log.Printf("[DEBUG] get network, resp:%+v", resp)
	if err != nil {
		return diag.FromErr(err)
	}
	if resp.Network == nil || resp.Network.BizID == "" {
		d.SetId("")
	}
	log.Printf("[DEBUG] get Network, resp:%+v", resp)
	return diags
}

func gcpResourceNetworkDelete(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	netID := d.Id()
	networkCli := network.NewNetworkAPI(c)
	log.Printf("[DEBUG] delete network, id[%s]", netID)
	err := networkCli.DeleteNetwork(ctx, netID)
	if err != nil {
		return diag.FromErr(err)
	}
	return diags
}
