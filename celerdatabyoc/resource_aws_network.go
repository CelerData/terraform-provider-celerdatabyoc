package celerdatabyoc

import (
	"context"
	"log"
	"regexp"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/network"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func resourceNetwork() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceNetworkCreate,
		ReadContext:   resourceNetworkRead,
		DeleteContext: resourceNetworkDelete,
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
			"subnet_id": {
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				ExactlyOneOf: []string{"subnet_id", "subnet_ids"},
			},
			"security_group_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"deployment_credential_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"vpc_endpoint_id": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"region": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"subnet_ids": {
				Type:         schema.TypeSet,
				Optional:     true,
				ForceNew:     true,
				MaxItems:     3,
				MinItems:     3,
				Elem:         &schema.Schema{Type: schema.TypeString},
				Set:          schema.HashString,
				ExactlyOneOf: []string{"subnet_id", "subnet_ids"},
			},
			"multi_az": {
				Type:     schema.TypeBool,
				Optional: true,
				Computed: true,
			},
		},
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
	}
}

func resourceNetworkCreate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	subnetIDsSet := d.Get("subnet_ids").(*schema.Set)
	subnetIDs := make([]string, subnetIDsSet.Len())

	for i, v := range subnetIDsSet.List() {
		subnetIDs[i] = v.(string)
	}
	networkCli := network.NewNetworkAPI(c)
	req := &network.CreateNetworkReq{
		Name:            d.Get("name").(string),
		SubnetId:        d.Get("subnet_id").(string),
		SecurityGroupId: d.Get("security_group_id").(string),
		DeployCredID:    d.Get("deployment_credential_id").(string),
		SubnetIds:       subnetIDs,
		Csp:             "aws",
		Region:          d.Get("region").(string),
	}
	if v, ok := d.GetOk("vpc_endpoint_id"); ok {
		req.VpcEndpointId = v.(string)
	}

	resp, err := networkCli.CreateNetwork(ctx, req)
	if err != nil {
		return diag.FromErr(err)
	}

	log.Printf("[DEBUG] create network succeeded, id:%s]", resp.NetworkID)
	d.SetId(resp.NetworkID)
	return diags
}

func resourceNetworkRead(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	netID := d.Id()
	networkCli := network.NewNetworkAPI(c)
	// // Warning or errors can be collected in a slice type
	log.Printf("[DEBUG] get network, id[%s]", netID)
	resp, err := networkCli.GetNetwork(ctx, netID)
	if err != nil {
		return diag.FromErr(err)
	}

	d.Set("multi_az", resp.Network.MultiAz)
	if resp.Network.MultiAz {
		subnetIds := make([]string, 0, len(resp.Network.AZNetWorkInterfaces))
		for _, net := range resp.Network.AZNetWorkInterfaces {
			subnetIds = append(subnetIds, net.SubnetId)
		}

		d.Set("subnet_ids", subnetIds)
	}

	log.Printf("[DEBUG] get Network, resp:%+v", resp)
	return diags
}

func resourceNetworkDelete(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
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
