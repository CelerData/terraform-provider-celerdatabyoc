package celerdatabyoc

import (
	"context"
	"log"
	"regexp"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/credential"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func azureResourceDeploymentCredential() *schema.Resource {
	return &schema.Resource{
		CreateContext: azureResourceDeploymentCredentialCreate,
		UpdateContext: azureResourceDeploymentCredentialUpdate,
		ReadContext:   azureResourceDeploymentCredentialRead,
		DeleteContext: azureResourceDeploymentCredentialDelete,
		Schema: map[string]*schema.Schema{
			"id": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringMatch(regexp.MustCompile(`^[0-9a-zA-Z_-]{1,128}$`), "The name is restricted to a maximum length of 128 characters and can only consist of alphanumeric characters (a-z, A-Z, 0-9), hyphens (-), and underscores (_)."),
			},
			"application_id": {
				Type:        schema.TypeString,
				Description: "The Application (client) ID of your App registration.",
				Required:    true,
				ForceNew:    true,
			},
			"directory_id": {
				Type:        schema.TypeString,
				Description: "The Directory (tenant) ID of your App registration.",
				Required:    true,
				ForceNew:    true,
			},
			"client_secret_value": {
				Type:        schema.TypeString,
				Description: "The client secret value of the App registration. Client secret values cannot be viewed, except for immediately after creation. Be sure to save the secret when created before leaving the Azure console.",
				Required:    true,
				Sensitive:   true,
			},
			"ssh_key_resource_id": {
				Type:        schema.TypeString,
				Description: "The resource ID of the SSH key pair.",
				ForceNew:    true,
				Required:    true,
			},
		},
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
	}
}

func azureResourceDeploymentCredentialCreate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	credCli := credential.NewCredentialAPI(c)
	req := &credential.CreateDeployAkSkCredReq{
		Csp:               "azure",
		Name:              d.Get("name").(string),
		TenantId:          d.Get("directory_id").(string),
		ApplicationId:     d.Get("application_id").(string),
		ClientSecretValue: d.Get("client_secret_value").(string),
		SshKeyResourceId:  d.Get("ssh_key_resource_id").(string),
	}
	log.Printf("[DEBUG] create deployment credential, req:%+v", req)

	resp, err := credCli.CreateDeploymentAkSkCredential(ctx, req)
	if err != nil {
		return diag.FromErr(err)
	}
	d.SetId(resp.CredID)

	return diags
}

func azureResourceDeploymentCredentialUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {

	var diags diag.Diagnostics

	if !d.IsNewResource() && d.HasChange("client_secret_value") {
		log.Printf("[DEBUG] rotate deployment credential")
		c := m.(*client.CelerdataClient)

		credCli := credential.NewCredentialAPI(c)
		credID := d.Id()

		err := credCli.RotateAkSkCredential(ctx, &credential.RotateAkSkCredentialReq{
			CredID: credID,
			Ak:     d.Get("application_id").(string),
			Sk:     d.Get("client_secret_value").(string),
		})
		if err != nil {
			return diag.FromErr(err)
		}
	}

	return diags
}

func azureResourceDeploymentCredentialRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)

	credID := d.Id()
	credCli := credential.NewCredentialAPI(c)
	var diags diag.Diagnostics

	log.Printf("[DEBUG] get deployment credential, id[%s]", credID)
	resp, err := credCli.GetDeploymentAkSkCredential(ctx, credID)
	if err != nil {
		return diag.FromErr(err)
	}

	if resp.DeployRoleCred == nil || len(resp.DeployRoleCred.BizID) == 0 {
		d.SetId("")
	}

	log.Printf("[DEBUG] get deployment credential, resp:%+v", resp)

	return diags
}

func azureResourceDeploymentCredentialDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)

	// Warning or errors can be collected in a slice type
	credID := d.Id()
	credCli := credential.NewCredentialAPI(c)
	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	log.Printf("[DEBUG] delete deployment credential, id:%s", credID)
	err := credCli.DeleteDeploymentAkSkCredential(ctx, credID)
	if err != nil {
		return diag.FromErr(err)
	}

	// d.SetId("") is automatically called assuming delete returns no errors, but
	// it is added here for explicitness.
	d.SetId("")
	return diags
}
