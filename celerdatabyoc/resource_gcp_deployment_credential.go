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

func gcpResourceDeploymentCredential() *schema.Resource {
	return &schema.Resource{
		CreateContext: gcpResourceDeploymentCredentialCreate,
		UpdateContext: gcpResourceDeploymentCredentialUpdate,
		ReadContext:   gcpResourceDeploymentCredentialRead,
		DeleteContext: gcpResourceDeploymentCredentialDelete,
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
			"service_account": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
				Default:  "",
			},
			"project_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
		},
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
	}
}

func gcpResourceDeploymentCredentialCreate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	credCli := credential.NewCredentialAPI(c)
	req := &credential.CreateGcpDeployCredReq{
		Name:           d.Get("name").(string),
		ServiceAccount: d.Get("client_secret_value").(string),
		ProjectId:      d.Get("project_id").(string),
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

	if resp.DeployAkSkCred == nil || len(resp.DeployAkSkCred.BizID) == 0 {
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
