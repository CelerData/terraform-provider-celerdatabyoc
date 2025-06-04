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

func gcpResourceDataCredential() *schema.Resource {
	return &schema.Resource{
		CreateContext: gcpResourceDataCredentialCreate,
		ReadContext:   gcpResourceDataCredentialRead,
		DeleteContext: gcpResourceDataCredentialDelete,
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
			"bucket_name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"service_account": {
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

func gcpResourceDataCredentialCreate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	credCli := credential.NewCredentialAPI(c)
	req := &credential.CreateGcpDataCredReq{
		Name:           d.Get("name").(string),
		BucketName:     d.Get("bucket_name").(string),
		ServiceAccount: d.Get("service_account").(string),
	}
	log.Printf("[DEBUG] create data credential, req:%+v", req)
	resp, err := credCli.CreateGcpDataCredential(ctx, req)
	if err != nil {
		return diag.FromErr(err)
	}

	log.Printf("[DEBUG] create data credential[%s] success", resp.CredID)
	d.SetId(resp.CredID)
	return diags
}

func gcpResourceDataCredentialRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)

	credID := d.Id()
	credCli := credential.NewCredentialAPI(c)
	var diags diag.Diagnostics

	log.Printf("[DEBUG] get data credential, id[%s]", credID)
	resp, err := credCli.GetDataCredential(ctx, credID)
	if err != nil {
		return diag.FromErr(err)
	}

	if len(resp.DataCred.BizID) == 0 {
		d.SetId("")
	}

	log.Printf("[DEBUG] get data credential, resp:%+v", resp)

	return diags
}

func gcpResourceDataCredentialDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)

	credID := d.Id()
	credCli := credential.NewCredentialAPI(c)
	var diags diag.Diagnostics

	log.Printf("[DEBUG] delete data role credential, id:%s", credID)
	err := credCli.DeleteDataCredential(ctx, credID)
	if err != nil {
		return diag.FromErr(err)
	}
	return diags
}
