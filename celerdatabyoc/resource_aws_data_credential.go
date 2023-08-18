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

func resourceDataCredential() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceDataCredentialCreate,
		ReadContext:   resourceDataCredentialRead,
		DeleteContext: resourceDataCredentialDelete,
		Schema: map[string]*schema.Schema{
			"id": {
				Type:     schema.TypeString,
				Computed: true,
			},
			"name": {
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringMatch(regexp.MustCompile(`^[0-9a-zA-Z_-]{1,128}$`), "The name is restricted to a maximum length of 128 characters and can only consist of alphanumeric characters (a-z, A-Z, 0-9), hyphens (-), and underscores (_)."),
			},
			"role_arn": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"instance_profile_arn": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"bucket_name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"policy_version": {
				Type:     schema.TypeString,
				ForceNew: true,
				Required: true,
			},
		},
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
	}
}

func resourceDataCredentialCreate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	credCli := credential.NewCredentialAPI(c)
	req := &credential.CreateDataCredReq{
		Csp:                "aws",
		Name:               d.Get("name").(string),
		RoleArn:            d.Get("role_arn").(string),
		InstanceProfileArn: d.Get("instance_profile_arn").(string),
		BucketName:         d.Get("bucket_name").(string),
		PolicyVersion:      d.Get("policy_version").(string),
	}
	log.Printf("[DEBUG] create data credential, req:%+v", req)
	resp, err := credCli.CreateDataCredential(ctx, req)
	if err != nil {
		return diag.FromErr(err)
	}

	log.Printf("[DEBUG] create data credential[%s] success", resp.CredID)
	d.SetId(resp.CredID)

	return diags
}

func resourceDataCredentialRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)

	// Warning or errors can be collected in a slice type
	credID := d.Id()
	credCli := credential.NewCredentialAPI(c)
	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	log.Printf("[DEBUG] get data credential, id[%s]", credID)
	resp, err := credCli.GetDataCredential(ctx, credID)
	if err != nil {
		return diag.FromErr(err)
	}

	log.Printf("[DEBUG] get data credential, resp:%+v", resp)

	return diags
}

func resourceDataCredentialDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)

	// Warning or errors can be collected in a slice type
	credID := d.Id()
	credCli := credential.NewCredentialAPI(c)
	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	log.Printf("[DEBUG] delete data role credential, id:%s", credID)
	err := credCli.DeleteDataCredential(ctx, credID)
	if err != nil {
		return diag.FromErr(err)
	}

	// d.SetId("") is automatically called assuming delete returns no errors, but
	// it is added here for explicitness.
	d.SetId("")
	return diags
}
