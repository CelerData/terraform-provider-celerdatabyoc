package celerdatabyoc

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/credential"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func azureResourceDataCredential() *schema.Resource {
	return &schema.Resource{
		CreateContext: azureResourceDataCredentialCreate,
		ReadContext:   azureResourceDataCredentialRead,
		DeleteContext: azureResourceDataCredentialDelete,
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
				DefaultFunc: func() (any, error) {
					currDate := time.Now().Format("20060102")
					randomStr := uuid.NewString()
					return fmt.Sprintf("azure-data-credential-%s-%s", currDate, randomStr[:6]), nil
				},
			},
			"managed_identity_resource_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"storage_account_name": {
				Type:        schema.TypeString,
				Description: "The storage account which you want to use to storage the SQL profile.",
				Required:    true,
				ForceNew:    true,
			},
			"container_name": {
				Type:        schema.TypeString,
				Description: "The container which you want to use to storage the SQL profile.",
				Required:    true,
				ForceNew:    true,
			},
		},
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
	}
}

func azureResourceDataCredentialCreate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	credCli := credential.NewCredentialAPI(c)
	req := &credential.CreateDataCredReq{
		Csp:                "azure",
		Name:               d.Get("name").(string),
		InstanceProfileArn: d.Get("managed_identity_resource_id").(string),
		BucketName:         fmt.Sprintf("%s/%s", d.Get("storage_account_name").(string), d.Get("container_name").(string)),
		PolicyVersion:      "temp",
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

func azureResourceDataCredentialRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
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

	if len(resp.DataCred.BizID) == 0 {
		d.SetId("")
	}

	log.Printf("[DEBUG] get data credential, resp:%+v", resp)

	return diags
}

func azureResourceDataCredentialDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
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
