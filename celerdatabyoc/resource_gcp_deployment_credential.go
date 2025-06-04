package celerdatabyoc

import (
	"context"
	"log"
	"regexp"
	"strings"
	"sync"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/credential"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/retry"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func gcpResourceDeploymentCredential() *schema.Resource {
	return &schema.Resource{
		CreateContext: gcpResourceDeploymentCredentialCreate,
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
		ServiceAccount: d.Get("service_account").(string),
		ProjectId:      d.Get("project_id").(string),
	}
	log.Printf("[DEBUG] create deployment credential, req:%+v", req)
	err := gcpDelayRetryContext(ctx, 10*time.Second, time.Minute, func() *retry.RetryError {
		resp, err := credCli.CreateGcpDeploymentCredential(ctx, req)
		if err != nil {
			if strings.Contains(err.Error(), "Role arn or external id invalid.") {
				return retry.RetryableError(err)
			}
			return retry.NonRetryableError(err)
		}

		log.Printf("[DEBUG] create deployment role credential[%s] success", resp.CredID)
		d.SetId(resp.CredID)
		return nil
	})
	if err != nil {
		return diag.FromErr(err)
	}

	return diags
}

func gcpDelayRetryContext(ctx context.Context, delay, timeout time.Duration, f retry.RetryFunc) error {
	var resultErr error
	var resultErrMu sync.Mutex

	c := &retry.StateChangeConf{
		Delay:      delay,
		Pending:    []string{"retryableerror"},
		Target:     []string{"success"},
		Timeout:    timeout,
		MinTimeout: 500 * time.Millisecond,
		Refresh: func() (interface{}, string, error) {
			rerr := f()

			resultErrMu.Lock()
			defer resultErrMu.Unlock()

			if rerr == nil {
				resultErr = nil
				return 42, "success", nil
			}

			resultErr = rerr.Err

			if rerr.Retryable {
				return 42, "retryableerror", nil
			}
			return nil, "quit", rerr.Err
		},
	}

	_, waitErr := c.WaitForStateContext(ctx)
	resultErrMu.Lock()
	defer resultErrMu.Unlock()
	if resultErr == nil {
		return waitErr
	}
	return resultErr
}

func gcpResourceDeploymentCredentialRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)

	credID := d.Id()
	credCli := credential.NewCredentialAPI(c)
	var diags diag.Diagnostics

	log.Printf("[DEBUG] get deployment role credential, id[%s]", credID)
	resp, err := credCli.GetDeploymentRoleCredential(ctx, credID)
	if err != nil {
		return diag.FromErr(err)
	}

	log.Printf("[DEBUG] get deployment role credential, resp:%+v", resp)

	return diags
}

func gcpResourceDeploymentCredentialDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)
	credID := d.Id()
	credCli := credential.NewCredentialAPI(c)
	var diags diag.Diagnostics

	log.Printf("[DEBUG] delete deployment role credential, id:%s", credID)
	err := credCli.DeleteDeploymentRoleCredential(ctx, credID)
	if err != nil {
		return diag.FromErr(err)
	}
	return diags
}
