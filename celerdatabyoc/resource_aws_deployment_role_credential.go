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

func resourceDeploymentRoleCredential() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceDeploymentRoleCredentialCreate,
		ReadContext:   resourceDeploymentRoleCredentialRead,
		DeleteContext: resourceDeploymentRoleCredentialDelete,
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
			"external_id": {
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

func resourceDeploymentRoleCredentialCreate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)

	credCli := credential.NewCredentialAPI(c)
	req := &credential.CreateDeployRoleCredReq{
		Csp:           "aws",
		Name:          d.Get("name").(string),
		RoleArn:       d.Get("role_arn").(string),
		ExternalId:    d.Get("external_id").(string),
		PolicyVersion: d.Get("policy_version").(string),
	}
	log.Printf("[DEBUG] create deployment role credential, req:%+v", req)

	err := delayRetryContext(ctx, 10*time.Second, time.Minute, func() *retry.RetryError {
		resp, err := credCli.CreateDeploymentRoleCredential(ctx, req)
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

func delayRetryContext(ctx context.Context, delay, timeout time.Duration, f retry.RetryFunc) error {
	// These are used to pull the error out of the function; need a mutex to
	// avoid a data race.
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

	// Need to acquire the lock here to be able to avoid race using resultErr as
	// the return value
	resultErrMu.Lock()
	defer resultErrMu.Unlock()

	// resultErr may be nil because the wait timed out and resultErr was never
	// set; this is still an error
	if resultErr == nil {
		return waitErr
	}
	// resultErr takes precedence over waitErr if both are set because it is
	// more likely to be useful
	return resultErr
}

func resourceDeploymentRoleCredentialRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
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

func resourceDeploymentRoleCredentialDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)

	// Warning or errors can be collected in a slice type
	credID := d.Id()
	credCli := credential.NewCredentialAPI(c)
	// Warning or errors can be collected in a slice type
	var diags diag.Diagnostics

	log.Printf("[DEBUG] delete deployment role credential, id:%s", credID)
	err := credCli.DeleteDeploymentRoleCredential(ctx, credID)
	if err != nil {
		return diag.FromErr(err)
	}

	// d.SetId("") is automatically called assuming delete returns no errors, but
	// it is added here for explicitness.
	d.SetId("")
	return diags
}
