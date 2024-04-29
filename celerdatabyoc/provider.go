package celerdatabyoc

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/config"
	"terraform-provider-celerdatabyoc/celerdata-sdk/version"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

const DEFAULT_CELERDATA_HOST = "https://cloud-api.celerdata.com"

// Provider -
func Provider() *schema.Provider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"host": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "https://cloud-api.celerdata.com",
				DefaultFunc: schema.EnvDefaultFunc("CELERDATA_HOST", DEFAULT_CELERDATA_HOST),
				ValidateFunc: func(i interface{}, k string) (_ []string, errors []error) {
					v, ok := i.(string)
					if !ok {
						errors = append(errors, fmt.Errorf("expected type of %q to be string", k))
						return
					}

					if v == "" {
						errors = append(errors, fmt.Errorf("expected %q url to not be empty, got %v", k, i))
						return
					}

					u, err := url.Parse(v)
					if err != nil {
						errors = append(errors, fmt.Errorf("expected %q to be a valid url, got %v: %+v", k, v, err))
						return
					}

					if u.Host == "" {
						errors = append(errors, fmt.Errorf("expected %q to have a host, got %v", k, v))
						return
					}

					if u.Scheme == "https" && strings.HasSuffix(u.Host, "celerdata.com") {
						return
					}

					errors = append(errors, fmt.Errorf("expected %q to have a url whose schema is https and whose host suffix is celerdata.com, got %v", k, v))
					return
				},
			},
			"client_id": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("CELERDATA_CLIENT_ID", nil),
			},
			"client_secret": {
				Type:        schema.TypeString,
				Optional:    true,
				Sensitive:   true,
				DefaultFunc: schema.EnvDefaultFunc("CELERDATA_CLIENT_SECRET", nil),
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"celerdatabyoc_classic_cluster":                         resourceClassicCluster(),
			"celerdatabyoc_elastic_cluster":                         resourceElasticCluster(),
			"celerdatabyoc_aws_deployment_role_credential":          resourceDeploymentRoleCredential(),
			"celerdatabyoc_aws_data_credential":                     resourceDataCredential(),
			"celerdatabyoc_aws_network":                             resourceNetwork(),
			"celerdatabyoc_aws_deployment_credential_assume_policy": resourceAwsDeployCredAssumePolicy(),
			"celerdatabyoc_aws_data_credential_policy":              resourceAwsDataCredentialPolicy(),
			"celerdatabyoc_aws_deployment_credential_policy":        resourceAwsDeploymentCredentialPolicy(),
			"celerdatabyoc_cluster_endpoints":                       resourceClusterEndpoints(),
			"celerdatabyoc_cluster_user":                            resourceClusterUser(),
			"celerdatabyoc_cluster_domain_ssl_cert":                 resourceClusterSSLCert(),
			"celerdatabyoc_azure_data_credential":                   azureResourceDataCredential(),
			"celerdatabyoc_azure_deployment_credential":             azureResourceDeploymentCredential(),
			"celerdatabyoc_azure_network":                           azureResourceNetwork(),
		},
		DataSourcesMap: map[string]*schema.Resource{
			"celerdatabyoc_aws_data_credential_assume_policy": dataAwsDataCredentialAssumeRolePolicy(),
		},
		ConfigureContextFunc: providerConfigure,
	}
}

func providerConfigure(ctx context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
	host := d.Get("host").(string)
	clientID := d.Get("client_id").(string)
	clientSecret := d.Get("client_secret").(string)

	cfg := &config.Config{
		Host:         host,
		ClientID:     clientID,
		ClientSecret: clientSecret,
	}
	var diags diag.Diagnostics

	c, err := client.New(cfg)
	if err != nil {
		diags = append(diags, diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "Unable to create Celerdata client",
			Detail:   "Unable to create Celerdata client",
		})
		return nil, diags
	}

	err = c.Get(ctx, fmt.Sprintf("/api/%s/ping", version.API_1_0), nil, nil)
	if err != nil {
		diags = append(diags, diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "Verify client_id/client_secret failed",
			Detail:   err.Error(),
		})
		return nil, diags
	}

	return c, diags
}
