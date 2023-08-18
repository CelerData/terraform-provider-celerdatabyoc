package config

import (
	"context"
	"net/http"
	"terraform-provider-celerdatabyoc/celerdata-sdk/version"

	"github.com/hashicorp/go-multierror"
)

type Config struct {
	Host string `name:"host"`

	ClientID     string `name:"client_id"`
	ClientSecret string `name:"client_secret"`

	AuthType string `name:"auth_type"`

	HTTPTimeoutSeconds int `name:"http_timeout_seconds"`

	RateLimitPerSecond int `name:"rate_limit"`

	RetryTimeoutSeconds int `name:"retry_timeout_seconds"`
}

func (c *Config) ConfigAuthProvider(ctx context.Context) (AuthProvider, error) {
	authProviders := []AuthProvider{
		&OauthProvider{
			apiVersion: version.API_1_0,
		},
	}
	var result error

	for _, p := range authProviders {
		if c.AuthType != "" && p.Name() != c.AuthType {
			continue
		}

		err := p.Configure(ctx, c)
		if err == nil {
			return p, nil
		}

		result = multierror.Append(result, err)
	}

	return nil, result
}

type AuthProvider interface {
	Name() string

	Configure(context.Context, *Config) error

	Auth(r *http.Request) error
}
