package config

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"terraform-provider-celerdatabyoc/celerdata-sdk/version"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

const OauthProviderName = "OAUTH"

type OauthProvider struct {
	refreshTokenSource oauth2.TokenSource
	reuseTokenSource   oauth2.TokenSource
	cfg                *oauth2.Config
	apiVersion         version.ApiVersion
}

func (c *OauthProvider) Name() string {
	return OauthProviderName
}

func (c *OauthProvider) Configure(ctx context.Context, cfg *Config) error {
	if len(cfg.ClientID) == 0 || len(cfg.ClientSecret) == 0 {
		return errors.New("client id or client secret not found")
	}

	ts := (&clientcredentials.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		AuthStyle:    oauth2.AuthStyleInHeader,
		TokenURL:     fmt.Sprintf("%s/api/%s/token", cfg.Host, c.apiVersion),
		Scopes:       []string{"all-apis"},
	}).TokenSource(ctx)
	c.reuseTokenSource = oauth2.ReuseTokenSource(nil, ts)
	c.cfg = &oauth2.Config{
		Endpoint: oauth2.Endpoint{
			TokenURL: fmt.Sprintf("%s/api/%s/token", cfg.Host, c.apiVersion),
		},
		Scopes: []string{"all-apis"},
	}
	c.refreshTokenSource = c.cfg.TokenSource(ctx, nil)
	return nil
}

func (c *OauthProvider) Auth(r *http.Request) error {
	t, err := c.refreshTokenSource.Token()
	if err == nil {
		t.SetAuthHeader(r)
		return nil
	}

	token, err := c.reuseTokenSource.Token()
	if err != nil {
		return fmt.Errorf("%w", err)
	}

	c.refreshTokenSource = c.cfg.TokenSource(context.Background(), token)
	token.SetAuthHeader(r)
	return nil
}
