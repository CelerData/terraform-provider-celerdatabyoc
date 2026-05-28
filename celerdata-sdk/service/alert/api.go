package alert

import (
	"context"
	"fmt"
	"net/url"

	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/version"
)

// IAlertAPI covers the Alert Policy and PagerDuty Integration endpoints exposed
// under /api/<version>/. Policy endpoints are region-scoped; integration
// endpoints are central (account-scoped).
type IAlertAPI interface {
	// PagerDuty integrations.
	CreatePagerDutyIntegration(ctx context.Context, req *CreatePagerDutyIntegrationReq) (*CreatePagerDutyIntegrationResp, error)
	ListPagerDutyIntegrations(ctx context.Context) (*ListPagerDutyIntegrationsResp, error)
	GetPagerDutyIntegration(ctx context.Context, integrationID string) (*GetPagerDutyIntegrationResp, error)
	UpdatePagerDutyIntegration(ctx context.Context, integrationID string, req *UpdatePagerDutyIntegrationReq) error
	DeletePagerDutyIntegration(ctx context.Context, integrationID string) error

	// Alert policies.
	CreateAlertPolicy(ctx context.Context, req *UpsertAlertPolicyReq) (*CreateAlertPolicyResp, error)
	ListAlertPolicies(ctx context.Context, region string) (*ListAlertPoliciesResp, error)
	GetAlertPolicy(ctx context.Context, region, policyID string) (*GetAlertPolicyResp, error)
	UpdateAlertPolicy(ctx context.Context, policyID string, req *UpsertAlertPolicyReq) error
	DeleteAlertPolicy(ctx context.Context, region, policyID string) error
}

func NewAlertAPI(cli *client.CelerdataClient) IAlertAPI {
	return &alertAPI{cli: cli, apiVersion: version.API_1_0}
}

type alertAPI struct {
	cli        *client.CelerdataClient
	apiVersion version.ApiVersion
}

func (a *alertAPI) pdPath(suffix string) string {
	if suffix == "" {
		return fmt.Sprintf("/api/%s/pagerduty-integrations", a.apiVersion)
	}
	return fmt.Sprintf("/api/%s/pagerduty-integrations/%s", a.apiVersion, suffix)
}

func (a *alertAPI) policyPath(suffix string) string {
	if suffix == "" {
		return fmt.Sprintf("/api/%s/alert/policies", a.apiVersion)
	}
	return fmt.Sprintf("/api/%s/alert/policies/%s", a.apiVersion, suffix)
}

// PagerDuty.

func (a *alertAPI) CreatePagerDutyIntegration(ctx context.Context, req *CreatePagerDutyIntegrationReq) (*CreatePagerDutyIntegrationResp, error) {
	resp := &CreatePagerDutyIntegrationResp{}
	if err := a.cli.Post(ctx, a.pdPath(""), req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (a *alertAPI) ListPagerDutyIntegrations(ctx context.Context) (*ListPagerDutyIntegrationsResp, error) {
	resp := &ListPagerDutyIntegrationsResp{}
	if err := a.cli.Get(ctx, a.pdPath(""), nil, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (a *alertAPI) GetPagerDutyIntegration(ctx context.Context, integrationID string) (*GetPagerDutyIntegrationResp, error) {
	resp := &GetPagerDutyIntegrationResp{}
	if err := a.cli.Get(ctx, a.pdPath(integrationID), nil, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (a *alertAPI) UpdatePagerDutyIntegration(ctx context.Context, integrationID string, req *UpdatePagerDutyIntegrationReq) error {
	return a.cli.Put(ctx, a.pdPath(integrationID), req, nil)
}

func (a *alertAPI) DeletePagerDutyIntegration(ctx context.Context, integrationID string) error {
	return a.cli.Delete(ctx, a.pdPath(integrationID), nil, nil)
}

// Alert policies.

func (a *alertAPI) CreateAlertPolicy(ctx context.Context, req *UpsertAlertPolicyReq) (*CreateAlertPolicyResp, error) {
	resp := &CreateAlertPolicyResp{}
	if err := a.cli.Post(ctx, a.policyPath(""), req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (a *alertAPI) ListAlertPolicies(ctx context.Context, region string) (*ListAlertPoliciesResp, error) {
	resp := &ListAlertPoliciesResp{}
	path := a.policyPath("") + "?region=" + url.QueryEscape(region)
	if err := a.cli.Get(ctx, path, nil, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (a *alertAPI) GetAlertPolicy(ctx context.Context, region, policyID string) (*GetAlertPolicyResp, error) {
	resp := &GetAlertPolicyResp{}
	path := a.policyPath(policyID) + "?region=" + url.QueryEscape(region)
	if err := a.cli.Get(ctx, path, nil, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (a *alertAPI) UpdateAlertPolicy(ctx context.Context, policyID string, req *UpsertAlertPolicyReq) error {
	return a.cli.Put(ctx, a.policyPath(policyID), req, nil)
}

func (a *alertAPI) DeleteAlertPolicy(ctx context.Context, region, policyID string) error {
	path := a.policyPath(policyID) + "?region=" + url.QueryEscape(region)
	return a.cli.Delete(ctx, path, nil, nil)
}
