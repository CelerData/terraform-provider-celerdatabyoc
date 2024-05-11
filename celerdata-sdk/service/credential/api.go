package credential

import (
	"context"
	"errors"
	"fmt"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/version"
)

type ICredentialAPI interface {
	CreateDeploymentRoleCredential(ctx context.Context, req *CreateDeployRoleCredReq) (*CreateDeployRoleCredResp, error)
	GetDeploymentRoleCredential(ctx context.Context, credID string) (*GetDeployRoleCredResp, error)
	DeleteDeploymentRoleCredential(ctx context.Context, credID string) error
	CreateDataCredential(ctx context.Context, req *CreateDataCredReq) (*CreateDataCredResp, error)
	GetDataCredential(ctx context.Context, credID string) (*GetDataCredResp, error)
	DeleteDataCredential(ctx context.Context, credID string) error

	CreateDeploymentAkSkCredential(ctx context.Context, req *CreateDeployAkSkCredReq) (*CreateDeployAkSkCredResp, error)
	GetDeploymentAkSkCredential(ctx context.Context, credID string) (*GetDeployAkSkCredResp, error)
	RotateAkSkCredential(ctx context.Context, req *RotateAkSkCredentialReq) error
	DeleteDeploymentAkSkCredential(ctx context.Context, credID string) error
	CreateAzureDataCredential(ctx context.Context, req *CreateAzureDataCredReq) (*CreateDataCredResp, error)
}

func NewCredentialAPI(cli *client.CelerdataClient) ICredentialAPI {
	return &credentialAPI{cli: cli, apiVersion: version.API_1_0}
}

type credentialAPI struct {
	cli        *client.CelerdataClient
	apiVersion version.ApiVersion
}

// RotateAkSkCredential implements ICredentialAPI.
func (c *credentialAPI) RotateAkSkCredential(ctx context.Context, req *RotateAkSkCredentialReq) error {

	return c.cli.Post(ctx, fmt.Sprintf("/api/%s/deploy-ak-sk-credentials/%s/rotate", c.apiVersion, req.CredID), req, nil)
}

func (c *credentialAPI) CreateAzureDataCredential(ctx context.Context, req *CreateAzureDataCredReq) (*CreateDataCredResp, error) {
	resp := &CreateDataCredResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/azure-data-credentials", c.apiVersion), req, resp)
	if err != nil {
		return nil, err
	}

	if len(resp.CheckErrMsg) > 0 {
		return nil, errors.New(resp.CheckErrMsg)
	}

	return resp, nil
}

func (c *credentialAPI) CreateDeploymentAkSkCredential(ctx context.Context, req *CreateDeployAkSkCredReq) (*CreateDeployAkSkCredResp, error) {
	resp := &CreateDeployAkSkCredResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/deploy-ak-sk-credentials", c.apiVersion), req, resp)
	if err != nil {
		return nil, err
	}

	if len(resp.CheckErrMsg) > 0 {
		return nil, errors.New(resp.CheckErrMsg)
	}

	return resp, nil
}

func (c *credentialAPI) DeleteDeploymentAkSkCredential(ctx context.Context, credID string) error {
	return c.cli.Delete(ctx, fmt.Sprintf("/api/%s/deploy-ak-sk-credentials/%s", c.apiVersion, credID), nil, nil)
}

func (c *credentialAPI) GetDeploymentAkSkCredential(ctx context.Context, credID string) (*GetDeployAkSkCredResp, error) {
	resp := &GetDeployAkSkCredResp{}

	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/deploy-ak-sk-credentials/%s", c.apiVersion, credID), nil, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *credentialAPI) CreateDeploymentRoleCredential(ctx context.Context, req *CreateDeployRoleCredReq) (*CreateDeployRoleCredResp, error) {
	resp := &CreateDeployRoleCredResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/deploy-role-credentials", c.apiVersion), req, resp)
	if err != nil {
		return nil, err
	}

	if len(resp.CheckErrMsg) > 0 {
		return nil, errors.New(resp.CheckErrMsg)
	}

	return resp, nil
}

func (c *credentialAPI) GetDeploymentRoleCredential(ctx context.Context, credID string) (*GetDeployRoleCredResp, error) {
	resp := &GetDeployRoleCredResp{}

	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/deploy-role-credentials/%s", c.apiVersion, credID), nil, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *credentialAPI) DeleteDeploymentRoleCredential(ctx context.Context, credID string) error {
	return c.cli.Delete(ctx, fmt.Sprintf("/api/%s/deploy-role-credentials/%s", c.apiVersion, credID), nil, nil)
}

func (c *credentialAPI) CreateDataCredential(ctx context.Context, req *CreateDataCredReq) (*CreateDataCredResp, error) {
	resp := &CreateDataCredResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/data-credentials", c.apiVersion), req, resp)
	if err != nil {
		return nil, err
	}

	if len(resp.CheckErrMsg) > 0 {
		return nil, errors.New(resp.CheckErrMsg)
	}

	return resp, nil
}

func (c *credentialAPI) GetDataCredential(ctx context.Context, credID string) (*GetDataCredResp, error) {
	resp := &GetDataCredResp{}
	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/data-credentials/%s", c.apiVersion, credID), nil, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *credentialAPI) DeleteDataCredential(ctx context.Context, credID string) error {
	return c.cli.Delete(ctx, fmt.Sprintf("/api/%s/data-credentials/%s", c.apiVersion, credID), nil, nil)
}
