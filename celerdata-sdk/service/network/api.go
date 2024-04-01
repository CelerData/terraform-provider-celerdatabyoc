package network

import (
	"context"
	"errors"
	"fmt"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/version"
)

type INetworkAPI interface {
	CreateNetwork(ctx context.Context, req *CreateNetworkReq) (*CreateNetworkResp, error)
	CreateAzureNetwork(ctx context.Context, req *CreateAzureNetworkReq) (*CreateNetworkResp, error)
	GetNetwork(ctx context.Context, netID string) (*GetNetworkResp, error)
	DeleteNetwork(ctx context.Context, netID string) error
}

func NewNetworkAPI(cli *client.CelerdataClient) INetworkAPI {
	return &networkAPI{cli: cli, apiVersion: version.API_1_0}
}

type networkAPI struct {
	cli        *client.CelerdataClient
	apiVersion version.ApiVersion
}

func (c *networkAPI) CreateNetwork(ctx context.Context, req *CreateNetworkReq) (*CreateNetworkResp, error) {
	resp := &CreateNetworkResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/networks", c.apiVersion), req, resp)
	if err != nil {
		return nil, err
	}

	if len(resp.CheckErrMsg) > 0 {
		return nil, errors.New(resp.CheckErrMsg)
	}

	return resp, nil
}

func (c *networkAPI) CreateAzureNetwork(ctx context.Context, req *CreateAzureNetworkReq) (*CreateNetworkResp, error) {
	resp := &CreateNetworkResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/azure-networks", c.apiVersion), req, resp)
	if err != nil {
		return nil, err
	}

	if len(resp.CheckErrMsg) > 0 {
		return nil, errors.New(resp.CheckErrMsg)
	}

	return resp, nil
}

func (c *networkAPI) GetNetwork(ctx context.Context, netID string) (*GetNetworkResp, error) {
	resp := &GetNetworkResp{}

	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/networks/%s", c.apiVersion, netID), nil, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *networkAPI) DeleteNetwork(ctx context.Context, netID string) error {
	return c.cli.Delete(ctx, fmt.Sprintf("/api/%s/networks/%s", c.apiVersion, netID), nil, nil)
}
