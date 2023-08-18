package csp

import (
	"context"
	"fmt"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/version"
)

type ICspAPI interface {
	Get(ctx context.Context, req *GetReq) (*GetResp, error)
}

func NewCspAPI(cli *client.CelerdataClient) ICspAPI {
	return &cspAPI{cli: cli, apiVersion: version.API_1_0}
}

type cspAPI struct {
	cli        *client.CelerdataClient
	apiVersion version.ApiVersion
}

func (c *cspAPI) Get(ctx context.Context, req *GetReq) (*GetResp, error) {
	resp := &GetResp{}
	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/csps/%s", c.apiVersion, req.CspName), nil, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
