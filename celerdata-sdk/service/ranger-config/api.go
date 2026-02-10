package rangerconfig

import (
	"context"
	"fmt"
	"log"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/version"
)

type IRangerConfigAPI interface {
	CreateRangerConfig(ctx context.Context, req *CreateRangerConfigReq) (*CreateRangerConfigResp, error)
	UpdateRangerConfig(ctx context.Context, req *UpdateRangerConfigReq) error
	GetRangerConfig(ctx context.Context, req GetRangerConfigReq) (*GetRangerConfigResp, error)
	DelRangerConfig(ctx context.Context, req *DelRangerConfigReq) error
}

func NewRangerConfigAPI(cli *client.CelerdataClient) IRangerConfigAPI {
	return &rangerConfigAPI{cli: cli, apiVersion: version.API_1_0}
}

type rangerConfigAPI struct {
	cli        *client.CelerdataClient
	apiVersion version.ApiVersion
}

func (r *rangerConfigAPI) CreateRangerConfig(ctx context.Context, req *CreateRangerConfigReq) (*CreateRangerConfigResp, error) {
	log.Printf("[DEBUG] CreateRangerConfig, req:%+v", req)
	resp := &CreateRangerConfigResp{}

	err := r.cli.Post(ctx, fmt.Sprintf("/api/%s/ranger-config", r.apiVersion), req, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (r *rangerConfigAPI) UpdateRangerConfig(ctx context.Context, req *UpdateRangerConfigReq) error {
	log.Printf("[DEBUG] UpdateRangerConfig, req:%+v", req)
	resp := &CreateRangerConfigResp{}

	err := r.cli.Put(ctx, fmt.Sprintf("/api/%s/ranger-config", r.apiVersion), req, resp)
	if err != nil {
		return err
	}

	return nil
}

func (r *rangerConfigAPI) DelRangerConfig(ctx context.Context, req *DelRangerConfigReq) error {
	return r.cli.Delete(ctx, fmt.Sprintf("/api/%s/ranger-config/%s", r.apiVersion, req.BizID), nil, nil)
}

func (r *rangerConfigAPI) GetRangerConfig(ctx context.Context, req GetRangerConfigReq) (*GetRangerConfigResp, error) {
	resp := &GetRangerConfigResp{}

	err := r.cli.Get(ctx, fmt.Sprintf("/api/%s/ranger-config/%s", r.apiVersion, req.BizID), nil, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
