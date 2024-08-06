package cluster

import (
	"context"
	"fmt"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/version"
)

type IClusterAPI interface {
	Deploy(ctx context.Context, req *DeployReq) (*DeployResp, error)
	Get(ctx context.Context, req *GetReq) (*GetResp, error)
	Release(ctx context.Context, req *ReleaseReq) (*ReleaseResp, error)
	Suspend(ctx context.Context, req *SuspendReq) (*SuspendResp, error)
	Resume(ctx context.Context, req *ResumeReq) (*ResumeResp, error)
	GetState(ctx context.Context, req *GetStateReq) (*GetStateResp, error)
	ScaleIn(ctx context.Context, req *ScaleInReq) (*ScaleInResp, error)
	ScaleOut(ctx context.Context, req *ScaleOutReq) (*ScaleOutResp, error)
	ScaleUp(ctx context.Context, req *ScaleUpReq) (*ScaleUpResp, error)
	IncrStorageSize(ctx context.Context, req *IncrStorageSizeReq) (*IncrStorageSizeResp, error)
	UnlockFreeTier(ctx context.Context, clusterID string) error
	GetClusterEndpoints(ctx context.Context, req *GetClusterEndpointsReq) (*GetClusterEndpointsResp, error)
	AllocateClusterEndpoints(ctx context.Context, req *AllocateClusterEndpointsReq) error

	CheckDatabaseUser(ctx context.Context, req *CheckDatabaseUserReq) (*CheckDatabaseUserResp, error)
	CreateDatabaseUser(ctx context.Context, req *CreateDatabaseUserReq) (*CreateDatabaseUserResp, error)
	ResetDatabaseUserPassword(ctx context.Context, req *ResetDatabaseUserPasswordReq) (*ResetDatabaseUserPasswordResp, error)
	DropDatabaseUser(ctx context.Context, req *DropDatabaseUserReq) (*DropDatabaseUserResp, error)

	GetClusterInfraActionState(ctx context.Context, req *GetClusterInfraActionStateReq) (*GetClusterInfraActionStateResp, error)
	UpsertClusterSSLCert(ctx context.Context, req *UpsertClusterSSLCertReq) error
	GetClusterDomainSSLCert(ctx context.Context, req *GetClusterDomainSSLCertReq) (*GetClusterDomainSSLCertResp, error)
	UpsertClusterIdleConfig(ctx context.Context, req *UpsertClusterIdleConfigReq) error

	UpdateCustomConfig(ctx context.Context, req *SaveCustomConfigReq) error
	GetCustomConfig(ctx context.Context, req *ListCustomConfigReq) (*ListCustomConfigResp, error)
	ApplyCustomConfig(ctx context.Context, req *ApplyCustomConfigReq) (*ApplyCustomConfigResp, error)
	UpsertClusterLdapSSLCert(ctx context.Context, req *UpsertLDAPSSLCertsReq) (*UpsertLDAPSSLCertsResp, error)
	CleanCustomConfig(ctx context.Context, req *CleanCustomConfigReq) (*CleanCustomConfigResp, error)

	GetClusterVolumeDetail(ctx context.Context, req *GetClusterVolumeDetailReq) (*GetClusterVolumeDetailResp, error)
	ModifyClusterVolume(ctx context.Context, req *ModifyClusterVolumeReq) (*ModifyClusterVolumeResp, error)

	ListCluster(ctx context.Context) (*ListClusterResp, error)
}

func NewClustersAPI(cli *client.CelerdataClient) IClusterAPI {
	return &clusterAPI{cli: cli, apiVersion: version.API_1_0}
}

type clusterAPI struct {
	cli        *client.CelerdataClient
	apiVersion version.ApiVersion
}

func (c *clusterAPI) ListCluster(ctx context.Context) (*ListClusterResp, error) {
	resp := &ListClusterResp{}
	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/clusters", c.apiVersion), nil, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *clusterAPI) Deploy(ctx context.Context, req *DeployReq) (*DeployResp, error) {
	resp := &DeployResp{}
	req.SourceFrom = "terraform"
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters", c.apiVersion), req, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *clusterAPI) Get(ctx context.Context, req *GetReq) (*GetResp, error) {
	resp := &GetResp{}
	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/clusters/%s", c.apiVersion, req.ClusterID), nil, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *clusterAPI) Release(ctx context.Context, req *ReleaseReq) (*ReleaseResp, error) {
	resp := &ReleaseResp{}
	err := c.cli.Delete(ctx, fmt.Sprintf("/api/%s/clusters/%s", c.apiVersion, req.ClusterID), nil, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *clusterAPI) GetState(ctx context.Context, req *GetStateReq) (*GetStateResp, error) {
	resp := &GetStateResp{}
	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/clusters/%s/state", c.apiVersion, req.ClusterID), map[string]string{"action_id": req.ActionID}, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *clusterAPI) Suspend(ctx context.Context, req *SuspendReq) (*SuspendResp, error) {
	resp := &SuspendResp{}
	err := c.cli.Patch(ctx, fmt.Sprintf("/api/%s/clusters/%s/suspend", c.apiVersion, req.ClusterID), nil, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *clusterAPI) Resume(ctx context.Context, req *ResumeReq) (*ResumeResp, error) {
	resp := &ResumeResp{}
	err := c.cli.Patch(ctx, fmt.Sprintf("/api/%s/clusters/%s/resume", c.apiVersion, req.ClusterID), nil, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *clusterAPI) UnlockFreeTier(ctx context.Context, clusterID string) error {
	return c.cli.Patch(ctx, fmt.Sprintf("/api/%s/clusters/%s/unlock-free-tier", c.apiVersion, clusterID), nil, nil)
}

func (c *clusterAPI) ScaleIn(ctx context.Context, req *ScaleInReq) (*ScaleInResp, error) {
	resp := &ScaleInResp{}
	err := c.cli.Patch(ctx, fmt.Sprintf("/api/%s/clusters/%s/scale-in", c.apiVersion, req.ClusterId), req, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *clusterAPI) ScaleOut(ctx context.Context, req *ScaleOutReq) (*ScaleOutResp, error) {
	resp := &ScaleOutResp{}
	err := c.cli.Patch(ctx, fmt.Sprintf("/api/%s/clusters/%s/scale-out", c.apiVersion, req.ClusterId), req, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *clusterAPI) ScaleUp(ctx context.Context, req *ScaleUpReq) (*ScaleUpResp, error) {
	resp := &ScaleUpResp{}
	err := c.cli.Patch(ctx, fmt.Sprintf("/api/%s/clusters/%s/scale-up", c.apiVersion, req.ClusterId), req, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *clusterAPI) IncrStorageSize(ctx context.Context, req *IncrStorageSizeReq) (*IncrStorageSizeResp, error) {
	resp := &IncrStorageSizeResp{}
	err := c.cli.Patch(ctx, fmt.Sprintf("/api/%s/clusters/%s/incr-storage-size", c.apiVersion, req.ClusterId), req, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *clusterAPI) GetClusterEndpoints(ctx context.Context, req *GetClusterEndpointsReq) (*GetClusterEndpointsResp, error) {
	resp := &GetClusterEndpointsResp{}
	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/clusters/%s/endpoints", c.apiVersion, req.ClusterId), nil, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *clusterAPI) AllocateClusterEndpoints(ctx context.Context, req *AllocateClusterEndpointsReq) error {
	resp := &IncrStorageSizeResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/endpoints", c.apiVersion, req.ClusterId), nil, resp)
	if err != nil {
		return err
	}
	return nil
}

func (c *clusterAPI) CheckDatabaseUser(ctx context.Context, req *CheckDatabaseUserReq) (*CheckDatabaseUserResp, error) {
	resp := &CheckDatabaseUserResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/database/users/info", c.apiVersion), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) CreateDatabaseUser(ctx context.Context, req *CreateDatabaseUserReq) (*CreateDatabaseUserResp, error) {
	resp := &CreateDatabaseUserResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/database/users", c.apiVersion), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) ResetDatabaseUserPassword(ctx context.Context, req *ResetDatabaseUserPasswordReq) (*ResetDatabaseUserPasswordResp, error) {
	resp := &ResetDatabaseUserPasswordResp{}
	err := c.cli.Put(ctx, fmt.Sprintf("/api/%s/database/users/password", c.apiVersion), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) DropDatabaseUser(ctx context.Context, req *DropDatabaseUserReq) (*DropDatabaseUserResp, error) {
	resp := &DropDatabaseUserResp{}

	paramMap := make(map[string]string)
	paramMap["cluster_id"] = req.ClusterId
	paramMap["login_user"] = req.LoginUserInfo.UserName
	paramMap["login_password"] = req.LoginUserInfo.Password
	paramMap["user_name"] = req.UserInfo.UserName

	err := c.cli.Delete(ctx, fmt.Sprintf("/api/%s/database/users", c.apiVersion), paramMap, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) GetClusterInfraActionState(ctx context.Context, req *GetClusterInfraActionStateReq) (*GetClusterInfraActionStateResp, error) {
	resp := &GetClusterInfraActionStateResp{}
	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/clusters/%s/infra-action/state", c.apiVersion, req.ClusterId), map[string]string{"action_id": req.ActionId}, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) UpsertClusterSSLCert(ctx context.Context, req *UpsertClusterSSLCertReq) error {
	resp := &CommonResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/cert", c.apiVersion, req.ClusterId), req, resp)
	if err != nil {
		return err
	}
	return nil
}

func (c *clusterAPI) GetClusterDomainSSLCert(ctx context.Context, req *GetClusterDomainSSLCertReq) (*GetClusterDomainSSLCertResp, error) {
	resp := &GetClusterDomainSSLCertResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/cert/detail", c.apiVersion, req.ClusterId), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) UpsertClusterIdleConfig(ctx context.Context, req *UpsertClusterIdleConfigReq) error {
	return c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/idle-config", c.apiVersion, req.ClusterId), req, nil)
}

func (c *clusterAPI) GetCustomConfig(ctx context.Context, req *ListCustomConfigReq) (*ListCustomConfigResp, error) {
	resp := &ListCustomConfigResp{}

	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/clusters/%s/custom-config/detail", c.apiVersion, req.ClusterID), map[string]any{"config_type": int(req.ConfigType), "warehouse_id": req.WarehouseID}, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *clusterAPI) UpdateCustomConfig(ctx context.Context, req *SaveCustomConfigReq) error {
	return c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/custom-config", c.apiVersion, req.ClusterID), req, nil)
}

func (c *clusterAPI) ApplyCustomConfig(ctx context.Context, req *ApplyCustomConfigReq) (*ApplyCustomConfigResp, error) {
	resp := &ApplyCustomConfigResp{}
	err := c.cli.Patch(ctx, fmt.Sprintf("/api/%s/clusters/%s/apply-custom-config", c.apiVersion, req.ClusterID), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) UpsertClusterLdapSSLCert(ctx context.Context, req *UpsertLDAPSSLCertsReq) (*UpsertLDAPSSLCertsResp, error) {

	resp := &UpsertLDAPSSLCertsResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/ldap-ssl-certs", c.apiVersion, req.ClusterId), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) CleanCustomConfig(ctx context.Context, req *CleanCustomConfigReq) (*CleanCustomConfigResp, error) {
	resp := &CleanCustomConfigResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/clean-custom-config", c.apiVersion, req.ClusterID), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) GetClusterVolumeDetail(ctx context.Context, req *GetClusterVolumeDetailReq) (*GetClusterVolumeDetailResp, error) {

	resp := &GetClusterVolumeDetailResp{}
	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/clusters/%s/volume/detail", c.apiVersion, req.ClusterId), map[string]string{"type": string(req.Type)}, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
func (c *clusterAPI) ModifyClusterVolume(ctx context.Context, req *ModifyClusterVolumeReq) (*ModifyClusterVolumeResp, error) {

	resp := &ModifyClusterVolumeResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/volume", c.apiVersion, req.ClusterId), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
