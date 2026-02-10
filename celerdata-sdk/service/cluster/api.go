package cluster

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

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
	UpgradeAMI(ctx context.Context, req *UpgradeAMIReq) (*UpgradeAMIResp, error)
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
	UpsertClusterConfig(ctx context.Context, req *UpsertClusterConfigReq) (*UpsertClusterConfigResp, error)
	RemoveClusterConfig(ctx context.Context, req *RemoveClusterConfigReq) (*RemoveClusterConfigResp, error)

	CheckRangerCert(ctx context.Context, req *CheckRangerCertsReq) error
	UpsertClusterRangerCert(ctx context.Context, req *UpsertRangerCertsReq) (*UpsertRangerCertsResp, error)
	RemoveClusterRangerCert(ctx context.Context, req *RemoveRangerCertsReq) (*RemoveRangerCertsResp, error)

	GetClusterVolumeDetail(ctx context.Context, req *GetClusterVolumeDetailReq) (*GetClusterVolumeDetailResp, error)
	ModifyClusterVolume(ctx context.Context, req *ModifyClusterVolumeReq) (*ModifyClusterVolumeResp, error)
	VolumeParamVerification(ctx context.Context, req *ModifyClusterVolumeReq) error
	UpdateResourceTags(ctx context.Context, req *UpdateResourceTagsReq) error

	ListCluster(ctx context.Context) (*ListClusterResp, error)

	GetWarehouse(ctx context.Context, req *GetWarehouseReq) (*GetWarehouseResp, error)
	CreateWarehouse(ctx context.Context, req *CreateWarehouseReq) (*CreateWarehouseResp, error)
	ScaleWarehouseNum(ctx context.Context, req *ScaleWarehouseNumReq) (*ScaleWarehouseNumResp, error)
	ScaleWarehouseSize(ctx context.Context, req *ScaleWarehouseSizeReq) (*ScaleWarehouseSizeResp, error)
	ResumeWarehouse(ctx context.Context, req *ResumeWarehouseReq) (*ResumeWarehouseResp, error)
	SuspendWarehouse(ctx context.Context, req *SuspendWarehouseReq) (*SuspendWarehouseResp, error)
	ReleaseWarehouse(ctx context.Context, req *ReleaseWarehouseReq) (*ReleaseWarehouseResp, error)
	GetWarehouseIdleConfig(ctx context.Context, req *GetWarehouseIdleConfigReq) (*GetWarehouseIdleConfigResp, error)
	UpdateWarehouseIdleConfig(ctx context.Context, req *UpdateWarehouseIdleConfigReq) error
	SetWarehouseResumeWithCluster(ctx context.Context, req *SetWarehouseResumeWithClusterReq) error
	GetWarehouseAutoScalingConfig(ctx context.Context, req *GetWarehouseAutoScalingConfigReq) (*GetWarehouseAutoScalingConfigResp, error)
	SaveWarehouseAutoScalingConfig(ctx context.Context, req *SaveWarehouseAutoScalingConfigReq) (*SaveWarehouseAutoScalingConfigResp, error)
	DeleteWarehouseAutoScalingConfig(ctx context.Context, req *DeleteWarehouseAutoScalingConfigReq) error
	ChangeWarehouseDistribution(ctx context.Context, req *ChangeWarehouseDistributionReq) (*ChangeWarehouseDistributionResp, error)

	GetVmInfo(ctx context.Context, req *GetVmInfoReq) (*GetVmInfoResp, error)
	UpdateDeploymentScripts(ctx context.Context, req *UpdateDeploymentScriptsReq) error

	ListClusterSchedulePolicy(ctx context.Context, req *ListClusterSchedulePolicyReq) (*ListClusterSchedulePolicyResp, error)
	IsSchedulePolicyNameExist(ctx context.Context, req *CheckClusterSchedulePolicyReq) (*CheckClusterSchedulePolicyResp, error)
	SaveClusterSchedulePolicy(ctx context.Context, req *SaveClusterSchedulePolicyReq) (*SaveClusterSchedulePolicyResp, error)
	ModifyClusterSchedulePolicy(ctx context.Context, req *ModifyClusterSchedulePolicyReq) error
	DeleteClusterSchedulePolicy(ctx context.Context, req *DeleteClusterSchedulePolicyReq) error
	GetGlobalSqlSessionVariables(ctx context.Context, req *GetGlobalSqlSessionVariablesReq) (*GetGlobalSqlSessionVariablesResp, error)
	SetGlobalSqlSessionVariables(ctx context.Context, req *SetGlobalSqlSessionVariablesReq) (*SetGlobalSqlSessionVariablesResp, error)
	ResetGlobalSqlSessionVariables(ctx context.Context, req *ResetGlobalSqlSessionVariablesReq) (*ResetGlobalSqlSessionVariablesResp, error)

	GetClusterTerminationProtection(ctx context.Context, req *GetClusterTerminationProtectionReq) (*GetClusterTerminationProtectionResp, error)
	SetClusterTerminationProtection(ctx context.Context, clusterId string, req *SetClusterTerminationProtectionReq) error

	GetClusterTableNameCaseInsensitive(ctx context.Context, req *GetClusterTableNameCaseInsensitiveReq) (*GetClusterTableNameCaseInsensitiveResp, error)

	RunScripts(ctx context.Context, req *RunScriptsReq) error

	ApplyRangerConfigV2(ctx context.Context, req *ApplyRangerConfigV2Req) (*OperateRangerConfigV2Resp, error)
	CleanRangerConfigV2(ctx context.Context, req *CleanRangerConfigV2Req) (*OperateRangerConfigV2Resp, error)
}

func NewClustersAPI(cli *client.CelerdataClient) IClusterAPI {
	return &clusterAPI{cli: cli, apiVersion: version.API_1_0}
}

type clusterAPI struct {
	cli        *client.CelerdataClient
	apiVersion version.ApiVersion
}

func (c *clusterAPI) RunScripts(ctx context.Context, req *RunScriptsReq) error {
	return c.cli.Patch(ctx, fmt.Sprintf("/api/%s/clusters/%s/run-scripts", c.apiVersion, req.ClusterId), req, nil)
}

// GetVmInfo implements IClusterAPI.
func (c *clusterAPI) GetVmInfo(ctx context.Context, req *GetVmInfoReq) (*GetVmInfoResp, error) {
	resp := &GetVmInfoResp{}
	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/vm-instance/info", c.apiVersion), map[string]string{
		"csp":          req.Csp,
		"region":       req.Region,
		"process_type": req.ProcessType,
		"vm_cate":      req.VmCate,
	}, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) UpdateDeploymentScripts(ctx context.Context, req *UpdateDeploymentScriptsReq) error {
	return c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/deployment-scripts", c.apiVersion, req.ClusterId), req, nil)
}

func (c *clusterAPI) DeleteWarehouseAutoScalingConfig(ctx context.Context, req *DeleteWarehouseAutoScalingConfigReq) error {
	return c.cli.Delete(ctx, fmt.Sprintf("/api/%s/warehouses/%s/auto-scaling-policy", c.apiVersion, req.WarehouseId), nil, nil)
}

func (c *clusterAPI) SaveWarehouseAutoScalingConfig(ctx context.Context, req *SaveWarehouseAutoScalingConfigReq) (*SaveWarehouseAutoScalingConfigResp, error) {

	log.Printf("[DEBUG] Save warehouse auto scaling config, req:%+v", req)
	resp := &SaveWarehouseAutoScalingConfigResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/warehouses/%s/auto-scaling-policy", c.apiVersion, req.WarehouseId), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) GetWarehouseAutoScalingConfig(ctx context.Context, req *GetWarehouseAutoScalingConfigReq) (*GetWarehouseAutoScalingConfigResp, error) {

	log.Printf("[DEBUG] Query warehouse auto scaling config, warehouseId:%s", req.WarehouseId)
	resp := &GetWarehouseAutoScalingConfigResp{}
	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/warehouses/%s/auto-scaling-policy", c.apiVersion, req.WarehouseId), nil, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) UpdateWarehouseIdleConfig(ctx context.Context, req *UpdateWarehouseIdleConfigReq) error {
	return c.cli.Post(ctx, fmt.Sprintf("/api/%s/warehouses/%s/idle-conf", c.apiVersion, req.WarehouseId), req, nil)
}

func (c *clusterAPI) SetWarehouseResumeWithCluster(ctx context.Context, req *SetWarehouseResumeWithClusterReq) error {
	return c.cli.Patch(ctx, fmt.Sprintf("/api/%s/warehouses/%s/resume-with-cluster", c.apiVersion, req.WarehouseId), req, nil)
}

func (c *clusterAPI) GetWarehouseIdleConfig(ctx context.Context, req *GetWarehouseIdleConfigReq) (*GetWarehouseIdleConfigResp, error) {
	resp := &GetWarehouseIdleConfigResp{}
	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/warehouses/%s/idle-conf", c.apiVersion, req.WarehouseId), nil, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) ReleaseWarehouse(ctx context.Context, req *ReleaseWarehouseReq) (*ReleaseWarehouseResp, error) {
	resp := &ReleaseWarehouseResp{}
	err := c.cli.Delete(ctx, fmt.Sprintf("/api/%s/warehouses/%s", c.apiVersion, req.WarehouseId), nil, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) ScaleWarehouseSize(ctx context.Context, req *ScaleWarehouseSizeReq) (*ScaleWarehouseSizeResp, error) {
	resp := &ScaleWarehouseSizeResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/warehouses/%s/scale-size", c.apiVersion, req.WarehouseId), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) ScaleWarehouseNum(ctx context.Context, req *ScaleWarehouseNumReq) (*ScaleWarehouseNumResp, error) {
	resp := &ScaleWarehouseNumResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/warehouses/%s/scale-num", c.apiVersion, req.WarehouseId), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) SuspendWarehouse(ctx context.Context, req *SuspendWarehouseReq) (*SuspendWarehouseResp, error) {
	resp := &SuspendWarehouseResp{}
	err := c.cli.Patch(ctx, fmt.Sprintf("/api/%s/warehouses/%s/suspend", c.apiVersion, req.WarehouseId), nil, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) ResumeWarehouse(ctx context.Context, req *ResumeWarehouseReq) (*ResumeWarehouseResp, error) {
	resp := &ResumeWarehouseResp{}
	err := c.cli.Patch(ctx, fmt.Sprintf("/api/%s/warehouses/%s/resume", c.apiVersion, req.WarehouseId), nil, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) CreateWarehouse(ctx context.Context, req *CreateWarehouseReq) (*CreateWarehouseResp, error) {
	resp := &CreateWarehouseResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/warehouses", c.apiVersion), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) GetWarehouse(ctx context.Context, req *GetWarehouseReq) (*GetWarehouseResp, error) {
	warehouseInfo := &WarehouseInfo{}
	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/warehouses/%s", c.apiVersion, req.WarehouseId), nil, warehouseInfo)
	if err != nil {
		return nil, err
	}

	if len(warehouseInfo.WarehouseId) == 0 {
		warehouseInfo = nil
	}

	return &GetWarehouseResp{
		Info: warehouseInfo,
	}, nil
}

func (c *clusterAPI) ListCluster(ctx context.Context) (*ListClusterResp, error) {
	resp := &ListClusterResp{}
	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/clusters", c.apiVersion), nil, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *clusterAPI) ChangeWarehouseDistribution(ctx context.Context, req *ChangeWarehouseDistributionReq) (*ChangeWarehouseDistributionResp, error) {
	log.Printf("[DEBUG] change warehouse distribution , req:%+v", req)
	resp := &ChangeWarehouseDistributionResp{}
	err := c.cli.Patch(ctx, fmt.Sprintf("/api/%s/warehouses/%s/change-distribution", c.apiVersion, req.WarehouseID), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

var mu sync.Mutex

func (c *clusterAPI) Deploy(ctx context.Context, req *DeployReq) (*DeployResp, error) {
	mu.Lock()
	defer mu.Unlock()

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

func (c *clusterAPI) UpdateResourceTags(ctx context.Context, req *UpdateResourceTagsReq) error {
	err := c.cli.Patch(ctx, fmt.Sprintf("/api/%s/clusters/%s/resource-tags", c.apiVersion, req.ClusterId), req, nil)
	return err
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

func (c *clusterAPI) UpgradeAMI(ctx context.Context, req *UpgradeAMIReq) (*UpgradeAMIResp, error) {
	resp := &UpgradeAMIResp{}
	err := c.cli.Patch(ctx, fmt.Sprintf("/api/%s/clusters/%s/upgrade-ami", c.apiVersion, req.ClusterId), req, resp)
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

func (c *clusterAPI) UpsertClusterConfig(ctx context.Context, req *UpsertClusterConfigReq) (*UpsertClusterConfigResp, error) {

	resp := &UpsertClusterConfigResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/configs", c.apiVersion, req.ClusterID), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) RemoveClusterConfig(ctx context.Context, req *RemoveClusterConfigReq) (*RemoveClusterConfigResp, error) {

	resp := &RemoveClusterConfigResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/remove-configs", c.apiVersion, req.ClusterID), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) CheckRangerCert(ctx context.Context, req *CheckRangerCertsReq) error {
	return c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/ranger-certs/check", c.apiVersion, req.ClusterId), req, nil)
}

func (c *clusterAPI) UpsertClusterRangerCert(ctx context.Context, req *UpsertRangerCertsReq) (*UpsertRangerCertsResp, error) {

	resp := &UpsertRangerCertsResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/ranger-certs", c.apiVersion, req.ClusterId), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) RemoveClusterRangerCert(ctx context.Context, req *RemoveRangerCertsReq) (*RemoveRangerCertsResp, error) {

	resp := &RemoveRangerCertsResp{}
	err := c.cli.Delete(ctx, fmt.Sprintf("/api/%s/clusters/%s/ranger-certs", c.apiVersion, req.ClusterId), nil, resp)
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

func (c *clusterAPI) VolumeParamVerification(ctx context.Context, req *ModifyClusterVolumeReq) error {
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/volume/param-verification", c.apiVersion), req, nil)
	if err != nil {
		return err
	}
	return nil
}

func (c *clusterAPI) ListClusterSchedulePolicy(ctx context.Context, req *ListClusterSchedulePolicyReq) (*ListClusterSchedulePolicyResp, error) {
	var schedulePolicies []*ClusterSchedulePolicy
	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/clusters/%s/scheduling-policies", c.apiVersion, req.ClusterId), nil, &schedulePolicies)
	if err != nil {
		return nil, err
	}
	return &ListClusterSchedulePolicyResp{
		SchedulePolicies: schedulePolicies,
	}, nil
}

func (c *clusterAPI) IsSchedulePolicyNameExist(ctx context.Context, req *CheckClusterSchedulePolicyReq) (*CheckClusterSchedulePolicyResp, error) {
	resp := &CheckClusterSchedulePolicyResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/scheduling-policies/check-name", c.apiVersion, req.ClusterId), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) SaveClusterSchedulePolicy(ctx context.Context, req *SaveClusterSchedulePolicyReq) (*SaveClusterSchedulePolicyResp, error) {
	resp := &SaveClusterSchedulePolicyResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/scheduling-policies", c.apiVersion, req.ClusterId), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) ModifyClusterSchedulePolicy(ctx context.Context, req *ModifyClusterSchedulePolicyReq) error {
	err := c.cli.Put(ctx, fmt.Sprintf("/api/%s/clusters/%s/scheduling-policies/%s", c.apiVersion, req.ClusterId, req.PolicyId), req, nil)
	if err != nil {
		return err
	}
	return nil
}

func (c *clusterAPI) DeleteClusterSchedulePolicy(ctx context.Context, req *DeleteClusterSchedulePolicyReq) error {
	err := c.cli.Delete(ctx, fmt.Sprintf("/api/%s/clusters/%s/scheduling-policies/%s", c.apiVersion, req.ClusterId, req.PolicyId), nil, nil)
	if err != nil {
		return err
	}
	return nil
}

func (c *clusterAPI) GetGlobalSqlSessionVariables(ctx context.Context, req *GetGlobalSqlSessionVariablesReq) (*GetGlobalSqlSessionVariablesResp, error) {
	variables := make(map[string]string)
	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/clusters/%s/global/sql-session/variables", c.apiVersion, req.ClusterId), map[string]string{
		"variables": strings.Join(req.Variables, ","),
	}, &variables)
	if err != nil {
		return nil, err
	}
	return &GetGlobalSqlSessionVariablesResp{
		Variables: variables,
	}, nil
}

func (c *clusterAPI) SetGlobalSqlSessionVariables(ctx context.Context, req *SetGlobalSqlSessionVariablesReq) (*SetGlobalSqlSessionVariablesResp, error) {
	resp := &SetGlobalSqlSessionVariablesResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/global/sql-session/variables", c.apiVersion, req.ClusterId), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) ResetGlobalSqlSessionVariables(ctx context.Context, req *ResetGlobalSqlSessionVariablesReq) (*ResetGlobalSqlSessionVariablesResp, error) {
	resp := &ResetGlobalSqlSessionVariablesResp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/global/sql-session/variables/reset", c.apiVersion, req.ClusterId), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) GetClusterTerminationProtection(ctx context.Context, req *GetClusterTerminationProtectionReq) (*GetClusterTerminationProtectionResp, error) {
	resp := &GetClusterTerminationProtectionResp{}
	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/clusters/%s/cluster-config/termination-protection", c.apiVersion, req.ClusterId), nil, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) GetClusterTableNameCaseInsensitive(ctx context.Context, req *GetClusterTableNameCaseInsensitiveReq) (*GetClusterTableNameCaseInsensitiveResp, error) {
	resp := &GetClusterTableNameCaseInsensitiveResp{}
	err := c.cli.Get(ctx, fmt.Sprintf("/api/%s/clusters/%s/cluster-config/table-name-case-insensitive", c.apiVersion, req.ClusterId), nil, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) SetClusterTerminationProtection(ctx context.Context, clusterId string, req *SetClusterTerminationProtectionReq) error {
	return c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/cluster-config/termination-protection", c.apiVersion, clusterId), req, nil)
}

func (c *clusterAPI) ApplyRangerConfigV2(ctx context.Context, req *ApplyRangerConfigV2Req) (*OperateRangerConfigV2Resp, error) {
	resp := &OperateRangerConfigV2Resp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/deploy-ranger-config", c.apiVersion, req.ClusterID), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterAPI) CleanRangerConfigV2(ctx context.Context, req *CleanRangerConfigV2Req) (*OperateRangerConfigV2Resp, error) {
	resp := &OperateRangerConfigV2Resp{}
	err := c.cli.Post(ctx, fmt.Sprintf("/api/%s/clusters/%s/clean-ranger-config", c.apiVersion, req.ClusterID), req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
