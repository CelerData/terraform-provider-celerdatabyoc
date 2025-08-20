package cluster

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/go-cmp/cmp"
)

type ClusterModuleType string
type ClusterState string
type ClusterType string
type DomainAllocateState int32
type CustomConfigType int
type ClusterInfraActionState string
type WhScalingType int32
type MetricType int32
type DistributionPolicy string

var (
	WeekDays = []string{"SUNDAY", "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY"}

	SupportedConfigType = []string{"FE", "BE", "RANGER"}
	ClusterNodeType     = []string{"FE", "BE", "COORDINATOR"}

	WhScaleTypeArr = []string{"SCALE_OUT", "SCALE_IN"}

	ScaleTypeArr = []int32{
		int32(WhScalingType_SCALE_OUT),
		int32(WhScalingType_SCALE_IN),
	}

	ScaleTypeToStr = map[int32]string{
		int32(WhScalingType_SCALE_OUT): "SCALE_OUT",
		int32(WhScalingType_SCALE_IN):  "SCALE_IN",
	}

	MetricArr = []int32{
		int32(MetricType_AVERAGE_CPU_UTILIZATION),
		int32(MetricType_QUERY_QUEUE_LENGTH),
		int32(MetricType_EARLIEST_QUERY_PENDING_TIME),
		int32(MetricType_WAREHOUSE_RESOURCE_UTILIZATION),
	}

	MetricToStr = map[int32]string{
		int32(MetricType_AVERAGE_CPU_UTILIZATION):        "AVERAGE_CPU_UTILIZATION",
		int32(MetricType_QUERY_QUEUE_LENGTH):             "QUERY_QUEUE_LENGTH",
		int32(MetricType_EARLIEST_QUERY_PENDING_TIME):    "EARLIEST_QUERY_PENDING_TIME",
		int32(MetricType_WAREHOUSE_RESOURCE_UTILIZATION): "WAREHOUSE_RESOURCE_UTILIZATION",
	}

	MetricStrGroup = map[string]string{
		"AVERAGE_CPU_UTILIZATION":        AutoScalingMetricType_CPU,
		"QUERY_QUEUE_LENGTH":             AutoScalingMetricType_QUERY_QUEUE,
		"EARLIEST_QUERY_PENDING_TIME":    AutoScalingMetricType_QUERY_QUEUE,
		"WAREHOUSE_RESOURCE_UTILIZATION": AutoScalingMetricType_QUERY_QUEUE,
	}

	CpuBasedMetric = map[string][]string{
		"SCALE_OUT": {
			"AVERAGE_CPU_UTILIZATION",
		},
		"SCALE_IN": {
			"AVERAGE_CPU_UTILIZATION",
		},
	}

	QueryQueueBasedMetric = map[string][]string{
		"SCALE_OUT": {
			"QUERY_QUEUE_LENGTH", "EARLIEST_QUERY_PENDING_TIME",
		},
		"SCALE_IN": {
			"WAREHOUSE_RESOURCE_UTILIZATION",
		},
	}
)

const (
	CSP_AWS    = "aws"
	CSP_AZURE  = "azure"
	CSP_GOOGLE = "gcp"

	ClusterTypeClassic         = ClusterType("CLASSIC")
	ClusterTypeElasic          = ClusterType("ELASTIC")
	ClusterModuleTypeUnknown   = ClusterModuleType("Unknown")
	ClusterModuleTypeFE        = ClusterModuleType("FE")
	ClusterModuleTypeBE        = ClusterModuleType("BE")
	ClusterModuleTypeWarehouse = ClusterModuleType("Warehouse")
	ClusterStateDeploying      = ClusterState("Deploying")
	ClusterStateRunning        = ClusterState("Running")
	ClusterStateScaling        = ClusterState("Scaling")
	ClusterStateAbnormal       = ClusterState("Abnormal")
	ClusterStateSuspending     = ClusterState("Suspending")
	ClusterStateSuspended      = ClusterState("Suspended")
	ClusterStateResuming       = ClusterState("Resuming")
	ClusterStateReleasing      = ClusterState("Releasing")
	ClusterStateReleased       = ClusterState("Released")
	ClusterStateUpdating       = ClusterState("Updating")

	DomainAllocateStateUnknown   DomainAllocateState = 0
	DomainAllocateStateOngoing   DomainAllocateState = 3
	DomainAllocateStateSucceeded DomainAllocateState = 1
	DomainAllocateStateFailed    DomainAllocateState = 2

	ClusterInfraActionStatePending   ClusterInfraActionState = "Pending"
	ClusterInfraActionStateOngoing   ClusterInfraActionState = "Ongoing"
	ClusterInfraActionStateSucceeded ClusterInfraActionState = "Succeeded"
	ClusterInfraActionStateCompleted ClusterInfraActionState = "Completed"
	ClusterInfraActionStateFailed    ClusterInfraActionState = "Failed"

	CustomConfigTypeUnknown     CustomConfigType = 0
	CustomConfigTypeBE          CustomConfigType = 1
	CustomConfigTypeRanger      CustomConfigType = 2
	CustomConfigTypeLDAPSSLCert CustomConfigType = 3
	CustomConfigTypeFE          CustomConfigType = 4

	RANGER_CONFIG_KEY = "s3_path"

	WhScalingType_SCALE_IN  WhScalingType = 1
	WhScalingType_SCALE_OUT WhScalingType = 2

	MetricType_UNKNOWN                        MetricType = 0
	MetricType_AVERAGE_CPU_UTILIZATION        MetricType = 1
	MetricType_QUERY_QUEUE_LENGTH             MetricType = 2
	MetricType_EARLIEST_QUERY_PENDING_TIME    MetricType = 3
	MetricType_WAREHOUSE_RESOURCE_UTILIZATION MetricType = 4

	AutoScalingMetricType_CPU         = "CPU-based"
	AutoScalingMetricType_QUERY_QUEUE = "queue-based"

	DistributionPolicySpecifyAZ  DistributionPolicy = "specify_az"
	DistributionPolicyCrossingAZ DistributionPolicy = "crossing_az"
)

type Kv struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type DiskInfo struct {
	Number     uint32 `json:"number"`
	PerSize    uint64 `json:"per_size"` // unit:GB
	Iops       uint64 `json:"iops"`
	Throughput uint64 `json:"throughput"`
}

type ClusterItem struct {
	Type               ClusterModuleType `json:"type"`
	Name               string            `json:"name"`
	Num                uint32            `json:"num"`
	StorageSizeGB      uint64            `json:"storage_size_gb"` // deprecated
	InstanceType       string            `json:"instance_type"`
	DiskInfo           *DiskInfo         `json:"disk_info"`
	DistributionPolicy string            `json:"distribution_policy"`
	SpecifyAZ          string            `json:"specify_az"`
}

type Script struct {
	ScriptPath string `json:"script_path"`
	LogsDir    string `json:"logs_dir"`
}

type UpdateDeploymentScriptsReq struct {
	ClusterId string    `json:"cluster_id"`
	Scripts   []*Script `json:"scripts"`
	Parallel  bool      `json:"parallel"`
	Timeout   int32     `json:"timeout"`
}

type CustomAmi struct {
	AmiID string `json:"amiId"`
	OS    string `json:"os"`
}

type ClusterConf struct {
	ClusterId          string         `json:"cluster_id"`
	ClusterType        ClusterType    `json:"cluster_type"`
	Csp                string         `json:"csp"`
	Region             string         `json:"region"`
	ClusterName        string         `json:"cluster_name"`
	ClusterItems       []*ClusterItem `json:"cluster_items"`
	DeployCredlId      string         `json:"deploy_cred_id"`
	DataCredId         string         `json:"data_cred_id"`
	NetIfaceId         string         `json:"net_iface_id"`
	Password           string         `json:"password"`
	SslConnEnable      bool           `json:"ssl_conn_enable"`
	Tags               []*Kv          `json:"tags"`
	Scripts            []*Script      `json:"scripts"`
	RunScriptsParallel bool           `json:"run_scripts_parallel"`
	QueryPort          int32          `json:"query_port"`
	RunScriptsTimeout  int32          `json:"run_scripts_timeout"`
	CustomAmi          *CustomAmi     `json:"custom_ami"`
}

type GetReq struct {
	ClusterID string `json:"cluster_id" mapstructure:"cluster_id"`
}

type GetResp struct {
	Cluster *Cluster `json:"cluster" mapstructure:"cluster"`
}

type GetStateReq struct {
	ClusterID string `json:"cluster_id"  mapstructure:"action_id"`
	ActionID  string `json:"action_id" mapstructure:"action_id"`
}

type GetStateResp struct {
	ClusterState   string `json:"cluster_state" mapstructure:"cluster_state"`
	AbnormalReason string `json:"abnormal_reason" mapstructure:"abnormal_reason"`
}

type ReleaseReq struct {
	ClusterID string `json:"cluster_id" mapstructure:"cluster_id"`
}

type ReleaseResp struct {
	ActionID string `json:"action_id" mapstructure:"action_id"`
}

type SuspendReq struct {
	ClusterID string `json:"cluster_id" mapstructure:"cluster_id"`
}

type SuspendResp struct {
	ActionID string `json:"action_id" mapstructure:"action_id"`
}

type ResumeReq struct {
	ClusterID string `json:"cluster_id" mapstructure:"cluster_id"`
}

type ResumeResp struct {
	ActionID string `json:"action_id" mapstructure:"action_id"`
}

type DeployReq struct {
	RequestId   string       `json:"request_id"  mapstructure:"request_id"`
	ClusterConf *ClusterConf `json:"cluster_conf" mapstructure:"cluster_conf"`
	SourceFrom  string       `json:"source_from" mapstructure:"source_from"`
}

type DeployResp struct {
	ClusterID          string `json:"cluster_id" mapstructure:"cluster_id"`
	ActionID           string `json:"action_id" mapstructure:"action_id"`
	DefaultWarehouseId string `json:"default_warehouse_id" mapstructure:"default_warehouse_id"`
}

type Module struct {
	AmiId           string `json:"ami_id" mapstructure:"ami_id"`
	Num             uint32 `json:"num" mapstructure:"num"`
	StorageSizeGB   uint64 `json:"storage_size_gb" mapstructure:"storage_size_gb"`
	InstanceType    string `json:"instance_type" mapstructure:"instance_type"`
	VmVolSizeGB     int64  `json:"vm_vol_size_gb" mapstructure:"vm_vol_size_gb"`
	VmVolNum        int32  `json:"vm_vol_num" mapstructure:"vm_vol_num"`
	IsInstanceStore bool   `json:"is_instance_store" mapstructure:"is_instance_store"`
	Iops            int64  `json:"iops" mapstructure:"iops"`
	Throughput      int64  `json:"throughput" mapstructure:"throughput"`
	Arch            string `json:"arch" mapstructure:"arch"`
	Os              string `json:"os" mapstructure:"os"`
}

type Warehouse struct {
	Id                    string       `json:"id" mapstructure:"id"`
	Name                  string       `json:"name" mapstructure:"name"`
	State                 ClusterState `json:"state" mapstructure:"state"`
	Module                *Module      `json:"module" mapstructure:"module"`
	Deleted               bool         `json:"deleted" mapstructure:"deleted"`
	IsDefaultWarehouse    bool         `json:"is_default_warehouse" mapstructure:"is_default_warehouse"`
	CreatedAt             int64        `json:"created_at" mapstructure:"created_at"`
	DistributionPolicyStr string       `json:"distribution_policy_str" mapstructure:"distribution_policy_str"`
	SpecifyAZ             string       `json:"specify_az" mapstructure:"specify_az"`
}

type WarehouseExternalInfo struct {
	Id                 string `json:"id"`
	IsInstanceStore    bool   `json:"is_instance_store"`
	IsDefaultWarehouse bool   `json:"is_default_warehouse"`
}

type Cluster struct {
	ClusterID           string            `json:"cluster_id" mapstructure:"cluster_id"`
	ClusterName         string            `json:"cluster_name" mapstructure:"cluster_name"`
	ClusterState        ClusterState      `json:"cluster_state"  mapstructure:"cluster_state"`
	ClusterVersion      string            `json:"cluster_version" mapstructure:"cluster_version"`
	ClusterType         ClusterType       `json:"cluster_type" mapstructure:"cluster_type"`
	Csp                 string            `json:"csp" mapstructure:"csp"`
	Region              string            `json:"region" mapstructure:"region"`
	AccountID           string            `json:"account_id" mapstructure:"account_id"`
	FeModule            *Module           `json:"fe_module" mapstructure:"fe_module"`
	BeModule            *Module           `json:"be_module" mapstructure:"be_module"`
	SSLConnEnable       bool              `json:"ssl_conn_enable" mapstructure:"ssl_conn_enable"`
	NetIfaceID          string            `json:"net_iface_id" mapstructure:"net_iface_id"`
	DeployCredID        string            `json:"deploy_cred_id" mapstructure:"deploy_cred_id"`
	DataCredID          string            `json:"data_cred_id" mapstructure:"data_cred_id"`
	FreeTier            bool              `json:"free_tier" mapstructure:"free_tier"`
	QueryPort           int32             `json:"query_port" mapstructure:"query_port"`
	IdleSuspendInterval int32             `json:"idle_suspend_interval" mapstructure:"idle_suspend_interval"`
	LdapSslCerts        []string          `json:"ldap_ssl_certs"  mapstructure:"ldap_ssl_certs"`
	RangerCertsDirPath  string            `json:"ranger_certs_dir_path" mapstructure:"ranger_certs_dir_path"`
	Warehouses          []*Warehouse      `json:"warehouses" mapstructure:"warehouses"`
	IsMultiWarehouse    bool              `json:"is_multi_warehouse" mapstructure:"is_multi_warehouse"`
	Tags                map[string]string `json:"tags" mapstructure:"tags"`
	CustomAmi           *CustomAmi        `json:"custom_ami"`
}

type ScaleInReq struct {
	RequestId  string            `json:"request_id" mapstructure:"request_id"`
	ClusterId  string            `json:"cluster_id" mapstructure:"cluster_id"`
	ModuleType ClusterModuleType `json:"module_type" mapstructure:"module_type"`
	ExpectNum  int32             `json:"expect_num" mapstructure:"expect_num"`
}

type ScaleInResp struct {
	ActionId string `json:"action_id" mapstructure:"action_id"`
}

type ScaleOutReq struct {
	RequestId  string            `json:"request_id" mapstructure:"request_id"`
	ClusterId  string            `json:"cluster_id" mapstructure:"cluster_id"`
	ModuleType ClusterModuleType `json:"module_type"  mapstructure:"module_type"`
	ExpectNum  int32             `json:"expect_num" mapstructure:"expect_num"`
}

type ScaleOutResp struct {
	ActionId string `json:"action_id" mapstructure:"action_id"`
}

type ScaleUpReq struct {
	RequestId  string            `json:"request_id" mapstructure:"request_id"`
	ClusterId  string            `json:"cluster_id" mapstructure:"cluster_id"`
	ModuleType ClusterModuleType `json:"module_type" mapstructure:"module_type"`
	VmCategory string            `json:"vm_category" mapstructure:"vm_category"`
}

type ScaleUpResp struct {
	ActionId string `json:"action_id" mapstructure:"action_id"`
}

type IncrStorageSizeReq struct {
	RequestId     string            `json:"request_id" mapstructure:"request_id"`
	ClusterId     string            `json:"cluster_id" mapstructure:"cluster_id"`
	ModuleType    ClusterModuleType `json:"module_type" mapstructure:"module_type"`
	StorageSizeGB int64             `json:"storage_size_gb" mapstructure:"storage_size_gb"` // deprecated
	DiskInfo      *DiskInfo         `json:"disk_info" mapstructure:"disk_info"`
}

type IncrStorageSizeResp struct {
	ActionId string `json:"action_id" mapstructure:"action_id"`
}

type EndpointsInfo struct {
	NetworkMethod   string `json:"network_method" mapstructure:"network_method"`
	Host            string `json:"host" mapstructure:"host"`
	Port            int64  `json:"port" mapstructure:"port"`
	NLBEndpoint     string `json:"nlb_endpoint" mapstructure:"nlb_endpoint"`
	NlbEndpointType string `json:"nlb_endpoint_type" mapstructure:"nlb_endpoint_type"`
}

type GetClusterEndpointsReq struct {
	ClusterId string `json:"cluster_id" mapstructure:"cluster_id"`
}

type GetClusterEndpointsResp struct {
	State DomainAllocateState `json:"state" mapstructure:"state"`
	List  []*EndpointsInfo    `json:"list" mapstructure:"list"`
}

type AllocateClusterEndpointsReq struct {
	ClusterId string `json:"cluster_id" mapstructure:"cluster_id"`
}

type DatabaseUserInfo struct {
	UserName string `json:"user_name" mapstructure:"user_name"`
	Password string `json:"password" mapstructure:"password"`
}

type CheckDatabaseUserReq struct {
	ClusterId     string           `json:"cluster_id" mapstructure:"cluster_id"`
	LoginUserInfo DatabaseUserInfo `json:"login_user_info" mapstructure:"login_user_info"`
	UserInfo      DatabaseUserInfo `json:"user_info" mapstructure:"user_info"`
}

type CheckDatabaseUserResp struct {
	Exist bool `json:"exist" mapstructure:"exist"`
}

type CreateDatabaseUserReq struct {
	ClusterId     string           `json:"cluster_id" mapstructure:"cluster_id"`
	LoginUserInfo DatabaseUserInfo `json:"login_user_info" mapstructure:"login_user_info"`
	NewUserInfo   DatabaseUserInfo `json:"new_user_info" mapstructure:"new_user_info"`
}

type CreateDatabaseUserResp struct {
	Code   int32  `json:"code" mapstructure:"code"`
	ErrMsg string `json:"err_msg" mapstructure:"err_msg"`
}

type ResetDatabaseUserPasswordReq struct {
	ClusterId     string           `json:"cluster_id" mapstructure:"cluster_id"`
	LoginUserInfo DatabaseUserInfo `json:"login_user_info" mapstructure:"login_user_info"`
	UserInfo      DatabaseUserInfo `json:"user_info" mapstructure:"user_info"`
}

type ResetDatabaseUserPasswordResp struct {
	Code   int32  `json:"code" mapstructure:"code"`
	ErrMsg string `json:"err_msg" mapstructure:"err_msg"`
}

type DropDatabaseUserReq struct {
	ClusterId     string           `json:"cluster_id" mapstructure:"cluster_id"`
	LoginUserInfo DatabaseUserInfo `json:"login_user_info" mapstructure:"login_user_info"`
	UserInfo      DatabaseUserInfo `json:"user_info" mapstructure:"user_info"`
}

type DropDatabaseUserResp struct {
	Code   int32  `json:"code" mapstructure:"code"`
	ErrMsg string `json:"err_msg" mapstructure:"err_msg"`
}

type CommonResp struct {
	Code    int32  `json:"code" mapstructure:"code"`
	Message string `json:"message" mapstructure:"message"`
}

type UpsertClusterSSLCertReq struct {
	ClusterId string `json:"clusterId" mapstructure:"clusterId"`
	Domain    string `json:"domain" mapstructure:"domain"`
	CrtBucket string `json:"crtBucket" mapstructure:"crtBucket"`
	CrtPath   string `json:"crtPath" mapstructure:"crtPath"`
	KeyBucket string `json:"keyBucket" mapstructure:"keyBucket"`
	KeyPath   string `json:"keyPath" mapstructure:"keyPath"`
}

type GetClusterDomainSSLCertReq struct {
	ClusterId string `json:"clusterId" mapstructure:"clusterId"`
	Domain    string `json:"domain" mapstructure:"domain"`
}

type GetClusterDomainSSLCertResp struct {
	Exist      bool        `json:"exist" mapstructure:"exist"`
	DomainCert *DomainCert `json:"domainCert" mapstructure:"domainCert"`
}

type DomainCert struct {
	CertID     string `json:"certId" mapstructure:"certId"`
	ClusterID  string `json:"clusterId" mapstructure:"clusterId"`
	Domain     string `json:"domain" mapstructure:"domain"`
	CrtBucket  string `json:"crtBucket" mapstructure:"crtBucket"`
	CrtPath    string `json:"crtPath" mapstructure:"crtPath"`
	KeyBucket  string `json:"keyBucket" mapstructure:"keyBucket"`
	KeyPath    string `json:"keyPath" mapstructure:"keyPath"`
	DomainType string `json:"domainType" mapstructure:"domainType"`
	CertState  string `json:"certState" mapstructure:"certState"`
	InUse      bool   `json:"inUse" mapstructure:"inUse"`
}

type ListCustomConfigReq struct {
	ClusterID   string           `json:"cluster_id" mapstructure:"cluster_id"`
	ConfigType  CustomConfigType `json:"config_type" mapstructure:"config_type"`
	WarehouseID string           `json:"warehouse_id" mapstructure:"warehouse_id"`
}

type ListCustomConfigResp struct {
	Configs     map[string]string `json:"configs" mapstructure:"configs"`
	LastApplyAt int64             `json:"last_apply_at" mapstructure:"last_apply_at"`
	LastEditAt  int64             `json:"last_edit_at" mapstructure:"last_edit_at"`
}

type SaveCustomConfigReq struct {
	ClusterID   string            `json:"cluster_id" mapstructure:"cluster_id"`
	ConfigType  CustomConfigType  `json:"config_type" mapstructure:"config_type"`
	WarehouseID string            `json:"warehouse_id" mapstructure:"warehouse_id"`
	Configs     map[string]string `json:"configs" mapstructure:"configs"`
}

type ApplyCustomConfigReq struct {
	ClusterID   string           `json:"cluster_id" mapstructure:"cluster_id"`
	ConfigType  CustomConfigType `json:"config_type" mapstructure:"config_type"`
	WarehouseID string           `json:"warehouse_id" mapstructure:"warehouse_id"`
}

type ApplyCustomConfigResp struct {
	InfraActionId string `json:"infra_action_id" mapstructure:"infra_action_id"`
}

type GetClusterInfraActionStateReq struct {
	ClusterId string `json:"clusterId"`
	ActionId  string `json:"actionId"`
}

type GetClusterInfraActionStateResp struct {
	InfraActionState string `json:"infra_action_state" mapstructure:"infra_action_state"`
	ErrMsg           string `json:"err_msg" mapstructure:"err_msg"`
}

type UpsertClusterIdleConfigReq struct {
	ClusterId  string `json:"clusterId"`
	IntervalMs uint64 `json:"intervalMs"`
	Enable     bool   `json:"enable"`
}

type UpsertLDAPSSLCertsReq struct {
	ClusterId string   `json:"cluster_id"`
	S3Objects []string `json:"s3_objects"`
}

type UpsertLDAPSSLCertsResp struct {
	InfraActionId string `json:"infra_action_id" mapstructure:"infra_action_id"`
}

type CheckRangerCertsReq struct {
	ClusterId string `json:"cluster_id"`
	DirPath   string `json:"dir_path"`
}

type UpsertRangerCertsReq struct {
	ClusterId string `json:"cluster_id"`
	DirPath   string `json:"dir_path"`
}

type UpsertRangerCertsResp struct {
	InfraActionId string `json:"infra_action_id" mapstructure:"infra_action_id"`
}

type RemoveRangerCertsReq struct {
	ClusterId string `json:"cluster_id"`
}

type RemoveRangerCertsResp struct {
	InfraActionId string `json:"infra_action_id" mapstructure:"infra_action_id"`
}

type CleanCustomConfigReq struct {
	ClusterID   string           `json:"cluster_id" mapstructure:"cluster_id"`
	ConfigType  CustomConfigType `json:"config_type" mapstructure:"config_type"`
	WarehouseID string           `json:"warehouse_id" mapstructure:"warehouse_id"`
}

type CleanCustomConfigResp struct {
	InfraActionId string `json:"infra_action_id" mapstructure:"infra_action_id"`
}

type GetClusterVolumeDetailReq struct {
	ClusterId string            `json:"cluster_id"`
	Type      ClusterModuleType `json:"type"`
}

type GetClusterVolumeDetailResp struct {
	ClusterId  string            `json:"cluster_id" mapstructure:"cluster_id"`
	Type       ClusterModuleType `json:"type" mapstructure:"type"` // FE/BE
	VmVolCate  string            `json:"vm_vol_cate" mapstructure:"vm_vol_cate"`
	VmVolSize  int64             `json:"vm_vol_size" mapstructure:"vm_vol_size"` // unit:GB
	VmVolNum   int32             `json:"vm_vol_num" mapstructure:"vm_vol_num"`
	Iops       int64             `json:"iops" mapstructure:"iops"`
	Throughput int64             `json:"throughput" mapstructure:"throughput"`
}

type ModifyClusterVolumeReq struct {
	ClusterId   string            `json:"cluster_id"`
	WarehouseID string            `json:"warehouse_id"`
	Type        ClusterModuleType `json:"type"` // FE/BE
	VmVolCate   string            `json:"vm_vol_cate"`
	VmVolSize   int64             `json:"vm_vol_size"` // unit:GB
	VmVolNum    int32             `json:"vm_vol_num"`
	Iops        int64             `json:"iops"`
	Throughput  int64             `json:"throughput"`
}

type ModifyClusterVolumeResp struct {
	ActionID string `json:"action_id" mapstructure:"action_id"`
}

type ClusterInfo struct {
	ClusterId      string `json:"cluster_id" mapstructure:"cluster_id"`
	ClusterName    string `json:"cluster_name" mapstructure:"cluster_name"`
	ClusterVersion string `json:"cluster_version" mapstructure:"cluster_version"`
	ClusterType    string `json:"cluster_type" mapstructure:"cluster_type"`
}

type ListClusterResp struct {
	Total int64          `json:"total" mapstructure:"total"`
	List  []*ClusterInfo `json:"list" mapstructure:"list"`
}

type GetWarehouseReq struct {
	WarehouseId string `json:"warehouse_id" mapstructure:"warehouse_id"`
}

type WarehouseInfo struct {
	WarehouseId           string `json:"warehouse_id" mapstructure:"warehouse_id"`
	WarehouseName         string `json:"warehouse_name" mapstructure:"warehouse_name"`
	NodeCount             int32  `json:"node_count" mapstructure:"node_count"`
	State                 string `json:"state" mapstructure:"state"`
	IsDefault             bool   `json:"is_default" mapstructure:"is_default"`
	VmCate                string `json:"vm_cate" mapstructure:"vm_cate"`
	VmVolSizeGB           int64  `json:"vm_vol_size_gb" mapstructure:"vm_vol_size_gb"`
	VmVolNum              int32  `json:"vm_vol_num" mapstructure:"vm_vol_num"`
	IsInstanceStore       bool   `json:"is_instance_store" mapstructure:"is_instance_store"`
	DistributionPolicyStr string `json:"distribution_policy_str" mapstructure:"distribution_policy_str"`
	SpecifyAZ             string `json:"specify_az" mapstructure:"specify_az"`
}

type GetWarehouseResp struct {
	Info *WarehouseInfo
}

type CreateWarehouseReq struct {
	ClusterId          string `json:"cluster_id" mapstructure:"cluster_id"`
	Name               string `json:"name" mapstructure:"name"`
	Description        string `json:"description" mapstructure:"description"`
	VmCate             string `json:"vm_cate" mapstructure:"vm_cate"`
	VmNum              int32  `json:"vm_num" mapstructure:"vm_num"`
	VolumeSizeGB       int64  `json:"volume_size_gb" mapstructure:"volume_size_gb"`
	VolumeNum          int32  `json:"volume_num" mapstructure:"volume_num"`
	DistributionPolicy string `json:"distribution_policy" mapstructure:"distribution_policy"`
	SpecifyAZ          string `json:"specify_az" mapstructure:"specify_az"`
	Iops               int64  `json:"iops" mapstructure:"iops"`
	Throughput         int64  `json:"throughput" mapstructure:"throughput"`
}

type CreateWarehouseResp struct {
	WarehouseId string `json:"warehouse_id" mapstructure:"warehouse_id"`
	ActionID    string `json:"action_id" mapstructure:"action_id"`
}

type ScaleWarehouseNumReq struct {
	WarehouseId string `json:"warehouse_id" mapstructure:"warehouse_id"`
	VmNum       int32  `json:"vm_num" mapstructure:"vm_num"`
}

type ScaleWarehouseNumResp struct {
	ActionID string `json:"action_id" mapstructure:"action_id"`
}

type ScaleWarehouseSizeReq struct {
	WarehouseId string `json:"warehouse_id" mapstructure:"warehouse_id"`
	VmCate      string `json:"vm_cate" mapstructure:"vm_cate"`
}

type ScaleWarehouseSizeResp struct {
	ActionID string `json:"action_id" mapstructure:"action_id"`
}

type ResumeWarehouseReq struct {
	WarehouseId string `json:"warehouse_id" mapstructure:"warehouse_id"`
}

type ResumeWarehouseResp struct {
	ActionID string `json:"action_id" mapstructure:"action_id"`
}

type SuspendWarehouseReq struct {
	WarehouseId string `json:"warehouse_id" mapstructure:"warehouse_id"`
}

type SuspendWarehouseResp struct {
	ActionID string `json:"action_id" mapstructure:"action_id"`
}

type ReleaseWarehouseReq struct {
	WarehouseId string `json:"warehouse_id" mapstructure:"warehouse_id"`
}

type ReleaseWarehouseResp struct {
	ActionID string `json:"action_id" mapstructure:"action_id"`
}

type GetWarehouseIdleConfigReq struct {
	WarehouseId string `json:"warehouse_id" mapstructure:"warehouse_id"`
}

type GetWarehouseIdleConfigResp struct {
	Config *WarehouseIdleConfig `json:"config" mapstructure:"config"`
}

type WarehouseIdleConfig struct {
	ResourceNodeId string `json:"resource_node_id" mapstructure:"resource_node_id"`
	IntervalMs     int64  `json:"interval_ms" mapstructure:"interval_ms"`
	State          bool   `json:"state" mapstructure:"state"`
}

type UpdateWarehouseIdleConfigReq struct {
	WarehouseId string `json:"warehouse_id" mapstructure:"warehouse_id"`
	IntervalMs  int64  `json:"interval_ms" mapstructure:"interval_ms"`
	State       bool   `json:"state" mapstructure:"state"`
}

type GetWarehouseAutoScalingConfigReq struct {
	WarehouseId string `json:"warehouse_id" mapstructure:"warehouse_id"`
}

type WearhouseScalingCondition struct {
	Type            int32  `json:"type" mapstructure:"type"`
	DurationSeconds int64  `json:"duration_seconds" mapstructure:"duration_seconds"`
	Value           string `json:"value" mapstructure:"value"`
}

type WearhouseScalingPolicyItem struct {
	Type       int32                        `json:"type" mapstructure:"type"`
	StepSize   int32                        `json:"step_size" mapstructure:"step_size"`
	Conditions []*WearhouseScalingCondition `json:"conditions" mapstructure:"conditions"`
}

type WarehouseAutoScalingConfig struct {
	MinSize    int32                         `json:"min_size" mapstructure:"min_size"`
	MaxSize    int32                         `json:"max_size" mapstructure:"max_size"`
	PolicyItem []*WearhouseScalingPolicyItem `json:"policyItem" mapstructure:"policyItem"`
	State      bool                          `json:"state" mapstructure:"state"`
}

type GetWarehouseAutoScalingConfigResp struct {
	Policy *WarehouseAutoScalingConfig `json:"policy" mapstructure:"policy"`
}

type SaveWarehouseAutoScalingConfigReq struct {
	WarehouseAutoScalingConfig
	ClusterId   string `json:"cluster_id" mapstructure:"cluster_id"`
	WarehouseId string `json:"warehouse_id" mapstructure:"warehouse_id"`
	State       bool   `json:"state" mapstructure:"state"`
}

type SaveWarehouseAutoScalingConfigResp struct {
	BizId string `json:"biz_id" mapstructure:"biz_id"`
}

type DeleteWarehouseAutoScalingConfigReq struct {
	WarehouseId string `json:"warehouse_id" mapstructure:"warehouse_id"`
}

type ChangeWarehouseDistributionReq struct {
	WarehouseID        string `json:"warehouse_id" mapstructure:"warehouse_id"`
	DistributionPolicy string `json:"distribution_policy" mapstructure:"distribution_policy"`
	SpecifyAz          string `json:"specify_az" mapstructure:"specify_az"`
}

type ChangeWarehouseDistributionResp struct {
	InfraActionId string `json:"infra_action_id" mapstructure:"infra_action_id"`
}

type GetVmInfoReq struct {
	Csp         string `json:"csp" mapstructure:"csp"`
	Region      string `json:"region" mapstructure:"region"`
	ProcessType string `json:"process_type" mapstructure:"process_type"`
	VmCate      string `json:"vm_cate" mapstructure:"vm_cate"`
}

type VmVolumeInfo struct {
	BizId              string `json:"biz_id" mapstructure:"biz_id"`
	VolumeCateId       string `json:"volume_cate_id" mapstructure:"volume_cate_id"`
	SizeGb             int64  `json:"size_gb" mapstructure:"size_gb"`
	IsInstanceStore    bool   `json:"is_instance_store" mapstructure:"is_instance_store"`
	InstanceStoreCount int32  `json:"instance_store_count" mapstructure:"instance_store_count"`
	Available          bool   `json:"available" mapstructure:"available"`
	VmCate             string `json:"vm_cate" mapstructure:"vm_cate"`
	VolumeCate         string `json:"volume_cate" mapstructure:"volume_cate"`
	Arch               string `json:"arch" mapstructure:"arch"`
}

type VMInfo struct {
	ProcessType     string          `json:"process_type" mapstructure:"process_type"` // FE/BE
	VmCate          string          `json:"vm_cate" mapstructure:"vm_cate"`
	Arch            string          `json:"arch" mapstructure:"arch"`
	IsInstanceStore bool            `json:"is_instance_store" mapstructure:"is_instance_store"`
	VmVolumeInfos   []*VmVolumeInfo `json:"vm_volume_infos" mapstructure:"vm_volume_infos"`
}

type GetVmInfoResp struct {
	VmInfo *VMInfo `json:"vm_info" mapstructure:"vm_info"`
}

type UpdateResourceTagsReq struct {
	ClusterId   string            `json:"cluster_id"`
	WarehouseId string            `json:"warehouse_id"`
	Tags        map[string]string `json:"tags"`
}

type UpgradeAMIReq struct {
	ClusterId   string            `json:"cluster_id" validate:"required"`
	ModuleType  ClusterModuleType `json:"module_type" validate:"required"`
	Ami         string            `json:"ami" validate:"required"`
	Os          string            `json:"os" validate:"required"`
	WarehouseId string            `json:"warehouse_id"`
}

func (req UpgradeAMIReq) String() string {
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return fmt.Sprintf("UpgradeAMIReq: error marshaling to JSON: %v", err)
	}
	return fmt.Sprintf("UpgradeAMIReq: %s", string(reqJSON))
}

type UpgradeAMIResp struct {
	InfraActionId string `json:"infra_action_id" mapstructure:"infra_action_id"`
}

func ConvertStrToCustomConfigType(val string) CustomConfigType {
	var customConfigType CustomConfigType
	switch val {
	case "FE":
		customConfigType = CustomConfigTypeFE
	case "BE":
		customConfigType = CustomConfigTypeBE
	case "RANGER":
		customConfigType = CustomConfigTypeRanger
	}
	return customConfigType
}

func ConvertIntToCustomConfigType(val int) CustomConfigType {
	customConfigType := CustomConfigTypeUnknown
	switch val {
	case 4:
		customConfigType = CustomConfigTypeFE
	case 1:
		customConfigType = CustomConfigTypeBE
	case 2:
		customConfigType = CustomConfigTypeRanger
	}
	return customConfigType
}

func ConvertStrToClusterModuleType(val string) ClusterModuleType {
	nodeType := ClusterModuleTypeUnknown
	switch val {
	case "FE", "COORDINATOR":
		nodeType = ClusterModuleTypeFE
	case "BE":
		nodeType = ClusterModuleTypeBE
	case "WAREHOUSE":
		nodeType = ClusterModuleTypeWarehouse
	}
	return nodeType
}

type UpsertClusterConfigReq struct {
	ClusterID   string            `json:"cluster_id" mapstructure:"cluster_id"`
	ConfigType  CustomConfigType  `json:"config_type" mapstructure:"config_type"`
	WarehouseID string            `json:"warehouse_id" mapstructure:"warehouse_id"`
	Configs     map[string]string `json:"configs" mapstructure:"configs"`
}

type UpsertClusterConfigResp struct {
	InfraActionId string `json:"infra_action_id" mapstructure:"infra_action_id"`
}

type RemoveClusterConfigReq struct {
	ClusterID   string           `json:"cluster_id" mapstructure:"cluster_id"`
	ConfigType  CustomConfigType `json:"config_type" mapstructure:"config_type"`
	WarehouseID string           `json:"warehouse_id" mapstructure:"warehouse_id"`
}

type RemoveClusterConfigResp struct {
	InfraActionId string `json:"infra_action_id" mapstructure:"infra_action_id"`
}

type ListClusterSchedulePolicyReq struct {
	ClusterId string `json:"cluster_id" mapstructure:"cluster_id"`
}

type ListClusterSchedulePolicyResp struct {
	SchedulePolicies []*ClusterSchedulePolicy `json:"schedule_policies" mapstructure:"schedule_policies"`
}

type ClusterSchedulePolicy struct {
	PolicyId        string `json:"policy_id" mapstructure:"policy_id"`
	ClusterId       string `json:"cluster_id" mapstructure:"cluster_id"`
	PolicyName      string `json:"policy_name" mapstructure:"policy_name"`
	Description     string `json:"description" mapstructure:"description"`
	TimeZone        string `json:"time_zone" mapstructure:"time_zone"`
	ActiveDateValue string `json:"active_date_value" mapstructure:"active_date_value"`
	ResumeAt        string `json:"resume_at" mapstructure:"resume_at"`
	SuspendAt       string `json:"suspend_at" mapstructure:"suspend_at"`
	State           int32  `json:"state" mapstructure:"state"`
}
type CheckClusterSchedulePolicyReq struct {
	ClusterId  string `json:"cluster_id" mapstructure:"cluster_id"`
	PolicyName string `json:"policy_name" mapstructure:"policy_name"`
}

type CheckClusterSchedulePolicyResp struct {
	Exist bool `json:"exist" mapstructure:"exist"`
}

type DeleteClusterSchedulePolicyReq struct {
	ClusterId string `json:"cluster_id" mapstructure:"cluster_id"`
	PolicyId  string `json:"policy_id" mapstructure:"policy_id"`
}

type ModifyClusterSchedulePolicyReq struct {
	PolicyId    string `json:"policy_id" mapstructure:"policy_id"`
	ClusterId   string `json:"cluster_id" mapstructure:"cluster_id"`
	PolicyName  string `json:"policy_name" mapstructure:"policy_name"`
	Description string `json:"description" mapstructure:"description"`
	TimeZone    string `json:"time_zone" mapstructure:"time_zone"`
	ActiveDays  string `json:"active_days" mapstructure:"active_days"`
	ResumeAt    string `json:"resume_at" mapstructure:"resume_at"`
	SuspendAt   string `json:"suspend_at" mapstructure:"suspend_at"`
	State       bool   `json:"state" mapstructure:"state"`
}

type SaveClusterSchedulePolicyReq struct {
	ClusterId   string `json:"cluster_id" mapstructure:"cluster_id"`
	PolicyName  string `json:"policy_name" mapstructure:"policy_name"`
	Description string `json:"description" mapstructure:"description"`
	TimeZone    string `json:"time_zone" mapstructure:"time_zone"`
	ActiveDays  string `json:"active_days" mapstructure:"active_days"`
	ResumeAt    string `json:"resume_at" mapstructure:"resume_at"`
	SuspendAt   string `json:"suspend_at" mapstructure:"suspend_at"`
	State       bool   `json:"state" mapstructure:"state"`
}

type ResetGlobalSqlSessionVariablesResp struct {
	HasFailed       bool     `json:"has_failed" mapstructure:"has_failed"`
	FailedVariables []string `json:"failed_variables" mapstructure:"failed_variables"`
	ErrMsgArr       []string `json:"err_msg_arr" mapstructure:"err_msg_arr"`
}

type ResetGlobalSqlSessionVariablesReq struct {
	ClusterId string   `json:"cluster_id" mapstructure:"cluster_id"`
	Variables []string `json:"variables" mapstructure:"variables"`
}

type SetGlobalSqlSessionVariablesResp struct {
	HasFailed       bool     `json:"has_failed" mapstructure:"has_failed"`
	FailedVariables []string `json:"failed_variables" mapstructure:"failed_variables"`
	ErrMsgArr       []string `json:"err_msg_arr" mapstructure:"err_msg_arr"`
}

type SetGlobalSqlSessionVariablesReq struct {
	ClusterId string            `json:"cluster_id" mapstructure:"cluster_id"`
	Variables map[string]string `json:"variables" mapstructure:"variables"`
}

type GetGlobalSqlSessionVariablesReq struct {
	ClusterId string   `json:"cluster_id" mapstructure:"cluster_id"`
	Variables []string `json:"variables" mapstructure:"variables"`
}

type GetGlobalSqlSessionVariablesResp struct {
	Variables map[string]string `json:"variables" mapstructure:"variables"`
}

type SaveClusterSchedulePolicyResp struct {
	PolicyId string `json:"policy_id" mapstructure:"policy_id"`
}

func Equal(a, b interface{}) bool {
	return cmp.Equal(a, b)
}

func DefaultFeVolumeMap() map[string]interface{} {
	volumeConfig := make(map[string]interface{})
	volumeConfig["vol_size"] = 150
	return volumeConfig
}

func DefaultBeVolumeMap() map[string]interface{} {
	volumeConfig := make(map[string]interface{})
	volumeConfig["vol_number"] = 2
	volumeConfig["vol_size"] = 100
	return volumeConfig
}

func Contains[T comparable](arr []T, target T) bool {
	for _, v := range arr {
		if v == target {
			return true
		}
	}
	return false
}

func GetKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func GetVals[K comparable, V any](m map[K]V) []V {
	vals := make([]V, 0, len(m))
	for _, v := range m {
		vals = append(vals, v)
	}
	return vals
}

func IsValidTimeZoneName(tzName string) bool {
	_, err := time.LoadLocation(tzName)
	return err == nil
}
