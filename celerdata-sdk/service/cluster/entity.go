package cluster

type ClusterModuleType string
type ClusterState string
type ClusterType string
type DomainAllocateState int32

const (
	ClusterTypeClassic                               = ClusterType("CLASSIC")
	ClusterTypeElasic                                = ClusterType("ELASTIC")
	ClusterModuleTypeFE                              = ClusterModuleType("FE")
	ClusterModuleTypeBE                              = ClusterModuleType("BE")
	ClusterModuleTypeWarehouse                       = ClusterModuleType("Warehouse")
	ClusterStateDeploying                            = ClusterState("Deploying")
	ClusterStateRunning                              = ClusterState("Running")
	ClusterStateScaling                              = ClusterState("Scaling")
	ClusterStateAbnormal                             = ClusterState("Abnormal")
	ClusterStateSuspending                           = ClusterState("Suspending")
	ClusterStateSuspended                            = ClusterState("Suspended")
	ClusterStateResuming                             = ClusterState("Resuming")
	ClusterStateReleasing                            = ClusterState("Releasing")
	ClusterStateReleased                             = ClusterState("Released")
	DomainAllocateStateUnknown   DomainAllocateState = 0
	DomainAllocateStateOngoing   DomainAllocateState = 3
	DomainAllocateStateSucceeded DomainAllocateState = 1
	DomainAllocateStateFailed    DomainAllocateState = 2
)

type Kv struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type DiskInfo struct {
	Number  uint32 `json:"number"`
	PerSize uint64 `json:"per_size"` // unit:GB
}

type ClusterItem struct {
	Type          ClusterModuleType `json:"type"`
	Name          string            `json:"name"`
	Num           uint32            `json:"num"`
	StorageSizeGB uint64            `json:"storage_size_gb"` // deprecated
	InstanceType  string            `json:"instance_type"`
	DiskInfo      *DiskInfo         `json:"disk_info"`
}

type Script struct {
	ScriptPath string `json:"script_path"`
	LogsDir    string `json:"logs_dir"`
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
	ClusterID string `json:"cluster_id" mapstructure:"cluster_id"`
	ActionID  string `json:"action_id" mapstructure:"action_id"`
}

type Module struct {
	AmiId         string `json:"ami_id" mapstructure:"ami_id"`
	Num           uint32 `json:"num" mapstructure:"num"`
	StorageSizeGB uint64 `json:"storage_size_gb" mapstructure:"storage_size_gb"`
	InstanceType  string `json:"instance_type" mapstructure:"instance_type"`
	VmVolSizeGB   int64  `json:"vm_vol_size_gb" mapstructure:"vm_vol_size_gb"`
	VmVolNum      int32  `json:"vm_vol_num" mapstructure:"vm_vol_num"`
}

type Cluster struct {
	ClusterID      string       `json:"cluster_id" mapstructure:"cluster_id"`
	ClusterName    string       `json:"cluster_name" mapstructure:"cluster_name"`
	ClusterState   ClusterState `json:"cluster_state"  mapstructure:"cluster_state"`
	ClusterVersion string       `json:"cluster_version" mapstructure:"cluster_version"`
	ClusterType    ClusterType  `json:"cluster_type" mapstructure:"cluster_type"`
	Csp            string       `json:"csp" mapstructure:"csp"`
	Region         string       `json:"region" mapstructure:"region"`
	AccountID      string       `json:"account_id" mapstructure:"account_id"`
	FeModule       *Module      `json:"fe_module" mapstructure:"fe_module"`
	BeModule       *Module      `json:"be_module" mapstructure:"be_module"`
	SSLConnEnable  bool         `json:"ssl_conn_enable" mapstructure:"ssl_conn_enable"`
	NetIfaceID     string       `json:"net_iface_id" mapstructure:"net_iface_id"`
	DeployCredID   string       `json:"deploy_cred_id" mapstructure:"deploy_cred_id"`
	DataCredID     string       `json:"data_cred_id" mapstructure:"data_cred_id"`
	FreeTier       bool         `json:"free_tier" mapstructure:"free_tier"`
	QueryPort      int32        `json:"query_port" mapstructure:"query_port"`
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
	NetworkMethod string `json:"network_method" mapstructure:"network_method"`
	Host          string `json:"host" mapstructure:"host"`
	Port          int64  `json:"port" mapstructure:"port"`
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
