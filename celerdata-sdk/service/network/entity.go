package network

type CreateNetworkReq struct {
	Csp             string   `json:"csp" mapstructure:"csp"`
	Region          string   `json:"region" mapstructure:"region"`
	Name            string   `json:"name" mapstructure:"name"`
	SubnetId        string   `json:"subnet_id" mapstructure:"subnet_id"`
	SecurityGroupId string   `json:"security_group_id" mapstructure:"security_group_id"`
	VpcEndpointId   string   `json:"vpc_endpoint_id" mapstructure:"vpc_endpoint_id"`
	PublicAccess    bool     `json:"public_access" mapstructure:"public_access"`
	DeployCredID    string   `json:"deploy_cred_id" mapstructure:"deploy_cred_id"`
	SubnetIds       []string `json:"subnet_ids" mapstructure:"subnet_ids"`
}

type CreateAzureNetworkReq struct {
	DeploymentCredentialID   string `json:"deployment_credential_id" mapstructure:"deployment_credential_id"`
	Csp                      string `json:"csp" mapstructure:"csp"`
	Region                   string `json:"region" mapstructure:"region"`
	Name                     string `json:"name" mapstructure:"name"`
	VirtualNetworkResourceId string `json:"virtual_network_resource_id" mapstructure:"virtual_network_resource_id"`
	SubnetName               string `json:"subnet_name" mapstructure:"subnet_name"`
	PublicAccess             bool   `json:"public_access" mapstructure:"public_access"`
}

type CreateNetworkResp struct {
	NetworkID   string `json:"network_id" mapstructure:"network_id"`
	CheckErrMsg string `json:"check_err_msg" mapstructure:"check_err_msg"`
}

type AZNetWorkInterface struct {
	Az       string `json:"az" mapstructure:"az"`
	SubnetId string `json:"subnet_id" mapstructure:"subnet_id"`
}

type Network struct {
	BizID               string                `json:"biz_id" mapstructure:"biz_id"`
	CspId               string                `json:"csp_id" mapstructure:"csp_id"`
	RegionId            string                `json:"region_id" mapstructure:"region_id"`
	Name                string                `json:"name" mapstructure:"name"`
	SubnetId            string                `json:"subnet_id" mapstructure:"subnet_id"`
	SecurityGroupId     string                `json:"security_group_id" mapstructure:"security_group_id"`
	VpcEndpointId       string                `json:"vpc_endpoint_id" mapstructure:"vpc_endpoint_id"`
	MultiAz             bool                  `json:"multi_az" mapstructure:"multi_az"`
	AZNetWorkInterfaces []*AZNetWorkInterface `json:"az_netWork_interfaces" mapstructure:"az_netWork_interfaces"`
}

type GetNetworkResp struct {
	Network *Network `json:"network" mapstructure:"network"`
}

type CreateGcpNetworkReq struct {
	DeploymentCredentialID string   `json:"deployment_credential_id" mapstructure:"deployment_credential_id"`
	Name                   string   `json:"name" mapstructure:"name"`
	Region                 string   `json:"region" mapstructure:"region"`
	NetworkTag             string   `json:"network_tag" mapstructure:"network_tag"`
	Subnet                 string   `json:"subnet" mapstructure:"subnet"`
	PscConnectionId        string   `json:"psc_connection_id" mapstructure:"psc_connection_id"`
	MultiAz                bool     `json:"multi_az" mapstructure:"multi_az"`
	AvailabilityZones      []string `json:"availability_zones" mapstructure:"availability_zones"`
}
