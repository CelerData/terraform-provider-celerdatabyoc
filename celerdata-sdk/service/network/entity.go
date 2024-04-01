package network

type CreateNetworkReq struct {
	Csp             string `json:"csp" mapstructure:"csp"`
	Region          string `json:"region" mapstructure:"region"`
	Name            string `json:"name" mapstructure:"name"`
	SubnetId        string `json:"subnet_id" mapstructure:"subnet_id"`
	SecurityGroupId string `json:"security_group_id" mapstructure:"security_group_id"`
	VpcEndpointId   string `json:"vpc_endpoint_id" mapstructure:"vpc_endpoint_id"`
	PublicAccess    bool   `json:"public_access" mapstructure:"public_access"`
	DeployCredID    string `json:"deploy_cred_id" mapstructure:"deploy_cred_id"`
}

type CreateAzureNetworkReq struct {
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

type Network struct {
	BizID           string `json:"biz_id" mapstructure:"biz_id"`
	CspId           string `json:"csp_id" mapstructure:"csp_id"`
	RegionId        string `json:"region_id" mapstructure:"region_id"`
	Name            string `json:"name" mapstructure:"name"`
	SubnetId        string `json:"subnet_id" mapstructure:"subnet_id"`
	SecurityGroupId string `json:"security_group_id" mapstructure:"security_group_id"`
	VpcEndpointId   string `json:"vpc_endpoint_id" mapstructure:"vpc_endpoint_id"`
}

type GetNetworkResp struct {
	Network *Network `json:"network" mapstructure:"network"`
}
