package credential

type CreateDeployRoleCredReq struct {
	Csp           string `json:"csp" mapstructure:"csp"`
	Name          string `json:"name" mapstructure:"name"`
	RoleArn       string `json:"role_arn" mapstructure:"role_arn"`
	ExternalId    string `json:"external_id" mapstructure:"external_id"`
	PolicyVersion string `json:"policy_version" mapstructure:"policy_version"`
}

type CreateDeployRoleCredResp struct {
	CredID      string `json:"cred_id" mapstructure:"cred_id"`
	CheckErrMsg string `json:"check_err_msg" mapstructure:"check_err_msg"`
}

type GetDeployRoleCredResp struct {
	DeployRoleCred *DeploymentRoleCredential `json:"deploy_role_cred" mapstructure:"deploy_role_cred"`
}

type DeploymentRoleCredential struct {
	BizID          string `json:"biz_id" mapstructure:"biz_id"`
	Name           string `json:"name" mapstructure:"name"`
	RoleArn        string `json:"role_arn" mapstructure:"role_arn"`
	ExternalId     string `json:"external_id" mapstructure:"external_id"`
	TrustAccountId string `json:"trust_account_id" mapstructure:"trust_account_id"`
	PolicyVersion  string `json:"policy_version" mapstructure:"policy_version"`
}

type CreateDataCredReq struct {
	Csp                string `json:"csp" mapstructure:"csp"`
	Region             string `json:"region" mapstructure:"region"`
	Name               string `json:"name" mapstructure:"name"`
	RoleArn            string `json:"role_arn" mapstructure:"role_arn"`
	InstanceProfileArn string `json:"instance_profile_arn" mapstructure:"isntance_profile_arn"`
	BucketName         string `json:"bucket_name" mapstructure:"bucket_name"`
	PolicyVersion      string `json:"policy_version" mapstructure:"policy_version"`
}

type CreateAzureDataCredReq struct {
	Csp                       string `json:"csp" mapstructure:"csp"`
	Name                      string `json:"name" mapstructure:"name"`
	ManagedIdentityResourceId string `json:"managed_identity_resource_id" mapstructure:"managed_identity_resource_id"`
	StorageAccountName        string `json:"storage_account_name" mapstructure:"storage_account_name"`
	ContainerName             string `json:"container_name" mapstructure:"container_name"`
}

type CreateDataCredResp struct {
	CredID      string `json:"cred_id" mapstructure:"cred_id"`
	CheckErrMsg string `json:"check_err_msg" mapstructure:"check_err_msg"`
}

type GetDataCredResp struct {
	DataCred DataCredential `json:"data_cred" mapstructure:"data_cred"`
}

type DataCredential struct {
	BizID              string `json:"biz_id" mapstructure:"biz_id"`
	Name               string `json:"name" mapstructure:"name"`
	RoleArn            string `json:"role_arn" mapstructure:"role_arn"`
	InstanceProfileArn string `json:"instance_profile_arn" mapstructure:"instance_profile_arn"`
	BucketName         string `json:"bucket_name" mapstructure:"bucket_name"`
	PolicyVersion      string `json:"policy_version" mapstructure:"policy_version"`
}

type CreateDeployAkSkCredReq struct {
	Csp               string `json:"csp" mapstructure:"csp"`
	Name              string `json:"name" mapstructure:"name"`
	TenantId          string `json:"tenant_id" mapstructure:"tenant_id"`
	ApplicationId     string `json:"application_id" mapstructure:"application_id"`
	ClientSecretValue string `json:"client_secret_value" mapstructure:"client_secret_value"`
	SshKeyResourceId  string `json:"ssh_key_resource_id" mapstructure:"ssh_key_resource_id"`
}

type CreateDeployAkSkCredResp struct {
	CredID      string `json:"cred_id" mapstructure:"cred_id"`
	CheckErrMsg string `json:"check_err_msg" mapstructure:"check_err_msg"`
}

type RotateAkSkCredentialReq struct {
	CredID string `json:"cred_id" mapstructure:"cred_id"`
	Ak     string `json:"ak" mapstructure:"ak"`
	Sk     string `json:"sk" mapstructure:"sk"`
}

type GetDeployAkSkCredResp struct {
	DeployRoleCred *DeploymentAkSkCredential `json:"deploy_role_cred" mapstructure:"deploy_role_cred"`
}

type DeploymentAkSkCredential struct {
	BizID        string `json:"biz_id" mapstructure:"biz_id"`
	CspId        string `json:"csp_id" mapstructure:"csp_id"`
	CspAccountId string `json:"csp_account_id" mapstructure:"csp_account_id"`
	CspOrgId     string `json:"csp_org_id" mapstructure:"csp_org_id"`
	Name         string `json:"name" mapstructure:"name"`
	Ak           string `json:"ak" mapstructure:"ak"`
	Sk           string `json:"sk" mapstructure:"sk"`
	SshKey       string `json:"ssh_key" mapstructure:"ssh_key"`
	Unverified   bool   `json:"unverified" mapstructure:"unverified"`
}
