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
