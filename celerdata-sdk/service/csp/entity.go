package csp

type GetReq struct {
	CspName string `json:"csp_name" mapstructure:"csp_name"`
}

type GetResp struct {
	Csp *CSP `json:"csp" mapstructure:"csp"`
}

type CSP struct {
	CspId                   string `json:"csp_id" mapstructure:"csp_id"`
	CspName                 string `json:"csp_name" mapstructure:"csp_name"`
	DeployCredPolicy        string `json:"deploy_cred_policy" mapstructure:"deploy_cred_policy"`
	TrustAccountId          string `json:"trust_account_id" mapstructure:"trust_account_id"`
	DeployCredPolicyVersion string `json:"deploy_cred_policy_version" mapstructure:"deploy_cred_policy_version"`
	DataCredPolicy          string `json:"data_cred_policy" mapstructure:"data_cred_policy"`
	DataCredPolicyVersion   string `json:"data_cred_policy_version" mapstructure:"data_cred_policy_version"`
}
