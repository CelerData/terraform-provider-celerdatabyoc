package rangerconfig

type RangerConfig struct {
	Name                               string `json:"name" mapstructure:"name"`
	BizID                              string `json:"biz_id" mapstructure:"biz_id"`
	RangerStarrocksSecurityXmlPath     string `json:"ranger_starrocks_security_xml_path" mapstructure:"ranger_starrocks_security_xml_path"`
	RangerStarrocksAuditXmlPath        string `json:"ranger_starrocks_audit_xml_path" mapstructure:"ranger_starrocks_audit_xml_path"`
	RangerStarrocksPolicymgrSslXmlPath string `json:"ranger_starrocks_policymgr_ssl_xml_path" mapstructure:"ranger_starrocks_policymgr_ssl_xml_path"`
	RangerStarrocksTrustStorePath      string `json:"ranger_starrocks_trust_store_path" mapstructure:"ranger_starrocks_trust_store_path"`
	RangerStarrocksTrustStoreCredPath  string `json:"ranger_starrocks_trust_store_cred_path" mapstructure:"ranger_starrocks_trust_store_cred_path"`
	RangerStarrocksKeyStorePath        string `json:"ranger_starrocks_key_store_path" mapstructure:"ranger_starrocks_key_store_path"`
	RangerStarrocksKeyStoreCredPath    string `json:"ranger_starrocks_key_store_cred_path" mapstructure:"ranger_starrocks_key_store_cred_path"`
	RangerHiveSecurityXmlPath          string `json:"ranger_hive_security_xml_path" mapstructure:"ranger_hive_security_xml_path"`
	RangerHiveAuditXmlPath             string `json:"ranger_hive_audit_xml_path" mapstructure:"ranger_hive_audit_xml_path"`
}

type CreateRangerConfigReq struct {
	RangerConfig RangerConfig `json:"ranger_config,omitempty" mapstructure:"ranger_config"`
}

type CreateRangerConfigResp struct {
	RangerConfig RangerConfig `json:"ranger_config,omitempty" mapstructure:"ranger_config"`
}

type UpdateRangerConfigReq struct {
	RangerConfig RangerConfig `json:"ranger_config,omitempty" mapstructure:"ranger_config"`
}

type GetRangerConfigReq struct {
	BizID string `json:"biz_id" mapstructure:"biz_id"`
}

type GetRangerConfigResp struct {
	RangerConfig RangerConfig `json:"ranger_config,omitempty" mapstructure:"ranger_config"`
}

type DelRangerConfigReq struct {
	BizID string `json:"biz_id" mapstructure:"biz_id"`
}

type DeployRangerConfigReq struct {
	BizID     string `json:"biz_id" mapstructure:"biz_id"`
	ClusterID string `json:"cluster_id" mapstructure:"cluster_id"`
}

type DeployRangerConfigResp struct {
	InfraActionId string `json:"infra_action_id" mapstructure:"infra_action_id"`
}
