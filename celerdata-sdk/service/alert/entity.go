package alert

// PagerDutyIntegration mirrors the central-side PagerDutyIntegration proto.
// Routing key is masked on read (only its tail is returned).
type PagerDutyIntegration struct {
	IntegrationId    string `json:"integrationId,omitempty" mapstructure:"integrationId"`
	AccountId        string `json:"accountId,omitempty" mapstructure:"accountId"`
	Name             string `json:"name,omitempty" mapstructure:"name"`
	RoutingKeyMasked string `json:"routingKeyMasked,omitempty" mapstructure:"routingKeyMasked"`
	DefaultSeverity  string `json:"defaultSeverity,omitempty" mapstructure:"defaultSeverity"`
	CreatedAt        int64  `json:"createdAt,omitempty" mapstructure:"createdAt"`
	UpdatedAt        int64  `json:"updatedAt,omitempty" mapstructure:"updatedAt"`
}

type CreatePagerDutyIntegrationReq struct {
	Name            string `json:"name"`
	RoutingKey      string `json:"routingKey"`
	DefaultSeverity string `json:"defaultSeverity,omitempty"`
}

type CreatePagerDutyIntegrationResp struct {
	IntegrationId string `json:"integrationId" mapstructure:"integrationId"`
}

type ListPagerDutyIntegrationsResp struct {
	Integrations []*PagerDutyIntegration `json:"integrations" mapstructure:"integrations"`
}

type GetPagerDutyIntegrationResp struct {
	Integration *PagerDutyIntegration `json:"integration" mapstructure:"integration"`
}

type UpdatePagerDutyIntegrationReq struct {
	Name            string `json:"name"`
	RoutingKey      string `json:"routingKey,omitempty"`
	DefaultSeverity string `json:"defaultSeverity,omitempty"`
}

// Alert policy types.

type AlertExpr struct {
	Func         string  `json:"func,omitempty" mapstructure:"func"`
	ClusterName  string  `json:"clusterName,omitempty" mapstructure:"clusterName"`
	ClusterId    string  `json:"clusterId,omitempty" mapstructure:"clusterId"`
	Metric       string  `json:"metric,omitempty" mapstructure:"metric"`
	Operator     string  `json:"operator,omitempty" mapstructure:"operator"`
	Value        float32 `json:"value,omitempty" mapstructure:"value"`
	ClusterState int32   `json:"clusterState,omitempty" mapstructure:"clusterState"`
}

type EmailConfig struct {
	EmailAddr []string `json:"emailAddr,omitempty" mapstructure:"emailAddr"`
	Subject   string   `json:"subject,omitempty" mapstructure:"subject"`
	EmailCc   []string `json:"emailCc,omitempty" mapstructure:"emailCc"`
}

type PolicyPagerDutyBinding struct {
	IntegrationId    string `json:"integrationId,omitempty" mapstructure:"integrationId"`
	SeverityOverride string `json:"severityOverride,omitempty" mapstructure:"severityOverride"`
	IntegrationName  string `json:"integrationName,omitempty" mapstructure:"integrationName"`
}

type AlertPolicy struct {
	PolicyId          string                    `json:"policyId,omitempty" mapstructure:"policyId"`
	Name              string                    `json:"name,omitempty" mapstructure:"name"`
	AccountId         string                    `json:"accountId,omitempty" mapstructure:"accountId"`
	Exprs             []*AlertExpr              `json:"exprs,omitempty" mapstructure:"exprs"`
	Interval          string                    `json:"interval,omitempty" mapstructure:"interval"`
	AlertState        string                    `json:"alertState,omitempty" mapstructure:"alertState"`
	Method            string                    `json:"method,omitempty" mapstructure:"method"`
	CreateTime        int64                     `json:"createTime,omitempty" mapstructure:"createTime"`
	EmailConfig       *EmailConfig              `json:"emailConfig,omitempty" mapstructure:"emailConfig"`
	Expr              string                    `json:"expr,omitempty" mapstructure:"expr"`
	Region            string                    `json:"region,omitempty" mapstructure:"region"`
	Creator           string                    `json:"creator,omitempty" mapstructure:"creator"`
	CalculationWindow string                    `json:"calculationWindow,omitempty" mapstructure:"calculationWindow"`
	PagerDutyEnabled  bool                      `json:"pagerDutyEnabled,omitempty" mapstructure:"pagerDutyEnabled"`
	PagerDutyBindings []*PolicyPagerDutyBinding `json:"pagerDutyBindings,omitempty" mapstructure:"pagerDutyBindings"`
}

type UpsertAlertPolicyReq struct {
	Region string       `json:"region"`
	Policy *AlertPolicy `json:"policy"`
}

type CreateAlertPolicyResp struct {
	PolicyId   string `json:"policyId" mapstructure:"policyId"`
	CreateTime string `json:"createTime" mapstructure:"createTime"`
	Region     string `json:"region" mapstructure:"region"`
}

type ListAlertPoliciesResp struct {
	Policies []*AlertPolicy `json:"policies" mapstructure:"policies"`
}

type GetAlertPolicyResp struct {
	Policy *AlertPolicy `json:"policy" mapstructure:"policy"`
}
