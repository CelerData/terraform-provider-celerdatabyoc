package celerdatabyoc

import (
	"context"
	"log"
	"regexp"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	rangerconfig "terraform-provider-celerdatabyoc/celerdata-sdk/service/ranger-config"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func resourceRangerConfig() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceRangerConfigCreate,
		ReadContext:   resourceRangerConfigRead,
		DeleteContext: resourceRangerConfigDelete,
		UpdateContext: resourceRangerConfigUpdate,
		Schema: map[string]*schema.Schema{
			"name": {
				Type:         schema.TypeString,
				ForceNew:     true,
				Required:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},
			"ranger_starrocks_security_xml_path": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringMatch(regexp.MustCompile(`\.xml$`), "must end with .xml"),
			},
			"ranger_starrocks_audit_xml_path": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringMatch(regexp.MustCompile(`\.xml$`), "must end with .xml"),
			},
			"ranger_starrocks_policymgr_ssl_xml_path": {
				Type: schema.TypeString, Optional: true,
				ValidateFunc: validation.StringMatch(regexp.MustCompile(`\.xml$`), "must end with .xml"),
			},
			"ranger_starrocks_trust_store_path": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringMatch(regexp.MustCompile(`\.jks$`), "must end with .jks"),
			},
			"ranger_starrocks_trust_store_cred_path": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringMatch(regexp.MustCompile(`\.jceks$`), "must end with .jceks"),
			},
			"ranger_starrocks_key_store_path": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringMatch(regexp.MustCompile(`\.jks$`), "must end with .jks"),
			},
			"ranger_starrocks_key_store_cred_path": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringMatch(regexp.MustCompile(`\.jceks$`), "must end with .jceks"),
			},
			"ranger_hive_security_xml_path": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringMatch(regexp.MustCompile(`\.xml$`), "must end with .xml"),
			},
			"ranger_hive_audit_xml_path": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringMatch(regexp.MustCompile(`\.xml$`), "must end with .xml"),
			},
		},
	}
}

func resourceRangerConfigCreate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)
	cli := rangerconfig.NewRangerConfigAPI(c)

	req := &rangerconfig.CreateRangerConfigReq{
		RangerConfig: rangerconfig.RangerConfig{
			Name:                               d.Get("name").(string),
			RangerStarrocksSecurityXmlPath:     d.Get("ranger_starrocks_security_xml_path").(string),
			RangerStarrocksAuditXmlPath:        d.Get("ranger_starrocks_audit_xml_path").(string),
			RangerStarrocksPolicymgrSslXmlPath: d.Get("ranger_starrocks_policymgr_ssl_xml_path").(string),
			RangerStarrocksTrustStorePath:      d.Get("ranger_starrocks_trust_store_path").(string),
			RangerStarrocksTrustStoreCredPath:  d.Get("ranger_starrocks_trust_store_cred_path").(string),
			RangerStarrocksKeyStorePath:        d.Get("ranger_starrocks_key_store_path").(string),
			RangerStarrocksKeyStoreCredPath:    d.Get("ranger_starrocks_key_store_cred_path").(string),
			RangerHiveSecurityXmlPath:          d.Get("ranger_hive_security_xml_path").(string),
			RangerHiveAuditXmlPath:             d.Get("ranger_hive_audit_xml_path").(string),
		},
	}

	resp, err := cli.CreateRangerConfig(ctx, req)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(resp.RangerConfig.BizID)
	return diags
}

func resourceRangerConfigUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)
	cli := rangerconfig.NewRangerConfigAPI(c)

	req := &rangerconfig.UpdateRangerConfigReq{
		RangerConfig: rangerconfig.RangerConfig{
			Name:                               d.Get("name").(string),
			BizID:                              d.Id(),
			RangerStarrocksSecurityXmlPath:     d.Get("ranger_starrocks_security_xml_path").(string),
			RangerStarrocksAuditXmlPath:        d.Get("ranger_starrocks_audit_xml_path").(string),
			RangerStarrocksPolicymgrSslXmlPath: d.Get("ranger_starrocks_policymgr_ssl_xml_path").(string),
			RangerStarrocksTrustStorePath:      d.Get("ranger_starrocks_trust_store_path").(string),
			RangerStarrocksTrustStoreCredPath:  d.Get("ranger_starrocks_trust_store_cred_path").(string),
			RangerStarrocksKeyStorePath:        d.Get("ranger_starrocks_key_store_path").(string),
			RangerStarrocksKeyStoreCredPath:    d.Get("ranger_starrocks_key_store_cred_path").(string),
			RangerHiveSecurityXmlPath:          d.Get("ranger_hive_security_xml_path").(string),
			RangerHiveAuditXmlPath:             d.Get("ranger_hive_audit_xml_path").(string),
		},
	}

	err := cli.UpdateRangerConfig(ctx, req)
	if err != nil {
		return diag.FromErr(err)
	}

	return diags
}

func resourceRangerConfigRead(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)
	cli := rangerconfig.NewRangerConfigAPI(c)

	resp, err := cli.GetRangerConfig(ctx, rangerconfig.GetRangerConfigReq{BizID: d.Id()})
	log.Printf("[DEBUG] get ranger config, resp:%+v", resp)
	if err != nil {
		return diag.FromErr(err)
	}
	if resp == nil || resp.RangerConfig.BizID == "" {
		d.SetId("")
	}
	return diags
}

func resourceRangerConfigDelete(ctx context.Context, d *schema.ResourceData, m interface{}) (diags diag.Diagnostics) {
	c := m.(*client.CelerdataClient)
	cli := rangerconfig.NewRangerConfigAPI(c)

	bizID := d.Id()
	log.Printf("[DEBUG] delete ranger config, id[%s]", bizID)

	err := cli.DelRangerConfig(ctx, &rangerconfig.DelRangerConfigReq{BizID: bizID})
	if err != nil {
		return diag.FromErr(err)
	}
	return diags
}
