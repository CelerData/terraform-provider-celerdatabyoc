package celerdatabyoc

import (
	"context"
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"log"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"
)

func resourceClusterSSLCert() *schema.Resource {

	return &schema.Resource{
		CreateContext: resourceClusterSSLCertCreate,
		UpdateContext: resourceClusterSSLCertUpdate,
		ReadContext:   resourceClusterSSLCertRead,
		DeleteContext: resourceClusterSSLCertDelete,
		Schema: map[string]*schema.Schema{
			"cluster_id": {
				Type:     schema.TypeString,
				ForceNew: true,
				Required: true,
			}, "domain": {
				Type:     schema.TypeString,
				Required: true,
			}, "s3_bucket": {
				Type:     schema.TypeString,
				Required: true,
			}, "s3_key_of_ssl_crt": {
				Type:     schema.TypeString,
				Required: true,
			}, "s3_key_of_ssl_crt_key": {
				Type:     schema.TypeString,
				Required: true,
			}, "cert_id": {
				Type:     schema.TypeString,
				Computed: true,
			}, "cert_state": {
				Type:     schema.TypeString,
				Computed: true,
			},
		},
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
	}
}

func resourceClusterSSLCertDelete(ctx context.Context, data *schema.ResourceData, i interface{}) diag.Diagnostics {
	return resourceClusterSSLCertRead(ctx, data, i)
}

func resourceClusterSSLCertCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	c := m.(*client.CelerdataClient)
	clusterAPI := cluster.NewClustersAPI(c)
	clusterId := d.Get("cluster_id").(string)

	log.Printf("[DEBUG] get cluster, cluster[%s]", clusterId)
	resp, err := clusterAPI.Get(ctx, &cluster.GetReq{ClusterID: clusterId})
	if err != nil {
		log.Printf("[ERROR] get cluster, error:%s", err.Error())
		return diag.FromErr(err)
	}

	if resp.Cluster == nil || resp.Cluster.ClusterState == cluster.ClusterStateReleased {
		d.SetId("")
		return diag.FromErr(fmt.Errorf("cluster %s not found", clusterId))
	}

	if resp.Cluster.ClusterState != cluster.ClusterStateRunning {
		return diag.FromErr(fmt.Errorf("cluster %s is not running, state: %s", clusterId, resp.Cluster.ClusterState))
	}

	upsertCertReq := &cluster.UpsertClusterSSLCertReq{
		ClusterId: clusterId,
		Domain:    d.Get("domain").(string),
		CrtBucket: d.Get("s3_bucket").(string),
		CrtPath:   d.Get("s3_key_of_ssl_crt").(string),
		KeyBucket: d.Get("s3_bucket").(string),
		KeyPath:   d.Get("s3_key_of_ssl_crt_key").(string),
	}

	err = clusterAPI.UpsertClusterSSLCert(ctx, upsertCertReq)

	if err != nil {
		log.Printf("[ERROR] allocate cluster endpoints failed, error:%s", err.Error())
		return diag.FromErr(err)
	}
	d.SetId(clusterId)
	return resourceClusterSSLCertRead(ctx, d, m)
}

func resourceClusterSSLCertRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {

	log.Printf("[DEBUG] resourceClusterSSLCertRead, cluster id:%s", d.Id())
	c := m.(*client.CelerdataClient)
	clusterAPI := cluster.NewClustersAPI(c)
	clusterId := d.Id()

	var diags diag.Diagnostics

	domainSSLCertResp, err := clusterAPI.GetClusterDomainSSLCert(ctx, &cluster.GetClusterDomainSSLCertReq{
		ClusterId: clusterId,
		Domain:    d.Get("domain").(string),
	})
	if err != nil {
		log.Printf("[ERROR] query cluster domain cert info failed, error:%s", err.Error())
		return diag.FromErr(err)
	}

	if !domainSSLCertResp.Exist {
		d.SetId("")
	} else {
		domainCert := domainSSLCertResp.DomainCert
		log.Printf("[INFO] resourceClusterSSLCertRead, cluster id:%s domainCert=%+v", d.Id(), domainCert)
		d.Set("cert_id", domainCert.CertID)
		d.Set("cert_state", domainCert.CertState)
		d.Set("domain", domainCert.Domain)
		d.Set("s3_bucket", domainCert.CrtBucket)
		d.Set("s3_key_of_ssl_crt", domainCert.CrtPath)
		d.Set("s3_key_of_ssl_crt_key", domainCert.KeyPath)
		d.SetId(clusterId)
	}

	return diags
}

func resourceClusterSSLCertUpdate(ctx context.Context, data *schema.ResourceData, i interface{}) diag.Diagnostics {
	return resourceClusterSSLCertCreate(ctx, data, i)
}
