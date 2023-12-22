package celerdatabyoc

import (
	"context"
	"crypto/md5"
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"log"
	"terraform-provider-celerdatabyoc/celerdata-sdk/client"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"
)

func resourceDatabaseUser() *schema.Resource {
	return &schema.Resource{
		CreateContext: createDatabaseUser,
		ReadContext:   queryDatabaseUser,
		UpdateContext: updateDatabaseUser,
		DeleteContext: deleteDatabaseUser,
		Schema: map[string]*schema.Schema{
			"cluster_id": {
				Type:         schema.TypeString,
				ForceNew:     true,
				Required:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},
			"admin_user_name": {
				Description:  "User who has the authority to create new users",
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},
			"admin_user_password": {
				Type:         schema.TypeString,
				Required:     true,
				Sensitive:    true,
				ValidateFunc: validation.StringIsNotEmpty,
			},
			"user_name": {
				Description:  "The user to be created",
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},
			"user_password": {
				Type:         schema.TypeString,
				Required:     true,
				Sensitive:    true,
				ValidateFunc: validation.StringIsNotEmpty,
			},
		},
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
	}
}

func queryDatabaseUser(ctx context.Context, data *schema.ResourceData, i interface{}) diag.Diagnostics {

	clusterApi := getApiClient(i)
	clusterId := data.Get("cluster_id").(string)
	_, diagnostics := getCluster(ctx, clusterApi, clusterId, true)
	if len(diagnostics) > 0 {
		return diagnostics
	}

	checkDatabaseUserResp, err := clusterApi.CheckDatabaseUser(ctx, &cluster.CheckDatabaseUserReq{
		ClusterId: clusterId,
		UserInfo: cluster.DatabaseUserInfo{
			UserName: data.Get("user_name").(string),
			Password: data.Get("user_password").(string),
		},
		LoginUserInfo: cluster.DatabaseUserInfo{
			UserName: data.Get("admin_user_name").(string),
			Password: data.Get("admin_user_password").(string),
		},
	})
	if err != nil {
		log.Printf("[ERROR] check database user error:%s", err.Error())
		return diag.Errorf("Failed to query user %s info. errMsg:%s", data.Get("user_name").(string), err.Error())
	}

	if !checkDatabaseUserResp.Exist {
		data.SetId("")
	}
	return diag.Diagnostics{}
}

func createDatabaseUser(ctx context.Context, data *schema.ResourceData, i interface{}) diag.Diagnostics {

	clusterApi := getApiClient(i)
	clusterId := data.Get("cluster_id").(string)
	_, diagnostics := getCluster(ctx, clusterApi, clusterId, true)
	if len(diagnostics) > 0 {
		return diagnostics
	}

	_, err := clusterApi.CreateDatabaseUser(ctx, &cluster.CreateDatabaseUserReq{
		ClusterId: clusterId,
		NewUserInfo: cluster.DatabaseUserInfo{
			UserName: data.Get("user_name").(string),
			Password: data.Get("user_password").(string),
		},
		LoginUserInfo: cluster.DatabaseUserInfo{
			UserName: data.Get("admin_user_name").(string),
			Password: data.Get("admin_user_password").(string),
		},
	})

	if err != nil {
		log.Printf("[ERROR] create database user %s error:%s", data.Get("user_name").(string), err.Error())
		return diag.Errorf("Failed to create database user %s errMsg:%s", data.Get("user_name").(string), err.Error())
	}

	key := fmt.Sprintf("%s-%s", clusterId, data.Get("user_name").(string))
	id := fmt.Sprintf("%x", md5.Sum([]byte(key)))
	data.SetId(id)
	return queryDatabaseUser(ctx, data, i)
}

func deleteDatabaseUser(ctx context.Context, data *schema.ResourceData, i interface{}) diag.Diagnostics {
	clusterApi := getApiClient(i)
	clusterId := data.Get("cluster_id").(string)
	_, diagnostics := getCluster(ctx, clusterApi, clusterId, true)
	if len(diagnostics) > 0 {
		return diagnostics
	}

	_, err := clusterApi.DropDatabaseUser(ctx, &cluster.DropDatabaseUserReq{
		ClusterId: clusterId,
		UserInfo: cluster.DatabaseUserInfo{
			UserName: data.Get("user_name").(string),
			Password: data.Get("user_password").(string),
		},
		LoginUserInfo: cluster.DatabaseUserInfo{
			UserName: data.Get("admin_user_name").(string),
			Password: data.Get("admin_user_password").(string),
		},
	})

	if err != nil {
		log.Printf("[ERROR] drop database user %s error:%s", data.Get("user_name").(string), err.Error())
		return diag.Errorf("Failed to drop database user %s. errMsg:%s", data.Get("user_name").(string), err.Error())
	}
	data.SetId("")
	return diag.Diagnostics{}
}

func updateDatabaseUser(ctx context.Context, data *schema.ResourceData, i interface{}) diag.Diagnostics {
	clusterApi := getApiClient(i)
	clusterId := data.Get("cluster_id").(string)
	_, diagnostics := getCluster(ctx, clusterApi, clusterId, true)
	if len(diagnostics) > 0 {
		return diagnostics
	}

	_, err := clusterApi.ResetDatabaseUserPassword(ctx, &cluster.ResetDatabaseUserPasswordReq{
		ClusterId: clusterId,
		UserInfo: cluster.DatabaseUserInfo{
			UserName: data.Get("user_name").(string),
			Password: data.Get("user_password").(string),
		},
		LoginUserInfo: cluster.DatabaseUserInfo{
			UserName: data.Get("admin_user_name").(string),
			Password: data.Get("admin_user_password").(string),
		},
	})

	if err != nil {
		log.Printf("[ERROR] Failed to change user %s's password, errMsg:%s", data.Get("user_name").(string), err.Error())
		return diag.Errorf("Failed to change %s's password. errMsg:%s", data.Get("user_name").(string), err.Error())
	}
	return diag.Diagnostics{}
}

func getApiClient(i interface{}) cluster.IClusterAPI {
	c := i.(*client.CelerdataClient)
	return cluster.NewClustersAPI(c)
}

func getCluster(ctx context.Context, clusterApi cluster.IClusterAPI, clusterId string,
	needRunning bool) (*cluster.Cluster,
	diag.Diagnostics) {

	var diagnostics diag.Diagnostics
	log.Printf("[DEBUG] get cluster, cluster[%s]", clusterId)
	resp, err := clusterApi.Get(ctx, &cluster.GetReq{ClusterID: clusterId})
	if err != nil {
		log.Printf("[ERROR] get cluster, error:%s", err.Error())
		diagnostics = append(diagnostics, diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "server error",
			Detail:   fmt.Sprintf("query cluster info failed, error:%s", err.Error()),
		})
	}

	clusterInfo := resp.Cluster
	if clusterInfo == nil {
		diagnostics = append(diagnostics, diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "cluster not found",
			Detail:   fmt.Sprintf("cluster %s not found", clusterId),
		})
	}

	if clusterInfo.ClusterState != cluster.ClusterStateRunning {
		var severity diag.Severity
		if needRunning {
			severity = diag.Error
		} else {
			severity = diag.Warning
		}
		diagnostics = append(diagnostics, diag.Diagnostic{
			Severity: severity,
			Summary:  "cluster is not running",
			Detail: fmt.Sprintf("cluster [%s] is not running, database operations require the cluster to be running",
				clusterInfo.ClusterName),
		})
	}

	return clusterInfo, diagnostics
}
