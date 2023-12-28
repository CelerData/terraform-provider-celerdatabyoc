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
	"terraform-provider-celerdatabyoc/common"
)

func resourceClusterUser() *schema.Resource {
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
			"login_user": {
				Description:  "User who has the authority to create new users",
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringIsNotEmpty,
			},
			"login_password": {
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
				Type:             schema.TypeString,
				Required:         true,
				Sensitive:        true,
				ValidateDiagFunc: common.ValidatePassword(),
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

	var diagnostics diag.Diagnostics
	resp, err := clusterApi.Get(ctx, &cluster.GetReq{ClusterID: clusterId})

	if err != nil {
		return genErrorDiag(clusterId, err)
	}

	clusterInfo := resp.Cluster
	if clusterInfo == nil || clusterInfo.ClusterState == cluster.ClusterStateReleased {
		data.SetId("")
		return diag.Diagnostics{}
	}

	diagnostics, hasDiag := checkClusterIsRunning(clusterInfo, diagnostics, false)
	if hasDiag {
		return diagnostics
	}

	checkDatabaseUserResp, err := clusterApi.CheckDatabaseUser(ctx, &cluster.CheckDatabaseUserReq{
		ClusterId: clusterId,
		UserInfo: cluster.DatabaseUserInfo{
			UserName: data.Get("user_name").(string),
		},
		LoginUserInfo: cluster.DatabaseUserInfo{
			UserName: data.Get("login_user").(string),
			Password: data.Get("login_password").(string),
		},
	})
	if err != nil {
		log.Printf("[ERROR] check database user error:%s", err.Error())
		return diag.Errorf("Failed to query user %s info. errMsg:%s", data.Get("user_name").(string), err.Error())
	}

	if !checkDatabaseUserResp.Exist {
		data.SetId("")
	} else {
		data.SetId(genDatabaseUserId(clusterId, data.Get("user_name").(string)))
	}
	return diag.Diagnostics{}
}

func createDatabaseUser(ctx context.Context, data *schema.ResourceData, i interface{}) diag.Diagnostics {

	clusterApi := getApiClient(i)
	clusterId := data.Get("cluster_id").(string)

	var diagnostics diag.Diagnostics
	resp, err := clusterApi.Get(ctx, &cluster.GetReq{ClusterID: clusterId})

	if err != nil {
		return genErrorDiag(clusterId, err)
	}

	clusterInfo := resp.Cluster
	diagnostics, hasDiag := checkIsClusterExist(clusterInfo, clusterId)
	if hasDiag {
		return diagnostics
	}

	diagnostics, hasDiag = checkClusterIsRunning(clusterInfo, diagnostics, true)
	if hasDiag {
		return diagnostics
	}

	_, err = clusterApi.CreateDatabaseUser(ctx, &cluster.CreateDatabaseUserReq{
		ClusterId: clusterId,
		NewUserInfo: cluster.DatabaseUserInfo{
			UserName: data.Get("user_name").(string),
			Password: data.Get("user_password").(string),
		},
		LoginUserInfo: cluster.DatabaseUserInfo{
			UserName: data.Get("login_user").(string),
			Password: data.Get("login_password").(string),
		},
	})

	if err != nil {
		log.Printf("[ERROR] create database user %s error:%s", data.Get("user_name").(string), err.Error())
		return diag.Errorf("Failed to create database user %s errMsg:%s", data.Get("user_name").(string), err.Error())
	}
	data.SetId(genDatabaseUserId(clusterId, data.Get("user_name").(string)))
	return diag.Diagnostics{}
}

func checkClusterIsRunning(clusterInfo *cluster.Cluster, diagnostics diag.Diagnostics, needRunning bool) (diag.Diagnostics, bool) {
	if clusterInfo.ClusterState != cluster.ClusterStateRunning {

		severity := diag.Warning
		if needRunning {
			severity = diag.Error
		}

		diagnostics = append(diagnostics, diag.Diagnostic{
			Severity: severity,
			Summary:  "cluster not in \"running\" state",
			Detail: fmt.Sprintf("cluster[%s] not in \"running\" state, "+
				"database operations require cluster to be in \"running\" state",
				clusterInfo.ClusterName),
		})
		return diagnostics, true
	}
	return nil, false
}

func genErrorDiag(clusterId string, err error) diag.Diagnostics {
	log.Printf("[ERROR] get cluster, error:%s", err.Error())
	return diag.FromErr(err)
}

func checkIsClusterExist(clusterInfo *cluster.Cluster, clusterId string) (diag.Diagnostics, bool) {
	var diagnostics diag.Diagnostics
	if clusterInfo == nil || clusterInfo.ClusterState == cluster.ClusterStateReleased {
		diagnostics = append(diagnostics, diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "cluster doesn't exist",
			Detail:   fmt.Sprintf("cluster[%s] does not exist", clusterId),
		})
		return diagnostics, true
	}
	return diagnostics, false
}

func genDatabaseUserId(clusterId string, userName string) string {
	key := fmt.Sprintf("%s-%s", clusterId, userName)
	id := fmt.Sprintf("%x", md5.Sum([]byte(key)))
	return id
}

func deleteDatabaseUser(ctx context.Context, data *schema.ResourceData, i interface{}) diag.Diagnostics {
	clusterApi := getApiClient(i)
	clusterId := data.Get("cluster_id").(string)

	var diagnostics diag.Diagnostics
	resp, err := clusterApi.Get(ctx, &cluster.GetReq{ClusterID: clusterId})

	if err != nil {
		return genErrorDiag(clusterId, err)
	}

	clusterInfo := resp.Cluster
	if clusterInfo == nil || clusterInfo.ClusterState == cluster.ClusterStateReleased {
		data.SetId("")
		diagnostics = append(diagnostics, diag.Diagnostic{
			Severity: diag.Warning,
			Summary:  "cluster doesn't exist",
			Detail:   fmt.Sprintf("cluster[%s] does not exist", clusterId),
		})
		return diagnostics
	}

	diagnostics, hasDiag := checkClusterIsRunning(clusterInfo, diagnostics, false)
	if hasDiag {
		return diagnostics
	}

	_, err = clusterApi.DropDatabaseUser(ctx, &cluster.DropDatabaseUserReq{
		ClusterId: clusterId,
		UserInfo: cluster.DatabaseUserInfo{
			UserName: data.Get("user_name").(string),
		},
		LoginUserInfo: cluster.DatabaseUserInfo{
			UserName: data.Get("login_user").(string),
			Password: data.Get("login_password").(string),
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

	var diagnostics diag.Diagnostics
	resp, err := clusterApi.Get(ctx, &cluster.GetReq{ClusterID: clusterId})

	if err != nil {
		return genErrorDiag(clusterId, err)
	}

	clusterInfo := resp.Cluster
	diagnostics, hasDiag := checkIsClusterExist(clusterInfo, clusterId)
	if hasDiag {
		return diagnostics
	}

	diagnostics, hasDiag = checkClusterIsRunning(clusterInfo, diagnostics, true)
	if hasDiag {
		return diagnostics
	}

	_, err = clusterApi.ResetDatabaseUserPassword(ctx, &cluster.ResetDatabaseUserPasswordReq{
		ClusterId: clusterId,
		UserInfo: cluster.DatabaseUserInfo{
			UserName: data.Get("user_name").(string),
			Password: data.Get("user_password").(string),
		},
		LoginUserInfo: cluster.DatabaseUserInfo{
			UserName: data.Get("login_user").(string),
			Password: data.Get("login_password").(string),
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
