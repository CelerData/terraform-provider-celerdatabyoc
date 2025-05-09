package celerdatabyoc

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func resourceAutoScalingPolicy() *schema.Resource {
	resource := &schema.Resource{
		ReadContext: func(ctx context.Context, rd *schema.ResourceData, i interface{}) diag.Diagnostics {
			autoScalingConfig := ToAutoScalingConfigStruct(rd)
			bytes, err := json.Marshal(autoScalingConfig)
			if err != nil {
				return diag.FromErr(fmt.Errorf("generate auto scaling config json failed, errMsg:%s", err.Error()))
			}
			rd.Set("policy_json", string(bytes))
			return nil
		},
		CreateContext: func(ctx context.Context, rd *schema.ResourceData, i interface{}) diag.Diagnostics {
			autoScalingConfig := ToAutoScalingConfigStruct(rd)
			bytes, err := json.Marshal(autoScalingConfig)
			if err != nil {
				return diag.FromErr(fmt.Errorf("generate auto scaling config json failed, errMsg:%s", err.Error()))
			}
			hash := md5.Sum(bytes)
			md5Str := hex.EncodeToString(hash[:])
			rd.Set("policy_json", string(bytes))
			rd.SetId(md5Str)
			return nil
		},
		DeleteContext: func(ctx context.Context, rd *schema.ResourceData, i interface{}) diag.Diagnostics {
			rd.SetId("")
			return nil
		},
		Schema: map[string]*schema.Schema{
			"min_size": {
				Type:         schema.TypeInt,
				Optional:     true,
				ForceNew:     true,
				Default:      1,
				ValidateFunc: validation.IntAtLeast(1),
			},
			"max_size": {
				Type:         schema.TypeInt,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.IntAtLeast(1),
			},
			"policy_item": {
				Type:     schema.TypeSet,
				Required: true,
				ForceNew: true,
				MinItems: 2,
				MaxItems: 2,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"type": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringInSlice(cluster.WarehouseAutoScalingPolicyType, false),
						},
						"step_size": {
							Type:         schema.TypeInt,
							Required:     true,
							ValidateFunc: validation.IntAtLeast(1),
						},
						"condition": {
							Type:     schema.TypeSet,
							Optional: true,
							MinItems: 1,
							MaxItems: 1,
							Set: func(v interface{}) int {
								m := v.(map[string]interface{})
								return schema.HashString(m["type"].(string))
							},
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"type": {
										Type:         schema.TypeString,
										Required:     true,
										ValidateFunc: validation.StringInSlice(cluster.WarehouseAutoScalingPolicyConditionType, false),
									},
									"duration_seconds": {
										Type:         schema.TypeInt,
										Required:     true,
										ValidateFunc: validation.IntAtLeast(300),
									},
									"value": {
										Type:     schema.TypeFloat,
										Required: true,
										ValidateFunc: func(i interface{}, k string) (s []string, es []error) {
											v, ok := i.(float64)
											if !ok {
												es = append(es, fmt.Errorf("expected type of %s to be float", k))
												return
											}

											if v < float64(0) {
												es = append(es, fmt.Errorf("expected %s to be at least (%f), got %f", k, float64(0), v))
												return
											}

											if v > float64(100) {
												es = append(es, fmt.Errorf("expected %s to be at most (%f), got %f", k, float64(100), v))
												return
											}
											return
										},
									},
								},
							},
						},
					},
				},
			},
			"policy_json": {
				Type:     schema.TypeString,
				Computed: true,
			},
		},
		CustomizeDiff: func(ctx context.Context, rd *schema.ResourceDiff, i interface{}) error {
			min, _ := rd.Get("min_size").(int)
			max, _ := rd.Get("max_size").(int)
			if min > max {
				return fmt.Errorf("field `max_size` should be greater than or equal `min_size`")
			}
			return nil
		},
	}
	return resource
}

func ToAutoScalingConfigStruct(d *schema.ResourceData) *cluster.WarehouseAutoScalingConfig {

	policyItems := make([]*cluster.WearhouseScalingPolicyItem, 0)
	policyItemsArr := d.Get("policy_item").(*schema.Set).List()
	for _, v := range policyItemsArr {
		policyItemMap := v.(map[string]interface{})
		policyTypeStr := policyItemMap["type"].(string)
		stepSize := int32(policyItemMap["step_size"].(int))

		conditons := make([]*cluster.WearhouseScalingCondition, 0)
		policyConditionMap := policyItemMap["condition"].(*schema.Set).List()[0].(map[string]interface{})
		value := math.Round(policyConditionMap["value"].(float64)*100) / 100
		conditons = append(conditons, &cluster.WearhouseScalingCondition{
			Type:            int32(cluster.WearhouseScalingConditionType_AVERAGE_CPU_UTILIZATION),
			DurationSeconds: int64(policyConditionMap["duration_seconds"].(int)),
			Value:           fmt.Sprintf("%.2f", value),
		})

		policyType := cluster.WearhouseScalingType_SCALE_OUT
		if strings.EqualFold(policyTypeStr, "SCALE_IN") {
			policyType = cluster.WearhouseScalingType_SCALE_IN
		}

		policyItems = append(policyItems, &cluster.WearhouseScalingPolicyItem{
			Type:       int32(policyType),
			StepSize:   stepSize,
			Conditions: conditons,
		})
	}

	return &cluster.WarehouseAutoScalingConfig{
		MinSize:    int32(d.Get("min_size").(int)),
		MaxSize:    int32(d.Get("max_size").(int)),
		PolicyItem: policyItems,
		State:      true,
	}
}

func ValidateAutoScalingPolicyStr(jsonStr string) error {
	autoScalingConfig := &cluster.WarehouseAutoScalingConfig{}
	err := json.Unmarshal([]byte(jsonStr), autoScalingConfig)
	if err != nil {
		return fmt.Errorf("policy json is invalid, errMsg:%s", err.Error())
	}

	if autoScalingConfig.MinSize < 1 {
		return fmt.Errorf("expected %s to be at least (%d), got %d", "min_size", 1, autoScalingConfig.MinSize)
	}

	if autoScalingConfig.MaxSize < autoScalingConfig.MinSize {
		return fmt.Errorf("min_size (%d) cannot be larger than max_size (%d)", autoScalingConfig.MinSize, autoScalingConfig.MaxSize)
	}

	if len(autoScalingConfig.PolicyItem) == 0 {
		return fmt.Errorf("policyItem field can`t be empty")
	}

	for _, item := range autoScalingConfig.PolicyItem {
		if !isInArray([]int32{int32(cluster.WearhouseScalingType_SCALE_IN), int32(cluster.WearhouseScalingType_SCALE_OUT)}, item.Type) {
			return fmt.Errorf("policyItem.type is invalid, expect:[%s] get %d", "1: SCALE_IN, 2: SCALE_OUT,", item.Type)
		}
		if item.StepSize < 1 {
			return fmt.Errorf("expected %s to be at least (%d), got %d", "policyItem.step_size", 1, item.StepSize)
		}
		if len(item.Conditions) == 0 {
			return fmt.Errorf("policyItem.conditions can`t be empty")
		}
		for _, cond := range item.Conditions {
			if !isInArray([]int32{int32(cluster.WearhouseScalingConditionType_AVERAGE_CPU_UTILIZATION)}, cond.Type) {
				return fmt.Errorf("policyItem.type is invalid, expect:[%s] get %d", "1: AVERAGE_CPU_UTILIZATION,", cond.Type)
			}
			if cond.DurationSeconds < 300 {
				return fmt.Errorf("expected %s to be at least (%d), got %d", "policyItem.conditions.duration_seconds", 300, cond.DurationSeconds)
			}
			floatValue, err := strconv.ParseFloat(cond.Value, 64)
			if err != nil {
				return fmt.Errorf("policyItem.conditions.value is invalid, expect:float get %s", cond.Value)
			}
			if floatValue < 0 || floatValue > 100 {
				return fmt.Errorf("policyItem.conditions.value is invalid, expect:[0,100] get %s", cond.Value)
			}
		}
	}
	return nil
}

func isInArray(arr []int32, target int32) bool {
	for _, num := range arr {
		if num == target {
			return true
		}
	}
	return false
}
