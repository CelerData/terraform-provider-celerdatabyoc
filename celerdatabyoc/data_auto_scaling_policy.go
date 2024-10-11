package celerdatabyoc

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func dataSourceAutoScalingPolicy() *schema.Resource {
	resource := &schema.Resource{
		ReadContext: func(ctx context.Context, rd *schema.ResourceData, i interface{}) diag.Diagnostics {
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
	}
	resource.Schema["max_size"].DiffSuppressFunc = func(k, old, new string, d *schema.ResourceData) bool {
		min, _ := d.Get("min_size").(int)
		max, _ := d.Get("max_size").(int)
		return max > min
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
		conditons = append(conditons, &cluster.WearhouseScalingCondition{
			Type:            int32(cluster.WearhouseScalingConditionType_AVERAGE_CPU_UTILIZATION),
			DurationSeconds: int64(policyConditionMap["duration_seconds"].(int)),
			Value:           fmt.Sprintf("%f", policyConditionMap["value"].(float64)),
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
	var autoScalingPolicy cluster.WarehouseAutoScalingConfig
	err := json.Unmarshal([]byte(jsonStr), autoScalingPolicy)
	if err != nil {
		return fmt.Errorf("policy json is invalid, errMsg:%s", err.Error())
	}

	if autoScalingPolicy.MinSize < 1 {
		return fmt.Errorf("expected %s to be at least (%d), got %d", "min_size", 1, autoScalingPolicy.MinSize)
	}

	if autoScalingPolicy.MaxSize < autoScalingPolicy.MinSize {
		return fmt.Errorf("min_size (%d) cannot be larger than max_size (%d)", autoScalingPolicy.MinSize, autoScalingPolicy.MaxSize)
	}

	if len(autoScalingPolicy.PolicyItem) == 0 {
		return fmt.Errorf("policyItem field can`t be empty")
	}

	for _, item := range autoScalingPolicy.PolicyItem {
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
