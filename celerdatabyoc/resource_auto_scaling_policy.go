package celerdatabyoc

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
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
			minSize := int32(rd.Get("min_size").(int))
			maxSize := int32(rd.Get("max_size").(int))
			autoScalingUnit := rd.Get("auto_scaling_unit").(string)
			policyItemsArr := rd.Get("policy_item").(*schema.Set).List()
			autoScalingConfig, err := ToAutoScalingConfigStruct(minSize, maxSize, autoScalingUnit, policyItemsArr)
			if err != nil {
				return diag.FromErr(fmt.Errorf("generate auto scaling config failed, errMsg:%s", err.Error()))
			}
			bytes, err := json.Marshal(autoScalingConfig)
			if err != nil {
				return diag.FromErr(fmt.Errorf("generate auto scaling config failed, errMsg:%s", err.Error()))
			}
			rd.Set("policy_json", string(bytes))
			return nil
		},
		CreateContext: func(ctx context.Context, rd *schema.ResourceData, i interface{}) diag.Diagnostics {
			minSize := int32(rd.Get("min_size").(int))
			maxSize := int32(rd.Get("max_size").(int))
			autoScalingUnit := rd.Get("auto_scaling_unit").(string)
			policyItemsArr := rd.Get("policy_item").(*schema.Set).List()
			autoScalingConfig, err := ToAutoScalingConfigStruct(minSize, maxSize, autoScalingUnit, policyItemsArr)
			if err != nil {
				return diag.FromErr(fmt.Errorf("generate auto scaling config failed, errMsg:%s", err.Error()))
			}
			bytes, err := json.Marshal(autoScalingConfig)
			if err != nil {
				return diag.FromErr(fmt.Errorf("generate auto scaling config failed, errMsg:%s", err.Error()))
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
			"auto_scaling_unit": {
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringInSlice([]string{string(cluster.AutoScalingUnitDisplay_SINGLE), string(cluster.AutoScalingUnitDisplay_CN_GROUP)}, false),
			},
			"policy_item": {
				Type:     schema.TypeSet,
				Required: true,
				ForceNew: true,
				MinItems: 2,
				MaxItems: 6,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"type": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.StringInSlice(cluster.WhScaleTypeArr, false),
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
										Type:     schema.TypeString,
										Required: true,
									},
									"duration_seconds": {
										Type:     schema.TypeInt,
										Optional: true,
										Default:  0,
									},
									"value": {
										Type:         schema.TypeFloat,
										Required:     true,
										ValidateFunc: validation.FloatAtLeast(0.01),
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
			warehouseAutoScalingConfig, err := ToAutoScalingConfigStruct(int32(rd.Get("min_size").(int)), int32(rd.Get("max_size").(int)), rd.Get("auto_scaling_unit").(string), rd.Get("policy_item").(*schema.Set).List())
			if err != nil {
				return err
			}
			return ValidateAutoScalingPolicy(warehouseAutoScalingConfig)
		},
	}
	return resource
}

func ToAutoScalingConfigStruct(minSize, maxSize int32, autoScalingUnit string, policyItemsArr []interface{}) (*cluster.WarehouseAutoScalingConfig, error) {

	policyItems := make([]*cluster.WearhouseScalingPolicyItem, 0)
	for _, v := range policyItemsArr {
		policyItemMap := v.(map[string]interface{})
		policyTypeStr := policyItemMap["type"].(string)
		stepSize := int32(policyItemMap["step_size"].(int))

		conditions := make([]*cluster.WearhouseScalingCondition, 0)
		for _, conditionItem := range policyItemMap["condition"].(*schema.Set).List() {
			conditionMap := conditionItem.(map[string]interface{})
			value := math.Round(conditionMap["value"].(float64)*100) / 100
			conditionTypeStr := conditionMap["type"].(string)

			conditionType := cluster.MetricType_UNKNOWN
			switch conditionTypeStr {
			case "AVERAGE_CPU_UTILIZATION":
				conditionType = cluster.MetricType_AVERAGE_CPU_UTILIZATION
			case "QUERY_QUEUE_LENGTH":
				conditionType = cluster.MetricType_QUERY_QUEUE_LENGTH
			case "EARLIEST_QUERY_PENDING_TIME":
				conditionType = cluster.MetricType_EARLIEST_QUERY_PENDING_TIME
			case "WAREHOUSE_RESOURCE_UTILIZATION":
				conditionType = cluster.MetricType_WAREHOUSE_RESOURCE_UTILIZATION
			}

			if conditionType == cluster.MetricType_UNKNOWN {
				return nil, fmt.Errorf("for policy item:`%s`, unknown condition metric type: %s", policyTypeStr, conditionTypeStr)
			}

			conditions = append(conditions, &cluster.WearhouseScalingCondition{
				Type:            int32(conditionType),
				DurationSeconds: int64(conditionMap["duration_seconds"].(int)),
				Value:           fmt.Sprintf("%.2f", value),
			})
		}

		policyType := cluster.WhScalingType_SCALE_OUT
		if strings.EqualFold(policyTypeStr, "SCALE_IN") {
			policyType = cluster.WhScalingType_SCALE_IN
		}

		policyItems = append(policyItems, &cluster.WearhouseScalingPolicyItem{
			Type:       int32(policyType),
			StepSize:   stepSize,
			Conditions: conditions,
		})
	}

	unit, ok := cluster.AutoScalingUnitDisplayToType[cluster.AutoScalingUnitDisplay(autoScalingUnit)]
	if !ok {
		unit = cluster.AutoScalingUnit_SINGLE
	}

	return &cluster.WarehouseAutoScalingConfig{
		MinSize:         minSize,
		MaxSize:         maxSize,
		PolicyItem:      policyItems,
		AutoScalingUnit: unit,
		State:           true,
	}, nil
}

func ValidateAutoScalingPolicyStr(jsonStr string) error {

	autoScalingConfig := &cluster.WarehouseAutoScalingConfig{}
	err := json.Unmarshal([]byte(jsonStr), autoScalingConfig)
	if err != nil {
		return fmt.Errorf("policy json is invalid,jsonStr:%s errMsg:%s", jsonStr, err.Error())
	}
	return ValidateAutoScalingPolicy(autoScalingConfig)
}

func ValidateAutoScalingPolicy(autoScalingConfig *cluster.WarehouseAutoScalingConfig) error {

	bytes, _ := json.Marshal(autoScalingConfig)
	log.Printf("[DEBUG] validate auto-scaling policy, autoScalingConfig:%s", string(bytes))
	if autoScalingConfig.MinSize < 1 {
		return fmt.Errorf("expected %s to be at least (%d), got %d", "min_size", 1, autoScalingConfig.MinSize)
	}

	if autoScalingConfig.MaxSize < autoScalingConfig.MinSize {
		return fmt.Errorf("min_size (%d) cannot be larger than max_size (%d)", autoScalingConfig.MinSize, autoScalingConfig.MaxSize)
	}

	if len(autoScalingConfig.PolicyItem) == 0 {
		return fmt.Errorf("policyItem field can`t be empty")
	}

	// Metric type check
	firMt := int32(0)
	firMtStr := ""
	firMtGroup := ""
	for _, item := range autoScalingConfig.PolicyItem {
		if !cluster.Contains([]int32{int32(cluster.WhScalingType_SCALE_IN), int32(cluster.WhScalingType_SCALE_OUT)}, item.Type) {
			return fmt.Errorf("policyItem.type:%d is invalid, optional values:%+v, meaning of each value:%+v", item.Type, cluster.GetKeys(cluster.ScaleTypeToStr), cluster.ScaleTypeToStr)
		}
		if item.StepSize < 1 {
			return fmt.Errorf("expected %s to be at least (%d), got %d", "policyItem.step_size", 1, item.StepSize)
		}
		if len(item.Conditions) == 0 {
			return fmt.Errorf("policyItem.conditions can`t be empty")
		}

		ptStr := cluster.ScaleTypeToStr[item.Type]
		for _, cond := range item.Conditions {
			mt := cond.Type
			val := cond.Value
			ds := cond.DurationSeconds

			if !cluster.Contains(cluster.MetricArr, mt) {
				return fmt.Errorf("policy['%+v'] metric type:%d is invalid, optional values:%+v, meaning of each value:%+v", item, mt, cluster.GetKeys(cluster.MetricToStr), cluster.MetricToStr)
			}

			mtStr := cluster.MetricToStr[mt]
			mtGroup := cluster.MetricStrGroup[mtStr]
			if firMtGroup == "" {
				firMt = mt
				firMtStr = mtStr
				firMtGroup = mtGroup
			}
			if mtGroup != firMtGroup {
				return fmt.Errorf("metric[%d('%s')] of type %s cannot be declared together with metric[%d('%s')] of type %s", mt, mtStr, mtGroup, firMt, firMtStr, firMtGroup)
			}
			if mtGroup == cluster.AutoScalingMetricType_CPU {
				if v, ok := cluster.CpuBasedMetric[mtStr]; ok {
					if !cluster.Contains(v, mtStr) {
						return fmt.Errorf("metric:%d('%s') for %s - %s is not supported. only the following metrics are supported: [%s]", mt, mtStr, mtGroup, ptStr, strings.Join(v, ","))
					}
				}
			} else if mtGroup == cluster.AutoScalingMetricType_QUERY_QUEUE {
				if v, ok := cluster.QueryQueueBasedMetric[ptStr]; ok {
					if !cluster.Contains(v, mtStr) {
						return fmt.Errorf("metric:%d('%s') for %s - %s is not supported. only the following metrics are supported: [%s]", mt, mtStr, mtGroup, ptStr, strings.Join(v, ","))
					}
				}
			}

			if cluster.Contains([]int32{int32(cluster.MetricType_QUERY_QUEUE_LENGTH), int32(cluster.MetricType_EARLIEST_QUERY_PENDING_TIME)}, mt) {
				if ds != 0 {
					return fmt.Errorf("for condition metric type: %d(`%s`) field `duration_seconds` value should be 0", mt, cluster.MetricToStr[mt])
				}
			} else if ds < 180 {
				return fmt.Errorf("expected %s to be at least (%d), got %d", "policyItem.conditions.duration_seconds", 180, ds)
			}
			floatValue, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return fmt.Errorf("policyItem.conditions.value is invalid, expect:float get %s", val)
			}
			if floatValue < 0 {
				return fmt.Errorf("policyItem.conditions.value is invalid and should be greater than 0, current:%s", val)
			}
			if cluster.Contains([]int32{int32(cluster.MetricType_AVERAGE_CPU_UTILIZATION), int32(cluster.MetricType_WAREHOUSE_RESOURCE_UTILIZATION)}, mt) {
				if floatValue > 100 {
					return fmt.Errorf("policyItem.conditions.value is invalid, expect:[0,100] get %s", val)
				}
			}
		}
	}
	return nil
}
