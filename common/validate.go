package common

import (
	"fmt"
	"strings"
	"terraform-provider-celerdatabyoc/celerdata-sdk/service/cluster"
	"time"

	"github.com/dlclark/regexp2"
	"github.com/hashicorp/go-cty/cty"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func ValidatePassword() schema.SchemaValidateDiagFunc {
	return func(v interface{}, p cty.Path) diag.Diagnostics {
		value := v.(string)
		var diags diag.Diagnostics
		if len(value) == 0 {
			diag := diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "Invalid value",
				Detail:   "Password is required",
			}
			diags = append(diags, diag)
			return diags
		}

		passwordRegex := "^(?=.*[a-zA-Z])(?=.*\\d)(?=.*[!@#$%^&*_]).{8,16}$"
		re := regexp2.MustCompile(passwordRegex, 0)
		if isMatch, _ := re.MatchString(value); !isMatch {
			diag := diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "Invalid value",
				Detail: "The password is required and should be between 8 and 16 characters in length." +
					"It is a mix of letters, numbers and symbols. The symbols we now support are !@#$%^&*_",
			}
			diags = append(diags, diag)
		}
		return diags
	}
}

func ValidateVolumeSize() schema.SchemaValidateDiagFunc {
	return func(v interface{}, p cty.Path) diag.Diagnostics {
		value := v.(int)
		var diags diag.Diagnostics

		m := 16 * 1000
		if value <= 0 || value > m {
			diag := diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "Invalid value",
				Detail:   fmt.Sprintf("Param `vol_size` is invalid. The range of values is: [1,%d]", m),
			}
			diags = append(diags, diag)
		}
		return diags
	}
}

func ValidateSchedulingPolicyTimeZone(i interface{}, k string) ([]string, []error) {
	v, ok := i.(string)
	if !ok {
		return nil, []error{fmt.Errorf("expected type of %s to be string", k)}
	}
	if !cluster.IsValidTimeZoneName(v) {
		return nil, []error{fmt.Errorf("for param `%s`, value:%s is not a valid IANA Time-Zone", k, v)}
	}
	return nil, nil
}

func ValidateSchedulingPolicyDateTime(i interface{}, k string) ([]string, []error) {
	v, ok := i.(string)
	if !ok {
		return nil, []error{fmt.Errorf("expected type of %q to be string", k)}
	}

	if strings.TrimSpace(v) == "" {
		return nil, []error{fmt.Errorf("expected %q to not be an empty string or whitespace", k)}
	}
	_, err := time.Parse("2006-01-02 15:04:05", fmt.Sprintf("2025-08-12 10:%s", v))
	if err != nil {
		return nil, []error{fmt.Errorf("invalid time format `%s`. Please enter time in \"HH:mm\" format", v)}
	}
	return nil, nil
}
