package common

import (
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
