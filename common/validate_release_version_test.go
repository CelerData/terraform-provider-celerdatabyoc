package common

import "testing"

func TestValidateReleaseVersion(t *testing.T) {
	for _, v := range []string{"stable", "ga", "preview", "GA", "Preview", " Stable "} {
		if _, errs := ValidateReleaseVersion(v, "release_version"); len(errs) != 0 {
			t.Errorf("ValidateReleaseVersion(%q) = %v, want no error", v, errs)
		}
	}
	for _, v := range []string{"", "beta", "4.0.15-ee", "latest"} {
		if _, errs := ValidateReleaseVersion(v, "release_version"); len(errs) == 0 {
			t.Errorf("ValidateReleaseVersion(%q) accepted, want error", v)
		}
	}
	if _, errs := ValidateReleaseVersion(42, "release_version"); len(errs) == 0 {
		t.Errorf("ValidateReleaseVersion(42) accepted, want a type error")
	}
}

func TestNormalizeReleaseVersion(t *testing.T) {
	for in, want := range map[string]string{"GA": "ga", " Preview ": "preview", "stable": "stable", "": ""} {
		if got := NormalizeReleaseVersion(in); got != want {
			t.Errorf("NormalizeReleaseVersion(%q) = %q, want %q", in, got, want)
		}
	}
}
