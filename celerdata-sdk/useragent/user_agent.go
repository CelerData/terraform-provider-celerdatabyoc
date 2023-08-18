package useragent

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"terraform-provider-celerdatabyoc/celerdata-sdk/version"

	"golang.org/x/mod/semver"
)

type ContextKey int

const (
	userAgentCtxKey ContextKey = 2
)

type info struct {
	Key   string
	Value string
}

func (u info) String() string {
	return fmt.Sprintf("%s/%s", u.Key, u.Value)
}

type data []info

func (d data) With(key, value string) data {
	var c data
	var found bool
	for _, i := range d {
		if i.Key == key {
			i.Value = value
			found = true
		}
		c = append(c, i)
	}
	if !found {
		c = append(c, info{key, value})
	}
	return c
}

func (d data) String() string {
	pairs := []string{}
	for _, v := range d {
		pairs = append(pairs, v.String())
	}
	return strings.Join(pairs, " ")
}

func InContext(ctx context.Context, key, value string) context.Context {
	uac, _ := ctx.Value(userAgentCtxKey).(data)
	uac = uac.With(key, value)
	return context.WithValue(ctx, userAgentCtxKey, uac)
}

func FromContext(ctx context.Context) string {
	base := data{
		{"celerdata-sdk-go", version.Version},
		{"go", goVersion()},
		{"os", runtime.GOOS},
	}
	uac, _ := ctx.Value(userAgentCtxKey).(data)
	return append(base, uac...).String()
}

func goVersion() string {
	gv := runtime.Version()
	ssv := strings.ReplaceAll(gv, "go", "v")
	sv := semver.Canonical(ssv)
	return strings.TrimPrefix(sv, "v")
}
