package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"

	"terraform-provider-celerdatabyoc/celerdata-sdk/config"
	"terraform-provider-celerdatabyoc/celerdata-sdk/retries"
	"terraform-provider-celerdatabyoc/celerdata-sdk/useragent"

	"github.com/google/go-querystring/query"
	"github.com/mitchellh/mapstructure"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func New(cfg *config.Config) (*CelerdataClient, error) {
	ctx := context.Background()
	authProvider, err := cfg.ConfigAuthProvider(ctx)
	if err != nil {
		return nil, err
	}

	retryTimeout := time.Duration(orDefault(cfg.RetryTimeoutSeconds, 300)) * time.Second
	httpTimeout := time.Duration(orDefault(cfg.HTTPTimeoutSeconds, 60)) * time.Second
	rateLimiter := rate.NewLimiter(rate.Limit(orDefault(cfg.RateLimitPerSecond, 15)), 1)
	cfg.AuthType = authProvider.Name()
	return &CelerdataClient{
		Config:       cfg,
		AuthProvider: authProvider,
		retryTimeout: retryTimeout,
		rateLimiter:  rateLimiter,
		// httpClient:   &myHttpClient{timeout: httpTimeout},
		httpClient: &http.Client{
			Timeout: httpTimeout,
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          100,
				MaxIdleConnsPerHost:   runtime.GOMAXPROCS(0) + 1,
				IdleConnTimeout:       180 * time.Second,
				TLSHandshakeTimeout:   30 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		},
	}, nil
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
	CloseIdleConnections()
}

// type myHttpClient struct {
// 	timeout time.Duration
// }

// type ClusterResp struct {
// 	ClusterName string
// 	ID          string
// }

// func (c *myHttpClient) Do(req *http.Request) (*http.Response, error) {
// 	fmt.Printf("req: %s\n", req.URL)
// 	respStr, _ := json.Marshal(&ClusterResp{ClusterName: "test_cluster", ID: "331c5c69-03a4-4323-b8ab-127fb247b60c"})
// 	return &http.Response{
// 		Status:     "200 OK",
// 		StatusCode: 200,
// 		Proto:      "HTTP/1.0",
// 		Body:       io.NopCloser(strings.NewReader(string(respStr))),
// 	}, nil
// }

// func (c *myHttpClient) CloseIdleConnections() {}

type CelerdataClient struct {
	Config       *config.Config
	AuthProvider config.AuthProvider
	rateLimiter  *rate.Limiter
	retryTimeout time.Duration
	httpClient   httpClient
}

type Resp struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

// Get on path
func (c *CelerdataClient) Get(ctx context.Context, path string, request any, response any) error {
	return c.Do(ctx, http.MethodGet, path, request, response)
}

// Post on path
func (c *CelerdataClient) Post(ctx context.Context, path string, request any, response any) error {
	return c.Do(ctx, http.MethodPost, path, request, response)
}

// Delete on path
func (c *CelerdataClient) Delete(ctx context.Context, path string, request any, response any) error {
	return c.Do(ctx, http.MethodDelete, path, request, response)
}

// Patch on path
func (c *CelerdataClient) Patch(ctx context.Context, path string, request any, response any) error {
	return c.Do(ctx, http.MethodPatch, path, request, response)
}

// Put on path
func (c *CelerdataClient) Put(ctx context.Context, path string, request any, response any) error {
	return c.Do(ctx, http.MethodPut, path, request, response)
}

func (c *CelerdataClient) Do(ctx context.Context, method, path string,
	request, response any, visitors ...func(*http.Request) error) error {
	body, err := c.perform(ctx, method, path, request, visitors...)
	if err != nil {
		return err
	}

	resp := &Resp{}
	err = c.unmarshal(body, resp)
	if err != nil {
		return errors.New(string(body))
	}

	log.Printf("[DEBUG] request[%s] finished, orgin resp:%+v", path, resp)

	if resp.Code != 0 && resp.Code != 20000 {
		if resp.Code == 40004 {
			return status.Error(codes.NotFound, resp.Message)
		}

		return errors.New(resp.Message)
	}

	if response == nil || resp.Data == nil {
		return nil
	}

	return mapstructure.Decode(resp.Data, response)
}

func (c *CelerdataClient) unmarshal(body []byte, response any) error {
	if response == nil {
		return nil
	}
	if len(body) == 0 {
		return nil
	}
	// If the destination is bytes.Buffer, write the body over there
	if raw, ok := response.(*bytes.Buffer); ok {
		_, err := raw.Write(body)
		return err
	}
	// If the destination is a byte slice, pass the body verbatim.
	if raw, ok := response.(*[]byte); ok {
		*raw = body
		return nil
	}
	return json.Unmarshal(body, &response)
}

func (c *CelerdataClient) perform(
	ctx context.Context,
	method,
	requestURL string,
	data interface{},
	visitors ...func(*http.Request) error,
) ([]byte, error) {
	requestBody, err := makeRequestBody(method, &requestURL, data)
	if err != nil {
		return nil, fmt.Errorf("request marshal: %w", err)
	}
	visitors = append([]func(*http.Request) error{
		c.AuthProvider.Auth,
		c.addHostToRequestUrl,
		c.addAuthHeaderToUserAgent,
	}, visitors...)
	resp, err := retries.Poll(ctx, c.retryTimeout,
		c.attempt(ctx, method, requestURL, requestBody, visitors...))
	if err != nil {
		// Don't re-wrap, as upper layers may depend on handling apierr.APIError.
		return nil, err
	}
	return resp.Bytes(), nil
}

func (c *CelerdataClient) attempt(
	ctx context.Context,
	method string,
	requestURL string,
	requestBody []byte,
	visitors ...func(*http.Request) error,
) func() (*bytes.Buffer, *retries.Err) {
	return func() (*bytes.Buffer, *retries.Err) {
		err := c.rateLimiter.Wait(ctx)
		if err != nil {
			return nil, retries.Halt(err)
		}
		request, err := http.NewRequestWithContext(ctx, method, requestURL,
			bytes.NewBuffer(requestBody))
		if err != nil {
			return nil, retries.Halt(err)
		}
		for _, requestVisitor := range visitors {
			err = requestVisitor(request)
			if err != nil {
				return nil, retries.Halt(err)
			}
		}
		// request.Context() holds context potentially enhanced by visitors
		request.Header.Set("User-Agent", useragent.FromContext(request.Context()))

		// attempt the actual request
		response, err := c.httpClient.Do(request)

		// Read responseBody immediately because we need it regardless of success or failure.
		//
		// Fully read and close HTTP response responseBody to release HTTP connections.
		//
		// HTTP client's Transport may not reuse HTTP/1.x "keep-alive" TCP connections
		// if the Body is not read to completion and closed.
		//
		// See: https://groups.google.com/g/golang-nuts/c/IoSvPz-rpfc
		var responseBody bytes.Buffer
		var responseBodyErr error
		if response != nil {
			_, responseBodyErr = responseBody.ReadFrom(response.Body)
			response.Body.Close()
		}

		if err != nil {
			err = fmt.Errorf("failed request: %w", err)
		}
		if err == nil && response == nil {
			err = fmt.Errorf("no response: %s %s", method, requestURL)
		}
		if err == nil && responseBodyErr != nil {
			err = fmt.Errorf("response body: %w", responseBodyErr)
		}

		if err == nil {
			return &responseBody, nil
		}

		// proactively release the connections in HTTP connection pool
		c.httpClient.CloseIdleConnections()
		if canRetry(response) {
			return nil, retries.Continue(err)
		}

		return nil, retries.Halt(err)
	}
}

func canRetry(r *http.Response) bool {
	if r == nil {
		return false
	}

	switch r.StatusCode {
	case 429:
	case 503:
	default:
		return false
	}

	return true
}

func orDefault(configured, _default int) int {
	if configured == 0 {
		return _default
	}
	return configured
}

func (c *CelerdataClient) addHostToRequestUrl(r *http.Request) error {
	if r.URL == nil {
		return fmt.Errorf("no URL found in request")
	}
	r.Header.Set("Content-Type", "application/json")
	url, err := url.Parse(c.Config.Host)
	if err != nil {
		return err
	}
	r.URL.Host = url.Host
	r.URL.Scheme = url.Scheme
	return nil
}

func (c *CelerdataClient) addAuthHeaderToUserAgent(r *http.Request) error {
	ctx := useragent.InContext(r.Context(), "auth", c.Config.AuthType)
	*r = *r.WithContext(ctx)
	return nil
}

func makeRequestBody(method string, requestURL *string, data interface{}) ([]byte, error) {
	var requestBody []byte
	if data == nil && (method == "DELETE" || method == "GET") {
		return requestBody, nil
	}

	if method == "GET" || method == "DELETE" {
		qs, err := makeQueryString(data)
		if err != nil {
			return nil, err
		}
		*requestURL += qs
		return requestBody, nil
	}
	if bytes, ok := data.([]byte); ok {
		return bytes, nil
	}
	if reader, ok := data.(io.Reader); ok {
		raw, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read from reader: %w", err)
		}
		return raw, nil
	}
	if str, ok := data.(string); ok {
		return []byte(str), nil
	}
	bodyBytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("request marshal failure: %w", err)
	}
	return bodyBytes, nil
}

func makeQueryString(data interface{}) (string, error) {
	inputVal := reflect.ValueOf(data)
	inputType := reflect.TypeOf(data)
	if inputType.Kind() == reflect.Map {
		s := []string{}
		keys := inputVal.MapKeys()
		// sort map keys by their string repr, so that tests can be deterministic
		sort.Slice(keys, func(i, j int) bool {
			return keys[i].String() < keys[j].String()
		})
		for _, k := range keys {
			v := inputVal.MapIndex(k)
			if v.IsZero() {
				continue
			}
			s = append(s, fmt.Sprintf("%s=%s",
				strings.Replace(url.QueryEscape(fmt.Sprintf("%v", k.Interface())), "+", "%20", -1),
				strings.Replace(url.QueryEscape(fmt.Sprintf("%v", v.Interface())), "+", "%20", -1)))
		}
		return "?" + strings.Join(s, "&"), nil
	}
	if inputType.Kind() == reflect.Struct {
		params, err := query.Values(data)
		if err != nil {
			return "", fmt.Errorf("cannot create query string: %w", err)
		}
		return "?" + params.Encode(), nil
	}
	return "", fmt.Errorf("unsupported query string data: %#v", data)
}
