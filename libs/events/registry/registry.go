// Package registry is a small Confluent Schema Registry REST client over
// net/http: register a schema, set and check BACKWARD compatibility, fetch by
// id. It needs no Kafka client.
package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const contentType = "application/vnd.schemaregistry.v1+json"

// Compatibility levels the registry accepts (only BACKWARD is used here).
const Backward = "BACKWARD"

// Client talks to one registry. The zero HTTPClient means http.DefaultClient.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

// New returns a Client for baseURL, e.g. "http://localhost:8081".
func New(baseURL string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/")}
}

// APIError is an error response from the registry.
type APIError struct {
	Status    int
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("schema registry: HTTP %d, error_code %d: %s", e.Status, e.ErrorCode, e.Message)
}

// Schema is a schema fetched by id.
type Schema struct {
	Type   string `json:"schemaType"` // empty means AVRO per the registry API
	Schema string `json:"schema"`
}

// Register registers a Protobuf schema under subject and returns its global
// id. Registering identical text again returns the existing id, which is what
// makes bootstrapping idempotent. An incompatible schema is rejected with an
// *APIError (HTTP 409).
func (c *Client) Register(ctx context.Context, subject, schema string) (int, error) {
	var out struct {
		ID int `json:"id"`
	}
	body := map[string]string{"schemaType": "PROTOBUF", "schema": schema}
	if err := c.do(ctx, http.MethodPost, "/subjects/"+url.PathEscape(subject)+"/versions", body, &out); err != nil {
		return 0, err
	}
	return out.ID, nil
}

// SetCompatibility sets the compatibility level for subject.
func (c *Client) SetCompatibility(ctx context.Context, subject, level string) error {
	return c.do(ctx, http.MethodPut, "/config/"+url.PathEscape(subject), map[string]string{"compatibility": level}, nil)
}

// Compatibility returns the level configured for subject itself (not the
// global default). ok is false if the subject has no config of its own.
func (c *Client) Compatibility(ctx context.Context, subject string) (level string, ok bool, err error) {
	var out struct {
		Level string `json:"compatibilityLevel"`
	}
	err = c.do(ctx, http.MethodGet, "/config/"+url.PathEscape(subject)+"?defaultToGlobal=false", nil, &out)
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return out.Level, true, nil
}

// IsCompatible asks whether schema is compatible with the latest registered
// version of subject under the subject's configured level. A subject with no
// versions yet is reported as compatible.
func (c *Client) IsCompatible(ctx context.Context, subject, schema string) (bool, error) {
	var out struct {
		IsCompatible bool `json:"is_compatible"`
	}
	body := map[string]string{"schemaType": "PROTOBUF", "schema": schema}
	err := c.do(ctx, http.MethodPost, "/compatibility/subjects/"+url.PathEscape(subject)+"/versions/latest", body, &out)
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound && apiErr.ErrorCode == 40401 {
		return true, nil // subject not found: nothing to be incompatible with
	}
	if err != nil {
		return false, err
	}
	return out.IsCompatible, nil
}

// SchemaByID fetches a schema by its global id.
func (c *Client) SchemaByID(ctx context.Context, id int) (*Schema, error) {
	var out Schema
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/schemas/ids/%d", id), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// LatestVersion returns the newest version number registered under subject.
func (c *Client) LatestVersion(ctx context.Context, subject string) (int, error) {
	var out struct {
		Version int `json:"version"`
	}
	if err := c.do(ctx, http.MethodGet, "/subjects/"+url.PathEscape(subject)+"/versions/latest", nil, &out); err != nil {
		return 0, err
	}
	return out.Version, nil
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var rdr io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, rdr)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", contentType)
	if in != nil {
		req.Header.Set("Content-Type", contentType)
	}
	hc := c.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		apiErr := &APIError{Status: resp.StatusCode}
		if json.Unmarshal(raw, apiErr) != nil || apiErr.Message == "" {
			apiErr.Message = strings.TrimSpace(string(raw))
		}
		return apiErr
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}
