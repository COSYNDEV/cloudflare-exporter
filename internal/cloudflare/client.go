package cloudflare

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const DefaultBaseURL = "https://api.cloudflare.com/client/v4"

type Client struct {
	baseURL   string
	token     string
	userAgent string
	http      *http.Client
}

func New(baseURL, token, userAgent string) *Client {
	return &Client{
		baseURL:   strings.TrimSuffix(baseURL, "/"),
		token:     token,
		userAgent: userAgent,
		http:      &http.Client{Timeout: 30 * time.Second},
	}
}

type Zone struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func (c *Client) Zones(ctx context.Context, account string) ([]Zone, error) {
	zones, err := list[Zone](ctx, c, "/zones", url.Values{"account.id": {account}, "per_page": {"50"}})
	if err != nil {
		return nil, fmt.Errorf("list zones: %w", err)
	}
	return zones, nil
}

// ProxiedHostnames returns the lowercased names of the zone's proxied DNS records,
// including wildcard names such as "*.example.com".
func (c *Client) ProxiedHostnames(ctx context.Context, zoneID string) ([]string, error) {
	records, err := list[struct {
		Name    string `json:"name"`
		Proxied bool   `json:"proxied"`
	}](ctx, c, "/zones/"+url.PathEscape(zoneID)+"/dns_records", url.Values{"proxied": {"true"}, "per_page": {"100"}})
	if err != nil {
		return nil, err
	}
	var names []string
	for _, r := range records {
		if r.Proxied {
			names = append(names, strings.ToLower(r.Name))
		}
	}
	return names, nil
}

func list[T any](ctx context.Context, c *Client, path string, q url.Values) ([]T, error) {
	var all []T
	for page := 1; ; page++ {
		q.Set("page", strconv.Itoa(page))
		var resp struct {
			Success    bool        `json:"success"`
			Errors     []restError `json:"errors"`
			Result     []T         `json:"result"`
			ResultInfo struct {
				TotalPages int `json:"total_pages"`
			} `json:"result_info"`
		}
		if err := c.do(ctx, http.MethodGet, path+"?"+q.Encode(), nil, &resp); err != nil {
			return nil, err
		}
		if !resp.Success {
			return nil, fmt.Errorf("GET %s: %v", path, resp.Errors)
		}
		all = append(all, resp.Result...)
		if page >= resp.ResultInfo.TotalPages {
			return all, nil
		}
	}
}

type restError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e restError) String() string { return fmt.Sprintf("%d: %s", e.Code, e.Message) }

type graphqlError struct {
	Message string `json:"message"`
}

func (c *Client) GraphQL(ctx context.Context, query string, vars map[string]any, out any) error {
	body, err := json.Marshal(map[string]any{"query": query, "variables": vars})
	if err != nil {
		return err
	}
	var resp struct {
		Data   json.RawMessage `json:"data"`
		Errors []graphqlError  `json:"errors"`
	}
	if err := c.do(ctx, http.MethodPost, "/graphql", body, &resp); err != nil {
		return fmt.Errorf("graphql: %w", err)
	}
	if len(resp.Errors) > 0 {
		msgs := make([]string, len(resp.Errors))
		for i, e := range resp.Errors {
			msgs[i] = e.Message
		}
		return fmt.Errorf("graphql: %s", strings.Join(msgs, "; "))
	}
	if len(resp.Data) == 0 || string(resp.Data) == "null" {
		return errors.New("graphql: empty data")
	}
	return json.Unmarshal(resp.Data, out)
}

func (c *Client) do(ctx context.Context, method, path string, body []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("User-Agent", c.userAgent)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(res.Body, 64<<20))
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%s %s: HTTP %d: %s", method, req.URL.Path, res.StatusCode, snippet(data))
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode %s: %w", req.URL.Path, err)
	}
	return nil
}

func snippet(b []byte) string {
	const limit = 512
	if len(b) > limit {
		return string(b[:limit]) + "..."
	}
	return string(b)
}
