package cloudflare

import (
	"context"
	"errors"
	"time"
)

// Limit is the per-node row limit; a node returning Limit rows may be missing rows.
const Limit = 10000

const maxZonesPerQuery = 10

type ZoneRows[T any] struct {
	ZoneTag string `json:"zoneTag"`
	Rows    []T    `json:"rows"`
}

type HTTPRequestsGroup struct {
	Count uint64 `json:"count"`
	Sum   struct {
		EdgeResponseBytes uint64 `json:"edgeResponseBytes"`
	} `json:"sum"`
	Dimensions struct {
		Host        string `json:"clientRequestHTTPHost"`
		Status      int    `json:"edgeResponseStatus"`
		CacheStatus string `json:"cacheStatus"`
	} `json:"dimensions"`
}

type FirewallEvent struct {
	Action         string `json:"action"`
	Source         string `json:"source"`
	SampleInterval uint64 `json:"sampleInterval"`
}

type WorkersGroup struct {
	Sum struct {
		Requests    uint64 `json:"requests"`
		Errors      uint64 `json:"errors"`
		Subrequests uint64 `json:"subrequests"`
		CPUTimeUs   uint64 `json:"cpuTimeUs"`
	} `json:"sum"`
	Dimensions struct {
		Script string `json:"scriptName"`
		Status string `json:"status"`
	} `json:"dimensions"`
}

type R2OperationsGroup struct {
	Sum struct {
		Requests uint64 `json:"requests"`
	} `json:"sum"`
	Dimensions struct {
		Bucket     string `json:"bucketName"`
		Action     string `json:"actionType"`
		StatusCode int    `json:"responseStatusCode"`
	} `json:"dimensions"`
}

type R2StorageGroup struct {
	Max struct {
		PayloadSize uint64 `json:"payloadSize"`
		ObjectCount uint64 `json:"objectCount"`
	} `json:"max"`
	Dimensions struct {
		Bucket       string    `json:"bucketName"`
		StorageClass string    `json:"storageClass"`
		Datetime     time.Time `json:"datetime"`
	} `json:"dimensions"`
}

const httpRequestsQuery = `query($zones: [String!], $from: Time!, $to: Time!, $limit: Int!) {
  viewer { zones(filter: {zoneTag_in: $zones}) { zoneTag
    rows: httpRequestsAdaptiveGroups(limit: $limit, filter: {datetime_geq: $from, datetime_lt: $to, requestSource: "eyeball"}) {
      count sum { edgeResponseBytes } dimensions { clientRequestHTTPHost edgeResponseStatus cacheStatus }
    }
  } }
}`

const firewallEventsQuery = `query($zones: [String!], $from: Time!, $to: Time!, $limit: Int!) {
  viewer { zones(filter: {zoneTag_in: $zones}) { zoneTag
    rows: firewallEventsAdaptive(limit: $limit, filter: {datetime_geq: $from, datetime_lt: $to}) { action source sampleInterval }
  } }
}`

const workersQuery = `query($account: String!, $from: Time!, $to: Time!, $limit: Int!) {
  viewer { accounts(filter: {accountTag: $account}) {
    rows: workersInvocationsAdaptive(limit: $limit, filter: {datetime_geq: $from, datetime_lt: $to}) {
      sum { requests errors subrequests cpuTimeUs } dimensions { scriptName status }
    }
  } }
}`

const r2OperationsQuery = `query($account: String!, $from: Time!, $to: Time!, $limit: Int!) {
  viewer { accounts(filter: {accountTag: $account}) {
    rows: r2OperationsAdaptiveGroups(limit: $limit, filter: {datetime_geq: $from, datetime_lt: $to}) {
      sum { requests } dimensions { bucketName actionType responseStatusCode }
    }
  } }
}`

const r2StorageQuery = `query($account: String!, $from: Time!, $limit: Int!) {
  viewer { accounts(filter: {accountTag: $account}) {
    rows: r2StorageAdaptiveGroups(limit: $limit, filter: {datetime_geq: $from}, orderBy: [datetime_DESC]) {
      max { payloadSize objectCount } dimensions { bucketName storageClass datetime }
    }
  } }
}`

func (c *Client) HTTPRequests(ctx context.Context, zones []string, from, to time.Time) ([]ZoneRows[HTTPRequestsGroup], bool, error) {
	return queryZones[HTTPRequestsGroup](ctx, c, httpRequestsQuery, zones, from, to)
}

func (c *Client) FirewallEvents(ctx context.Context, zones []string, from, to time.Time) ([]ZoneRows[FirewallEvent], bool, error) {
	return queryZones[FirewallEvent](ctx, c, firewallEventsQuery, zones, from, to)
}

func (c *Client) WorkersInvocations(ctx context.Context, account string, from, to time.Time) ([]WorkersGroup, bool, error) {
	return queryAccount[WorkersGroup](ctx, c, workersQuery, map[string]any{"account": account, "from": from.UTC(), "to": to.UTC()})
}

func (c *Client) R2Operations(ctx context.Context, account string, from, to time.Time) ([]R2OperationsGroup, bool, error) {
	return queryAccount[R2OperationsGroup](ctx, c, r2OperationsQuery, map[string]any{"account": account, "from": from.UTC(), "to": to.UTC()})
}

func (c *Client) R2Storage(ctx context.Context, account string, from time.Time) ([]R2StorageGroup, bool, error) {
	return queryAccount[R2StorageGroup](ctx, c, r2StorageQuery, map[string]any{"account": account, "from": from.UTC()})
}

func queryZones[T any](ctx context.Context, c *Client, query string, zones []string, from, to time.Time) ([]ZoneRows[T], bool, error) {
	var all []ZoneRows[T]
	truncated := false
	for start := 0; start < len(zones); start += maxZonesPerQuery {
		chunk := zones[start:min(start+maxZonesPerQuery, len(zones))]
		var data struct {
			Viewer struct {
				Zones []ZoneRows[T] `json:"zones"`
			} `json:"viewer"`
		}
		vars := map[string]any{"zones": chunk, "from": from.UTC(), "to": to.UTC(), "limit": Limit}
		if err := c.GraphQL(ctx, query, vars, &data); err != nil {
			return nil, false, err
		}
		for _, z := range data.Viewer.Zones {
			truncated = truncated || len(z.Rows) >= Limit
		}
		all = append(all, data.Viewer.Zones...)
	}
	return all, truncated, nil
}

func queryAccount[T any](ctx context.Context, c *Client, query string, vars map[string]any) ([]T, bool, error) {
	vars["limit"] = Limit
	var data struct {
		Viewer struct {
			Accounts []struct {
				Rows []T `json:"rows"`
			} `json:"accounts"`
		} `json:"viewer"`
	}
	if err := c.GraphQL(ctx, query, vars, &data); err != nil {
		return nil, false, err
	}
	if len(data.Viewer.Accounts) != 1 {
		return nil, false, errors.New("graphql: account not visible to token")
	}
	rows := data.Viewer.Accounts[0].Rows
	return rows, len(rows) >= Limit, nil
}
