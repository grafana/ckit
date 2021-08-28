package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Client invokes requests against an API.
type Client struct {
	baseURL string
	c       *http.Client
}

var _ KV = (*Client)(nil)

// NewClient returns a new Client.
func NewClient(baseURL string, c *http.Client) *Client {
	return &Client{baseURL: baseURL, c: c}
}

// Set creates or updates a key.
func (c *Client) Set(ctx context.Context, key, value string) error {
	sr := strings.NewReader(value)
	url := fmt.Sprintf("%s/api/kv/%s", c.baseURL, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, sr)
	if err != nil {
		return fmt.Errorf("failed to format http request: %w", err)
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	default:
		return fmt.Errorf("unexpected response: %s", resp.Status)
	}
}

// Delete deletes a key.
func (c *Client) Delete(ctx context.Context, key string) error {
	url := fmt.Sprintf("%s/api/kv/%s", c.baseURL, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("failed to format http request: %w", err)
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusNotFound:
		return ErrNotFound{Key: key}
	default:
		return fmt.Errorf("unexpected response: %s", resp.Status)
	}
}

// Get retrieves the value of a key.
func (c *Client) Get(ctx context.Context, key string) (val string, err error) {
	url := fmt.Sprintf("%s/api/kv/%s", c.baseURL, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to format http request: %w", err)
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return "", fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var sb strings.Builder
		_, err := io.Copy(&sb, resp.Body)
		if err != nil {
			return "", fmt.Errorf("failed to read response: %w", err)
		}
		return sb.String(), nil
	case http.StatusNotFound:
		return "", ErrNotFound{Key: key}
	default:
		return "", fmt.Errorf("unexpected response: %s", resp.Status)
	}
}
