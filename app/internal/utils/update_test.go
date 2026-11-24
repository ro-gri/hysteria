package utils

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

type updateTransport func(*http.Request) (*http.Response, error)

func (f updateTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCheckUpdate(t *testing.T) {
	for _, tc := range []struct {
		name, current, tag, asset                string
		draft, prerelease, wantUpdate, wantError bool
	}{
		{name: "fork update", current: "v2.0.0+hy.2.14.0", tag: "app/v2.0.1+hy.2.14.0", asset: "hysteria-linux-amd64", wantUpdate: true},
		{name: "upstream base update", current: "v2.0.1+hy.2.14.0", tag: "app/v2.1.0+hy.2.15.0", asset: "hysteria-linux-amd64", wantUpdate: true},
		{name: "metadata is not ordered", current: "v2.0.0+hy.2.14.0", tag: "app/v2.0.0+hy.2.15.0", asset: "hysteria-linux-amd64"},
		{name: "fork downgrade", current: "v2.1.0+hy.2.14.0", tag: "app/v2.0.0+hy.2.15.0", asset: "hysteria-linux-amd64"},
		{name: "newer", current: "v2.9.0", tag: "app/v2.10.0", asset: "hysteria-linux-amd64", wantUpdate: true},
		{name: "same", current: "v2.10.0", tag: "app/v2.10.0", asset: "hysteria-linux-amd64"},
		{name: "no downgrade", current: "v2.11.0", tag: "app/v2.10.0", asset: "hysteria-linux-amd64"},
		{name: "unknown build", current: "Unknown", tag: "app/v2.10.0", asset: "hysteria-linux-amd64"},
		{name: "missing architecture", current: "v2.9.0", tag: "app/v2.10.0", asset: "hysteria-linux-arm64"},
		{name: "draft", current: "v2.9.0", tag: "app/v2.10.0", asset: "hysteria-linux-amd64", draft: true},
		{name: "prerelease", current: "v2.9.0", tag: "app/v2.10.0-rc.1", asset: "hysteria-linux-amd64", prerelease: true},
		{name: "invalid tag", current: "v2.9.0", tag: "experimental", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewServerUpdateChecker(tc.current, "linux", "amd64", "release")
			c.Client.Transport = updateTransport(func(r *http.Request) (*http.Response, error) {
				if r.URL.String() != "https://api.github.com/repos/ro-gri/laz-hysteria/releases/latest" {
					t.Fatalf("unexpected update source: %s", r.URL)
				}
				body := fmt.Sprintf(`{"tag_name":%q,"html_url":"https://github.com/ro-gri/laz-hysteria/releases/tag/app/v2.10.0","draft":%t,"prerelease":%t,"assets":[{"name":%q}]}`, tc.tag, tc.draft, tc.prerelease, tc.asset)
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
			})
			got, err := c.Check()
			if (err != nil) != tc.wantError {
				t.Fatalf("error = %v", err)
			}
			if err != nil {
				return
			}
			if got.HasUpdate != tc.wantUpdate {
				t.Fatalf("HasUpdate = %v, want %v", got.HasUpdate, tc.wantUpdate)
			}
			if got.LatestVersion != strings.TrimPrefix(tc.tag, "app/") || !strings.HasPrefix(got.URL, "https://github.com/ro-gri/laz-hysteria/releases/") {
				t.Fatalf("unexpected result: %+v", got)
			}
		})
	}
}

func TestCheckUpdateErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"no release", 404, `{"message":"Not Found"}`},
		{"rate limited", 403, `{"message":"API rate limit exceeded"}`},
		{"invalid JSON", 200, `{`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewServerUpdateChecker("v2.9.0", "linux", "amd64", "release")
			c.Client.Transport = updateTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})
			if _, err := c.Check(); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
