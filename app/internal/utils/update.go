package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"golang.org/x/mod/semver"

	"github.com/apernet/hysteria/core/v2/client"
)

const (
	updateCheckEndpoint = "https://api.github.com/repos/ro-gri/laz-hysteria/releases/latest"
	updateCheckTimeout  = 10 * time.Second
)

type UpdateChecker struct {
	CurrentVersion string
	Platform       string
	Architecture   string
	Channel        string
	Side           string
	Client         *http.Client
}

func NewServerUpdateChecker(currentVersion, platform, architecture, channel string) *UpdateChecker {
	return &UpdateChecker{
		CurrentVersion: currentVersion,
		Platform:       platform,
		Architecture:   architecture,
		Channel:        channel,
		Side:           "server",
		Client: &http.Client{
			Timeout: updateCheckTimeout,
		},
	}
}

// NewClientUpdateChecker ensures that update checks are routed through a HyClient,
// not being sent directly. This safeguard is CRITICAL, especially in scenarios where
// users use Hysteria to bypass censorship. Making direct HTTPS requests to the API
// endpoint could be easily spotted by censors (through SNI, for example), and could
// serve as a signal to identify and penalize Hysteria users.
func NewClientUpdateChecker(currentVersion, platform, architecture, channel string, hyClient client.Client) *UpdateChecker {
	return &UpdateChecker{
		CurrentVersion: currentVersion,
		Platform:       platform,
		Architecture:   architecture,
		Channel:        channel,
		Side:           "client",
		Client: &http.Client{
			Timeout: updateCheckTimeout,
			Transport: &http.Transport{
				DialContext: func(_ context.Context, network, addr string) (net.Conn, error) {
					// Unfortunately HyClient doesn't support context for now
					return hyClient.TCP(addr)
				},
			},
		},
	}
}

type UpdateResponse struct {
	HasUpdate     bool   `json:"update"`
	LatestVersion string `json:"lver"`
	URL           string `json:"url"`
	Urgent        bool   `json:"urgent"`
}

func (uc *UpdateChecker) Check() (*UpdateResponse, error) {
	resp, err := uc.Client.Get(updateCheckEndpoint)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	var release struct {
		TagName    string `json:"tag_name"`
		URL        string `json:"html_url"`
		Draft      bool   `json:"draft"`
		Prerelease bool   `json:"prerelease"`
		Assets     []struct {
			Name string `json:"name"`
		} `json:"assets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return nil, err
	}
	latest := strings.TrimPrefix(release.TagName, "app/")
	if !semver.IsValid(latest) {
		return nil, fmt.Errorf("invalid release version: %q", release.TagName)
	}
	// Only announce releases that contain a binary for this installation.
	assetName := fmt.Sprintf("hysteria-%s-%s", uc.Platform, uc.Architecture)
	available := false
	for _, asset := range release.Assets {
		if asset.Name == assetName {
			available = true
			break
		}
	}
	current := strings.TrimPrefix(uc.CurrentVersion, "app/")
	return &UpdateResponse{
		HasUpdate: available && !release.Draft && !release.Prerelease &&
			semver.IsValid(current) && semver.Compare(latest, current) > 0,
		LatestVersion: latest,
		URL:           release.URL,
	}, nil
}
