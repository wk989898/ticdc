// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package rest

import (
	"net/url"
	"path"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/security"
)

// Config holds the common attributes that can be passed to a cdc REST client
type Config struct {
	// Host must be a host string, a host:port pair, or a URL to the base of the cdc server.
	Host string
	// APIPath is a sub-path that points to an API root.
	APIPath string
	// Credential holds the security Credential used for generating tls config.
	Credential *security.Credential
	// authentication holds the basic authentication information used for the REST client.
	authentication BasicAuth
	// API verion
	Version string
	// Extra query parameters
	Values url.Values
}

// parseAuthentication parses the authentication information from the config and
// removes the user and password from the values.
func (c *Config) parseAuthentication() {
	c.authentication = BasicAuth{
		User:     c.Values.Get("user"),
		Password: c.Values.Get("password"),
	}
	c.Values.Del("user")
	c.Values.Del("password")
}

// defaultServerURLFromConfig is used to build base URL and api path.
func defaultServerURLFromConfig(config *Config) (*url.URL, string, error) {
	host := config.Host
	if host == "" {
		host = "127.0.0.1:8300"
	}
	base := host
	hostURL, err := url.Parse(base)
	if err != nil || hostURL.Scheme == "" || hostURL.Host == "" {
		scheme := "http://"
		if config.Credential != nil && config.Credential.IsTLSEnabled() {
			scheme = "https://"
		}
		hostURL, err = url.Parse(scheme + base)
		if err != nil {
			return nil, "", errors.Trace(err)
		}
		if hostURL.Path != "" && hostURL.Path != "/" {
			return nil, "", errors.ErrInvalidHost.GenWithStackByArgs(base)
		}
	}
	versionedPath := path.Join("/", config.APIPath, config.Version)
	return hostURL, versionedPath, nil
}

// CDCRESTClientFromConfig creates a CDCRESTClient from specific config items.
func CDCRESTClientFromConfig(config *Config) (*CDCRESTClient, error) {
	if config.Version == "" {
		return nil, errors.New("Version is required when initializing a CDCRESTClient")
	}
	if config.APIPath == "" {
		return nil, errors.New("APIPath is required when initializing a CDCRESTClient")
	}

	config.parseAuthentication()
	httpClient, err := httputil.NewClient(config.Credential)
	if err != nil {
		return nil, errors.Trace(err)
	}

	baseURL, versionedAPIPath, err := defaultServerURLFromConfig(config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	restClient, err := NewCDCRESTClient(baseURL, versionedAPIPath, httpClient, config.authentication, config.Values)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return restClient, nil
}
