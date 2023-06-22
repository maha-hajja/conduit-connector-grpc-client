// Copyright © 2023 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcclient

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.uber.org/multierr"
)

// Config has the generic parameters needed for a gRPC client
type Config struct {
	// url to gRPC server
	URL string `json:"url" validate:"required"`
	// the bandwidth limit in bytes/second, use "0" to disable rate limiting.
	RateLimit int `json:"rateLimit" default:"0" validate:"gt=-1"`
	// mTLS configurations.
	MTLS MTLSConfig `json:"mtls"`
}

type MTLSConfig struct {
	// the client certificate path.
	ClientCertPath string `json:"client.certPath"`
	// the client private key path.
	ClientKeyPath string `json:"client.keyPath"`
	// the root CA certificate path.
	CACertPath string `json:"CA.certPath"`
	// flag to disable mTLS secure connection, set it to `true` for an insecure connection.
	Disable bool `json:"disable" default:"false"`
}

// ParseMTLSFiles parses and validates mTLS params values, returns the parsed client certificate, and CA certificate pool,
// and an error if the parsing fails
func (c *Config) ParseMTLSFiles() (tls.Certificate, *x509.CertPool, error) {
	err := c.validateRequiredMTLSParams()
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("error validating \"mtls\": mTLS security is enabled and some"+
			" configurations are missing, if you wish to disable mTLS, set the config option \"mtls.disable\" to true: %w", err)
	}
	clientCert, err := tls.LoadX509KeyPair(c.MTLS.ClientCertPath, c.MTLS.ClientKeyPath)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("failed to load client key pair: %w", err)
	}
	// Load CA certificate
	caCert, err := os.ReadFile(c.MTLS.CACertPath)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}
	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
		return tls.Certificate{}, nil, errors.New("failed to append CA certs")
	}
	return clientCert, caCertPool, nil
}

func (c *Config) validateRequiredMTLSParams() error {
	var multiErr error
	if c.MTLS.CACertPath == "" {
		multiErr = multierr.Append(multiErr, fmt.Errorf("error validating \"mtls.CA.certPath\": %w", sdk.ErrRequiredParameterMissing))
	}
	if c.MTLS.ClientCertPath == "" {
		multiErr = multierr.Append(multiErr, fmt.Errorf("error validating \"mtls.client.certPath\": %w", sdk.ErrRequiredParameterMissing))
	}
	if c.MTLS.ClientKeyPath == "" {
		multiErr = multierr.Append(multiErr, fmt.Errorf("error validating \"mtls.client.keyPath\": %w", sdk.ErrRequiredParameterMissing))
	}
	return multiErr
}
