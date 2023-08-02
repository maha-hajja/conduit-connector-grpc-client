// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-connector-sdk/tree/main/cmd/paramgen

package grpcclient

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func (DestConfig) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		"maxDowntime": {
			Default:     "10m",
			Description: "max downtime accepted for the server to be off.",
			Type:        sdk.ParameterTypeDuration,
			Validations: []sdk.Validation{},
		},
		"mtls.ca.certPath": {
			Default:     "",
			Description: "the root CA certificate path.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{},
		},
		"mtls.client.certPath": {
			Default:     "",
			Description: "the client certificate path.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{},
		},
		"mtls.client.keyPath": {
			Default:     "",
			Description: "the client private key path.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{},
		},
		"mtls.disabled": {
			Default:     "false",
			Description: "option to disable mTLS secure connection, set it to `true` for an insecure connection.",
			Type:        sdk.ParameterTypeBool,
			Validations: []sdk.Validation{},
		},
		"rateLimit": {
			Default:     "0",
			Description: "the bandwidth limit in bytes/second, use \"0\" to disable rate limiting.",
			Type:        sdk.ParameterTypeInt,
			Validations: []sdk.Validation{
				sdk.ValidationGreaterThan{Value: -1},
			},
		},
		"reconnectDelay": {
			Default:     "5s",
			Description: "delay between each gRPC request retry.",
			Type:        sdk.ParameterTypeDuration,
			Validations: []sdk.Validation{},
		},
		"url": {
			Default:     "",
			Description: "url to gRPC server",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
	}
}
