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

package grpc

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// version is set during the build process with ldflags (see Makefile).
// Default version matches default from runtime/debug.
var version = "(devel)"

// Specification returns the connector's specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:    "grpc-client",
		Summary: "A gRPC Source & Destination Clients.",
		// todo: first version will only include the destination.
		Description: "Conduit gRPC source and destination clients, initiates the connection with a gRPC server connector (github.com/conduitio-labs/conduit-connector-grpc-server).",
		Version:     version,
		Author:      "Meroxa, Inc.",
	}
}
