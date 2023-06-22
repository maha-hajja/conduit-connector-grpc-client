# Conduit Connector for gRPC Client
The gRPC Client connector is one of [Conduit](https://conduit.io) plugins. It provides a destination gRPC Client connector.

This connector should be paired with another Conduit instance or pipeline, that provides a 
[gRPC server source](https://github.com/conduitio-labs/conduit-connector-grpc-server). Where the client will initiate
the connection with the server, and start sending records to it.


## How to build?
Run `make build` to build the connector.

## Testing
Run `make test` to run all the unit tests.

## Destination
A client gRPC destination connector initiates connection with a gRPC server using the `url` provided as
a parameter. It creates a bidirectional stream with the server and uses the stream to write records to the
server, then waits for acknowledgments to be received from the server through the same stream.

### Configuration

| name                   | description                                                                          | required                              | default value |
|------------------------|--------------------------------------------------------------------------------------|---------------------------------------|---------------|
| `url`                  | url to gRPC server.                                                                  | true                                  |               |
| `rateLimit`            | the bandwidth limit in bytes/second, use `0` to disable rate limiting.               | false                                 | `0`           |
| `mtls.disable`         | flag to disable mTLS secure connection, set it to `true` for an insecure connection. | false                                 | `false`       |
| `mtls.client.certPath` | the client certificate path.                                                         | required if `mtls.disable` is `false` |               |
| `mtls.client.keyPath`  | the client private key path.                                                         | required if `mtls.disable` is `false` |               |
| `mtls.CA.certPath`     | the root CA certificate path.                                                        | required if `mtls.disable` is `false` |               |

## Mutual TLS (mTLS)
Mutual TLS is used by default to connect to the server, to disable mTLS you can set the parameter `mtls.disable`
to `true`, this will result in an insecure connection to the server.

This repo contains self-signed certificates that can be used for local testing purposes, you can find them
under `./test/certs`, note that these certificates are not meant to be used in production environment.

To generate your own secure mTLS certificates, check
[this tutorial](https://medium.com/weekly-webtips/how-to-generate-keys-for-mutual-tls-authentication-a90f53bcec64).

## Planned work
- Add a source for gRPC client. 