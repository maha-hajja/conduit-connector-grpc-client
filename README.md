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

| name                  | description                                                                          | required                             | default value |
|-----------------------|--------------------------------------------------------------------------------------|--------------------------------------|---------------|
| `url`                 | url to gRPC server.                                                                  | true                                 |               |
| `rateLimit`           | the bandwidth limit in bytes/second, use `0` to disable rate limiting.               | false                                | `0`           |
| `reconnectDelay`      | delay between each gRPC request retry.                                               | false                                | `5s`          |
| `maxDowntime`         | max downtime accepted for the server to be off.                                      | false                                | `10m`         |
| `tls.disable`         | flag to disable mTLS secure connection, set it to `true` for an insecure connection. | false                                | `false`       |
| `tls.client.certPath` | the client certificate path.                                                         | required if `tls.disable` is `false` |               |
| `tls.client.keyPath`  | the client private key path.                                                         | required if `tls.disable` is `false` |               |
| `tls.CA.certPath`     | the root CA certificate path.                                                        | required if `tls.disable` is `false` |               |

## Mutual TLS (mTLS)
Mutual TLS is used by default to connect to the server, to disable mTLS you can set the parameter `tls.disable`
to `true`, this will result in an insecure connection to the server.

To generate self-signed certificates for both a server and a client, run `make generate-certs`, this will generate certificates
under the directory `./test/certs`.

## Planned work
- Add a source for gRPC client. 