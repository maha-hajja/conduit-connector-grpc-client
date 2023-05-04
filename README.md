# Conduit Connector for gRPC Client
The gRPC Client connector is one of [Conduit](https://conduit.io) plugins. It provides a destination gRPC Client connector.

## How to build?
Run `make build` to build the connector.

## Testing
Run `make test` to run all the unit tests. Run `make test-integration` to run the integration tests.

The Docker compose file at `test/docker-compose.yml` can be used to run the required resource locally.

## Destination
A client gRPC destination connector initiates connection with a gRPC server using the `url` provided as
a parameter. It creates a bidirectional stream with the server and uses the stream to write records to the
server, then waits for acknowledgments to be received from the server through the same stream.

### Configuration

| name                 | description                                | required | default value |
|----------------------|--------------------------------------------|----------|---------------|
| `url`                | url to gRPC server.                        | true     |               |

## Planned work
- Add a source for gRPC client. 