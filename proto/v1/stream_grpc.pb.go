// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             (unknown)
// source: v1/stream.proto

package protov1

import (
	context "context"
	v1 "github.com/conduitio/conduit-connector-protocol/proto/opencdc/v1"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	StreamService_Stream_FullMethodName = "/proto.v1.StreamService/Stream"
)

// StreamServiceClient is the client API for StreamService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type StreamServiceClient interface {
	Stream(ctx context.Context, opts ...grpc.CallOption) (StreamService_StreamClient, error)
}

type streamServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewStreamServiceClient(cc grpc.ClientConnInterface) StreamServiceClient {
	return &streamServiceClient{cc}
}

func (c *streamServiceClient) Stream(ctx context.Context, opts ...grpc.CallOption) (StreamService_StreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &StreamService_ServiceDesc.Streams[0], StreamService_Stream_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &streamServiceStreamClient{stream}
	return x, nil
}

type StreamService_StreamClient interface {
	Send(*v1.Record) error
	Recv() (*Ack, error)
	grpc.ClientStream
}

type streamServiceStreamClient struct {
	grpc.ClientStream
}

func (x *streamServiceStreamClient) Send(m *v1.Record) error {
	return x.ClientStream.SendMsg(m)
}

func (x *streamServiceStreamClient) Recv() (*Ack, error) {
	m := new(Ack)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// StreamServiceServer is the server API for StreamService service.
// All implementations must embed UnimplementedStreamServiceServer
// for forward compatibility
type StreamServiceServer interface {
	Stream(StreamService_StreamServer) error
	mustEmbedUnimplementedStreamServiceServer()
}

// UnimplementedStreamServiceServer must be embedded to have forward compatible implementations.
type UnimplementedStreamServiceServer struct {
}

func (UnimplementedStreamServiceServer) Stream(StreamService_StreamServer) error {
	return status.Errorf(codes.Unimplemented, "method Stream not implemented")
}
func (UnimplementedStreamServiceServer) mustEmbedUnimplementedStreamServiceServer() {}

// UnsafeStreamServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to StreamServiceServer will
// result in compilation errors.
type UnsafeStreamServiceServer interface {
	mustEmbedUnimplementedStreamServiceServer()
}

func RegisterStreamServiceServer(s grpc.ServiceRegistrar, srv StreamServiceServer) {
	s.RegisterService(&StreamService_ServiceDesc, srv)
}

func _StreamService_Stream_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(StreamServiceServer).Stream(&streamServiceStreamServer{stream})
}

type StreamService_StreamServer interface {
	Send(*Ack) error
	Recv() (*v1.Record, error)
	grpc.ServerStream
}

type streamServiceStreamServer struct {
	grpc.ServerStream
}

func (x *streamServiceStreamServer) Send(m *Ack) error {
	return x.ServerStream.SendMsg(m)
}

func (x *streamServiceStreamServer) Recv() (*v1.Record, error) {
	m := new(v1.Record)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// StreamService_ServiceDesc is the grpc.ServiceDesc for StreamService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var StreamService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "proto.v1.StreamService",
	HandlerType: (*StreamServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Stream",
			Handler:       _StreamService_Stream_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "v1/stream.proto",
}
