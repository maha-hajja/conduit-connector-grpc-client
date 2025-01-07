// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             (unknown)
// source: v1/stream.proto

package v1

import (
	context "context"
	v1 "github.com/conduitio/conduit-commons/proto/opencdc/v1"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	SourceService_Stream_FullMethodName = "/protov1.SourceService/Stream"
)

// SourceServiceClient is the client API for SourceService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type SourceServiceClient interface {
	Stream(ctx context.Context, opts ...grpc.CallOption) (SourceService_StreamClient, error)
}

type sourceServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewSourceServiceClient(cc grpc.ClientConnInterface) SourceServiceClient {
	return &sourceServiceClient{cc}
}

func (c *sourceServiceClient) Stream(ctx context.Context, opts ...grpc.CallOption) (SourceService_StreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &SourceService_ServiceDesc.Streams[0], SourceService_Stream_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &sourceServiceStreamClient{stream}
	return x, nil
}

type SourceService_StreamClient interface {
	Send(*v1.Record) error
	Recv() (*Ack, error)
	grpc.ClientStream
}

type sourceServiceStreamClient struct {
	grpc.ClientStream
}

func (x *sourceServiceStreamClient) Send(m *v1.Record) error {
	return x.ClientStream.SendMsg(m)
}

func (x *sourceServiceStreamClient) Recv() (*Ack, error) {
	m := new(Ack)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// SourceServiceServer is the server API for SourceService service.
// All implementations must embed UnimplementedSourceServiceServer
// for forward compatibility
type SourceServiceServer interface {
	Stream(SourceService_StreamServer) error
	mustEmbedUnimplementedSourceServiceServer()
}

// UnimplementedSourceServiceServer must be embedded to have forward compatible implementations.
type UnimplementedSourceServiceServer struct {
}

func (UnimplementedSourceServiceServer) Stream(SourceService_StreamServer) error {
	return status.Errorf(codes.Unimplemented, "method Stream not implemented")
}
func (UnimplementedSourceServiceServer) mustEmbedUnimplementedSourceServiceServer() {}

// UnsafeSourceServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to SourceServiceServer will
// result in compilation errors.
type UnsafeSourceServiceServer interface {
	mustEmbedUnimplementedSourceServiceServer()
}

func RegisterSourceServiceServer(s grpc.ServiceRegistrar, srv SourceServiceServer) {
	s.RegisterService(&SourceService_ServiceDesc, srv)
}

func _SourceService_Stream_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(SourceServiceServer).Stream(&sourceServiceStreamServer{stream})
}

type SourceService_StreamServer interface {
	Send(*Ack) error
	Recv() (*v1.Record, error)
	grpc.ServerStream
}

type sourceServiceStreamServer struct {
	grpc.ServerStream
}

func (x *sourceServiceStreamServer) Send(m *Ack) error {
	return x.ServerStream.SendMsg(m)
}

func (x *sourceServiceStreamServer) Recv() (*v1.Record, error) {
	m := new(v1.Record)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// SourceService_ServiceDesc is the grpc.ServiceDesc for SourceService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var SourceService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "protov1.SourceService",
	HandlerType: (*SourceServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Stream",
			Handler:       _SourceService_Stream_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "v1/stream.proto",
}
