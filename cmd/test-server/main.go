package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"

	pb "github.com/conduitio-labs/conduit-connector-grpc-client/proto/v1"
	"google.golang.org/grpc"
)

var (
	addr = flag.String("address", ":8080", "gRPC server address")
)

func main() {
	flag.Parse()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("could not listen on %v: %v", *addr, err)
	}
	log.Printf("starting gRPC server on %v", *addr)

	srv := grpc.NewServer()
	ss := &sourceService{}
	pb.RegisterSourceServiceServer(srv, ss)

	// start repl
	go repl{
		server: srv,
		ss:     ss,
	}.run()

	if err := srv.Serve(&loggingListener{ln}); err != nil {
		log.Fatalf("gRPC server stopped with an error: %v", err)
	}
}

type repl struct {
	server *grpc.Server
	ss     *sourceService
}

func (r repl) run() {
	fmt.Print("> ")
	reader := bufio.NewReader(os.Stdin)
	for {
		text, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalf("failed to read from stdin: %v", err)
		}
		text = strings.Replace(text, "\n", "", -1)
		cmd := strings.Split(text, " ")
		switch cmd[0] {
		case "":
			fmt.Print("> ")
			continue
		case "help":
			r.help()
		case "ack":
			if len(cmd) == 1 {
				printf("you need to specify the ack position")
				continue
			}
			position := strings.Join(cmd[1:], " ")
			if err := r.ss.SendAck([]byte(position)); err != nil {
				printf("failed to send ack: %v", err)
				continue
			}
			printf("ack successfully sent for position %v", position)
		case "q", "quit", "exit":
			r.server.Stop()
		default:
			printf("unknown command, type 'help' to list available commands")
		}

	}
}

func (repl) help() {
	printf(`
The REPL can be used to send acknowledgments to the client.

Commands:
- ack [position] - send an acknowledgment for the specified position
- quit - stop the server
`)
}

type sourceService struct {
	pb.UnimplementedSourceServiceServer
	stream atomic.Pointer[pb.SourceService_StreamServer]
}

func (s *sourceService) Stream(stream pb.SourceService_StreamServer) error {
	if !s.stream.CompareAndSwap(nil, &stream) {
		printf("refusing to open a second stream")
		return errors.New("only one client connection is supported at a time")
	}
	printf("stream opened")
	defer s.stream.Store(nil)
	go func() {
		rec, err := stream.Recv()
		printf("received a message, rec: %v, err: %v", rec, err)
		if err != nil {
			return
		}
	}()
	<-stream.Context().Done()
	printf("stream done: %v", stream.Context().Err())
	return stream.Context().Err()
}

func (s *sourceService) SendAck(position []byte) error {
	str := s.stream.Load()
	if str == nil {
		return errors.New("stream is not open")
	}
	return (*str).Send(&pb.Ack{AckPosition: position})
}

type loggingListener struct {
	net.Listener
}

func (l *loggingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		printf("error accepting connection: %v", err)
	} else {
		printf("connection accepted: %s", conn.RemoteAddr())
	}
	return conn, err
}

func printf(format string, vals ...any) {
	fmt.Printf(format+"\n> ", vals...)
}
