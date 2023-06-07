package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/conduitio-labs/conduit-connector-grpc-client/proto/v1"
	v1 "github.com/conduitio/conduit-connector-protocol/proto/opencdc/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	url = flag.String("url", "localhost:8012", "gRPC server address")
)

func main() {
	ctx := context.Background()
	log.Printf("dialing %v", *url)
	ctxDial, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	conn, err := grpc.DialContext(
		ctxDial,
		*url,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Fatalf("failed to dial server: %v", err)
	}
	log.Println("connected")

	c := pb.NewSourceServiceClient(conn)
	stream, err := c.Stream(ctx)
	if err != nil {
		log.Fatalf("failed to open stream: %v", err)
	}

	go func() {
		for {
			ack, err := stream.Recv()
			printf("received an ack, ack: %v, err: %v", ack, err)
			if err != nil {
				os.Exit(1)
			}
		}
	}()

	repl{stream: stream}.run()
}

type repl struct {
	stream pb.SourceService_StreamClient
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
		case "record":
			if len(cmd) == 1 {
				printf("you need to specify the ack position")
				continue
			}
			position := strings.Join(cmd[1:], " ")
			rec := &v1.Record{
				Position:  []byte(position),
				Operation: v1.Operation_OPERATION_CREATE,
				Metadata:  nil,
				Key:       &v1.Data{Data: &v1.Data_RawData{RawData: []byte("foo")}},
				Payload: &v1.Change{
					Before: nil,
					After:  &v1.Data{Data: &v1.Data_RawData{RawData: []byte("bar")}},
				},
			}
			if err := r.stream.Send(rec); err != nil {
				printf("failed to send ack: %v", err)
			}
			printf("record successfully sent: %v", rec)
		case "q", "quit", "exit":
			if len(cmd) > 1 {
				exitCode, err := strconv.Atoi(cmd[1])
				if err != nil {
					os.Exit(255)
				}
				os.Exit(exitCode)
			}
			return
		default:
			printf("unknown command, type 'help' to list available commands")
		}
	}
}

func (repl) help() {
	printf(`
The REPL can be used to send records to the server.

Commands:
- record [position] - send an acknowledgment for the specified position
- quit - stop the client
`)
}

func printf(format string, vals ...any) {
	fmt.Printf(format+"\n> ", vals...)
}
