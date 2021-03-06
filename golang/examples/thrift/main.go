package main

// Copyright (c) 2015 Uber Technologies, Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"reflect"
	"runtime"
	"strings"
	"time"

	"golang.org/x/net/context"

	tchannel "github.com/uber/tchannel/golang"
	gen "github.com/uber/tchannel/golang/examples/thrift/gen-go/test"
	tthrift "github.com/uber/tchannel/golang/thrift"
)

func main() {
	var (
		listener net.Listener
		err      error
	)

	if listener, err = setupServer(); err != nil {
		log.Fatalf("setupServer failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	opts := tthrift.TChanOutboundOptions{
		Context:          ctx,
		Dst:              listener.Addr().String(),
		HyperbahnService: "server",
	}

	if err := runClient1(opts); err != nil {
		log.Fatalf("runClient1 failed: %v", err)
	}

	if err := runClient2(opts); err != nil {
		log.Fatalf("runClient2 failed: %v", err)
	}

	go listenConsole()

	// Run for 10 seconds, then stop
	time.Sleep(time.Second * 10)
}

func setupServer() (net.Listener, error) {
	tchan, err := tchannel.NewChannel("server", optsFor("server"))
	if err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}

	server := tthrift.NewServer(tchan)

	fh := &firstHandler{}
	server.Register("First", reflect.TypeOf(fh), gen.NewFirstProcessor(&firstHandler{}))

	sh := &secondHandler{}
	server.Register("Second", reflect.TypeOf(sh), gen.NewSecondProcessor(&secondHandler{}))

	go tchan.Serve(listener)
	return listener, nil
}

func runClient1(opts tthrift.TChanOutboundOptions) error {
	tchan, err := tchannel.NewChannel("client1", optsFor("client1"))
	if err != nil {
		return err
	}
	opts.ThriftService = "First"
	protocol := tthrift.NewTChanOutbound(tchan, opts)

	client := gen.NewFirstClientProtocol(nil, protocol, protocol)
	go func() {
		for {
			res, err := client.Echo("Hi")
			log.Println("Echo(Hi) = ", res, ", err: ", err)
			client.OneWay()
			log.Println("AppError = ", client.AppError())
			time.Sleep(100 * time.Millisecond)
		}
	}()
	return nil
}

func runClient2(opts tthrift.TChanOutboundOptions) error {
	tchan, err := tchannel.NewChannel("client2", optsFor("client2"))
	if err != nil {
		return err
	}

	opts.ThriftService = "Second"
	protocol := tthrift.NewTChanOutbound(tchan, opts)

	client := gen.NewSecondClientProtocol(nil, protocol, protocol)
	go func() {
		for {
			client.Test()
			time.Sleep(100 * time.Millisecond)
		}
	}()
	return nil
}

func listenConsole() {
	rdr := bufio.NewReader(os.Stdin)
	for {
		line, _ := rdr.ReadString('\n')
		switch strings.TrimSpace(line) {
		case "s":
			printStack()
		default:
			fmt.Println("Unrecognized command:", line)
		}
	}
}

func printStack() {
	buf := make([]byte, 10000)
	runtime.Stack(buf, true /* all */)
	fmt.Println("Stack:\n", string(buf))
}

type firstHandler struct{}

func (h *firstHandler) Healthcheck() (*gen.HealthCheckRes, error) {
	log.Printf("first: HealthCheck()\n")
	return &gen.HealthCheckRes{true, "OK"}, nil
}

func (h *firstHandler) Echo(msg string) (r string, err error) {
	log.Printf("first: Echo(%v)\n", msg)
	return msg, nil
}

func (h *firstHandler) AppError() error {
	log.Printf("first: AppError()\n")
	return errors.New("app error")
}

func (h *firstHandler) OneWay() error {
	log.Printf("first: OneWay()\n")
	return errors.New("OneWay error...won't be seen by client")
}

type secondHandler struct{}

func (h *secondHandler) Test() error {
	log.Println("secondHandler: Test()")
	return nil
}

func optsFor(processName string) *tchannel.ChannelOptions {
	return &tchannel.ChannelOptions{
		ProcessName: processName,
		Logger:      tchannel.SimpleLogger,
	}
}
