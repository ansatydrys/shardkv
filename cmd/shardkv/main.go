package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/ansatydrys/shardkv/internal/cluster"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: shardkv serve|put|get|delete [flags] [key] [value]")
	}
	cmd := os.Args[1]
	f := flag.NewFlagSet(cmd, flag.ContinueOnError)
	config := f.String("config", "cluster.local.json", "cluster configuration")
	id := f.String("id", "", "node ID (serve)")
	dir := f.String("data", "data", "WAL directory (serve)")
	addr := f.String("addr", "", "explicit replica address (client)")
	if err := f.Parse(os.Args[2:]); err != nil {
		return err
	}
	c, err := cluster.Load(*config)
	if err != nil {
		return err
	}
	if cmd == "serve" {
		n, err := cluster.New(*id, *dir, c)
		if err != nil {
			return err
		}
		defer n.Close()
		listener, err := net.Listen("tcp", c.Nodes[*id])
		if err != nil {
			return err
		}
		g := n.Server()
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		go func() { <-ctx.Done(); g.Stop() }()
		log.Printf("node %s listening on %s", *id, listener.Addr())
		return g.Serve(listener)
	}
	method := map[string]string{"put": "Put", "get": "Get", "delete": "Delete"}[cmd]
	if method == "" {
		return fmt.Errorf("unknown command %s", cmd)
	}
	want := 1
	if cmd == "put" {
		want = 2
	}
	if f.NArg() != want {
		return fmt.Errorf("%s requires %d arguments (flags before key)", cmd, want)
	}
	value := ""
	if want == 2 {
		value = f.Arg(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, err := cluster.Request(ctx, c, *addr, method, f.Arg(0), value)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(out.AsMap(), "", "  ")
	if err == nil {
		fmt.Println(string(b))
	}
	return err
}
