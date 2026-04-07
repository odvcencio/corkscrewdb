// Command corkscrewdb runs an embedded CorkScrewDB server over the built-in RPC transport.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/odvcencio/corkscrewdb"
	"github.com/odvcencio/corkscrewdb/offload"
)

func main() {
	var (
		dataDir    = flag.String("data", "./corkscrewdb-data", "database directory")
		addr       = flag.String("addr", "0.0.0.0:4040", "listen address")
		token      = flag.String("token", "", "auth token (empty = no auth)")
		peers      = flag.String("peers", "", "comma-separated peer addresses")
		bitWidth   = flag.Int("bit-width", 2, "default quantization bit width")
		offloadDir = flag.String("offload-dir", "", "local directory for cold storage offload (empty = disabled)")
	)
	flag.Parse()

	opts := []corkscrewdb.Option{
		corkscrewdb.WithWALSegmentSize(8 << 20),
	}
	if *token != "" {
		opts = append(opts, corkscrewdb.WithToken(*token))
	}
	if *peers != "" {
		peerList := strings.Split(*peers, ",")
		cleaned := make([]string, 0, len(peerList))
		for _, p := range peerList {
			p = strings.TrimSpace(p)
			if p != "" {
				cleaned = append(cleaned, p)
			}
		}
		if len(cleaned) > 0 {
			opts = append(opts, corkscrewdb.WithPeers(cleaned...))
		}
	}
	_ = bitWidth // default bit width is set in Open

	db, err := corkscrewdb.Open(*dataDir, opts...)
	if err != nil {
		log.Fatalf("corkscrewdb: open: %v", err)
	}

	var offloadMgr *offload.Manager
	if *offloadDir != "" {
		offloadMgr, err = offload.NewManager(offload.Config{
			Backend: offload.NewFSBackend(*offloadDir),
			DBPath:  *dataDir,
		})
		if err != nil {
			log.Fatalf("corkscrewdb: offload: %v", err)
		}
		offloadMgr.Start()
		log.Printf("corkscrewdb: offload enabled → %s", *offloadDir)
	}

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("corkscrewdb: listen: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- db.Serve(listener)
	}()

	fmt.Fprintf(os.Stderr, "corkscrewdb %s listening on %s (data=%s)\n", corkscrewdb.PackageVersion, listener.Addr(), *dataDir)

	select {
	case sig := <-sigCh:
		log.Printf("corkscrewdb: shutting down on %s", sig)
	case err := <-doneCh:
		if err != nil {
			log.Fatalf("corkscrewdb: serve: %v", err)
		}
	}

	_ = listener.Close()
	if offloadMgr != nil {
		offloadMgr.Stop()
		if err := offloadMgr.PushNow(); err != nil {
			log.Printf("corkscrewdb: final offload: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		log.Fatalf("corkscrewdb: close: %v", err)
	}
}
