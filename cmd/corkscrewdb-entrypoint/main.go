// Command corkscrewdb-entrypoint builds the correct argv for the
// corkscrewdb server based on HOSTNAME ordinal suffix and re-execs
// the server binary. This is the image entrypoint, used by the
// StatefulSet to decide per-pod replication flags without a shell.
package main

import (
	"fmt"
	"os"
	"strings"
	"syscall"
)

const corkscrewdbBinary = "/corkscrewdb"

func buildArgs(hostname, token, peers, replicateCollections string) []string {
	args := []string{
		"-data=/var/lib/corkscrewdb",
		"-addr=0.0.0.0:4040",
		"-token=" + token,
		"-peers=" + peers,
	}
	// The primary StatefulSet pod is always ordinal 0. Any other ordinal
	// runs as a replication follower.
	if !strings.HasSuffix(hostname, "-0") {
		args = append(args,
			"-replicate-from=corkscrewdb-0.corkscrewdb.m31labs.svc.cluster.local:4040",
			"-replicate-collections="+replicateCollections,
		)
	}
	return args
}

func main() {
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Fprintf(os.Stderr, "corkscrewdb-entrypoint: hostname: %v\n", err)
		os.Exit(1)
	}
	token := os.Getenv("CORKSCREWDB_TOKEN")
	peers := os.Getenv("PEER_LIST")
	replicateCollections := os.Getenv("REPLICATE_COLLECTIONS")
	args := buildArgs(hostname, token, peers, replicateCollections)
	argv := append([]string{corkscrewdbBinary}, args...)
	fmt.Fprintf(os.Stderr, "corkscrewdb-entrypoint: exec %v\n", argv)
	if err := syscall.Exec(corkscrewdbBinary, argv, os.Environ()); err != nil {
		fmt.Fprintf(os.Stderr, "corkscrewdb-entrypoint: exec: %v\n", err)
		os.Exit(1)
	}
}
