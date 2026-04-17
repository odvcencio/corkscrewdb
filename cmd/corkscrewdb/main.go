// Command corkscrewdb runs an embedded CorkScrewDB server over the gRPC transport.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/odvcencio/corkscrewdb"
	"github.com/odvcencio/corkscrewdb/offload"
	"github.com/odvcencio/corkscrewdb/replica"
)

// serverOpts bundles every configuration knob the corkscrewdb binary exposes.
// It lets runServer be called directly from tests without shelling out.
type serverOpts struct {
	DataDir              string
	Addr                 string
	Token                string
	Peers                string
	BitWidth             int
	OffloadDir           string
	ReplicateFrom        string
	ReplicateCollections string
	// ReadyCh, if non-nil, receives the bound listener address after the
	// listener is bound and replication followers are wired. Tests use
	// this to know when it is safe to dial the server's advertised port.
	ReadyCh chan<- net.Addr
}

// validateReplicationFlags enforces that -replicate-from and
// -replicate-collections are either both empty (primary mode) or both
// non-empty (follower mode).
func validateReplicationFlags(from, cols string) error {
	from = strings.TrimSpace(from)
	cols = strings.TrimSpace(cols)
	if from == "" && cols == "" {
		return nil
	}
	if from != "" && cols == "" {
		return fmt.Errorf("-replicate-collections is required when -replicate-from is set")
	}
	if from == "" && cols != "" {
		return fmt.Errorf("-replicate-from is required when -replicate-collections is set")
	}
	return nil
}

// splitAndTrim splits s by sep, trims whitespace on each item, and drops empties.
func splitAndTrim(s, sep string) []string {
	parts := strings.Split(s, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func main() {
	var (
		dataDir              = flag.String("data", "./corkscrewdb-data", "database directory")
		addr                 = flag.String("addr", "0.0.0.0:4040", "listen address")
		token                = flag.String("token", "", "auth token (empty = no auth)")
		peers                = flag.String("peers", "", "comma-separated peer addresses")
		bitWidth             = flag.Int("bit-width", 2, "default quantization bit width")
		offloadDir           = flag.String("offload-dir", "", "local directory for cold storage offload (empty = disabled)")
		replicateFrom        = flag.String("replicate-from", "", "gRPC address of a primary to follow (empty = primary mode)")
		replicateCollections = flag.String("replicate-collections", "", "comma-separated collection names to replicate (required with -replicate-from)")
	)
	flag.Parse()

	if err := validateReplicationFlags(*replicateFrom, *replicateCollections); err != nil {
		log.Fatalf("corkscrewdb: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("corkscrewdb: shutting down on %s", sig)
		cancel()
	}()

	opts := serverOpts{
		DataDir:              *dataDir,
		Addr:                 *addr,
		Token:                *token,
		Peers:                *peers,
		BitWidth:             *bitWidth,
		OffloadDir:           *offloadDir,
		ReplicateFrom:        *replicateFrom,
		ReplicateCollections: *replicateCollections,
	}
	if err := runServer(ctx, opts); err != nil {
		log.Fatalf("corkscrewdb: %v", err)
	}
}

// runServer boots the corkscrewdb server with the given options and blocks
// until ctx is canceled or the underlying db.Serve call returns. It handles
// offload startup, optional replication-follower wiring, and clean teardown.
func runServer(ctx context.Context, opts serverOpts) error {
	if err := validateReplicationFlags(opts.ReplicateFrom, opts.ReplicateCollections); err != nil {
		return err
	}
	_ = opts.BitWidth // default bit width is set in Open

	dbOpts := []corkscrewdb.Option{
		corkscrewdb.WithWALSegmentSize(8 << 20),
	}
	if opts.Token != "" {
		dbOpts = append(dbOpts, corkscrewdb.WithToken(opts.Token))
	}
	if cleaned := splitAndTrim(opts.Peers, ","); len(cleaned) > 0 {
		dbOpts = append(dbOpts, corkscrewdb.WithPeers(cleaned...))
	}

	db, err := corkscrewdb.Open(opts.DataDir, dbOpts...)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}

	var offloadMgr *offload.Manager
	if opts.OffloadDir != "" {
		offloadMgr, err = offload.NewManager(offload.Config{
			Backend: offload.NewFSBackend(opts.OffloadDir),
			DBPath:  opts.DataDir,
		})
		if err != nil {
			_ = db.Close()
			return fmt.Errorf("offload: %w", err)
		}
		offloadMgr.Start()
		log.Printf("corkscrewdb: offload enabled → %s", opts.OffloadDir)
	}

	var (
		remote    *corkscrewdb.DB
		followers []*replica.Follower
	)
	if opts.ReplicateFrom != "" {
		remote, followers, err = startReplicationFollowers(ctx, db, opts)
		if err != nil {
			if offloadMgr != nil {
				offloadMgr.Stop()
			}
			_ = db.Close()
			return err
		}
	}

	listener, err := net.Listen("tcp", opts.Addr)
	if err != nil {
		for _, f := range followers {
			f.Stop()
		}
		if remote != nil {
			_ = remote.Close()
		}
		if offloadMgr != nil {
			offloadMgr.Stop()
		}
		_ = db.Close()
		return fmt.Errorf("listen: %w", err)
	}

	if opts.ReadyCh != nil {
		select {
		case opts.ReadyCh <- listener.Addr():
		case <-ctx.Done():
			_ = listener.Close()
			for _, f := range followers {
				f.Stop()
			}
			if remote != nil {
				_ = remote.Close()
			}
			if offloadMgr != nil {
				offloadMgr.Stop()
			}
			_ = db.Close()
			return ctx.Err()
		}
	}

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- db.Serve(listener)
	}()

	fmt.Fprintf(os.Stderr, "corkscrewdb %s listening on %s (data=%s)\n", corkscrewdb.PackageVersion, listener.Addr(), opts.DataDir)

	var serveErr error
	select {
	case <-ctx.Done():
		_ = listener.Close()
		serveErr = <-doneCh
	case serveErr = <-doneCh:
		_ = listener.Close()
	}

	for _, f := range followers {
		f.Stop()
	}
	if remote != nil {
		if err := remote.Close(); err != nil {
			log.Printf("corkscrewdb: close replication client: %v", err)
		}
	}

	if offloadMgr != nil {
		offloadMgr.Stop()
		if err := offloadMgr.PushNow(); err != nil {
			log.Printf("corkscrewdb: final offload: %v", err)
		}
	}

	var errs []error
	if serveErr != nil {
		errs = append(errs, fmt.Errorf("serve: %w", serveErr))
	}
	if err := db.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close: %w", err))
	}
	return errors.Join(errs...)
}

// replicationStartupTimeout bounds how long the connect + catch-up phase
// is allowed to take before startReplicationFollowers gives up. The
// parent context can still cancel earlier; this is just a ceiling so
// startup does not hang if the primary is wedged.
const replicationStartupTimeout = 30 * time.Second

// dedupeCollections returns the slice unchanged iff every entry is unique,
// otherwise an error naming the first duplicate. Duplicates would otherwise
// silently create two followers for the same collection and double the
// primary's replication load.
func dedupeCollections(names []string) ([]string, error) {
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("duplicate replicate collection %q", name)
		}
		seen[name] = struct{}{}
	}
	return names, nil
}

// startReplicationFollowers connects to the configured primary, builds one
// Follower per replicated collection backed by a shared DBApplier, catches
// each one up, and starts its streaming loop. Connect + catch-up are
// bounded by replicationStartupTimeout and honor ctx cancellation so a
// wedged primary does not block shutdown indefinitely.
func startReplicationFollowers(ctx context.Context, db *corkscrewdb.DB, opts serverOpts) (*corkscrewdb.DB, []*replica.Follower, error) {
	collections, err := dedupeCollections(splitAndTrim(opts.ReplicateCollections, ","))
	if err != nil {
		return nil, nil, err
	}

	startupCtx, cancel := context.WithTimeout(ctx, replicationStartupTimeout)
	defer cancel()

	remote, err := connectPrimary(startupCtx, opts)
	if err != nil {
		return nil, nil, err
	}

	puller, err := corkscrewdb.NewRPCPuller(remote)
	if err != nil {
		_ = remote.Close()
		return nil, nil, fmt.Errorf("rpc puller: %w", err)
	}
	// One applier is shared across every per-collection Follower.
	applier, err := corkscrewdb.NewDBApplier(db)
	if err != nil {
		_ = remote.Close()
		return nil, nil, fmt.Errorf("db applier: %w", err)
	}

	followers := make([]*replica.Follower, 0, len(collections))
	cleanup := func() {
		for _, prev := range followers {
			prev.Stop()
		}
		_ = remote.Close()
	}

	for _, name := range collections {
		if err := startupCtx.Err(); err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("replication startup canceled: %w", err)
		}
		f, err := replica.NewFollower(replica.FollowerConfig{
			Collection: name,
			Applier:    applier,
			Puller:     puller,
			Interval:   500 * time.Millisecond,
		})
		if err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("new follower %q: %w", name, err)
		}
		if err := catchUpWithContext(startupCtx, f, opts.Token); err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("catchup %q: %w", name, err)
		}
		f.Start()
		followers = append(followers, f)
	}
	return remote, followers, nil
}

// connectPrimary dials the configured primary with ctx-aware cancellation.
// corkscrewdb.Connect does not currently accept a context, so we drive the
// dial from a goroutine and race its completion against ctx.Done.
func connectPrimary(ctx context.Context, opts serverOpts) (*corkscrewdb.DB, error) {
	remoteOpts := []corkscrewdb.Option{}
	if opts.Token != "" {
		remoteOpts = append(remoteOpts, corkscrewdb.WithToken(opts.Token))
	}
	type result struct {
		db  *corkscrewdb.DB
		err error
	}
	doneCh := make(chan result, 1)
	go func() {
		db, err := corkscrewdb.Connect(opts.ReplicateFrom, remoteOpts...)
		doneCh <- result{db: db, err: err}
	}()
	select {
	case r := <-doneCh:
		if r.err != nil {
			return nil, fmt.Errorf("connect primary: %w", r.err)
		}
		// Re-check ctx: Connect may have returned after cancellation.
		if err := ctx.Err(); err != nil {
			_ = r.db.Close()
			return nil, fmt.Errorf("connect primary canceled: %w", err)
		}
		return r.db, nil
	case <-ctx.Done():
		// Let the orphan Connect goroutine finish in the background and
		// close whatever connection it eventually produces so we do not
		// leak a gRPC conn.
		go func() {
			r := <-doneCh
			if r.db != nil {
				_ = r.db.Close()
			}
		}()
		return nil, fmt.Errorf("connect primary canceled: %w", ctx.Err())
	}
}

// catchUpWithContext runs Follower.CatchUp in a goroutine so it can be
// canceled via ctx. Follower.CatchUp does not accept a context itself, so
// we race its completion against ctx.Done. If ctx fires first the caller
// must still clean up the Follower.
func catchUpWithContext(ctx context.Context, f *replica.Follower, token string) error {
	doneCh := make(chan error, 1)
	go func() { doneCh <- f.CatchUp(token) }()
	select {
	case err := <-doneCh:
		return err
	case <-ctx.Done():
		// Let the orphan CatchUp goroutine finish in the background; the
		// caller will Stop() the follower on cleanup, which drains its
		// own loop state.
		go func() { <-doneCh }()
		return ctx.Err()
	}
}
