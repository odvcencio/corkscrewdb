package main

import (
	"context"
	"fmt"

	"github.com/odvcencio/corkscrewdb"
)

// DBClients holds two remote CorkScrewDB connections: one pinned to
// the primary (rw) for writes and read-your-writes, and one pointing
// at the read-load-balancer (ro) for staleness-tolerant reads.
type DBClients struct {
	rw *corkscrewdb.DB
	ro *corkscrewdb.DB
}

// NewDBClients opens both clients and verifies the primary reports the
// expected embedding provider ID. It returns an error if either Connect
// fails or the provider ID does not match.
//
// The context controls connect + Info timeout. On any failure, any
// already-opened client is closed before returning.
func NewDBClients(ctx context.Context, cfg Config) (*DBClients, error) {
	rw, err := dialWithContext(ctx, cfg.AddrRW, cfg.CorkscrewDBToken)
	if err != nil {
		return nil, fmt.Errorf("connect rw: %w", err)
	}
	info, err := rw.RemoteInfo()
	if err != nil {
		_ = rw.Close()
		return nil, fmt.Errorf("rw info: %w", err)
	}
	if info.Embedding.ID != cfg.ExpectedProviderID {
		_ = rw.Close()
		return nil, fmt.Errorf(
			"rw provider ID mismatch: expected %q, got %q",
			cfg.ExpectedProviderID, info.Embedding.ID,
		)
	}
	ro, err := dialWithContext(ctx, cfg.AddrRO, cfg.CorkscrewDBToken)
	if err != nil {
		_ = rw.Close()
		return nil, fmt.Errorf("connect ro: %w", err)
	}
	return &DBClients{rw: rw, ro: ro}, nil
}

// dialWithContext calls corkscrewdb.Connect with the given token and
// honors ctx cancellation by racing the blocking call against ctx.Done.
// If ctx fires first, a drain goroutine closes any *DB that arrives
// after the timeout so we do not leak a connection.
func dialWithContext(ctx context.Context, addr, token string) (*corkscrewdb.DB, error) {
	type result struct {
		db  *corkscrewdb.DB
		err error
	}
	ch := make(chan result, 1)
	go func() {
		db, err := corkscrewdb.Connect(addr, corkscrewdb.WithToken(token))
		ch <- result{db, err}
	}()
	select {
	case r := <-ch:
		return r.db, r.err
	case <-ctx.Done():
		go func() {
			r := <-ch
			if r.db != nil {
				_ = r.db.Close()
			}
		}()
		return nil, ctx.Err()
	}
}

// WriteClient returns the rw (primary-pinned) client. Use for writes
// and for reads that need read-your-own-writes consistency.
func (c *DBClients) WriteClient() *corkscrewdb.DB { return c.rw }

// ReadClient returns a client suitable for reads. When staleOK is true
// the caller is explicitly opting into possibly-stale replica reads,
// so the ro client is returned; otherwise reads go to rw for
// read-your-writes.
func (c *DBClients) ReadClient(staleOK bool) *corkscrewdb.DB {
	if staleOK {
		return c.ro
	}
	return c.rw
}

// Close closes both underlying clients, returning the first non-nil
// error encountered so a caller sees one failure and both clients are
// released.
func (c *DBClients) Close() error {
	var firstErr error
	if c.rw != nil {
		if err := c.rw.Close(); err != nil {
			firstErr = err
		}
	}
	if c.ro != nil {
		if err := c.ro.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
