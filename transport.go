package corkscrewdb

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"strings"
)

var ErrUnauthorized = errors.New("corkscrewdb: unauthorized")

const rpcServiceName = "CorkScrewDB"

type rpcClient struct {
	client *rpc.Client
	token  string
}

type RPCInfoRequest struct {
	Token string
}

type RPCInfoResponse struct {
	PackageVersion string
	Embedding      embeddingConfig
	Peers          []string
}

type RPCEmpty struct{}

type RPCEnsureCollectionRequest struct {
	Token    string
	Name     string
	BitWidth int
}

type RPCPutRequest struct {
	Token      string
	Collection string
	ID         string
	Entry      Entry
}

type RPCPutVectorRequest struct {
	Token      string
	Collection string
	ID         string
	Vector     []float32
	Text       string
	Metadata   map[string]string
}

type RPCDeleteRequest struct {
	Token      string
	Collection string
	ID         string
}

type RPCFilter struct {
	Key   string
	Value string
}

type RPCSearchRequest struct {
	Token      string
	Collection string
	Query      string
	K          int
	Filters    []RPCFilter
	UseAt      bool
	AtLamport  uint64
}

type RPCSearchVectorRequest struct {
	Token      string
	Collection string
	Query      []float32
	K          int
	Filters    []RPCFilter
	UseAt      bool
	AtLamport  uint64
}

type RPCSearchResponse struct {
	Results []SearchResult
}

type RPCHistoryRequest struct {
	Token      string
	Collection string
	ID         string
	UseAt      bool
	AtLamport  uint64
}

type RPCHistoryResponse struct {
	Versions []Version
}

type rpcServer struct {
	db *DB
}

// Connect opens a remote CorkScrewDB client over the built-in RPC transport.
func Connect(addr string, opts ...Option) (*DB, error) {
	if strings.TrimSpace(addr) == "" {
		return nil, errors.New("corkscrewdb: address is required")
	}
	cfg := dbConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt.applyDB(&cfg)
		}
	}
	if cfg.providerSet {
		return nil, errors.New("corkscrewdb: Connect does not accept WithProvider")
	}

	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	rc := &rpcClient{client: client, token: cfg.token}
	info, err := rc.Info()
	if err != nil {
		_ = rc.Close()
		return nil, err
	}

	return &DB{
		path:        addr,
		remote:      rc,
		token:       cfg.token,
		peers:       append([]string(nil), info.Peers...),
		manifest:    manifest{ModuleVersion: info.PackageVersion, Embedding: info.Embedding},
		collections: make(map[string]*Collection),
	}, nil
}

// Serve exposes the DB over the built-in RPC transport.
func (db *DB) Serve(listener net.Listener) error {
	if db == nil {
		return errors.New("corkscrewdb: nil database")
	}
	if db.remote != nil {
		return errors.New("corkscrewdb: remote clients cannot serve")
	}
	server := rpc.NewServer()
	if err := server.RegisterName(rpcServiceName, &rpcServer{db: db}); err != nil {
		return err
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go server.ServeConn(conn)
	}
}

// ListenAndServe binds a TCP listener and serves requests until the listener closes.
func (db *DB) ListenAndServe(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return db.Serve(listener)
}

func (db *DB) remoteCollection(name string, opts ...CollectionOption) *Collection {
	db.mu.Lock()
	defer db.mu.Unlock()

	if existing, ok := db.collections[name]; ok {
		var cfg collectionConfig
		for _, opt := range opts {
			if opt != nil {
				opt.applyCollection(&cfg)
			}
		}
		if cfg.bitWidth != 0 && existing.bitWidth != 0 && cfg.bitWidth != existing.bitWidth {
			return &Collection{db: db, name: name, remote: db.remote, err: fmt.Errorf("corkscrewdb: collection %q already exists with bit width %d", name, existing.bitWidth)}
		}
		if cfg.bitWidth != 0 && existing.bitWidth == 0 {
			if err := db.remote.EnsureCollection(name, cfg.bitWidth); err != nil {
				return &Collection{db: db, name: name, remote: db.remote, err: err}
			}
			existing.bitWidth = cfg.bitWidth
		}
		return existing
	}

	var cfg collectionConfig
	for _, opt := range opts {
		if opt != nil {
			opt.applyCollection(&cfg)
		}
	}
	if err := db.remote.EnsureCollection(name, cfg.bitWidth); err != nil {
		return &Collection{db: db, name: name, remote: db.remote, err: err}
	}
	coll := &Collection{
		db:       db,
		name:     name,
		bitWidth: cfg.bitWidth,
		remote:   db.remote,
	}
	db.collections[name] = coll
	return coll
}

func (c *rpcClient) call(method string, request any, response any) error {
	return c.client.Call(rpcServiceName+"."+method, request, response)
}

func (c *rpcClient) Close() error {
	return c.client.Close()
}

func (c *rpcClient) Info() (RPCInfoResponse, error) {
	var resp RPCInfoResponse
	err := c.call("Info", RPCInfoRequest{Token: c.token}, &resp)
	return resp, err
}

func (c *rpcClient) EnsureCollection(name string, bitWidth int) error {
	return c.call("EnsureCollection", RPCEnsureCollectionRequest{
		Token:    c.token,
		Name:     name,
		BitWidth: bitWidth,
	}, &RPCEmpty{})
}

func (c *rpcClient) Put(collection, id string, entry Entry) error {
	return c.call("Put", RPCPutRequest{
		Token:      c.token,
		Collection: collection,
		ID:         id,
		Entry:      entry,
	}, &RPCEmpty{})
}

func (c *rpcClient) PutVector(collection, id string, vector []float32, text string, metadata map[string]string) error {
	return c.call("PutVector", RPCPutVectorRequest{
		Token:      c.token,
		Collection: collection,
		ID:         id,
		Vector:     cloneVector(vector),
		Text:       text,
		Metadata:   cloneMetadata(metadata),
	}, &RPCEmpty{})
}

func (c *rpcClient) Search(collection, query string, k int, filters []FilterOption, useAt bool, atLamport uint64) ([]SearchResult, error) {
	var resp RPCSearchResponse
	err := c.call("Search", RPCSearchRequest{
		Token:      c.token,
		Collection: collection,
		Query:      query,
		K:          k,
		Filters:    toRPCFilters(filters),
		UseAt:      useAt,
		AtLamport:  atLamport,
	}, &resp)
	return resp.Results, err
}

func (c *rpcClient) SearchVector(collection string, query []float32, k int, filters []FilterOption, useAt bool, atLamport uint64) ([]SearchResult, error) {
	var resp RPCSearchResponse
	err := c.call("SearchVector", RPCSearchVectorRequest{
		Token:      c.token,
		Collection: collection,
		Query:      cloneVector(query),
		K:          k,
		Filters:    toRPCFilters(filters),
		UseAt:      useAt,
		AtLamport:  atLamport,
	}, &resp)
	return resp.Results, err
}

func (c *rpcClient) History(collection, id string, useAt bool, atLamport uint64) ([]Version, error) {
	var resp RPCHistoryResponse
	err := c.call("History", RPCHistoryRequest{
		Token:      c.token,
		Collection: collection,
		ID:         id,
		UseAt:      useAt,
		AtLamport:  atLamport,
	}, &resp)
	return resp.Versions, err
}

func (c *rpcClient) Delete(collection, id string) error {
	return c.call("Delete", RPCDeleteRequest{
		Token:      c.token,
		Collection: collection,
		ID:         id,
	}, &RPCEmpty{})
}

func (s *rpcServer) Info(req RPCInfoRequest, resp *RPCInfoResponse) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	*resp = RPCInfoResponse{
		PackageVersion: PackageVersion,
		Embedding:      s.db.manifest.Embedding,
		Peers:          append([]string(nil), s.db.peers...),
	}
	return nil
}

func (s *rpcServer) EnsureCollection(req RPCEnsureCollectionRequest, _ *RPCEmpty) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	var coll *Collection
	if req.BitWidth != 0 {
		coll = s.db.Collection(req.Name, WithBitWidth(req.BitWidth))
	} else {
		coll = s.db.Collection(req.Name)
	}
	return coll.err
}

func (s *rpcServer) Put(req RPCPutRequest, _ *RPCEmpty) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	return s.db.Collection(req.Collection).Put(req.ID, req.Entry)
}

func (s *rpcServer) PutVector(req RPCPutVectorRequest, _ *RPCEmpty) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	opts := make([]PutVectorOption, 0, 2)
	if req.Text != "" {
		opts = append(opts, WithText(req.Text))
	}
	if len(req.Metadata) > 0 {
		opts = append(opts, WithMetadata(req.Metadata))
	}
	return s.db.Collection(req.Collection).PutVector(req.ID, req.Vector, opts...)
}

func (s *rpcServer) Search(req RPCSearchRequest, resp *RPCSearchResponse) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	coll := s.db.Collection(req.Collection)
	if req.UseAt {
		results, err := coll.At(req.AtLamport).Search(req.Query, req.K, fromRPCFilters(req.Filters)...)
		resp.Results = results
		return err
	}
	results, err := coll.Search(req.Query, req.K, fromRPCFilters(req.Filters)...)
	resp.Results = results
	return err
}

func (s *rpcServer) SearchVector(req RPCSearchVectorRequest, resp *RPCSearchResponse) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	coll := s.db.Collection(req.Collection)
	if req.UseAt {
		results, err := coll.At(req.AtLamport).SearchVector(req.Query, req.K, fromRPCFilters(req.Filters)...)
		resp.Results = results
		return err
	}
	results, err := coll.SearchVector(req.Query, req.K, fromRPCFilters(req.Filters)...)
	resp.Results = results
	return err
}

func (s *rpcServer) History(req RPCHistoryRequest, resp *RPCHistoryResponse) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	coll := s.db.Collection(req.Collection)
	if req.UseAt {
		versions, err := coll.At(req.AtLamport).History(req.ID)
		resp.Versions = versions
		return err
	}
	versions, err := coll.History(req.ID)
	resp.Versions = versions
	return err
}

func (s *rpcServer) Delete(req RPCDeleteRequest, _ *RPCEmpty) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	return s.db.Collection(req.Collection).Delete(req.ID)
}

func (s *rpcServer) authorize(token string) error {
	if s.db.token == "" {
		return nil
	}
	if token != s.db.token {
		return ErrUnauthorized
	}
	return nil
}

func toRPCFilters(filters []FilterOption) []RPCFilter {
	if len(filters) == 0 {
		return nil
	}
	out := make([]RPCFilter, len(filters))
	for i, filter := range filters {
		out[i] = RPCFilter{Key: filter.key, Value: filter.value}
	}
	return out
}

func fromRPCFilters(filters []RPCFilter) []FilterOption {
	if len(filters) == 0 {
		return nil
	}
	out := make([]FilterOption, len(filters))
	for i, filter := range filters {
		out[i] = Filter(filter.Key, filter.Value)
	}
	return out
}

var _ io.Closer = (*rpcClient)(nil)
