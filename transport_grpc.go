package corkscrewdb

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"strings"
	"time"

	grpcapi "github.com/odvcencio/corkscrewdb/grpc"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultGRPCDialTimeout        = 5 * time.Second
	defaultGRPCMaxMessage         = 256 << 20
	defaultReplicaStreamHeartbeat = 2 * time.Second
)

type grpcClient struct {
	conn   *grpcgo.ClientConn
	client grpcapi.CorkScrewDBClient
	token  string
}

func newGRPCClient(addr, token string) (*grpcClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultGRPCDialTimeout)
	defer cancel()

	conn, err := grpcgo.DialContext(
		ctx,
		addr,
		grpcgo.WithBlock(),
		grpcgo.WithTransportCredentials(insecure.NewCredentials()),
		grpcgo.WithDefaultCallOptions(
			grpcgo.MaxCallRecvMsgSize(defaultGRPCMaxMessage),
			grpcgo.MaxCallSendMsgSize(defaultGRPCMaxMessage),
		),
	)
	if err != nil {
		return nil, err
	}
	return &grpcClient{
		conn:   conn,
		client: grpcapi.NewCorkScrewDBClient(conn),
		token:  token,
	}, nil
}

func (c *grpcClient) Close() error {
	return c.conn.Close()
}

func (c *grpcClient) Info() (RPCInfoResponse, error) {
	resp, err := c.client.Info(context.Background(), &grpcapi.InfoRequest{Token: c.token})
	if err != nil {
		return RPCInfoResponse{}, normalizeTransportError(err)
	}
	return fromProtoInfoResponse(resp), nil
}

func (c *grpcClient) EnsureCollection(name string, bitWidth int) error {
	bits, err := int32FromInt(bitWidth, "bit width")
	if err != nil {
		return err
	}
	_, err = c.client.EnsureCollection(context.Background(), &grpcapi.EnsureCollectionRequest{
		Token:    c.token,
		Name:     name,
		BitWidth: bits,
	})
	return normalizeTransportError(err)
}

func (c *grpcClient) DropCollection(name string) error {
	_, err := c.client.DropCollection(context.Background(), &grpcapi.DropCollectionRequest{
		Token: c.token,
		Name:  name,
	})
	return normalizeTransportError(err)
}

func (c *grpcClient) Put(collection, id string, entry Entry, internal bool) error {
	_, err := c.client.Put(context.Background(), &grpcapi.PutRequest{
		Token:      c.token,
		Internal:   internal,
		Collection: collection,
		Id:         id,
		Entry:      toProtoEntry(entry),
	})
	return normalizeTransportError(err)
}

func (c *grpcClient) PutVector(collection, id string, vector []float32, text string, metadata map[string]string, internal bool) error {
	_, err := c.client.PutVector(context.Background(), &grpcapi.PutVectorRequest{
		Token:      c.token,
		Internal:   internal,
		Collection: collection,
		Id:         id,
		Vector:     cloneVector(vector),
		Text:       text,
		Metadata:   cloneMetadata(metadata),
	})
	return normalizeTransportError(err)
}

func (c *grpcClient) Search(collection, query string, k int, filters []FilterOption, useAt bool, atLamport uint64, internal bool) ([]SearchResult, error) {
	limit, err := int32FromInt(k, "search k")
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Search(context.Background(), &grpcapi.SearchRequest{
		Token:      c.token,
		Internal:   internal,
		Collection: collection,
		Query:      query,
		K:          limit,
		Filters:    toProtoRPCFilters(toRPCFilters(filters)),
		UseAt:      useAt,
		AtLamport:  atLamport,
	})
	if err != nil {
		return nil, normalizeTransportError(err)
	}
	return fromProtoSearchResults(resp.GetResults()), nil
}

func (c *grpcClient) SearchVector(collection string, query []float32, k int, filters []FilterOption, useAt bool, atLamport uint64, internal bool) ([]SearchResult, error) {
	limit, err := int32FromInt(k, "search k")
	if err != nil {
		return nil, err
	}
	resp, err := c.client.SearchVector(context.Background(), &grpcapi.SearchVectorRequest{
		Token:      c.token,
		Internal:   internal,
		Collection: collection,
		Query:      cloneVector(query),
		K:          limit,
		Filters:    toProtoRPCFilters(toRPCFilters(filters)),
		UseAt:      useAt,
		AtLamport:  atLamport,
	})
	if err != nil {
		return nil, normalizeTransportError(err)
	}
	return fromProtoSearchResults(resp.GetResults()), nil
}

func (c *grpcClient) History(collection, id string, useAt bool, atLamport uint64, internal bool) ([]Version, error) {
	resp, err := c.client.History(context.Background(), &grpcapi.HistoryRequest{
		Token:      c.token,
		Internal:   internal,
		Collection: collection,
		Id:         id,
		UseAt:      useAt,
		AtLamport:  atLamport,
	})
	if err != nil {
		return nil, normalizeTransportError(err)
	}
	return fromProtoVersions(resp.GetVersions()), nil
}

func (c *grpcClient) Delete(collection, id string, internal bool) error {
	_, err := c.client.Delete(context.Background(), &grpcapi.DeleteRequest{
		Token:      c.token,
		Internal:   internal,
		Collection: collection,
		Id:         id,
	})
	return normalizeTransportError(err)
}

func (c *grpcClient) PullEntries(req RPCPullEntriesRequest) (RPCPullEntriesResponse, error) {
	maxEntries, err := int32FromInt(req.MaxEntries, "max entries")
	if err != nil {
		return RPCPullEntriesResponse{}, err
	}
	resp, err := c.client.PullEntries(context.Background(), &grpcapi.PullEntriesRequest{
		Token:      c.token,
		Collection: req.Collection,
		SinceClock: req.SinceClock,
		MaxEntries: maxEntries,
	})
	if err != nil {
		return RPCPullEntriesResponse{}, normalizeTransportError(err)
	}
	return fromProtoPullEntriesResponse(resp), nil
}

func (c *grpcClient) StreamEntries(ctx context.Context, req RPCPullEntriesRequest, onResponse func(RPCPullEntriesResponse) error) error {
	maxEntries, err := int32FromInt(req.MaxEntries, "max entries")
	if err != nil {
		return err
	}
	stream, err := c.client.StreamEntries(ctx, &grpcapi.PullEntriesRequest{
		Token:      c.token,
		Collection: req.Collection,
		SinceClock: req.SinceClock,
		MaxEntries: maxEntries,
	})
	if err != nil {
		return normalizeTransportError(err)
	}
	for {
		resp, err := stream.Recv()
		if err == nil {
			if onResponse == nil {
				continue
			}
			if callErr := onResponse(fromProtoPullEntriesResponse(resp)); callErr != nil {
				return callErr
			}
			continue
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		return normalizeTransportError(err)
	}
}

func (c *grpcClient) PullSnapshot(req RPCPullSnapshotRequest) (RPCPullSnapshotResponse, error) {
	resp, err := c.client.PullSnapshot(context.Background(), &grpcapi.PullSnapshotRequest{
		Token:      c.token,
		Collection: req.Collection,
	})
	if err != nil {
		return RPCPullSnapshotResponse{}, normalizeTransportError(err)
	}
	return fromProtoPullSnapshotResponse(resp), nil
}

func (c *grpcClient) PrepareRebalance(shards []ShardAssignment) error {
	_, err := c.client.PrepareRebalance(context.Background(), &grpcapi.RebalanceRequest{
		Token:  c.token,
		Shards: toProtoShardAssignments(shards),
	})
	return normalizeTransportError(err)
}

func (c *grpcClient) CommitRebalance(shards []ShardAssignment) error {
	_, err := c.client.CommitRebalance(context.Background(), &grpcapi.RebalanceRequest{
		Token:  c.token,
		Shards: toProtoShardAssignments(shards),
	})
	return normalizeTransportError(err)
}

func (c *grpcClient) PruneRebalance(shards []ShardAssignment) error {
	_, err := c.client.PruneRebalance(context.Background(), &grpcapi.RebalanceRequest{
		Token:  c.token,
		Shards: toProtoShardAssignments(shards),
	})
	return normalizeTransportError(err)
}

type grpcServer struct {
	handler *transportServer
}

func serveGRPC(db *DB, listener net.Listener) error {
	server := grpcgo.NewServer(
		grpcgo.MaxRecvMsgSize(defaultGRPCMaxMessage),
		grpcgo.MaxSendMsgSize(defaultGRPCMaxMessage),
	)
	defer server.Stop()

	grpcapi.RegisterCorkScrewDBServer(server, &grpcServer{
		handler: &transportServer{db: db},
	})

	err := server.Serve(listener)
	if errors.Is(err, grpcgo.ErrServerStopped) || isClosedListenerError(err) {
		return nil
	}
	return err
}

func (s *grpcServer) Info(_ context.Context, req *grpcapi.InfoRequest) (*grpcapi.InfoResponse, error) {
	var resp RPCInfoResponse
	if err := s.handler.Info(RPCInfoRequest{Token: req.GetToken()}, &resp); err != nil {
		return nil, grpcStatusError(err)
	}
	return toProtoInfoResponse(resp), nil
}

func (s *grpcServer) EnsureCollection(_ context.Context, req *grpcapi.EnsureCollectionRequest) (*grpcapi.Empty, error) {
	if err := s.handler.EnsureCollection(RPCEnsureCollectionRequest{
		Token:    req.GetToken(),
		Name:     req.GetName(),
		BitWidth: int(req.GetBitWidth()),
	}, &RPCEmpty{}); err != nil {
		return nil, grpcStatusError(err)
	}
	return &grpcapi.Empty{}, nil
}

func (s *grpcServer) DropCollection(_ context.Context, req *grpcapi.DropCollectionRequest) (*grpcapi.Empty, error) {
	if err := s.handler.DropCollection(RPCEnsureCollectionRequest{
		Token: req.GetToken(),
		Name:  req.GetName(),
	}, &RPCEmpty{}); err != nil {
		return nil, grpcStatusError(err)
	}
	return &grpcapi.Empty{}, nil
}

func (s *grpcServer) Put(_ context.Context, req *grpcapi.PutRequest) (*grpcapi.Empty, error) {
	if err := s.handler.Put(RPCPutRequest{
		Token:      req.GetToken(),
		Internal:   req.GetInternal(),
		Collection: req.GetCollection(),
		ID:         req.GetId(),
		Entry:      fromProtoEntry(req.GetEntry()),
	}, &RPCEmpty{}); err != nil {
		return nil, grpcStatusError(err)
	}
	return &grpcapi.Empty{}, nil
}

func (s *grpcServer) PutVector(_ context.Context, req *grpcapi.PutVectorRequest) (*grpcapi.Empty, error) {
	if err := s.handler.PutVector(RPCPutVectorRequest{
		Token:      req.GetToken(),
		Internal:   req.GetInternal(),
		Collection: req.GetCollection(),
		ID:         req.GetId(),
		Vector:     cloneVector(req.GetVector()),
		Text:       req.GetText(),
		Metadata:   cloneMetadata(req.GetMetadata()),
	}, &RPCEmpty{}); err != nil {
		return nil, grpcStatusError(err)
	}
	return &grpcapi.Empty{}, nil
}

func (s *grpcServer) Search(_ context.Context, req *grpcapi.SearchRequest) (*grpcapi.SearchResponse, error) {
	var resp RPCSearchResponse
	if err := s.handler.Search(RPCSearchRequest{
		Token:      req.GetToken(),
		Internal:   req.GetInternal(),
		Collection: req.GetCollection(),
		Query:      req.GetQuery(),
		K:          int(req.GetK()),
		Filters:    fromProtoRPCFilters(req.GetFilters()),
		UseAt:      req.GetUseAt(),
		AtLamport:  req.GetAtLamport(),
	}, &resp); err != nil {
		return nil, grpcStatusError(err)
	}
	return &grpcapi.SearchResponse{Results: toProtoSearchResults(resp.Results)}, nil
}

func (s *grpcServer) SearchVector(_ context.Context, req *grpcapi.SearchVectorRequest) (*grpcapi.SearchResponse, error) {
	var resp RPCSearchResponse
	if err := s.handler.SearchVector(RPCSearchVectorRequest{
		Token:      req.GetToken(),
		Internal:   req.GetInternal(),
		Collection: req.GetCollection(),
		Query:      cloneVector(req.GetQuery()),
		K:          int(req.GetK()),
		Filters:    fromProtoRPCFilters(req.GetFilters()),
		UseAt:      req.GetUseAt(),
		AtLamport:  req.GetAtLamport(),
	}, &resp); err != nil {
		return nil, grpcStatusError(err)
	}
	return &grpcapi.SearchResponse{Results: toProtoSearchResults(resp.Results)}, nil
}

func (s *grpcServer) History(_ context.Context, req *grpcapi.HistoryRequest) (*grpcapi.HistoryResponse, error) {
	var resp RPCHistoryResponse
	if err := s.handler.History(RPCHistoryRequest{
		Token:      req.GetToken(),
		Internal:   req.GetInternal(),
		Collection: req.GetCollection(),
		ID:         req.GetId(),
		UseAt:      req.GetUseAt(),
		AtLamport:  req.GetAtLamport(),
	}, &resp); err != nil {
		return nil, grpcStatusError(err)
	}
	return &grpcapi.HistoryResponse{Versions: toProtoVersions(resp.Versions)}, nil
}

func (s *grpcServer) Delete(_ context.Context, req *grpcapi.DeleteRequest) (*grpcapi.Empty, error) {
	if err := s.handler.Delete(RPCDeleteRequest{
		Token:      req.GetToken(),
		Internal:   req.GetInternal(),
		Collection: req.GetCollection(),
		ID:         req.GetId(),
	}, &RPCEmpty{}); err != nil {
		return nil, grpcStatusError(err)
	}
	return &grpcapi.Empty{}, nil
}

func (s *grpcServer) PullEntries(_ context.Context, req *grpcapi.PullEntriesRequest) (*grpcapi.PullEntriesResponse, error) {
	resp, err := s.handler.pullEntries(RPCPullEntriesRequest{
		Token:      req.GetToken(),
		Collection: req.GetCollection(),
		SinceClock: req.GetSinceClock(),
		MaxEntries: int(req.GetMaxEntries()),
	})
	if err != nil {
		return nil, grpcStatusError(err)
	}
	return toProtoPullEntriesResponse(resp), nil
}

func (s *grpcServer) StreamEntries(req *grpcapi.PullEntriesRequest, stream grpcapi.CorkScrewDB_StreamEntriesServer) error {
	current := req.GetSinceClock()
	for {
		resp, err := s.handler.pullEntriesBlocking(RPCPullEntriesRequest{
			Token:      req.GetToken(),
			Collection: req.GetCollection(),
			SinceClock: current,
			MaxEntries: int(req.GetMaxEntries()),
		}, defaultReplicaStreamHeartbeat)
		if err != nil {
			return grpcStatusError(err)
		}
		if err := stream.Send(toProtoPullEntriesResponse(resp)); err != nil {
			return err
		}
		if resp.LatestClock > current {
			current = resp.LatestClock
		}
		if err := stream.Context().Err(); err != nil {
			return err
		}
	}
}

func (s *grpcServer) PullSnapshot(_ context.Context, req *grpcapi.PullSnapshotRequest) (*grpcapi.PullSnapshotResponse, error) {
	var resp RPCPullSnapshotResponse
	if err := s.handler.PullSnapshot(RPCPullSnapshotRequest{
		Token:      req.GetToken(),
		Collection: req.GetCollection(),
	}, &resp); err != nil {
		return nil, grpcStatusError(err)
	}
	return toProtoPullSnapshotResponse(resp), nil
}

func (s *grpcServer) PrepareRebalance(_ context.Context, req *grpcapi.RebalanceRequest) (*grpcapi.Empty, error) {
	if err := s.handler.PrepareRebalance(RPCRebalanceRequest{
		Token:  req.GetToken(),
		Shards: fromProtoShardAssignments(req.GetShards()),
	}, &RPCEmpty{}); err != nil {
		return nil, grpcStatusError(err)
	}
	return &grpcapi.Empty{}, nil
}

func (s *grpcServer) CommitRebalance(_ context.Context, req *grpcapi.RebalanceRequest) (*grpcapi.Empty, error) {
	if err := s.handler.CommitRebalance(RPCRebalanceRequest{
		Token:  req.GetToken(),
		Shards: fromProtoShardAssignments(req.GetShards()),
	}, &RPCEmpty{}); err != nil {
		return nil, grpcStatusError(err)
	}
	return &grpcapi.Empty{}, nil
}

func (s *grpcServer) PruneRebalance(_ context.Context, req *grpcapi.RebalanceRequest) (*grpcapi.Empty, error) {
	if err := s.handler.PruneRebalance(RPCRebalanceRequest{
		Token:  req.GetToken(),
		Shards: fromProtoShardAssignments(req.GetShards()),
	}, &RPCEmpty{}); err != nil {
		return nil, grpcStatusError(err)
	}
	return &grpcapi.Empty{}, nil
}

func int32FromInt(v int, label string) (int32, error) {
	if v < math.MinInt32 || v > math.MaxInt32 {
		return 0, errors.New("corkscrewdb: " + label + " out of range")
	}
	return int32(v), nil
}

func grpcStatusError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrUnauthorized) {
		return status.Error(codes.Unauthenticated, ErrUnauthorized.Error())
	}
	return status.Error(codes.Unknown, err.Error())
}

func normalizeTransportError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() {
	case codes.OK:
		return nil
	case codes.Unauthenticated:
		return ErrUnauthorized
	default:
		return errors.New(st.Message())
	}
}

func isClosedListenerError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "use of closed network connection")
}

func toProtoEntry(entry Entry) *grpcapi.Entry {
	return &grpcapi.Entry{
		Text:     entry.Text,
		Vector:   cloneVector(entry.Vector),
		Metadata: cloneMetadata(entry.Metadata),
	}
}

func fromProtoEntry(entry *grpcapi.Entry) Entry {
	if entry == nil {
		return Entry{}
	}
	return Entry{
		Text:     entry.GetText(),
		Vector:   cloneVector(entry.GetVector()),
		Metadata: cloneMetadata(entry.GetMetadata()),
	}
}

func toProtoInfoResponse(resp RPCInfoResponse) *grpcapi.InfoResponse {
	return &grpcapi.InfoResponse{
		PackageVersion: resp.PackageVersion,
		Embedding: &grpcapi.EmbeddingConfig{
			Id:  resp.Embedding.ID,
			Dim: int32(resp.Embedding.Dim),
		},
		Peers:       append([]string(nil), resp.Peers...),
		Collections: toProtoCollectionInfo(resp.Collections),
		Shards:      toProtoShardAssignments(resp.Shards),
	}
}

func fromProtoInfoResponse(resp *grpcapi.InfoResponse) RPCInfoResponse {
	if resp == nil {
		return RPCInfoResponse{}
	}
	info := RPCInfoResponse{
		PackageVersion: resp.GetPackageVersion(),
		Peers:          append([]string(nil), resp.GetPeers()...),
		Collections:    fromProtoCollectionInfo(resp.GetCollections()),
		Shards:         fromProtoShardAssignments(resp.GetShards()),
	}
	if embedding := resp.GetEmbedding(); embedding != nil {
		info.Embedding = embeddingConfig{
			ID:  embedding.GetId(),
			Dim: int(embedding.GetDim()),
		}
	}
	return info
}

func toProtoCollectionInfo(infos []RPCCollectionInfo) []*grpcapi.CollectionInfo {
	if len(infos) == 0 {
		return nil
	}
	out := make([]*grpcapi.CollectionInfo, len(infos))
	for i, info := range infos {
		out[i] = &grpcapi.CollectionInfo{
			Name:     info.Name,
			BitWidth: int32(info.BitWidth),
		}
	}
	return out
}

func fromProtoCollectionInfo(infos []*grpcapi.CollectionInfo) []RPCCollectionInfo {
	if len(infos) == 0 {
		return nil
	}
	out := make([]RPCCollectionInfo, len(infos))
	for i, info := range infos {
		out[i] = RPCCollectionInfo{
			Name:     info.GetName(),
			BitWidth: int(info.GetBitWidth()),
		}
	}
	return out
}

func toProtoShardAssignments(shards []ShardAssignment) []*grpcapi.ShardAssignment {
	if len(shards) == 0 {
		return nil
	}
	out := make([]*grpcapi.ShardAssignment, len(shards))
	for i, shard := range shards {
		out[i] = &grpcapi.ShardAssignment{
			Id:    shard.ID,
			Owner: shard.Owner,
			Start: shard.Start,
			End:   shard.End,
		}
	}
	return out
}

func fromProtoShardAssignments(shards []*grpcapi.ShardAssignment) []ShardAssignment {
	if len(shards) == 0 {
		return nil
	}
	out := make([]ShardAssignment, len(shards))
	for i, shard := range shards {
		out[i] = ShardAssignment{
			ID:    shard.GetId(),
			Owner: shard.GetOwner(),
			Start: shard.GetStart(),
			End:   shard.GetEnd(),
		}
	}
	return out
}

func toProtoRPCFilters(filters []RPCFilter) []*grpcapi.Filter {
	if len(filters) == 0 {
		return nil
	}
	out := make([]*grpcapi.Filter, len(filters))
	for i, filter := range filters {
		out[i] = &grpcapi.Filter{
			Key:   filter.Key,
			Value: filter.Value,
		}
	}
	return out
}

func fromProtoRPCFilters(filters []*grpcapi.Filter) []RPCFilter {
	if len(filters) == 0 {
		return nil
	}
	out := make([]RPCFilter, len(filters))
	for i, filter := range filters {
		out[i] = RPCFilter{
			Key:   filter.GetKey(),
			Value: filter.GetValue(),
		}
	}
	return out
}

func toProtoSearchResults(results []SearchResult) []*grpcapi.SearchResult {
	if len(results) == 0 {
		return nil
	}
	out := make([]*grpcapi.SearchResult, len(results))
	for i, result := range results {
		out[i] = &grpcapi.SearchResult{
			Id:       result.ID,
			Score:    result.Score,
			Text:     result.Text,
			Metadata: cloneMetadata(result.Metadata),
			Version:  result.Version,
		}
	}
	return out
}

func fromProtoSearchResults(results []*grpcapi.SearchResult) []SearchResult {
	if len(results) == 0 {
		return nil
	}
	out := make([]SearchResult, len(results))
	for i, result := range results {
		out[i] = SearchResult{
			ID:       result.GetId(),
			Score:    result.GetScore(),
			Text:     result.GetText(),
			Metadata: cloneMetadata(result.GetMetadata()),
			Version:  result.GetVersion(),
		}
	}
	return out
}

func toProtoVersions(versions []Version) []*grpcapi.Version {
	if len(versions) == 0 {
		return nil
	}
	out := make([]*grpcapi.Version, len(versions))
	for i, version := range versions {
		out[i] = &grpcapi.Version{
			Embedding:    cloneVector(version.Embedding),
			Text:         version.Text,
			Metadata:     cloneMetadata(version.Metadata),
			LamportClock: version.LamportClock,
			ActorId:      version.ActorID,
			WallClock:    toProtoTimestamp(version.WallClock),
			Tombstone:    version.Tombstone,
		}
	}
	return out
}

func fromProtoVersions(versions []*grpcapi.Version) []Version {
	if len(versions) == 0 {
		return nil
	}
	out := make([]Version, len(versions))
	for i, version := range versions {
		out[i] = Version{
			Embedding:    cloneVector(version.GetEmbedding()),
			Text:         version.GetText(),
			Metadata:     cloneMetadata(version.GetMetadata()),
			LamportClock: version.GetLamportClock(),
			ActorID:      version.GetActorId(),
			WallClock:    fromProtoTimestamp(version.GetWallClock()),
			Tombstone:    version.GetTombstone(),
		}
	}
	return out
}

func toProtoPullEntriesResponse(resp RPCPullEntriesResponse) *grpcapi.PullEntriesResponse {
	out := &grpcapi.PullEntriesResponse{
		LatestClock: resp.LatestClock,
		HasMore:     resp.HasMore,
	}
	if len(resp.Entries) == 0 {
		return out
	}
	out.Entries = make([]*grpcapi.ReplicaEntry, len(resp.Entries))
	for i, entry := range resp.Entries {
		out.Entries[i] = &grpcapi.ReplicaEntry{
			Kind:         uint32(entry.Kind),
			CollectionId: entry.CollectionID,
			VectorId:     entry.VectorID,
			Embedding:    cloneVector(entry.Embedding),
			Text:         entry.Text,
			Metadata:     cloneMetadata(entry.Metadata),
			LamportClock: entry.LamportClock,
			ActorId:      entry.ActorID,
			WallClock:    toProtoTimestamp(entry.WallClock),
		}
	}
	return out
}

func fromProtoPullEntriesResponse(resp *grpcapi.PullEntriesResponse) RPCPullEntriesResponse {
	if resp == nil {
		return RPCPullEntriesResponse{}
	}
	out := RPCPullEntriesResponse{
		LatestClock: resp.GetLatestClock(),
		HasMore:     resp.GetHasMore(),
	}
	if len(resp.GetEntries()) == 0 {
		return out
	}
	out.Entries = make([]RPCReplicaEntry, len(resp.GetEntries()))
	for i, entry := range resp.GetEntries() {
		out.Entries[i] = RPCReplicaEntry{
			Kind:         uint8(entry.GetKind()),
			CollectionID: entry.GetCollectionId(),
			VectorID:     entry.GetVectorId(),
			Embedding:    cloneVector(entry.GetEmbedding()),
			Text:         entry.GetText(),
			Metadata:     cloneMetadata(entry.GetMetadata()),
			LamportClock: entry.GetLamportClock(),
			ActorID:      entry.GetActorId(),
			WallClock:    fromProtoTimestamp(entry.GetWallClock()),
		}
	}
	return out
}

func toProtoPullSnapshotResponse(resp RPCPullSnapshotResponse) *grpcapi.PullSnapshotResponse {
	out := &grpcapi.PullSnapshotResponse{
		Collection: resp.Collection,
		BitWidth:   int32(resp.BitWidth),
		Seed:       resp.Seed,
		Dim:        int32(resp.Dim),
		MaxLamport: resp.MaxLamport,
	}
	if len(resp.Records) == 0 {
		return out
	}
	out.Records = make([]*grpcapi.SnapshotRecord, len(resp.Records))
	for i, record := range resp.Records {
		item := &grpcapi.SnapshotRecord{
			Id:       record.ID,
			Versions: make([]*grpcapi.SnapshotVersion, len(record.Versions)),
		}
		for j, version := range record.Versions {
			item.Versions[j] = &grpcapi.SnapshotVersion{
				Embedding:    cloneVector(version.Embedding),
				Text:         version.Text,
				Metadata:     cloneMetadata(version.Metadata),
				LamportClock: version.LamportClock,
				ActorId:      version.ActorID,
				WallClock:    toProtoTimestamp(version.WallClock),
				Tombstone:    version.Tombstone,
			}
		}
		out.Records[i] = item
	}
	return out
}

func fromProtoPullSnapshotResponse(resp *grpcapi.PullSnapshotResponse) RPCPullSnapshotResponse {
	if resp == nil {
		return RPCPullSnapshotResponse{}
	}
	out := RPCPullSnapshotResponse{
		Collection: resp.GetCollection(),
		BitWidth:   int(resp.GetBitWidth()),
		Seed:       resp.GetSeed(),
		Dim:        int(resp.GetDim()),
		MaxLamport: resp.GetMaxLamport(),
	}
	if len(resp.GetRecords()) == 0 {
		return out
	}
	out.Records = make([]RPCSnapshotRecord, len(resp.GetRecords()))
	for i, record := range resp.GetRecords() {
		item := RPCSnapshotRecord{
			ID:       record.GetId(),
			Versions: make([]RPCSnapshotVersion, len(record.GetVersions())),
		}
		for j, version := range record.GetVersions() {
			item.Versions[j] = RPCSnapshotVersion{
				Embedding:    cloneVector(version.GetEmbedding()),
				Text:         version.GetText(),
				Metadata:     cloneMetadata(version.GetMetadata()),
				LamportClock: version.GetLamportClock(),
				ActorID:      version.GetActorId(),
				WallClock:    fromProtoTimestamp(version.GetWallClock()),
				Tombstone:    version.GetTombstone(),
			}
		}
		out.Records[i] = item
	}
	return out
}

func toProtoTimestamp(ts time.Time) *timestamppb.Timestamp {
	if ts.IsZero() {
		return nil
	}
	return timestamppb.New(ts)
}

func fromProtoTimestamp(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime().UTC()
}

var _ remoteClient = (*grpcClient)(nil)
