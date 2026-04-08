package corkscrewdb

import (
	"fmt"
	"sort"
	"strings"
)

// LocalShardOwner marks a shard assignment as belonging to the local node.
const LocalShardOwner = "self"

// ShardAssignment defines one contiguous ownership range on the hash ring.
type ShardAssignment struct {
	ID    string `json:"id"`
	Owner string `json:"owner"`
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

func cloneShardAssignments(shards []ShardAssignment) []ShardAssignment {
	if len(shards) == 0 {
		return nil
	}
	out := make([]ShardAssignment, len(shards))
	copy(out, shards)
	return out
}

func normalizeShardAssignments(shards []ShardAssignment) ([]ShardAssignment, error) {
	if len(shards) == 0 {
		return nil, nil
	}
	out := cloneShardAssignments(shards)
	for i := range out {
		out[i].ID = strings.TrimSpace(out[i].ID)
		out[i].Owner = strings.TrimSpace(out[i].Owner)
		if out[i].Owner == "" {
			return nil, fmt.Errorf("corkscrewdb: shard %d owner is required", i)
		}
		if out[i].Start > out[i].End {
			return nil, fmt.Errorf("corkscrewdb: shard %q has invalid range %d..%d", out[i].ID, out[i].Start, out[i].End)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Start != out[j].Start {
			return out[i].Start < out[j].Start
		}
		if out[i].End != out[j].End {
			return out[i].End < out[j].End
		}
		if out[i].Owner != out[j].Owner {
			return out[i].Owner < out[j].Owner
		}
		return out[i].ID < out[j].ID
	})

	seenIDs := make(map[string]struct{}, len(out))
	for i := range out {
		if out[i].ID == "" {
			out[i].ID = fmt.Sprintf("shard-%02d", i)
		}
		if _, ok := seenIDs[out[i].ID]; ok {
			return nil, fmt.Errorf("corkscrewdb: duplicate shard id %q", out[i].ID)
		}
		seenIDs[out[i].ID] = struct{}{}
		if i == 0 {
			if out[i].Start != 0 {
				return nil, fmt.Errorf("corkscrewdb: shard layout must start at 0, got %d", out[i].Start)
			}
			continue
		}
		prev := out[i-1]
		if prev.End == ^uint64(0) {
			return nil, fmt.Errorf("corkscrewdb: shard %q extends to max uint64 before layout ends", prev.ID)
		}
		expected := prev.End + 1
		if out[i].Start != expected {
			return nil, fmt.Errorf("corkscrewdb: shard %q starts at %d, want %d", out[i].ID, out[i].Start, expected)
		}
	}
	if out[len(out)-1].End != ^uint64(0) {
		return nil, fmt.Errorf("corkscrewdb: shard layout must end at %d, got %d", ^uint64(0), out[len(out)-1].End)
	}
	return out, nil
}

func peersFromShardAssignments(shards []ShardAssignment) []string {
	if len(shards) == 0 {
		return nil
	}
	out := make([]string, 0, len(shards))
	seen := make(map[string]struct{}, len(shards))
	for _, shard := range shards {
		owner := strings.TrimSpace(shard.Owner)
		if owner == "" || owner == LocalShardOwner {
			continue
		}
		if _, ok := seen[owner]; ok {
			continue
		}
		seen[owner] = struct{}{}
		out = append(out, owner)
	}
	sort.Strings(out)
	return out
}
