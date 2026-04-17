package main

import (
	"reflect"
	"testing"
)

func TestBuildArgs(t *testing.T) {
	base := []string{"-data=/var/lib/corkscrewdb", "-addr=0.0.0.0:4040"}
	peers := "corkscrewdb-0.corkscrewdb.m31labs.svc.cluster.local:4040,corkscrewdb-1.corkscrewdb.m31labs.svc.cluster.local:4040"
	replicateCols := "agent-memory,mirage-runs"

	cases := []struct {
		name     string
		hostname string
		want     []string
	}{
		{
			name:     "primary pod (corkscrewdb-0)",
			hostname: "corkscrewdb-0",
			want:     append(append([]string{}, base...), "-token=T", "-peers="+peers),
		},
		{
			name:     "replica pod (corkscrewdb-1)",
			hostname: "corkscrewdb-1",
			want: append(append([]string{}, base...),
				"-token=T",
				"-peers="+peers,
				"-replicate-from=corkscrewdb-0.corkscrewdb.m31labs.svc.cluster.local:4040",
				"-replicate-collections="+replicateCols),
		},
		{
			name:     "any non-zero ordinal (corkscrewdb-2 hypothetical)",
			hostname: "corkscrewdb-2",
			want: append(append([]string{}, base...),
				"-token=T",
				"-peers="+peers,
				"-replicate-from=corkscrewdb-0.corkscrewdb.m31labs.svc.cluster.local:4040",
				"-replicate-collections="+replicateCols),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildArgs(tc.hostname, "T", peers, replicateCols)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v\nwant %v", got, tc.want)
			}
		})
	}
}
