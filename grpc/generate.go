package grpcapi

//go:generate protoc -I ../proto --go_out=.. --go_opt=module=github.com/odvcencio/corkscrewdb --go-grpc_out=.. --go-grpc_opt=module=github.com/odvcencio/corkscrewdb,require_unimplemented_servers=false ../proto/corkscrewdb.proto
