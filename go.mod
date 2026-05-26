module m31labs.dev/corkscrewdb

go 1.25.1

require (
	google.golang.org/grpc v1.80.0
	google.golang.org/protobuf v1.36.11
	m31labs.dev/manta v0.0.13
	m31labs.dev/turboquant v0.2.0
)

require (
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/odvcencio/gotreesitter v0.15.2 // indirect
	golang.org/x/net v0.49.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
	golang.org/x/text v0.33.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260120221211-b8f7ae30c516 // indirect
	lukechampine.com/blake3 v1.4.1 // indirect
	m31labs.dev/mll v0.1.0 // indirect
)

replace m31labs.dev/mll => ../mll

replace m31labs.dev/manta => ../barracuda

replace m31labs.dev/turboquant => ../turboquant
