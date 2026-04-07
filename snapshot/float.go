package snapshot

import "math"

func mathFloat32bits(v float32) uint32 {
	return math.Float32bits(v)
}

func mathFloat32frombits(v uint32) float32 {
	return math.Float32frombits(v)
}
