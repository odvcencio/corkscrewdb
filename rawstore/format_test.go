package rawstore

import (
	"bytes"
	"errors"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	val := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	var buf bytes.Buffer
	if err := writeSegmentHeader(&buf); err != nil {
		t.Fatal(err)
	}
	if err := writeFrame(&buf, key, val); err != nil {
		t.Fatal(err)
	}
	r := bytes.NewReader(buf.Bytes())
	if err := readSegmentHeader(r); err != nil {
		t.Fatal(err)
	}
	gotKey, gotVal, _, err := readFrame(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotKey, key) || !bytes.Equal(gotVal, val) {
		t.Fatalf("frame mismatch: key=%x val=%x", gotKey, gotVal)
	}
}

func TestSegmentHeaderFloorGuard(t *testing.T) {
	bad := []byte{0x00, 0x00, 0x00, 0x00, 0x00} // wrong magic
	if err := readSegmentHeader(bytes.NewReader(bad)); !errors.Is(err, ErrRVSFormat) {
		t.Fatalf("want ErrRVSFormat, got %v", err)
	}
}
