package snapshot

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

const (
	maxCompactOrdinalChildren        = 1 << 20
	maxCompactChildBlockBytes uint64 = 512 << 20

	// maxSnapshotFieldBytes bounds any single length-prefixed field in a
	// snapshot.  Snapshots are full-collection persisted state, so they can
	// legitimately be larger than a single WAL segment; we allow a generous
	// 64 MiB per field — far above any plausible ID, text, or JSON blob.
	// An inflated prefix above this ceiling is corruption, not a valid record.
	maxSnapshotFieldBytes = uint32(64 << 20)
)

func LoadFile(path string) (Data, error) {
	file, err := os.Open(path)
	if err != nil {
		return Data{}, err
	}
	defer file.Close()
	return read(file)
}

func FindLatestFile(dir string) (string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "snapshot-*.csdb"))
	if err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return "", nil
	}
	sort.Strings(matches)
	return matches[len(matches)-1], nil
}

func read(r io.Reader) (Data, error) {
	h := crc32.NewIEEE()
	mr := io.TeeReader(r, h)
	read := func(value any) error {
		return binary.Read(mr, binary.LittleEndian, value)
	}
	readBytes := func() ([]byte, error) {
		var length uint32
		if err := read(&length); err != nil {
			return nil, err
		}
		// Bound the declared length before allocating so an inflated length prefix
		// (corruption or crafted input) surfaces as a typed error rather than an
		// OOM-inducing multi-GiB allocation that swallows the rest of the file.
		if length > maxSnapshotFieldBytes {
			return nil, fmt.Errorf("snapshot: field length too large %d (max %d)", length, maxSnapshotFieldBytes)
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(mr, buf); err != nil {
			return nil, err
		}
		return buf, nil
	}
	readFixedBytes := func(length uint64) ([]byte, error) {
		if length > maxReadableBytes() {
			return nil, fmt.Errorf("snapshot: byte block too large %d", length)
		}
		buf := make([]byte, int(length))
		if _, err := io.ReadFull(mr, buf); err != nil {
			return nil, err
		}
		return buf, nil
	}
	readString := func() (string, error) {
		buf, err := readBytes()
		return string(buf), err
	}
	// readCount reads a u32 element count and bounds it against a generous
	// ceiling so an inflated count cannot trigger a giant allocation.  Each
	// element costs at least one byte, so no legitimate count can exceed the
	// maxReadableBytes ceiling.
	const maxSnapshotCount = uint32(1 << 24) // 16 M items: far above any real snapshot
	readCount := func(what string) (uint32, error) {
		var count uint32
		if err := read(&count); err != nil {
			return 0, err
		}
		if count > maxSnapshotCount {
			return 0, fmt.Errorf("snapshot: %s count too large %d (max %d)", what, count, maxSnapshotCount)
		}
		return count, nil
	}

	var data Data
	var magic uint32
	if err := read(&magic); err != nil {
		return data, err
	}
	if magic != snapshotMagic {
		return data, fmt.Errorf("snapshot: invalid magic %x", magic)
	}
	var formatVersion uint8
	if err := read(&formatVersion); err != nil {
		return data, err
	}
	if formatVersion < snapshotMinVersion {
		return data, fmt.Errorf("snapshot: %w: version %d", ErrFormatTooOld, formatVersion)
	}
	if formatVersion > snapshotVersion {
		return data, fmt.Errorf("snapshot: unsupported version %d (max known %d)", formatVersion, snapshotVersion)
	}
	// v6 predates QuantizedVector.Norm; decoded v6 payloads default Norm to 1,
	// preserving their old unit-space (cosine) scoring until re-quantized.
	hasNorm := formatVersion >= 7

	var err error
	data.Collection, err = readString()
	if err != nil {
		return data, err
	}
	var bitWidth int32
	if err := read(&bitWidth); err != nil {
		return data, err
	}
	data.BitWidth = int(bitWidth)
	if err := read(&data.Seed); err != nil {
		return data, err
	}
	var dim uint32
	if err := read(&dim); err != nil {
		return data, err
	}
	data.Dim = int(dim)
	var rawStoreByte, sparseByte uint8
	if err := read(&rawStoreByte); err != nil {
		return data, err
	}
	if err := read(&sparseByte); err != nil {
		return data, err
	}
	data.RawStore = rawStoreByte == 1
	data.SparseEnabled = sparseByte == 1
	if err := read(&data.MaxLamport); err != nil {
		return data, err
	}
	var createdAt int64
	if err := read(&createdAt); err != nil {
		return data, err
	}
	data.CreatedAt = time.Unix(0, createdAt).UTC()

	recordCount, err := readCount("records")
	if err != nil {
		return data, err
	}
	data.Records = make([]Record, 0, recordCount)
	for range recordCount {
		id, err := readString()
		if err != nil {
			return data, err
		}
		versionCount, err := readCount("versions")
		if err != nil {
			return data, err
		}
		record := Record{ID: id, Versions: make([]Version, 0, versionCount)}
		for range versionCount {
			version := Version{}
			var hasQuantized uint8
			if err := read(&hasQuantized); err != nil {
				return data, err
			}
			if hasQuantized == 1 {
				mse, err := readBytes()
				if err != nil {
					return data, err
				}
				signs, err := readBytes()
				if err != nil {
					return data, err
				}
				var resNormBits uint32
				if err := read(&resNormBits); err != nil {
					return data, err
				}
				norm := float32(1) // v6 default: unit norm (legacy cosine semantics)
				if hasNorm {
					var normBits uint32
					if err := read(&normBits); err != nil {
						return data, err
					}
					norm = math.Float32frombits(normBits)
					// m1: reject NaN/Inf/negative, matching turboquant's own wire codec
					// (wire.go:144). A negative norm silently inverts ScoreUpperBound
					// into a lower bound and drops real top-k members.
					if math.IsNaN(float64(norm)) || math.IsInf(float64(norm), 0) || norm < 0 {
						return data, fmt.Errorf("snapshot: invalid norm %v", norm)
					}
				}
				version.Quantized = &QuantizedVector{
					MSE:     append([]byte(nil), mse...),
					Signs:   append([]byte(nil), signs...),
					ResNorm: math.Float32frombits(resNormBits),
					Norm:    norm,
				}
			}
			// RawHash block.
			var hasRawHash uint8
			if err := read(&hasRawHash); err != nil {
				return data, err
			}
			if hasRawHash == 1 {
				rawHash := make([]byte, 32)
				if _, err := io.ReadFull(mr, rawHash); err != nil {
					return data, err
				}
				version.RawHash = rawHash
			}
			// Sparse block.
			var hasSparse uint8
			if err := read(&hasSparse); err != nil {
				return data, err
			}
			if hasSparse == 1 {
				count, err := readCount("sparse")
				if err != nil {
					return data, err
				}
				sparse := &SparseBlock{
					Indices: make([]uint32, count),
					Values:  make([]float32, count),
				}
				for i := range sparse.Indices {
					if err := read(&sparse.Indices[i]); err != nil {
						return data, err
					}
				}
				for i := range sparse.Values {
					var bits uint32
					if err := read(&bits); err != nil {
						return data, err
					}
					sparse.Values[i] = math.Float32frombits(bits)
				}
				version.Sparse = sparse
			}
			{
				childCount, err := readCount("children")
				if err != nil {
					return data, err
				}
				var childEncoding uint8
				if err := read(&childEncoding); err != nil {
					return data, err
				}
				switch childEncoding {
				case childEncodingLegacy:
					version.Children = make([]ChildVector, 0, childCount)
					for range childCount {
						child := ChildVector{}
						child.ID, err = readString()
						if err != nil {
							return data, err
						}
						var hasQuantized uint8
						if err := read(&hasQuantized); err != nil {
							return data, err
						}
						if hasQuantized == 1 {
							mse, err := readBytes()
							if err != nil {
								return data, err
							}
							signs, err := readBytes()
							if err != nil {
								return data, err
							}
							var resNormBits uint32
							if err := read(&resNormBits); err != nil {
								return data, err
							}
							childNorm := float32(1) // v6 default: unit norm (legacy cosine semantics)
							if hasNorm {
								var normBits uint32
								if err := read(&normBits); err != nil {
									return data, err
								}
								childNorm = math.Float32frombits(normBits)
								// m1: reject NaN/Inf/negative, matching turboquant's own wire
								// codec (wire.go:144). A negative norm silently inverts
								// ScoreUpperBound into a lower bound and drops real top-k members.
								if math.IsNaN(float64(childNorm)) || math.IsInf(float64(childNorm), 0) || childNorm < 0 {
									return data, fmt.Errorf("snapshot: invalid child norm %v", childNorm)
								}
							}
							child.Quantized = &QuantizedVector{
								MSE:     append([]byte(nil), mse...),
								Signs:   append([]byte(nil), signs...),
								ResNorm: math.Float32frombits(resNormBits),
								Norm:    childNorm,
							}
						}
						var childDim uint32
						if err := read(&childDim); err != nil {
							return data, err
						}
						child.Dim = int(childDim)
						child.Text, err = readString()
						if err != nil {
							return data, err
						}
						childMetaJSON, err := readBytes()
						if err != nil {
							return data, err
						}
						if len(childMetaJSON) > 0 {
							if err := json.Unmarshal(childMetaJSON, &child.Metadata); err != nil {
								return data, err
							}
						}
						version.Children = append(version.Children, child)
					}
				case childEncodingCompactQuantizedOrdinal:
					var childDim, mseLen, signsLen uint32
					if err := read(&childDim); err != nil {
						return data, err
					}
					if err := read(&mseLen); err != nil {
						return data, err
					}
					if err := read(&signsLen); err != nil {
						return data, err
					}
					mseBlockLen, signsBlockLen, resNormBlockLen, normBlockLen, err := validateCompactQuantizedOrdinalLengths(childCount, childDim, mseLen, signsLen, hasNorm)
					if err != nil {
						return data, err
					}
					mseBlock, err := readFixedBytes(mseBlockLen)
					if err != nil {
						return data, err
					}
					signsBlock, err := readFixedBytes(signsBlockLen)
					if err != nil {
						return data, err
					}
					if resNormBlockLen > maxReadableBytes() {
						return data, fmt.Errorf("snapshot: compact child resnorm block too large %d", resNormBlockLen)
					}
					if normBlockLen > maxReadableBytes() {
						return data, fmt.Errorf("snapshot: compact child norm block too large %d", normBlockLen)
					}
					version.Children = make([]ChildVector, 0, childCount)
					for i := range childCount {
						var resNormBits uint32
						if err := read(&resNormBits); err != nil {
							return data, err
						}
						mseStart := int(i) * int(mseLen)
						signsStart := int(i) * int(signsLen)
						child := ChildVector{
							ID:  strconv.Itoa(int(i)),
							Dim: int(childDim),
							Quantized: &QuantizedVector{
								MSE:     append([]byte(nil), mseBlock[mseStart:mseStart+int(mseLen)]...),
								Signs:   append([]byte(nil), signsBlock[signsStart:signsStart+int(signsLen)]...),
								ResNorm: math.Float32frombits(resNormBits),
								Norm:    1, // v6 default; overwritten below when hasNorm
							},
						}
						version.Children = append(version.Children, child)
					}
					// The Norm column follows the entire ResNorm column (column-major
					// layout), so it can only be read after every child's ResNorm has
					// been consumed above.
					if hasNorm {
						for i := range version.Children {
							var normBits uint32
							if err := read(&normBits); err != nil {
								return data, err
							}
							compactNorm := math.Float32frombits(normBits)
							// m1: reject NaN/Inf/negative, matching turboquant's own wire
							// codec (wire.go:144). A negative norm silently inverts
							// ScoreUpperBound into a lower bound and drops real top-k members.
							if math.IsNaN(float64(compactNorm)) || math.IsInf(float64(compactNorm), 0) || compactNorm < 0 {
								return data, fmt.Errorf("snapshot: invalid compact child norm %v", compactNorm)
							}
							version.Children[i].Quantized.Norm = compactNorm
						}
					}
				default:
					return data, fmt.Errorf("snapshot: unsupported child encoding %d", childEncoding)
				}
			}
			version.Text, err = readString()
			if err != nil {
				return data, err
			}
			metaJSON, err := readBytes()
			if err != nil {
				return data, err
			}
			if len(metaJSON) > 0 {
				if err := json.Unmarshal(metaJSON, &version.Metadata); err != nil {
					return data, err
				}
			}
			if err := read(&version.LamportClock); err != nil {
				return data, err
			}
			version.ActorID, err = readString()
			if err != nil {
				return data, err
			}
			var wallClock int64
			if err := read(&wallClock); err != nil {
				return data, err
			}
			version.WallClock = time.Unix(0, wallClock).UTC()
			var tombstone uint8
			if err := read(&tombstone); err != nil {
				return data, err
			}
			version.Tombstone = tombstone == 1
			record.Versions = append(record.Versions, version)
		}
		data.Records = append(data.Records, record)
	}

	computed := h.Sum32()
	var stored uint32
	if err := binary.Read(r, binary.LittleEndian, &stored); err != nil {
		return data, err
	}
	if computed != stored {
		return data, fmt.Errorf("snapshot: crc mismatch: computed %x, stored %x", computed, stored)
	}
	return data, nil
}

// validateCompactQuantizedOrdinalLengths validates and returns the byte
// lengths of the MSE, Signs, ResNorm, and (when hasNorm) Norm columns of a
// compact-ordinal child block. normBlockLen is 0 when hasNorm is false (v6
// payloads carry no Norm column).
func validateCompactQuantizedOrdinalLengths(childCount, childDim, mseLen, signsLen uint32, hasNorm bool) (uint64, uint64, uint64, uint64, error) {
	if childCount == 0 {
		return 0, 0, 0, 0, fmt.Errorf("snapshot: compact child count is zero")
	}
	if childCount > maxCompactOrdinalChildren {
		return 0, 0, 0, 0, fmt.Errorf("snapshot: compact child count too large %d", childCount)
	}
	if childDim == 0 {
		return 0, 0, 0, 0, fmt.Errorf("snapshot: compact child dim is zero")
	}
	if mseLen == 0 {
		return 0, 0, 0, 0, fmt.Errorf("snapshot: compact child MSE length is zero")
	}
	if signsLen == 0 {
		return 0, 0, 0, 0, fmt.Errorf("snapshot: compact child signs length is zero")
	}
	mseBlockLen, ok := checkedMulUint32(childCount, mseLen)
	if !ok {
		return 0, 0, 0, 0, fmt.Errorf("snapshot: compact child MSE block size overflows")
	}
	signsBlockLen, ok := checkedMulUint32(childCount, signsLen)
	if !ok {
		return 0, 0, 0, 0, fmt.Errorf("snapshot: compact child signs block size overflows")
	}
	resNormBlockLen, ok := checkedMulUint32(childCount, 4)
	if !ok {
		return 0, 0, 0, 0, fmt.Errorf("snapshot: compact child resnorm block size overflows")
	}
	var normBlockLen uint64
	if hasNorm {
		normBlockLen, ok = checkedMulUint32(childCount, 4)
		if !ok {
			return 0, 0, 0, 0, fmt.Errorf("snapshot: compact child norm block size overflows")
		}
	}
	if mseBlockLen > maxCompactChildBlockBytes {
		return 0, 0, 0, 0, fmt.Errorf("snapshot: compact child MSE block too large %d", mseBlockLen)
	}
	if signsBlockLen > maxCompactChildBlockBytes {
		return 0, 0, 0, 0, fmt.Errorf("snapshot: compact child signs block too large %d", signsBlockLen)
	}
	totalBlockLen := mseBlockLen + signsBlockLen + resNormBlockLen + normBlockLen
	if totalBlockLen < mseBlockLen || totalBlockLen > maxCompactChildBlockBytes {
		return 0, 0, 0, 0, fmt.Errorf("snapshot: compact child block too large %d", totalBlockLen)
	}
	return mseBlockLen, signsBlockLen, resNormBlockLen, normBlockLen, nil
}

func checkedMulUint32(a, b uint32) (uint64, bool) {
	product := uint64(a) * uint64(b)
	return product, product <= maxReadableBytes()
}

func maxReadableBytes() uint64 {
	return uint64(int(^uint(0) >> 1))
}
