#!/usr/bin/env python3
"""Convert an ann-benchmarks HDF5 dataset into the self-describing .csb binary
the Go bench harness reads. This is a ONE-TIME, OFFLINE step; the Go harness
itself has no Python/HDF5 dependency.

ann-benchmarks HDF5 layout (e.g. glove-25-angular.hdf5):
  train      (Nbase, dim)  float32  base vectors
  test       (Nquery, dim) float32  query vectors
  neighbors  (Nquery, 100) int32    ground-truth nearest-neighbor indices into train
  attrs['distance'] in {'angular','euclidean'}

For angular datasets we L2-normalize train+test so the index's inner-product
metric is identical to cosine similarity (ranking-preserving). The provided
ground-truth `neighbors` is over the FULL train set and is written verbatim, so
recall is measured against the dataset authors' exact neighbors with no
recomputation.

.csb format (all little-endian):
  magic   : 8 bytes  = "CSBENCH1"
  distance: int32    (0 = inner-product/angular-normalized, 1 = euclidean)
  dim     : int32
  nbase   : int32
  nquery  : int32
  gtk     : int32    (ground-truth neighbors per query)
  base    : nbase*dim   float32
  query   : nquery*dim  float32
  gt      : nquery*gtk  int32

Usage:
  python3 convert_hdf5.py <in.hdf5> <out.csb>
"""
import sys
import struct
import numpy as np
import h5py

MAGIC = b"CSBENCH1"


def main():
    if len(sys.argv) != 3:
        sys.stderr.write("usage: convert_hdf5.py <in.hdf5> <out.csb>\n")
        sys.exit(2)
    src, dst = sys.argv[1], sys.argv[2]
    with h5py.File(src, "r") as f:
        dist = str(f.attrs.get("distance", "angular"))
        train = np.asarray(f["train"], dtype=np.float32)
        test = np.asarray(f["test"], dtype=np.float32)
        gt = np.asarray(f["neighbors"], dtype=np.int32)

    dim = train.shape[1]
    assert test.shape[1] == dim, "train/test dim mismatch"
    assert gt.shape[0] == test.shape[0], "query/gt count mismatch"

    if dist == "angular":
        dist_code = 0
        train = l2norm(train)
        test = l2norm(test)
    elif dist == "euclidean":
        dist_code = 1
    else:
        sys.stderr.write(f"unsupported distance {dist!r}\n")
        sys.exit(1)

    nbase, nquery, gtk = train.shape[0], test.shape[0], gt.shape[1]
    sys.stderr.write(
        f"distance={dist} dim={dim} nbase={nbase} nquery={nquery} gtk={gtk}\n"
    )

    with open(dst, "wb") as out:
        out.write(MAGIC)
        out.write(struct.pack("<iiiii", dist_code, dim, nbase, nquery, gtk))
        out.write(np.ascontiguousarray(train, dtype="<f4").tobytes())
        out.write(np.ascontiguousarray(test, dtype="<f4").tobytes())
        out.write(np.ascontiguousarray(gt, dtype="<i4").tobytes())
    sys.stderr.write(f"wrote {dst}\n")


def l2norm(a):
    n = np.linalg.norm(a, axis=1, keepdims=True)
    n[n == 0] = 1.0
    return (a / n).astype(np.float32)


if __name__ == "__main__":
    main()
