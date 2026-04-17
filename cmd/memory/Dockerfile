# syntax=docker/dockerfile:1.6
#
# Build context: the corkscrewdb repo root (NOT cmd/memory/), because the
# memory binary depends on the sibling corkscrewdb package tree.
# Named context: mll — sibling repo referenced by the `replace` directive
# in go.mod (`github.com/odvcencio/mll => ../mll`).
# Build with:
#   docker build --build-context mll=../mll -t memory:dev -f cmd/memory/Dockerfile .
#
FROM golang:1.25-alpine AS build
WORKDIR /work
# mll is a local replace target in go.mod; copy it in so `go mod download`
# and the subsequent build can resolve it.
COPY --from=mll . /work/mll
WORKDIR /work/corkscrewdb
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/memory ./cmd/memory

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/memory /memory
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/memory"]
