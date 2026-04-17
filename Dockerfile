# syntax=docker/dockerfile:1.6
#
# Build context: the corkscrewdb repo (default).
# Named context: mll — sibling repo referenced by the `replace` directive in go.mod.
# Build with:
#   docker build --build-context mll=../mll -t corkscrewdb:dev .
#
FROM golang:1.25-alpine AS build
WORKDIR /work
# mll is a local replace target in go.mod; copy it in so `go mod download`
# and the subsequent builds can resolve it.
COPY --from=mll . /work/mll
WORKDIR /work/corkscrewdb
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/corkscrewdb ./cmd/corkscrewdb \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/corkscrewdb-entrypoint ./cmd/corkscrewdb-entrypoint

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/corkscrewdb /corkscrewdb
COPY --from=build /out/corkscrewdb-entrypoint /corkscrewdb-entrypoint
EXPOSE 4040
USER nonroot:nonroot
ENTRYPOINT ["/corkscrewdb-entrypoint"]
