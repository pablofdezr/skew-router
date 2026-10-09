# syntax=docker/dockerfile:1.7
# Build context: the repository root.
#   docker build -t <your-registry>/skew-router:<tag> .
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY *.go ./
ARG TARGETOS TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/skew-router .

# Static binary, no shell: nothing to exec into, nothing to exploit.
# nonroot = uid 65532, matching the pod securityContext.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/skew-router /skew-router
USER 65532:65532
EXPOSE 9090
ENTRYPOINT ["/skew-router"]
