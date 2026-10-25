FROM --platform=$BUILDPLATFORM golang:alpine AS builder

RUN apk --no-cache add git ca-certificates tzdata

# need to be outside of GOPATH for module support
WORKDIR /code

# have this separate for caching purposes
COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .
RUN mkdir /goback-bin

ARG TARGETOS TARGETARCH

# static, so the binary also runs when copied into other images
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -o /goback-bin/binary ./cmd/goback

FROM alpine AS runner

# copied from the builder so this stage runs nothing, and an arm64 image builds
# on an amd64 runner without emulation
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
WORKDIR /root

COPY --from=builder /goback-bin/binary /root/

ENTRYPOINT ["/root/binary"]
