# Build stage
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder

# Cross-compile from the build host's native arch; only the runtime
# stage below is emulated, which keeps arm64 builds fast.
ARG TARGETARCH

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -o trail ./cmd/trail

# Runtime stage
FROM alpine:3.21

RUN apk --no-cache add ca-certificates tzdata su-exec

RUN adduser -D -u 1000 trail
WORKDIR /app

COPY --from=builder /build/trail .
COPY entrypoint.sh .

RUN mkdir -p /data && chown trail:trail /data

EXPOSE 8080

ENTRYPOINT ["./entrypoint.sh"]
CMD ["./trail"]
