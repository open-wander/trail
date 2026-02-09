# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o trail ./cmd/trail

# Runtime stage
FROM alpine:3.21

RUN apk --no-cache add ca-certificates tzdata

RUN adduser -D -u 1000 trail
WORKDIR /app

COPY --from=builder /build/trail .

RUN mkdir -p /data && chown trail:trail /data

USER trail

EXPOSE 8080

CMD ["./trail"]
