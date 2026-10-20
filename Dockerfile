FROM golang:1.27-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /shardkv ./cmd/shardkv

FROM debian:bookworm-slim
RUN mkdir /data && chown 10001:10001 /data
WORKDIR /app
COPY --from=build /shardkv /app/shardkv
COPY cluster.docker.json /app/cluster.docker.json
USER 10001:10001
ENTRYPOINT ["/app/shardkv"]
