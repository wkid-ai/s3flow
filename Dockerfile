FROM golang:1.22 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /s3qos ./cmd/s3qos

FROM gcr.io/distroless/static:nonroot
COPY --from=build /s3qos /s3qos
USER nonroot
ENTRYPOINT ["/s3qos", "-config", "/etc/s3qos/config.json"]
