FROM golang:1.22 AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /s3flow ./cmd/s3flow

FROM gcr.io/distroless/static:nonroot
COPY --from=build /s3flow /s3flow
USER nonroot
ENTRYPOINT ["/s3flow", "-config", "/etc/s3flow/config.json"]
