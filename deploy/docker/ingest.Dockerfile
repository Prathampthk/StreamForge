FROM golang:1.26.0 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /ingest-api ./cmd/ingest-api

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /ingest-api /ingest-api
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/ingest-api"]
