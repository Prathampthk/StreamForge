FROM golang:1.26.0 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -trimpath -o /eventgen ./cmd/eventgen && CGO_ENABLED=0 go build -trimpath -o /replayctl ./cmd/replayctl
FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /eventgen /eventgen
COPY --from=build /replayctl /replayctl
USER 65532:65532
ENTRYPOINT ["/eventgen"]
