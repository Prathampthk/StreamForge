ARG SOURCE_DATE_EPOCH=0
FROM golang:1.26.10@sha256:cfa27959837144bf4649802ba38e59daffd0dd2a7994aa83daaf113285b0f551 AS build
ARG SOURCE_DATE_EPOCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags="-s -w -buildid=" -o /eventgen ./cmd/eventgen && CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags="-s -w -buildid=" -o /replayctl ./cmd/replayctl && touch -d "@${SOURCE_DATE_EPOCH}" /eventgen /replayctl
FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /eventgen /eventgen
COPY --from=build /replayctl /replayctl
USER 65532:65532
ENTRYPOINT ["/eventgen"]
