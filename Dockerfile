FROM golang:1.25-bookworm as build

WORKDIR /go/src/lassie

COPY go.* .
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 go build -o /go/bin/lassie ./cmd/lassie

FROM gcr.io/distroless/static-debian12
COPY --from=build /go/bin/lassie /usr/bin/

ENTRYPOINT ["/usr/bin/lassie"]
