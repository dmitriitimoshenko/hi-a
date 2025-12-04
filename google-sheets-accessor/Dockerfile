FROM golang:1.25 AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/gsa ./main.go

FROM alpine:3.20

RUN adduser -D appuser

COPY --from=builder /bin/gsa /usr/local/bin/gsa

USER appuser

ENTRYPOINT ["/usr/local/bin/gsa"]
