FROM golang:1.22-alpine AS builder

WORKDIR /app
ENV GONOSUMCHECK=*
ENV GOFLAGS=-mod=mod

COPY go.mod ./
COPY main.go .

RUN go mod tidy && \
    CGO_ENABLED=0 GOOS=linux go build -o sidecar .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/sidecar /sidecar
ENTRYPOINT ["/sidecar"]
