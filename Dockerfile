# Build stage
FROM golang:1.21.0-alpine3.18 AS builder
WORKDIR /tsbs
COPY ./ ./
RUN apk update && apk add --no-cache git
RUN go mod download && go install ./...

# Final stage
FROM alpine:3.18
RUN apk update && apk add --no-cache bash
COPY --from=builder /go/bin /
COPY --from=builder /tsbs/scripts /
# We need to keep the container running since there is no background process
ENTRYPOINT ["/bin/bash", "-c", "trap : TERM INT; (while true; do sleep 1000; done) & wait"]
