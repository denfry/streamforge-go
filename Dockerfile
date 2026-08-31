FROM golang:1.24-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/streamforge ./cmd/streamforge

FROM alpine:3.20.3

RUN addgroup -S app && adduser -S -G app app
WORKDIR /app
COPY --from=build /out/streamforge /app/streamforge
COPY migrations /app/migrations
USER app
EXPOSE 8080
ENTRYPOINT ["/app/streamforge"]
