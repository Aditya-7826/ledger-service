# One image for every Go service: build with --build-arg SERVICE=<folder under cmd/>.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE=api
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/service ./cmd/${SERVICE}

FROM alpine:3.22
RUN adduser -D -u 10001 app
USER app
COPY --from=build /out/service /usr/local/bin/service
ENTRYPOINT ["/usr/local/bin/service"]
