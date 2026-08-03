# Build the static binary, then ship it on a tiny base image.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /service ./cmd/service

FROM gcr.io/distroless/static-debian12
COPY --from=build /service /service
EXPOSE 8080
ENTRYPOINT ["/service"]
