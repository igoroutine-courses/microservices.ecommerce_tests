FROM golang:1.25

WORKDIR /tests

COPY integration-tests/go.mod integration-tests/go.sum ./
RUN go mod download || true

COPY integration-tests/. .

CMD ["go", "test", "-v", "./..."]
