FROM golang:1.25.6-alpine

WORKDIR /app

COPY . .

RUN go build -o drone .

CMD ["./drone"]