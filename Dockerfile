FROM golang:1.16-alpine as builder


WORKDIR /app

COPY go.* .
RUN go mod download
COPY *.go .
RUN go build -o ./post-room

FROM alpine
EXPOSE 80
WORKDIR /usr/src/app
COPY --from=builder /app/post-room .
CMD [ "./post-room" ]