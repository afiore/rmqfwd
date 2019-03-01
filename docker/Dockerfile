FROM alpine:3.7

RUN apk update && apk upgrade \
&& apk add ca-certificates \
&& rm -rf /var/cache/apk/*

COPY rmqfwd /

# Define default command.
RUN ["chmod", "+x", "/rmqfwd"]
ENTRYPOINT ["/rmqfwd"]

# default ports
EXPOSE 1337
