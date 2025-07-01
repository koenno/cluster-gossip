FROM ubuntu

COPY memberlist /

ENTRYPOINT [ "./memberlist" ]
