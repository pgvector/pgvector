FROM postgres:9.6.21-alpine

RUN apk add build-base

COPY . /usr/bin/pgvector
WORKDIR /usr/bin/pgvector
RUN make && \
	make install
