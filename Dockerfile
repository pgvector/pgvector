# syntax=docker/dockerfile:1

ARG PG_MAJOR="15"
# see https://hub.docker.com/r/postgis/postgis for valid images
ARG PG_TAG="15-3.4""

FROM $PG_IMAGE

ARG PG_MAJOR

LABEL org.opencontainers.image.source "https://github.com/x-b-e/pgvector"
LABEL org.opencontainers.image.description "XBE server postgres with postgis, pgvector"
LABEL org.opencontainers.image.licenses "PostgreSQL License"

COPY . /tmp/pgvector

RUN apt-get update && \
		apt-mark hold locales && \
		apt-get install -y --no-install-recommends build-essential postgresql-server-dev-${PG_MAJOR} && \
		cd /tmp/pgvector && \
		make clean && \
		make OPTFLAGS="" && \
		make install && \
		mkdir /usr/share/doc/pgvector && \
		cp LICENSE README.md /usr/share/doc/pgvector && \
		rm -r /tmp/pgvector && \
		apt-get remove -y build-essential postgresql-server-dev-${PG_MAJOR} && \
		apt-get autoremove -y && \
		apt-mark unhold locales && \
		rm -rf /var/lib/apt/lists/*
