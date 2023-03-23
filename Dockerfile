# syntax=docker/dockerfile:1

ARG PG_MAJOR="13"
ARG PG_TAG="13-13.3"

FROM postgis/postgis:13-3.3
# FROM postgis/postgis:${PG_TAG}

LABEL org.opencontainers.image.source "https://github.com/x-b-e/pgvector"
LABEL org.opencontainers.image.description "XBE server postgres with postgis, pgvector"
LABEL org.opencontainers.image.licenses "PostgreSQL License"

# ARG PG_MAJOR
# ENV PG_MAJOR=${PG_MAJOR}
ENV PG_MAJOR=13

COPY . /tmp/pgvector

RUN apt-get update && \
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
		rm -rf /var/lib/apt/lists/*
