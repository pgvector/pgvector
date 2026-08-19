# syntax=docker/dockerfile:1

############################
# Builder
############################
FROM postgres:18-bookworm AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        git \
        ca-certificates \
        postgresql-server-dev-18 && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

ADD https://github.com/pgvector/pgvector.git#v0.8.5 /tmp/pgvector

WORKDIR /tmp/pgvector

RUN make clean && \
    make OPTFLAGS="" && \
    make install DESTDIR=/tmp/install

############################
# Runtime
############################
FROM registry.redhat.io/rhel9/postgresql-18:9.8-1784150165

USER root

COPY --from=builder /tmp/install/ /

# OpenShift compatibility
RUN mkdir -p /var/lib/pgsql/data /var/run/postgresql && \
    chmod -R g=u /var/lib/pgsql /var/run/postgresql && \
    chmod g=u /etc/passwd || true

USER 26
