# syntax=docker/dockerfile:1

ARG PG_MAJOR=18
ARG DEBIAN_CODENAME=bookworm

FROM debian:$DEBIAN_CODENAME AS builder

ARG PG_MAJOR
# Upstream source tag from the official GitHub mirror. Keep the minor roughly in sync
# with the postgres:$PG_MAJOR-$DEBIAN_CODENAME runtime image (extension ABI is stable
# across minors, so a small drift is fine).
ARG PG_SOURCE_TAG=REL_18_4

# Toolchain comes from Debian's own repos ONLY. apt.postgresql.org is deliberately NOT
# used: the org network refuses that host entirely (verified on both 80 and 443), so
# PostgreSQL headers/pgxs are built from source below instead of postgresql-server-dev.
# Error-Mode=any makes apt fail loudly on a dead repo instead of silently ignoring it
# and dying later with a misleading "Unable to locate package".
# perl/flex/bison are required because a GitHub source snapshot (unlike release
# tarballs) ships no pre-generated parser/scanner files.
RUN apt-get update -o APT::Update::Error-Mode=any && \
    apt-get install -y --no-install-recommends \
        build-essential \
        perl \
        flex \
        bison \
        wget \
        unzip \
        git \
        ca-certificates \
        default-jdk-headless \
    && \
    rm -rf /var/lib/apt/lists/*

# PostgreSQL from source: gives pg_config, server headers and pgxs at /usr/local/pgsql
# without touching apt.postgresql.org. Readline/zlib/icu are irrelevant for building
# extensions, so they are disabled to keep the dependency list minimal.
RUN cd /tmp && \
    wget -O postgres.tar.gz https://github.com/postgres/postgres/archive/refs/tags/${PG_SOURCE_TAG}.tar.gz && \
    mkdir postgres && \
    tar -xzf postgres.tar.gz -C postgres --strip-components=1 && \
    rm postgres.tar.gz && \
    cd postgres && \
    ./configure --without-readline --without-zlib --without-icu && \
    make -j"$(nproc)" && \
    make install && \
    rm -rf /tmp/postgres

# PGXS-based extension builds pick this up (their Makefiles use PG_CONFIG ?= pg_config).
ENV PG_CONFIG=/usr/local/pgsql/bin/pg_config

RUN mkdir -p /tmp/pgvector && \
    cd /tmp && \
    wget -O pgvector.tar.gz https://github.com/WiWhite/pgvector/archive/refs/tags/v0.8.1.tar.gz && \
    tar -xzf pgvector.tar.gz -C /tmp/pgvector --strip-components=1 && \
    rm pgvector.tar.gz && \
    cd /tmp/pgvector && \
    make clean && \
    make OPTFLAGS="" && \
    make install && \
    rm -rf /tmp/pgvector

RUN cd /tmp && \
    git clone --depth 1 https://github.com/timescale/pg_textsearch.git && \
    cd pg_textsearch && \
    make && \
    make install && \
    rm -rf /tmp/pg_textsearch

RUN export _JAVA_OPTIONS="-Dfile.encoding=UTF-8" && \
    export LANG=C.UTF-8 && \
    wget https://github.com/brown-uk/dict_uk/archive/refs/heads/master.zip -O /tmp/master.zip && \
    unzip /tmp/master.zip -d /tmp && \
    cp -r /tmp/dict_uk-master /tmp/dict_uk && \
    cd /tmp/dict_uk && ./gradlew expand && \
    cd distr/hunspell && ../../gradlew hunspell && \
    rm -rf /tmp/master.zip /tmp/dict_uk-master

FROM postgres:$PG_MAJOR-$DEBIAN_CODENAME

ARG PG_MAJOR

# Source-built artifacts live under /usr/local/pgsql; copy them into the PGDG layout
# the runtime image actually loads from.
COPY --from=builder /usr/local/pgsql/lib/vector.so /usr/lib/postgresql/$PG_MAJOR/lib/
COPY --from=builder /usr/local/pgsql/share/extension/vector* /usr/share/postgresql/$PG_MAJOR/extension/

COPY --from=builder /usr/local/pgsql/lib/pg_textsearch.so /usr/lib/postgresql/$PG_MAJOR/lib/
COPY --from=builder /usr/local/pgsql/share/extension/pg_textsearch* /usr/share/postgresql/$PG_MAJOR/extension/

COPY --from=builder /tmp/dict_uk/distr/hunspell/build/hunspell/uk_UA.aff /usr/share/postgresql/$PG_MAJOR/tsearch_data/uk_ua.affix
COPY --from=builder /tmp/dict_uk/distr/hunspell/build/hunspell/uk_UA.dic /usr/share/postgresql/$PG_MAJOR/tsearch_data/uk_ua.dict
COPY --from=builder /tmp/dict_uk/distr/postgresql/ukrainian.stop /usr/share/postgresql/$PG_MAJOR/tsearch_data/ukrainian.stop

# pg_textsearch requires shared_preload_libraries
CMD ["postgres", "-c", "shared_preload_libraries=pg_textsearch"]
