ARG PG_MAJOR=16

FROM postgres:${PG_MAJOR}-alpine AS builder
ARG PG_MAJOR

RUN apk add --no-cache build-base clang15 llvm15 postgresql${PG_MAJOR}-dev

COPY . /tmp/pgvector
WORKDIR /tmp/pgvector
RUN make clean && \
    make OPTFLAGS="" && \
    make install && \
    mkdir -p /usr/share/doc/pgvector && \
    cp LICENSE README.md /usr/share/doc/pgvector

FROM postgres:${PG_MAJOR}-alpine AS runner
ARG PG_MAJOR

COPY --from=builder /usr/share/doc/pgvector /usr/share/doc/pgvector
COPY --from=builder /usr/local/lib/postgresql /usr/local/lib/postgresql
COPY --from=builder /var/lib/postgresql /var/lib/postgresql
COPY --from=builder /usr/local/share/postgresql /usr/local/share/postgresql
COPY --from=builder /usr/local/bin/postgres /usr/local/bin/postgres
COPY --from=builder /usr/local/include/postgresql /usr/local/include/postgresql
COPY --from=builder /usr/local/include/postgres_ext.h /usr/local/include/
COPY --from=builder /usr/local/bin/postgres /usr/local/bin/

STOPSIGNAL SIGINT

EXPOSE 5432

CMD ["postgres"]
