FROM postgres:14

COPY . /tmp/pgvector

RUN apt-get update && \
		apt-get install -y --no-install-recommends build-essential postgresql-server-dev-14 && \
		cd /tmp/pgvector && \
		make clean && \
		make OPTFLAGS="" && \
		make install && \
		mkdir /usr/share/doc/pgvector && \
		cp LICENSE README.md /usr/share/doc/pgvector && \
		rm -r /tmp/pgvector && \
		apt-get remove -y build-essential postgresql-server-dev-14 && \
		apt-get autoremove -y && \
		rm -rf /var/lib/apt/lists/*
