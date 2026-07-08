# syntax=docker/dockerfile:1

ARG MINIO_RELEASE=RELEASE.2025-10-15T17-29-55Z

FROM golang:1.24-bookworm AS builder

ARG MINIO_RELEASE

WORKDIR /src

RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates make \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/minio/minio.git . \
    && git checkout ${MINIO_RELEASE}

RUN go build -o /out/minio .

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/minio /usr/local/bin/minio

RUN useradd --system --uid 1001 --create-home --home-dir /home/minio minio \
    && mkdir -p /data \
    && chown -R minio:minio /data

USER minio

EXPOSE 9000 9001

ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]