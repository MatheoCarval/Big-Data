# MinIO ne publie plus d'images (ni Docker Hub, ni Quay) : on compile les sources, sur des versions fixées.
FROM golang:1.24-bookworm AS build
ARG MINIO_VERSION=RELEASE.2025-10-15T17-29-55Z
ARG MC_VERSION=RELEASE.2025-08-13T08-35-41Z
ENV CGO_ENABLED=0 GOBIN=/out
RUN go install -trimpath github.com/minio/minio@${MINIO_VERSION} \
    && go install -trimpath github.com/minio/mc@${MC_VERSION}

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --shell /usr/sbin/nologin minio \
    && mkdir /data \
    && chown minio:minio /data
COPY --from=build /out/minio /out/mc /usr/local/bin/
USER minio
EXPOSE 9000 9001
