# syntax=docker/dockerfile:1
# Built by GoReleaser: the static binary for each platform is placed at
# $TARGETPLATFORM/ in the build context, so nothing is compiled here.
FROM gcr.io/distroless/static-debian13:nonroot@sha256:25ac8f0c3b2334a9eb05452266bd4697864a8c8ef0ef71e5e6480d7f92868329
ARG TARGETPLATFORM
COPY $TARGETPLATFORM/cloudflare-exporter /usr/local/bin/cloudflare-exporter
EXPOSE 9184
ENTRYPOINT ["/usr/local/bin/cloudflare-exporter"]
