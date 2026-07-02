# Runtime image for flux-broker and flux-bench.
#
# Binaries are built on the host (or CI) with:
#   cargo build --release -p flux-broker --bin flux-broker --bin flux-bench
# then copied in. Build context is the repo root:
#   docker build -f deploy/docker/Dockerfile -t flux:latest .
#
# The build host must be glibc <= the base image (ubuntu:24.04 = glibc 2.39).
FROM ubuntu:24.04

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY target/release/flux-broker /usr/local/bin/flux-broker
COPY target/release/flux-bench /usr/local/bin/flux-bench
COPY migrations /migrations

EXPOSE 9000 9001
ENTRYPOINT ["/usr/local/bin/flux-broker"]
