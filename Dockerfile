# Stratum — Release Image
# Distroless Java 21 base with no shell.
# The launcher (tacplus-dev/launcher) reads LAUNCH_CMD and supervises the java
# process. It comes from a pinned tacplus-dev/launcher release, its sha256 from
# that release's checksums.txt: the build fails on any other binary. To move it,
# change the version and both checksums together.

# Stratum holds no durable state (the redo service is its durability), so the
# image has no volume.

ARG LAUNCHER_VERSION=v0.1.0
ARG LAUNCHER_SHA256_amd64=903693ab56879b5a5b54779c3e20efa3ddd5f1269fb0c2274882befb7df69e0f
ARG LAUNCHER_SHA256_arm64=f07933da11e08812afa87d814ecbd187087f20c2af7f62d0c1d356754643ec16

FROM scratch AS launcher-amd64
ARG LAUNCHER_VERSION
ARG LAUNCHER_SHA256_amd64
ADD --chmod=0755 --checksum=sha256:${LAUNCHER_SHA256_amd64} \
    https://github.com/tacplus-dev/launcher/releases/download/${LAUNCHER_VERSION}/launcher-linux-amd64 /launcher

FROM scratch AS launcher-arm64
ARG LAUNCHER_VERSION
ARG LAUNCHER_SHA256_arm64
ADD --chmod=0755 --checksum=sha256:${LAUNCHER_SHA256_arm64} \
    https://github.com/tacplus-dev/launcher/releases/download/${LAUNCHER_VERSION}/launcher-linux-arm64 /launcher

FROM launcher-${TARGETARCH} AS launcher

FROM gcr.io/distroless/java21-debian13

ARG BUILD_DATE
ARG VCS_REF
ARG VERSION

LABEL org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.authors="Jody Scott" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.vendor="tacplus-dev" \
      org.opencontainers.image.title="stratum" \
      org.opencontainers.image.description="Per-tenant, in-memory record store (Hazelcast); durability through the redo service"

COPY server/target/stratum.jar /app/stratum.jar

COPY --from=launcher /launcher /usr/local/bin/launcher

USER nonroot

# Extra JVM flags can be appended via JAVA_TOOL_OPTIONS (automatically read by the JVM).
ENV LAUNCH_CMD="java -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -XX:MinRAMPercentage=50.0 -XX:+ExitOnOutOfMemoryError -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/stratum-heapdump.hprof -Djava.net.preferIPv4Stack=true -Dio.netty.recycler.maxCapacityPerThread=0 -jar /app/stratum.jar"
# The launcher probes gRPC health in plaintext without HEALTH_CA, so it watches
# the chassis's plaintext health_probe_port, not the TLS port (tacplus-dev/launcher#7).
ENV HEALTH_PORT=8444

ENTRYPOINT ["launcher"]
