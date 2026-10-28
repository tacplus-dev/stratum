# Stratum — Release Image
# Distroless Java 21 base with no shell.
# The launcher binary reads LAUNCH_CMD and supervises the java process.

# Stratum holds no durable state (the redo service is its durability), so the
# image has no volume.

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

ARG TARGETARCH
COPY launcher-linux-${TARGETARCH} /usr/local/bin/launcher

USER nonroot

# Extra JVM flags can be appended via JAVA_TOOL_OPTIONS (automatically read by the JVM).
ENV LAUNCH_CMD="java -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -XX:MinRAMPercentage=50.0 -XX:+ExitOnOutOfMemoryError -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/stratum-heapdump.hprof -Djava.net.preferIPv4Stack=true -Dio.netty.recycler.maxCapacityPerThread=0 -jar /app/stratum.jar"
ENV HEALTH_PORT=8443

ENTRYPOINT ["launcher"]
