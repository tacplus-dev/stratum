# Stratum — Release Image
# Distroless Java 21 base with no shell.
# The launcher binary reads LAUNCH_CMD and supervises the java process.

# Pre-create /var/lib/stratum with correct ownership for nonroot (65532).
FROM busybox AS setup
RUN mkdir -p /var/lib/stratum/persistence

FROM gcr.io/distroless/java21-debian12

ARG BUILD_DATE
ARG VCS_REF
ARG VERSION

LABEL org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.authors="Seabeam LLC <cloudctlplane@seabeam.com>" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.vendor="Seabeam LLC" \
      org.opencontainers.image.title="stratum" \
      org.opencontainers.image.description="Supervisor LLM stateful SQL workspace with Hazelcast and Apache Calcite"

COPY --from=setup --chown=65532:65532 /var/lib/stratum /var/lib/stratum
COPY server/target/stratum.jar /app/stratum.jar

ARG TARGETARCH
COPY launcher-linux-${TARGETARCH} /usr/local/bin/stratum

USER nonroot

# Extra JVM flags can be appended via JAVA_TOOL_OPTIONS (automatically read by the JVM).
ENV LAUNCH_CMD="java -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -XX:MinRAMPercentage=50.0 -XX:+ExitOnOutOfMemoryError -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/stratum-heapdump.hprof -Djava.net.preferIPv4Stack=true -Dio.netty.recycler.maxCapacityPerThread=0 -jar /app/stratum.jar"
ENV HEALTH_PORT=8443

# Hazelcast Hot Restart persistence directory for the scratchpad map.
VOLUME /var/lib/stratum/persistence

ENTRYPOINT ["stratum"]
