# Stratum — Release Image
# Layers the stratum jar onto a JRE base.

FROM registry.gitlab.com/secondstarcavalier/schematic/containers/jre/schematic-jre-1-2:latest

ARG BUILD_DATE
ARG VCS_REF
ARG VERSION

LABEL org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.authors="Second Star Cavalier LLC <cloudctlplane@secondstarcavalier.com>" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.vendor="Second Star Cavalier LLC" \
      org.opencontainers.image.title="stratum" \
      org.opencontainers.image.description="Supervisor LLM stateful SQL workspace with Hazelcast and Apache Calcite"

RUN addgroup -S stratum && adduser -S stratum -G stratum \
    && mkdir -p /var/lib/stratum/persistence \
    && chown -R stratum:stratum /var/lib/stratum

COPY server/target/stratum.jar /app/stratum.jar
COPY stratum /usr/local/bin/stratum
RUN chmod +x /usr/local/bin/stratum

USER stratum

# Hazelcast Hot Restart persistence directory for the scratchpad map.
VOLUME /var/lib/stratum/persistence

ENTRYPOINT ["stratum"]
