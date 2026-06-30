# syntax=docker/dockerfile:1.7
#
# ranger-yunikorn-agent — runtime container image
#
# Base: Eclipse Temurin 17 JRE on Ubuntu Jammy. JRE only (no compiler shipped
# to production). Final image size ~250-300 MB depending on JRE variant.

FROM eclipse-temurin:17-jre-jammy

LABEL org.opencontainers.image.title="ranger-yunikorn-agent"
LABEL org.opencontainers.image.description="Ranger -> YuniKorn ConfigMap sync agent"
LABEL org.opencontainers.image.vendor="Acceldata"
LABEL org.opencontainers.image.source="https://github.com/acceldata-io/ranger-yunikorn-agent"

# Run as non-root for K8s best practice
RUN groupadd --system --gid 1000 agent \
    && useradd  --system --uid 1000 --gid agent --no-create-home --shell /sbin/nologin agent

WORKDIR /opt/agent

COPY target/ranger-yunikorn-agent-*.jar /opt/agent/agent.jar
COPY src/main/resources/log4j2.xml      /opt/agent/conf/log4j2.xml

# Config (agent.properties) is mounted at runtime from a K8s ConfigMap.
# Credentials (ranger token) come from a K8s Secret as env vars.
ENV AGENT_CONF_DIR=/etc/ranger-yunikorn-agent

# Java SPNEGO settings:
#
#   useSubjectCredsOnly=false
#     Tells GSSAPI to use Kerberos credentials from the JAAS subject populated
#     by Hadoop's UGI.loginUserFromKeytab(), rather than looking for OS-level
#     credentials (which don't exist in our container). Without this, the
#     SPNEGO handshake silently fails and the client gives up after the
#     initial 401 challenge instead of building the Negotiate token.
#
#   krb5.debug / spnego.debug
#     Off by default; turn on by overriding JAVA_OPTS at deploy time when
#     debugging "auth seems wrong" issues. They produce very verbose output
#     that's only useful while diagnosing handshake failures.
# Heap: -XX:MaxRAMPercentage lets the JVM size the heap from the container's
# cgroup memory limit instead of a hardcoded -Xmx, leaving headroom for
# Metaspace, thread stacks, and GSS/Kerberos + fabric8 off-heap/native memory.
# 75% of the limit is the heap; the remaining ~25% covers non-heap. This tracks
# whatever `resources.limits.memory` the Helm chart sets — raise or lower the
# limit and the heap follows, instead of OOMKilling or failing to start.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0 \
    -Djavax.security.auth.useSubjectCredsOnly=false \
    -Dlog4j.configurationFile=/opt/agent/conf/log4j2.xml"

EXPOSE 8080

USER agent

ENTRYPOINT ["/bin/sh", "-c", "exec java $JAVA_OPTS -jar /opt/agent/agent.jar ${AGENT_CONF_DIR}/agent.properties"]
