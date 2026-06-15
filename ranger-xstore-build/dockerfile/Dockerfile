######################################################################
# Ranger Base image...
######################################################################
# FROM 191579300362.dkr.ecr.us-east-1.amazonaws.com/acceldata/internal/custom/amazoncorretto:1.0.1-1776333911444 AS ranger-base

ARG SRC_CONTAINER_IMAGE=0
FROM ${SRC_CONTAINER_IMAGE} as ranger-base
ARG SRC_CONTAINER_IMAGE

# Install Python, utilities (Java 11 already provided by base image)
RUN apt-get update -y && \
    apt-get install curl -y && \
    DEBIAN_FRONTEND="noninteractive" apt-get -y install tzdata vim \
    python3 python3-pip bc iputils-ping ssh pdsh xmlstarlet && \
    pip3 install apache-ranger && \
    pip3 install requests

# Ranger setup scripts expect this Ambari helper wrapper.
RUN printf '%s\n' '#!/usr/bin/env sh' 'exec python3 "$@"' > /usr/local/bin/ambari-python-wrap && \
    chmod +x /usr/local/bin/ambari-python-wrap

# Set environment variables
ENV RANGER_DIST=/home/ranger/dist
ENV RANGER_SCRIPTS=/home/ranger/scripts
ENV RANGER_HOME=/opt/ranger
# Hadoop credential tooling (buildks / JCEKS) logs DEBUG noise if unset; any existing dir is enough.
ENV HADOOP_HOME=/opt/ranger/usersync
ENV PATH=/usr/java/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# setup groups, users, directories
RUN groupadd ranger && \
    useradd -g ranger -ms /bin/bash ranger && \
    useradd -g ranger -ms /bin/bash rangeradmin && \
    useradd -g ranger -ms /bin/bash rangerusersync && \
    useradd -g ranger -ms /bin/bash rangertagsync && \
    useradd -g ranger -ms /bin/bash rangerkms && \
    groupadd hadoop && \
    useradd -g hadoop -ms /bin/bash hdfs && \
    useradd -g hadoop -ms /bin/bash yarn && \
    useradd -g hadoop -ms /bin/bash hive && \
    useradd -g hadoop -ms /bin/bash hbase && \
    useradd -g hadoop -ms /bin/bash kafka && \
    groupadd knox && \
    useradd -g knox -ms /bin/bash knox && \
    mkdir -p /home/ranger/dist && \
    mkdir -p /home/ranger/scripts && \
    chown -R ranger:ranger /home/ranger && \
    mkdir -p /opt/ranger && \
    chown -R ranger:ranger /opt/ranger

ENTRYPOINT [ "/bin/bash" ]

######################################################################
# Ranger-Admin image...
######################################################################
FROM ranger-base AS ranger

ARG RANGER_ADMIN_TAR=ranger-2.5.0.3.3.6.2-1-admin.tar.gz
ARG RANGER_USERSYNC_TAR=ranger-2.5.0.3.3.6.2-1-usersync.tar.gz

COPY ranger-xstore-build/dockerfile/scripts/ranger.sh ${RANGER_SCRIPTS}/
COPY ranger-xstore-build/dockerfile/scripts/ranger-admin-install.properties ${RANGER_SCRIPTS}/ranger-admin-install.properties
COPY dev-support/ranger-docker/scripts/ranger-usersync-install.properties ${RANGER_SCRIPTS}/ranger-usersync-install.properties
COPY ranger-xstore-build/dockerfile/scripts/create-ranger-services.py ${RANGER_SCRIPTS}/

# Copy Ranger admin distro from build context (no network download)
COPY target/${RANGER_ADMIN_TAR} /home/ranger/dist/ranger-admin.tar.gz
COPY target/${RANGER_USERSYNC_TAR} /home/ranger/dist/ranger-usersync.tar.gz

RUN chmod +x ${RANGER_SCRIPTS}/*.sh ${RANGER_SCRIPTS}/*.py

RUN set -eux; \
    RANGER_TOP_DIR="$(tar tzf /home/ranger/dist/ranger-admin.tar.gz | head -1 | cut -d/ -f1)"; \
    tar xvfz /home/ranger/dist/ranger-admin.tar.gz --directory="${RANGER_HOME}"; \
    ln -s "${RANGER_HOME}/${RANGER_TOP_DIR}" "${RANGER_HOME}/admin"; \
    USERSYNC_TOP_DIR="$(tar tzf /home/ranger/dist/ranger-usersync.tar.gz | head -1 | cut -d/ -f1)"; \
    tar xvfz /home/ranger/dist/ranger-usersync.tar.gz --directory="${RANGER_HOME}"; \
    ln -s "${RANGER_HOME}/${USERSYNC_TOP_DIR}" "${RANGER_HOME}/usersync"; \
    echo "${RANGER_TOP_DIR}" | sed -e 's/^ranger-//' -e 's/-admin$//' > /home/ranger/dist/version; \
    rm -f /home/ranger/dist/ranger-admin.tar.gz; \
    rm -f /home/ranger/dist/ranger-usersync.tar.gz; \
    cp -f ${RANGER_SCRIPTS}/ranger-admin-install.properties ${RANGER_HOME}/admin/install.properties; \
    cp -f ${RANGER_SCRIPTS}/ranger-usersync-install.properties ${RANGER_HOME}/usersync/install.properties; \
    mkdir -p /var/run/ranger; \
    mkdir -p /var/log/ranger; \
    mkdir -p /var/log/ranger/usersync; \
    mkdir -p /etc/ranger; \
    touch /etc/init.d/ranger-usersync; \
    ln -s /etc/init.d/ranger-usersync /etc/rc2.d/S99ranger-usersync; \
    ln -s /etc/init.d/ranger-usersync /etc/rc2.d/K00ranger-usersync; \
    ln -s /etc/init.d/ranger-usersync /etc/rc3.d/S99ranger-usersync; \
    ln -s /etc/init.d/ranger-usersync /etc/rc3.d/K00ranger-usersync; \
    ln -s ${RANGER_HOME}/usersync/ranger-usersync-services.sh /usr/bin/ranger-usersync; \
    ln -s /usr/bin/python3 /usr/bin/python; \
    chown -R ranger:ranger ${RANGER_HOME}/admin/ ${RANGER_HOME}/usersync/ ${RANGER_SCRIPTS}/ /var/run/ranger/ /var/log/ranger/ /etc/ranger /etc/init.d/ranger-usersync; \
    mkdir -p /usr/share/java/; \
    mkdir -p ${RANGER_HOME}/admin/ews/webapp/WEB-INF/lib/

RUN curl -s -f -L -o /usr/share/java/postgresql.jar https://sonatype.nexus.acceldata.tech/repository/maven-public/org/postgresql/postgresql/42.2.16.jre7/postgresql-42.2.16.jre7.jar && \
    curl -s -f -L -o /usr/share/java/mysql-connector.jar https://sonatype.nexus.acceldata.tech/repository/maven-public/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar && \
    curl -s -f -L -o ${RANGER_HOME}/admin/ews/webapp/WEB-INF/lib/log4jdbc-1.2.jar https://sonatype.nexus.acceldata.tech/repository/maven-public/com/googlecode/log4jdbc/log4jdbc/1.2/log4jdbc-1.2.jar


ARG OCI_AUTHORS="Acceldata Inc."
ARG OCI_VENDOR="Acceldata Inc."
ARG OCI_URL="https://www.acceldata.io/"
ARG OCI_DOCS="https://docs.acceldata.io/"

LABEL org.opencontainers.image.authors="${OCI_AUTHORS}" \
      org.opencontainers.image.url="${OCI_URL}" \
      org.opencontainers.image.documentation="${OCI_DOCS}" \
      org.opencontainers.image.vendor="${OCI_VENDOR}" \
      org.opencontainers.image.baseImage=${SRC_CONTAINER_IMAGE}

USER ranger

ENTRYPOINT [ "/home/ranger/scripts/ranger.sh" ]
