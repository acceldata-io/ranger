# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

######################################################################
# Ranger Base image...
######################################################################
FROM ubuntu:22.04 AS ranger-base

# Install tzdata, Python, Java 11, python-requests
RUN apt-get update -y && \
    apt-get install curl -y && \
    DEBIAN_FRONTEND="noninteractive" apt-get -y install tzdata vim \
    python3 python3-pip openjdk-11-jdk bc iputils-ping ssh pdsh xmlstarlet && \
    pip3 install apache-ranger && \
    pip3 install requests

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV RANGER_DIST=/home/ranger/dist
ENV RANGER_SCRIPTS=/home/ranger/scripts
ENV RANGER_HOME=/opt/ranger
ENV PATH=/usr/java/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# RUN update-java-alternatives --set java-1.11.0-openjdk-amd64

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

COPY ranger-xstore-build/dockerfile/scripts/ranger.sh ${RANGER_SCRIPTS}/
COPY ranger-xstore-build/dockerfile/scripts/ranger-admin-install.properties ${RANGER_SCRIPTS}/ranger-admin-install.properties
COPY ranger-xstore-build/dockerfile/scripts/create-ranger-services.py ${RANGER_SCRIPTS}/

# Copy Ranger admin distro from build context (no network download)
COPY target/${RANGER_ADMIN_TAR} /home/ranger/dist/ranger-admin.tar.gz

RUN chmod +x ${RANGER_SCRIPTS}/*.sh ${RANGER_SCRIPTS}/*.py

RUN set -eux; \
    RANGER_TOP_DIR="$(tar tzf /home/ranger/dist/ranger-admin.tar.gz | head -1 | cut -d/ -f1)"; \
    tar xvfz /home/ranger/dist/ranger-admin.tar.gz --directory="${RANGER_HOME}"; \
    ln -s "${RANGER_HOME}/${RANGER_TOP_DIR}" "${RANGER_HOME}/admin"; \
    echo "${RANGER_TOP_DIR}" | sed -e 's/^ranger-//' -e 's/-admin$//' > /home/ranger/dist/version; \
    rm -f /home/ranger/dist/ranger-admin.tar.gz; \
    cp -f ${RANGER_SCRIPTS}/ranger-admin-install.properties ${RANGER_HOME}/admin/install.properties; \
    mkdir -p /var/run/ranger; \
    mkdir -p /var/log/ranger; \
    chown -R ranger:ranger ${RANGER_HOME}/admin/ ${RANGER_SCRIPTS}/ /var/run/ranger/ /var/log/ranger/; \
    mkdir -p /usr/share/java/; \
    mkdir -p ${RANGER_HOME}/admin/ews/webapp/WEB-INF/lib/

RUN curl -s -f -L -o /usr/share/java/postgresql.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.16.jre7/postgresql-42.2.16.jre7.jar && \
    curl -s -f -L -o /usr/share/java/mysql-connector.jar https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar && \
    curl -s -f -L -o ${RANGER_HOME}/admin/ews/webapp/WEB-INF/lib/log4jdbc-1.2.jar https://repo1.maven.org/maven2/com/googlecode/log4jdbc/log4jdbc/1.2/log4jdbc-1.2.jar

USER ranger

ENTRYPOINT [ "/home/ranger/scripts/ranger.sh" ]
