#!/bin/bash

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

export HADOOP_HOME="${HADOOP_HOME:-${RANGER_HOME}/usersync}"

ADMIN_SETUP_DONE_FILE="${RANGER_HOME}/.adminSetupDone"
USERSYNC_SETUP_DONE_FILE="${RANGER_HOME}/.usersyncSetupDone"

if [ ! -e "${ADMIN_SETUP_DONE_FILE}" ]
then
  cd "${RANGER_HOME}"/admin || exit
  if ./setup.sh;
  then
    echo "Ranger Admin setup completed."
    touch "${ADMIN_SETUP_DONE_FILE}"
  else
    echo "Ranger Admin Setup Script didn't complete proper execution."
    exit 1
  fi
fi

if [ ! -e "${USERSYNC_SETUP_DONE_FILE}" ] || [ ! -d "${RANGER_HOME}/usersync/conf" ]
then
  cd "${RANGER_HOME}"/usersync || exit
  sed -i "s|^POLICY_MGR_URL[[:space:]]*=.*|POLICY_MGR_URL = http://localhost:6080|g" install.properties
  if ./setup.sh;
  then
    echo "Ranger UserSync setup completed."
    touch "${USERSYNC_SETUP_DONE_FILE}"
  else
    echo "Ranger UserSync Setup Script didn't complete proper execution."
    exit 1
  fi
fi

# Apply custom ranger-ugsync-site.xml from init (e.g. /tmp/ranger) after setup owns usersync/conf.
# Avoid bind-mounting that file into conf/: Docker pre-creates conf/ as root and setup cannot
# create conf/cert (Permission denied).
RANGER_INIT_DIR="${RANGER_INIT_DIR:-/tmp/ranger}"
OVERLAY_SITE_XML="${RANGER_INIT_DIR}/ranger-ugsync-site.xml"
if [ -f "${OVERLAY_SITE_XML}" ] && [ -d "${RANGER_HOME}/usersync/conf" ]; then
  cp -f "${OVERLAY_SITE_XML}" "${RANGER_HOME}/usersync/conf/ranger-ugsync-site.xml"
  echo "Applied UserSync site XML from ${OVERLAY_SITE_XML}"
fi

cd ${RANGER_HOME}/admin && ./ews/ranger-admin-services.sh start

if [ ! -e "${RANGER_HOME}/.servicesBootstrapped" ]
then
  # Wait for Ranger Admin to become ready
  sleep 30
  python3 ${RANGER_SCRIPTS}/create-ranger-services.py
  touch "${RANGER_HOME}/.servicesBootstrapped"
fi

RANGER_ADMIN_PID=`ps -ef  | grep -v grep | grep -i "org.apache.ranger.server.tomcat.EmbeddedServer" | awk '{print $2}'`

# prevent the container from exiting
if [ -z "$RANGER_ADMIN_PID" ] || [ -z "$RANGER_USERSYNC_PID" ]
then
  echo "Ranger Admin or UserSync process probably exited, no process id found!"
  exit 1
else
  echo "Ranger Admin pid=${RANGER_ADMIN_PID}, UserSync pid=${RANGER_USERSYNC_PID}"
  while true
  do
    if ! kill -0 "${RANGER_ADMIN_PID}" 2>/dev/null; then
      echo "Ranger Admin process exited."
      exit 1
    fi
    if ! kill -0 "${RANGER_USERSYNC_PID}" 2>/dev/null; then
      echo "Ranger UserSync process exited."
      exit 1
    fi
    sleep 5
  done
fi
