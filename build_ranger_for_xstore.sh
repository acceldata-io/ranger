#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

if [[ ! -d "security-admin" ]]; then
  echo "ERROR: Run this script from the Ranger repo root."
  exit 1
fi

for bin in curl tar python3 docker; do
  if ! command -v "${bin}" >/dev/null 2>&1; then
    echo "ERROR: Missing required command: ${bin}"
    exit 1
  fi
done

if ! command -v aws >/dev/null 2>&1; then
  echo "ERROR: Missing required command: aws (AWS CLI)"
  exit 1
fi

MAVEN_VERSION="${MAVEN_VERSION:-3.9.6}"
MAVEN_TAR="apache-maven-${MAVEN_VERSION}-bin.tar.gz"
MAVEN_URL="https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/${MAVEN_TAR}"
MAVEN_HOME="/tmp/apache-maven-${MAVEN_VERSION}"

if [[ ! -d "${MAVEN_HOME}" ]]; then
  echo "Installing Maven ${MAVEN_VERSION}..."
  curl -fsSL "${MAVEN_URL}" -o "/tmp/${MAVEN_TAR}"
  tar -xzf "/tmp/${MAVEN_TAR}" -C /tmp
fi

export PATH="${MAVEN_HOME}/bin:${PATH}"

echo "Building Ranger distro with Maven..."
mvn clean -DskipTests -DskipDocs -Dpython.command.invoker=python3 -pl distro -am package

ADMIN_TAR_PATH="$(ls -t target/ranger-*-admin.tar.gz 2>/dev/null | head -1 || true)"
if [[ -z "${ADMIN_TAR_PATH}" || ! -f "${ADMIN_TAR_PATH}" ]]; then
  echo "ERROR: Ranger admin tar not found in target."
  exit 1
fi

RANGER_DOCKER_DIR="${SCRIPT_DIR}/ranger-xstore-build/dockerfile"
if [[ ! -d "${RANGER_DOCKER_DIR}" ]]; then
  echo "ERROR: Ranger docker build directory not found: ${RANGER_DOCKER_DIR}"
  exit 1
fi

ADMIN_TAR_NAME="$(basename "${ADMIN_TAR_PATH}")"
IMAGE_TAG="${IMAGE_TAG:-latest}"
LOCAL_IMAGE="ranger-admin-xstore:${IMAGE_TAG}"

echo "Building Ranger docker image..."
docker build \
  -f "${RANGER_DOCKER_DIR}/Dockerfile" \
  --build-arg "RANGER_ADMIN_TAR=${ADMIN_TAR_NAME}" \
  -t "${LOCAL_IMAGE}" \
  "${SCRIPT_DIR}"

ECR_REPO="191579300362.dkr.ecr.us-east-1.amazonaws.com/acceldata/xdp/dp"
ECR_IMAGE="${ECR_REPO}:${IMAGE_TAG}"

echo "Logging in to ECR..."
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin "191579300362.dkr.ecr.us-east-1.amazonaws.com"

echo "Tagging and pushing image to ECR..."
docker tag "${LOCAL_IMAGE}" "${ECR_IMAGE}"
docker push "${ECR_IMAGE}"
