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
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Configuration
BASE_IMAGE_NAME="${BASE_IMAGE_NAME:-ranger-build-base}"
BASE_IMAGE_TAG="${BASE_IMAGE_TAG:-latest}"

echo "=========================================="
echo "Building Ranger Build Base Image"
echo "=========================================="
echo "Image: ${BASE_IMAGE_NAME}:${BASE_IMAGE_TAG}"
echo "This will download ONLY ranger-admin dependencies"
echo ""

# Verify we're in the right directory
if [[ ! -f "${REPO_ROOT}/pom.xml" ]]; then
    echo "ERROR: pom.xml not found at ${REPO_ROOT}"
    echo "Please run this script from the Ranger repository root"
    exit 1
fi

# Build the Docker image
# The Dockerfile will copy pom.xml files and download only ranger-admin dependencies
echo "Building Docker image: ${BASE_IMAGE_NAME}:${BASE_IMAGE_TAG}..."
echo "This will download all ranger-admin dependencies (may take several minutes)..."
echo "Using --progress=plain to show all Maven output..."
echo ""

# BuildKit required for cache mount (faster repeated builds) and syntax directive
export DOCKER_BUILDKIT=1

# Use --progress=plain to show all output (including Maven logs) instead of progress bars
docker build \
    --progress=plain \
    -f "${SCRIPT_DIR}/Dockerfile.build-base" \
    -t "${BASE_IMAGE_NAME}:${BASE_IMAGE_TAG}" \
    "${REPO_ROOT}"

if [[ $? -eq 0 ]]; then
    echo ""
    echo "=========================================="
    echo "✅ Build base image created successfully!"
    echo "=========================================="
    echo "Image: ${BASE_IMAGE_NAME}:${BASE_IMAGE_TAG}"
    echo ""
    echo "This image contains ONLY ranger-admin dependencies"
    echo ""
    echo "To use this image for building ranger-admin:"
    echo "  docker run --rm -v \$(pwd):/ranger -w /ranger ${BASE_IMAGE_NAME}:${BASE_IMAGE_TAG} \\"
    echo "    mvn clean -Pranger-admin -DskipTests -DskipDocs -Dpython.command.invoker=python3 -pl distro -am package"
else
    echo ""
    echo "=========================================="
    echo "❌ Build failed!"
    echo "=========================================="
    exit 1
fi

echo "Done!"