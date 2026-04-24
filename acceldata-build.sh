#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Ranger Docker build and push script (Acceldata XDP edition).
# Uses the same Maven + Docker flow as build_ranger_for_xstore.sh (that script is unchanged).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}"
RANGER_DOCKER_DIR="${PROJECT_ROOT}/ranger-xstore-build/dockerfile"
LOCAL_IMAGE_BASENAME="ranger-admin-xstore"

# =============================================================================
# ACCELDATA ECR DEFAULTS (aligned with Gravitino acceldata-build.sh)
# =============================================================================
DEFAULT_ECR_REGISTRY="191579300362.dkr.ecr.us-east-1.amazonaws.com"
DEFAULT_ECR_REPO_PREFIX="acceldata/xdp/dp"

# =============================================================================

COMPONENT_TYPE="ranger-admin"
PLATFORM="linux/amd64"
REGISTRY="${DEFAULT_ECR_REGISTRY}"
IMAGE_NAME=""
TAG="latest"
PUSH=false
LATEST=false
BUILD_DISTRIBUTION=true
FORCE_BUILD=false
ECR_LOGIN=false
AWS_REGION="us-east-1"

USE_LOCAL_DOCKERFILE=false

SKIP_UNIT_TESTS=false
SKIP_INTEGRATION_TESTS=false
SKIP_DOCS=false
SKIP_RAT=false
SKIP_PMD=false
SKIP_SPOTBUGS=false
SKIP_CHECKSTYLE=false
SKIP_ENUNCIATE=false
SKIP_JACOCO=false

SAVE_IMAGE_TAR=false
SAVE_DIST_TAR=false
OUTPUT_DIR="./artifacts"

BUILD_ONLY=false

MAVEN_VERSION="${MAVEN_VERSION:-3.9.6}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SUPPORTED_COMPONENTS=(
    "ranger-admin"
    "xstore"
)

ADMIN_TAR_PATH=""
USERSYNC_TAR_PATH=""
MAVEN_HOME=""
FULL_IMAGE_NAME=""

usage() {
    cat << EOF
Ranger Docker build and push script (Acceldata XDP edition)

This script builds the Ranger distro with Maven and optionally builds and pushes
the Ranger Admin XStore Docker image (same steps as build_ranger_for_xstore.sh).

USAGE:
    $0 [OPTIONS]

DEFAULT ECR CONFIGURATION:
    Registry:  ${DEFAULT_ECR_REGISTRY}
    Prefix:    ${DEFAULT_ECR_REPO_PREFIX}

    Component to image mapping (default image name if -i omitted):
      ranger-admin, xstore  ->  ${DEFAULT_ECR_REPO_PREFIX}/ranger

OPTIONS:
    -h, --help                    Show this help message
    -t, --type COMPONENT          Component to build (default: ranger-admin)
                                  Supported: ${SUPPORTED_COMPONENTS[*]}
    -p, --platform PLATFORM       Target platform(s) (default: linux/amd64)
                                  Options: all, linux/amd64, linux/arm64
    -r, --registry REGISTRY     Docker registry URL (default: ${DEFAULT_ECR_REGISTRY})
    -i, --image IMAGE_NAME        Repository path without registry (default: ${DEFAULT_ECR_REPO_PREFIX}/ranger)
    --tag TAG                     Image tag (default: latest)
    --push                        Push image to registry after building
    --latest                      Also tag and push as 'latest' when pushing
    --image-only                  Skip Maven distro build; only build the Docker image using existing tars in target/
    --skip-distribution           Skip Maven distro build (use existing target/ranger-*-admin.tar.gz and usersync tar)
    --skip-unit-tests             Skip unit tests (-DskipTests)
    --skip-integration-tests      Skip integration tests (-DskipITs)
    --skip-docs                   Skip documentation (-DskipDocs)
    --skip-rat                    Skip Apache RAT license check (-Drat.skip=true)
    --skip-pmd                    Skip PMD static analysis (-Dpmd.skip=true)
    --skip-spotbugs               Skip SpotBugs bytecode analysis (-Dspotbugs.skip=true)
    --skip-checkstyle             Skip Checkstyle (-Dcheckstyle.skip=true)
    --skip-enunciate              Skip Enunciate API doc generation (-Denunciate.skip=true)
    --skip-jacoco                 Skip JaCoCo coverage agent and report (-Djacoco.skip=true)
    --skip-all-tests              Skip tests, docs, RAT, PMD, SpotBugs, Checkstyle, Enunciate, and JaCoCo
    --force                       Rebuild Docker image even if the local tag already exists
    --ecr-login                   Log in to AWS ECR before build/push (requires aws CLI)
    --aws-region REGION           AWS region for ECR login (default: us-east-1)
    --local-image                 Use Dockerfile.local (installs Java/deps from scratch) instead of the default Dockerfile (ECR base image)
    --save-image-tar              Save the built image as a gzip-compressed tar under --output-dir
    --save-dist-tar               Copy the Ranger admin distribution tar to --output-dir
    --output-dir DIR              Output directory for tar artifacts (default: ./artifacts)
    --build-only                  Only run the Maven distro build; skip Docker build and push

QUICK START:
    $0 --build-only
    $0 --ecr-login --push
    $0 --tag v1.0.0 --ecr-login --push --latest

NOTES:
    - Multi-platform (--platform all) uses docker buildx and requires --push (images are not loaded locally).
    - Default image path matches xcentral-style values (repository acceldata/xdp/dp/ranger). For the
      legacy single-segment repository name, pass: -i acceldata/xdp/dp
    - Authenticate to the registry separately unless you pass --ecr-login.

EOF
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

is_supported_component() {
    local component="$1"
    for supported in "${SUPPORTED_COMPONENTS[@]}"; do
        if [[ "$supported" == "$component" ]]; then
            return 0
        fi
    done
    return 1
}

get_git_info() {
    local git_commit=""
    local git_branch=""
    local git_tag=""

    if git rev-parse --git-dir > /dev/null 2>&1; then
        git_commit="$(git rev-parse --short HEAD)"
        git_branch="$(git rev-parse --abbrev-ref HEAD)"
        if git_tag="$(git describe --exact-match --tags HEAD 2>/dev/null)"; then
            echo "${git_tag}"
        elif [[ "$git_branch" == "main" ]]; then
            echo "main-${git_commit}"
        else
            echo "${git_branch}-${git_commit}"
        fi
    else
        echo "unknown"
    fi
}

ecr_login() {
    local registry="${1:-$DEFAULT_ECR_REGISTRY}"
    local region="${2:-us-east-1}"

    log_info "Logging into ECR: ${registry}"

    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed or not in PATH"
        log_info "Install AWS CLI: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
        return 1
    fi

    if aws ecr get-login-password --region "${region}" | docker login --username AWS --password-stdin "${registry}"; then
        log_success "ECR login successful"
        return 0
    fi
    log_error "ECR login failed"
    return 1
}

check_prereqs() {
    for bin in curl tar python3 docker; do
        if ! command -v "${bin}" &> /dev/null; then
            log_error "Missing required command: ${bin}"
            exit 1
        fi
    done
}

check_docker_setup() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        exit 1
    fi

    if [[ "$PLATFORM" == "all" ]]; then
        if ! docker buildx version &> /dev/null; then
            log_error "Docker Buildx is required for multi-platform builds"
            exit 1
        fi
        if [[ "$PUSH" != "true" ]]; then
            log_error "Multi-platform build (--platform all) requires --push (buildx cannot load multi-arch into the local engine)"
            exit 1
        fi
    fi

    log_info "Docker setup validated"
}

ensure_maven() {
    local maven_tar="apache-maven-${MAVEN_VERSION}-bin.tar.gz"
    local maven_url="https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/${maven_tar}"
    MAVEN_HOME="/tmp/apache-maven-${MAVEN_VERSION}"

    if [[ ! -d "${MAVEN_HOME}" ]]; then
        log_info "Installing Maven ${MAVEN_VERSION} to ${MAVEN_HOME}..."
        curl -fsSL "${maven_url}" -o "/tmp/${maven_tar}"
        tar -xzf "/tmp/${maven_tar}" -C /tmp
    fi
    export PATH="${MAVEN_HOME}/bin:${PATH}"
}

get_default_image_name() {
    echo "ranger"
}

setup_image_info() {
    if [[ -z "$IMAGE_NAME" ]]; then
        IMAGE_NAME="${DEFAULT_ECR_REPO_PREFIX}/$(get_default_image_name)"
        log_info "Using default ECR image path: ${IMAGE_NAME}"
    fi

    if [[ -z "$TAG" ]]; then
        TAG="$(get_git_info)"
    fi

    if [[ -n "$REGISTRY" ]]; then
        FULL_IMAGE_NAME="${REGISTRY}/${IMAGE_NAME}"
    else
        FULL_IMAGE_NAME="${IMAGE_NAME}"
    fi

    log_info "Image: ${FULL_IMAGE_NAME}:${TAG}"
    if [[ "$LATEST" == "true" ]]; then
        log_info "Also tagging as: ${FULL_IMAGE_NAME}:latest"
    fi
}

find_admin_tar() {
    ADMIN_TAR_PATH="$(ls -t "${PROJECT_ROOT}/target"/ranger-*-admin.tar.gz 2>/dev/null | head -1 || true)"
    if [[ -z "${ADMIN_TAR_PATH}" || ! -f "${ADMIN_TAR_PATH}" ]]; then
        return 1
    fi
    return 0
}

find_usersync_tar() {
    USERSYNC_TAR_PATH="$(ls -t "${PROJECT_ROOT}/target"/ranger-*-usersync.tar.gz 2>/dev/null | head -1 || true)"
    if [[ -z "${USERSYNC_TAR_PATH}" || ! -f "${USERSYNC_TAR_PATH}" ]]; then
        return 1
    fi
    return 0
}

build_distribution() {
    if [[ "$BUILD_DISTRIBUTION" == "false" ]]; then
        log_info "Skipping Maven distribution build (--skip-distribution)"
        if ! find_admin_tar; then
            log_error "No ranger admin tar found under target/. Build the distro first or remove --skip-distribution."
            exit 1
        fi
        if ! find_usersync_tar; then
            log_error "No ranger usersync tar found under target/. Build the distro first or remove --skip-distribution."
            exit 1
        fi
        log_info "Using existing tars: $(basename "${ADMIN_TAR_PATH}"), $(basename "${USERSYNC_TAR_PATH}")"
        return 0
    fi

    log_info "Building Ranger distro with Maven (distro + upstream modules)..."

    ensure_maven

    local mvn_cmd=(mvn clean -pl distro -am package)
    mvn_cmd+=("-Dpython.command.invoker=python3")

    if [[ "$SKIP_UNIT_TESTS" == "true" ]]; then
        mvn_cmd+=("-DskipTests")
        log_info "Skipping unit tests (-DskipTests)"
    fi

    if [[ "$SKIP_INTEGRATION_TESTS" == "true" ]]; then
        mvn_cmd+=("-DskipITs")
        log_info "Skipping integration tests (-DskipITs)"
    fi

    if [[ "$SKIP_DOCS" == "true" ]]; then
        mvn_cmd+=("-DskipDocs")
        log_info "Skipping documentation (-DskipDocs)"
    fi

    if [[ "$SKIP_RAT" == "true" ]]; then
        mvn_cmd+=("-Drat.skip=true")
        log_info "Skipping RAT (-Drat.skip=true)"
    fi

    if [[ "$SKIP_PMD" == "true" ]]; then
        mvn_cmd+=("-Dpmd.skip=true")
        log_info "Skipping PMD (-Dpmd.skip=true)"
    fi

    if [[ "$SKIP_SPOTBUGS" == "true" ]]; then
        mvn_cmd+=("-Dspotbugs.skip=true")
        log_info "Skipping SpotBugs (-Dspotbugs.skip=true)"
    fi

    if [[ "$SKIP_CHECKSTYLE" == "true" ]]; then
        mvn_cmd+=("-Dcheckstyle.skip=true")
        log_info "Skipping Checkstyle (-Dcheckstyle.skip=true)"
    fi

    if [[ "$SKIP_ENUNCIATE" == "true" ]]; then
        mvn_cmd+=("-Denunciate.skip=true")
        log_info "Skipping Enunciate (-Denunciate.skip=true)"
    fi

    if [[ "$SKIP_JACOCO" == "true" ]]; then
        mvn_cmd+=("-Djacoco.skip=true")
        log_info "Skipping JaCoCo (-Djacoco.skip=true)"
    fi

    log_info "Running: ${mvn_cmd[*]}"
    if ! (cd "${PROJECT_ROOT}" && "${mvn_cmd[@]}"); then
        log_error "Maven build failed"
        exit 1
    fi

    if ! find_admin_tar; then
        log_error "Ranger admin tar not found in target/ after Maven build."
        exit 1
    fi
    if ! find_usersync_tar; then
        log_error "Ranger usersync tar not found in target/ after Maven build."
        exit 1
    fi

    log_success "Maven distribution build completed: $(basename "${ADMIN_TAR_PATH}"), $(basename "${USERSYNC_TAR_PATH}")"
}

image_exists() {
    docker image inspect "${LOCAL_IMAGE_BASENAME}:${TAG}" > /dev/null 2>&1
}

build_image() {
    if [[ ! -d "${RANGER_DOCKER_DIR}" ]]; then
        log_error "Docker build directory not found: ${RANGER_DOCKER_DIR}"
        exit 1
    fi

    if ! find_admin_tar; then
        log_error "Ranger admin tar missing; cannot build Docker image."
        exit 1
    fi
    if ! find_usersync_tar; then
        log_error "Ranger usersync tar missing; cannot build Docker image."
        exit 1
    fi

    local admin_tar_name usersync_tar_name
    admin_tar_name="$(basename "${ADMIN_TAR_PATH}")"
    usersync_tar_name="$(basename "${USERSYNC_TAR_PATH}")"
    local local_image="${LOCAL_IMAGE_BASENAME}:${TAG}"

    local dockerfile_path
    if [[ "${USE_LOCAL_DOCKERFILE}" == "true" ]]; then
        dockerfile_path="${RANGER_DOCKER_DIR}/Dockerfile.local"
    else
        dockerfile_path="${RANGER_DOCKER_DIR}/Dockerfile"
    fi

    log_info "Building Docker image for platform: ${PLATFORM}"

    if [[ "$FORCE_BUILD" == "false" ]] && image_exists; then
        log_warning "Image ${local_image} already exists. Use --force to rebuild."
        return 0
    fi

    if [[ "$PLATFORM" == "all" ]]; then
        local buildx_tags=(-t "${FULL_IMAGE_NAME}:${TAG}")
        if [[ "$LATEST" == "true" ]]; then
            buildx_tags+=(-t "${FULL_IMAGE_NAME}:latest")
        fi
        log_info "Running: docker buildx build --platform linux/amd64,linux/arm64 ... --push"
        if ! docker buildx build \
            -f "${dockerfile_path}" \
            --platform linux/amd64,linux/arm64 \
            --build-arg "RANGER_ADMIN_TAR=${admin_tar_name}" \
            --build-arg "RANGER_USERSYNC_TAR=${usersync_tar_name}" \
            "${buildx_tags[@]}" \
            --push \
            "${PROJECT_ROOT}"; then
            log_error "docker buildx build failed"
            exit 1
        fi
        log_success "Multi-platform image pushed to ${FULL_IMAGE_NAME}:${TAG}"
        return 0
    fi

    log_info "Running: docker build --platform ${PLATFORM} -f ${dockerfile_path} -t ${local_image} ${PROJECT_ROOT}"
    if ! docker build \
        -f "${dockerfile_path}" \
        --platform "${PLATFORM}" \
        --build-arg "RANGER_ADMIN_TAR=${admin_tar_name}" \
        --build-arg "RANGER_USERSYNC_TAR=${usersync_tar_name}" \
        -t "${local_image}" \
        "${PROJECT_ROOT}"; then
        log_error "docker build failed"
        exit 1
    fi

    log_info "Tagging ${local_image} -> ${FULL_IMAGE_NAME}:${TAG}"
    docker tag "${local_image}" "${FULL_IMAGE_NAME}:${TAG}"

    if [[ "$LATEST" == "true" ]]; then
        docker tag "${local_image}" "${FULL_IMAGE_NAME}:latest"
    fi

    log_success "Docker image built successfully"
}

push_image() {
    if [[ "$PUSH" == "false" ]]; then
        log_info "Skipping push (use --push to enable)"
        return 0
    fi

    if [[ "$PLATFORM" == "all" ]]; then
        log_info "Push already performed by buildx for multi-platform build"
        return 0
    fi

    log_info "Pushing ${FULL_IMAGE_NAME}:${TAG}"
    if ! docker push "${FULL_IMAGE_NAME}:${TAG}"; then
        log_error "docker push failed"
        exit 1
    fi

    if [[ "$LATEST" == "true" ]]; then
        log_info "Pushing ${FULL_IMAGE_NAME}:latest"
        if ! docker push "${FULL_IMAGE_NAME}:latest"; then
            log_error "docker push failed for latest tag"
            exit 1
        fi
    fi

    log_success "Image pushed successfully"
}

save_distribution_tar() {
    if [[ "$SAVE_DIST_TAR" != "true" ]]; then
        return 0
    fi
    if ! find_admin_tar; then
        log_error "Cannot save distribution tar: admin tarball not found"
        return 1
    fi
    mkdir -p "$OUTPUT_DIR"
    local out_name="ranger-admin-distribution-${TAG}.tar.gz"
    log_info "Copying distribution tar to ${OUTPUT_DIR}/${out_name}"
    cp -f "${ADMIN_TAR_PATH}" "${OUTPUT_DIR}/${out_name}"
    log_success "Saved ${OUTPUT_DIR}/${out_name}"
}

save_docker_image_tar() {
    if [[ "$SAVE_IMAGE_TAR" != "true" ]]; then
        return 0
    fi
    if [[ "$PLATFORM" == "all" ]]; then
        log_warning "Skipping --save-image-tar for multi-platform builds (no single local image)"
        return 0
    fi

    mkdir -p "$OUTPUT_DIR"
    local out_base="${OUTPUT_DIR}/ranger-admin-image-${TAG}.tar"
    log_info "Saving image ${FULL_IMAGE_NAME}:${TAG} to ${out_base}"
    if ! docker save -o "${out_base}" "${FULL_IMAGE_NAME}:${TAG}"; then
        log_error "docker save failed"
        return 1
    fi
    gzip -f "${out_base}"
    log_success "Saved ${out_base}.gz"
}

main() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                usage
                exit 0
                ;;
            -t|--type)
                COMPONENT_TYPE="$2"
                shift 2
                ;;
            -p|--platform)
                PLATFORM="$2"
                shift 2
                ;;
            -r|--registry)
                REGISTRY="$2"
                shift 2
                ;;
            -i|--image)
                IMAGE_NAME="$2"
                shift 2
                ;;
            --tag)
                TAG="$2"
                shift 2
                ;;
            --push)
                PUSH=true
                shift
                ;;
            --latest)
                LATEST=true
                shift
                ;;
            --image-only)
                BUILD_DISTRIBUTION=false
                shift
                ;;
            --skip-distribution)
                BUILD_DISTRIBUTION=false
                shift
                ;;
            --skip-unit-tests)
                SKIP_UNIT_TESTS=true
                shift
                ;;
            --skip-integration-tests)
                SKIP_INTEGRATION_TESTS=true
                shift
                ;;
            --skip-docs)
                SKIP_DOCS=true
                shift
                ;;
            --skip-rat)
                SKIP_RAT=true
                shift
                ;;
            --skip-pmd)
                SKIP_PMD=true
                shift
                ;;
            --skip-spotbugs)
                SKIP_SPOTBUGS=true
                shift
                ;;
            --skip-checkstyle)
                SKIP_CHECKSTYLE=true
                shift
                ;;
            --skip-enunciate)
                SKIP_ENUNCIATE=true
                shift
                ;;
            --skip-jacoco)
                SKIP_JACOCO=true
                shift
                ;;
            --skip-all-tests)
                SKIP_UNIT_TESTS=true
                SKIP_INTEGRATION_TESTS=true
                SKIP_DOCS=true
                SKIP_RAT=true
                SKIP_PMD=true
                SKIP_SPOTBUGS=true
                SKIP_CHECKSTYLE=true
                SKIP_ENUNCIATE=true
                SKIP_JACOCO=true
                shift
                ;;
            --save-image-tar)
                SAVE_IMAGE_TAR=true
                shift
                ;;
            --save-dist-tar)
                SAVE_DIST_TAR=true
                shift
                ;;
            --output-dir)
                OUTPUT_DIR="$2"
                shift 2
                ;;
            --build-only)
                BUILD_ONLY=true
                shift
                ;;
            --local-image)
                USE_LOCAL_DOCKERFILE=true
                shift
                ;;
            --force)
                FORCE_BUILD=true
                shift
                ;;
            --ecr-login)
                ECR_LOGIN=true
                shift
                ;;
            --aws-region)
                AWS_REGION="$2"
                shift 2
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done

    if ! is_supported_component "$COMPONENT_TYPE"; then
        log_error "Unsupported component type: $COMPONENT_TYPE"
        log_info "Supported: ${SUPPORTED_COMPONENTS[*]}"
        exit 1
    fi

    if [[ "$PLATFORM" != "all" && "$PLATFORM" != "linux/amd64" && "$PLATFORM" != "linux/arm64" ]]; then
        log_error "Invalid platform: $PLATFORM"
        exit 1
    fi

    if [[ ! -d "${PROJECT_ROOT}/security-admin" ]]; then
        log_error "Run this script from the Ranger repository root (security-admin not found)."
        exit 1
    fi

    log_info "Starting Ranger Acceldata build"
    log_info "Component: ${COMPONENT_TYPE}"

    check_prereqs

    if [[ "$BUILD_ONLY" != "true" ]]; then
        check_docker_setup
    fi

    if [[ "$BUILD_ONLY" != "true" ]] && [[ "$ECR_LOGIN" == "true" ]]; then
        if ! ecr_login "$REGISTRY" "$AWS_REGION"; then
            exit 1
        fi
    fi

    cd "$PROJECT_ROOT"

    build_distribution

    save_distribution_tar

    if [[ "$BUILD_ONLY" == "true" ]]; then
        log_success "Build-only completed (Maven distribution)."
        return 0
    fi

    setup_image_info
    build_image
    push_image
    save_docker_image_tar

    log_success "Build process completed successfully."
    if [[ "$PUSH" == "true" ]] || [[ "$PLATFORM" == "all" ]]; then
        log_info "Image: ${FULL_IMAGE_NAME}:${TAG}"
    else
        log_info "Local image: ${LOCAL_IMAGE_BASENAME}:${TAG} (also tagged ${FULL_IMAGE_NAME}:${TAG})"
    fi
}

main "$@"
