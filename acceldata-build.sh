#!/usr/bin/env bash
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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Defaults ──────────────────────────────────────────────────────────────────
MVN_MODULES="distro"
SKIP_TESTS=false
SKIP_RAT=false
SKIP_PMD=false
SKIP_CHECKSTYLE=false
SKIP_SPOTBUGS=false

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Build the Apache Ranger distro for XStore/XCentral packaging.

Options:
  --modules <csv>      Maven modules to build (default: distro)
  --skip-tests         Skip unit tests (default: false)
  --skip-rat           Skip Apache RAT check
  --skip-pmd           Skip PMD analysis
  --skip-checkstyle    Skip Checkstyle
  --skip-spotbugs      Skip SpotBugs
  --skip-all-checks    Shorthand for --skip-tests --skip-rat --skip-pmd --skip-checkstyle --skip-spotbugs
  -h, --help           Show this help message
EOF
  exit 0
}

# ── Parse arguments ───────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --modules)          MVN_MODULES="$2"; shift 2 ;;
    --skip-tests)       SKIP_TESTS=true; shift ;;
    --skip-rat)         SKIP_RAT=true; shift ;;
    --skip-pmd)         SKIP_PMD=true; shift ;;
    --skip-checkstyle)  SKIP_CHECKSTYLE=true; shift ;;
    --skip-spotbugs)    SKIP_SPOTBUGS=true; shift ;;
    --skip-all-checks)
      SKIP_TESTS=true; SKIP_RAT=true; SKIP_PMD=true
      SKIP_CHECKSTYLE=true; SKIP_SPOTBUGS=true; shift ;;
    -h|--help)          usage ;;
    *)
      echo "ERROR: Unknown option: $1" >&2
      usage ;;
  esac
done

# ── Build Maven flags ────────────────────────────────────────────────────────
MVN_FLAGS=()
[[ "$SKIP_TESTS" == "true" ]]      && MVN_FLAGS+=("-DskipTests")
[[ "$SKIP_RAT" == "true" ]]        && MVN_FLAGS+=("-Drat.skip=true")
[[ "$SKIP_PMD" == "true" ]]        && MVN_FLAGS+=("-Dpmd.skip=true")
[[ "$SKIP_CHECKSTYLE" == "true" ]] && MVN_FLAGS+=("-Dcheckstyle.skip=true")
[[ "$SKIP_SPOTBUGS" == "true" ]]   && MVN_FLAGS+=("-Dspotbugs.skip=true")

echo "========================================"
echo "Ranger XStore Build"
echo "========================================"
echo "Modules:     ${MVN_MODULES}"
echo "Skip tests:  ${SKIP_TESTS}"
echo "Skip RAT:    ${SKIP_RAT}"
echo "Maven flags: ${MVN_FLAGS[*]:-<none>}"
echo "========================================"

cd "${SCRIPT_DIR}"

mvn clean package -pl "${MVN_MODULES}" -am "${MVN_FLAGS[@]}"

# ── Verify output ─────────────────────────────────────────────────────────────
ADMIN_TAR_PATH="$(ls -t target/ranger-*-admin.tar.gz 2>/dev/null | head -1 || true)"
if [[ -z "${ADMIN_TAR_PATH}" || ! -f "${ADMIN_TAR_PATH}" ]]; then
  echo "ERROR: Ranger admin tar not found in target/ after build."
  exit 1
fi

ADMIN_TAR_NAME="$(basename "${ADMIN_TAR_PATH}")"

echo "========================================"
echo "Build successful!"
echo "Admin tar: ${ADMIN_TAR_NAME}"
echo "========================================"

# Export for downstream CI steps (e.g., Docker build ARG)
export ADMIN_TAR_NAME
