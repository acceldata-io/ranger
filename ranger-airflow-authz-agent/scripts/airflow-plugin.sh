#!/bin/bash
#
# Copyright 2026 Acceldata Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Enable or disable the Ranger Airflow plugin, for installs not managed by
# Ambari. Shipped twice, as enable-airflow-plugin.sh and
# disable-airflow-plugin.sh; the action comes from the name, as the generic
# Ranger installer does.
#
# The generic installer only templates the plugin XMLs. Airflow's decision
# point is a separate process, so three more things have to happen here: the
# shared token, the agent's own properties file, and the agent service. The
# auth_manager setting in airflow.cfg is the fourth, and it is what actually
# turns enforcement on and off.
#
set -e

cd "$(dirname "$0")"
HERE=$(pwd)
ACTION=enable
case "$(basename "$0")" in disable-*) ACTION=disable ;; esac

prop() { grep -E "^$1=" install.properties | head -1 | cut -d= -f2- | tr -d ' '; }

# Resolved the way the generic installer does it: absolute as given, otherwise
# relative to the plugin directory's parent.
AIRFLOW_HOME=$(prop COMPONENT_INSTALL_DIR_NAME)
case "$AIRFLOW_HOME" in
  /*) ;;
  *) AIRFLOW_HOME="${HERE}/../${AIRFLOW_HOME}" ;;
esac
if [ ! -d "$AIRFLOW_HOME" ]; then
  echo "COMPONENT_INSTALL_DIR_NAME resolves to ${AIRFLOW_HOME}, which does not exist." >&2
  exit 1
fi
AIRFLOW_HOME=$(cd "$AIRFLOW_HOME" && pwd)

TOKEN_FILE=$(prop AGENT_TOKEN_FILE)
USER=$(prop CUSTOM_USER)
GROUP=$(prop CUSTOM_GROUP)
JAVA_HOME_P=$(prop AGENT_JAVA_HOME)
UNIT=/usr/lib/systemd/system/ranger-airflow-agent.service
CONF_DIR="${AIRFLOW_HOME}/conf"
AUTH_MANAGER=odp_airflow_ranger_auth_manager.ranger_auth_manager.RangerAuthManager

# [core] auth_manager in airflow.cfg. Setting it switches Airflow to Ranger;
# removing it falls back to the FAB auth manager.
set_auth_manager() {
  python3 - "$1" "$2" <<'PY'
import configparser, sys
path, value = sys.argv[1], sys.argv[2]
cfg = configparser.ConfigParser(); cfg.optionxform = str
cfg.read(path)
if not cfg.has_section("core"):
    cfg.add_section("core")
if value:
    cfg.set("core", "auth_manager", value)
else:
    cfg.remove_option("core", "auth_manager")
with open(path, "w") as fh:
    cfg.write(fh)
PY
}

if [ "$ACTION" = disable ]; then
  systemctl stop ranger-airflow-agent 2>/dev/null || true
  systemctl disable ranger-airflow-agent 2>/dev/null || true
  [ -f "${AIRFLOW_HOME}/airflow.cfg" ] && set_auth_manager "${AIRFLOW_HOME}/airflow.cfg" ""
  # The plugin XMLs are left in place on purpose; see disable-conf/README.txt.
  echo "Ranger Airflow plugin disabled. Restart the Airflow api-server."
  exit 0
fi

# Config templating is the generic installer's job. It reads JAVA_HOME from the
# environment rather than install.properties, so bridge the two.
export JAVA_HOME="${JAVA_HOME:-$JAVA_HOME_P}"
if [ -z "$JAVA_HOME" ]; then
  echo "Set AGENT_JAVA_HOME in install.properties, or JAVA_HOME in the environment." >&2
  exit 1
fi
# The generic installer normally derives the component from its own filename,
# which this wrapper has taken; tell it what it is instead.
RANGER_COMPONENT_NAME=airflow-plugin ./enable-agent.sh

# One token, created once. It is the agent's only authentication, so it is
# never regenerated: that would lock out an api-server already holding it.
if [ ! -f "$TOKEN_FILE" ]; then
  mkdir -p "$(dirname "$TOKEN_FILE")"
  head -c 48 /dev/urandom | base64 | tr -d '\n=' | tr '+/' '-_' > "$TOKEN_FILE"
  chown "${USER}:${GROUP}" "$TOKEN_FILE"
  chmod 400 "$TOKEN_FILE"
fi

# The agent's own settings, rendered from install.properties so the token path
# has a single source of truth.
mkdir -p "$CONF_DIR"
cat > "${CONF_DIR}/authz-agent.properties" <<PROPS
authz.agent.bind.address=127.0.0.1
authz.agent.port=$(prop AGENT_PORT)
authz.agent.token.file=${TOKEN_FILE}
authz.agent.thread.pool.size=$(prop AGENT_THREAD_POOL_SIZE)
authz.agent.cluster.name=$(prop CLUSTER_NAME)
authz.agent.service.type=airflow
authz.agent.app.id=airflow
PROPS
chown "${USER}:${GROUP}" "${CONF_DIR}/authz-agent.properties"
chmod 400 "${CONF_DIR}/authz-agent.properties"

# The agent runs in the foreground under systemd. ReadWritePaths is needed
# because the plugin caches policies under /etc, which ProtectSystem seals.
cat > "$UNIT" <<UNITFILE
[Unit]
Description=Ranger authorization agent for Airflow
After=network.target

[Service]
Type=simple
User=${USER}
Group=${GROUP}
ExecStart=${JAVA_HOME}/bin/java -cp "${CONF_DIR}:${HERE}/lib/*" io.acceldata.airflow.ranger.RangerAirflowAuthzAgent ${CONF_DIR}/authz-agent.properties
Restart=on-failure
RestartSec=5
# A clean SIGTERM stop leaves the JVM with 143; without this systemd records
# every normal stop as a failure.
SuccessExitStatus=143
ProtectSystem=full
ReadWritePaths=/etc/ranger

[Install]
WantedBy=multi-user.target
UNITFILE

systemctl daemon-reload
systemctl enable ranger-airflow-agent >/dev/null 2>&1 || true
systemctl restart ranger-airflow-agent

[ -f "${AIRFLOW_HOME}/airflow.cfg" ] && set_auth_manager "${AIRFLOW_HOME}/airflow.cfg" "$AUTH_MANAGER"

echo "Ranger Airflow plugin enabled. Restart the Airflow api-server."
