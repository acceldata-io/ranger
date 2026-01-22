#!/usr/bin/env python3
#
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

"""
Create Ranger services (hiveDev, hdfsDev) for Gravitino playground.
This script is called by ranger.sh after Ranger Admin starts.
"""

import time
import requests
from requests.auth import HTTPBasicAuth

RANGER_URL = "http://127.0.0.1:6080"
RANGER_USER = "admin"
RANGER_PASSWORD = "rangerR0cks!"

AUTH = HTTPBasicAuth(RANGER_USER, RANGER_PASSWORD)
HEADERS = {"Content-Type": "application/json"}


def wait_for_ranger():
    """Wait for Ranger Admin API to become available."""
    print("Waiting for Ranger Admin to be ready...")
    while True:
        try:
            resp = requests.get(
                f"{RANGER_URL}/service/public/v2/api/service",
                auth=AUTH,
                headers=HEADERS,
                timeout=10
            )
            if resp.status_code == 200:
                print("Ranger Admin is ready!")
                return
        except requests.exceptions.RequestException:
            pass
        print("Ranger not ready yet, retrying in 5 seconds...")
        time.sleep(5)


def create_service(service_def):
    """Create a Ranger service."""
    service_name = service_def.get("name", "unknown")
    print(f"Creating Ranger service: {service_name}")
    try:
        resp = requests.post(
            f"{RANGER_URL}/service/public/v2/api/service",
            auth=AUTH,
            headers=HEADERS,
            json=service_def,
            timeout=30
        )
        if resp.status_code in (200, 201):
            print(f"Successfully created service: {service_name}")
        elif resp.status_code == 400 and "already exists" in resp.text.lower():
            print(f"Service {service_name} already exists, skipping.")
        else:
            print(f"Failed to create service {service_name}: {resp.status_code} - {resp.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error creating service {service_name}: {e}")


def delete_policy(policy_id):
    """Delete a Ranger policy by ID."""
    print(f"Deleting policy ID: {policy_id}")
    try:
        resp = requests.delete(
            f"{RANGER_URL}/service/plugins/policies/{policy_id}",
            auth=AUTH,
            headers=HEADERS,
            timeout=10
        )
        if resp.status_code in (200, 204):
            print(f"Successfully deleted policy: {policy_id}")
        elif resp.status_code == 404:
            print(f"Policy {policy_id} not found, skipping.")
        else:
            print(f"Failed to delete policy {policy_id}: {resp.status_code} - {resp.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error deleting policy {policy_id}: {e}")


def main():
    # Wait for Ranger to be ready
    wait_for_ranger()

    # Define hiveDev service
    hive_dev = {
        "type": "hive",
        "name": "hiveDev",
        "displayName": "hiveDev",
        "description": "Hive service for Gravitino playground",
        "isEnabled": True,
        "configs": {
            "username": "admin",
            "password": "admin",
            "jdbc.driverClassName": "org.apache.hive.jdbc.HiveDriver",
            "jdbc.url": "jdbc:hive2://hive:10000"
        }
    }

    # Define hdfsDev service
    hdfs_dev = {
        "type": "hdfs",
        "name": "hdfsDev",
        "displayName": "hdfsDev",
        "description": "HDFS service for Gravitino playground",
        "isEnabled": True,
        "configs": {
            "username": "admin",
            "password": "admin",
            "hadoop.security.authentication": "simple",
            "hadoop.rpc.protection": "authentication",
            "hadoop.security.authorization": "false",
            "fs.default.name": "hdfs://hive:9000"
        }
    }

    # Create services
    create_service(hive_dev)
    create_service(hdfs_dev)

    # Delete default policies that may conflict
    # (matches init/ranger/init.sh behavior)
    for policy_id in [1, 3, 4]:
        delete_policy(policy_id)

    print("Ranger services initialization complete!")


if __name__ == "__main__":
    main()

