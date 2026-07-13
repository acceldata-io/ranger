-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Create persistent RMS deletion log so plugins can be served correct
-- delta downloads across Admin restarts and HA failovers, and add a
-- watermark column on x_rms_mapping_provider that records the lowest
-- mapping_version from which the deletion log is complete.

DROP PROCEDURE IF EXISTS create_rms_deletion_log;
DELIMITER ;;
CREATE PROCEDURE create_rms_deletion_log()
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = DATABASE()
      AND table_name = 'x_rms_deletion_log'
  ) THEN
    CREATE TABLE `x_rms_deletion_log` (
      `id` bigint(20) NOT NULL AUTO_INCREMENT,
      `version` bigint(20) NOT NULL,
      `change_timestamp` timestamp NULL DEFAULT NULL,
      `hl_resource_guid` varchar(64) DEFAULT NULL,
      `ll_resource_guid` varchar(64) NOT NULL,
      `ll_service_id` bigint(20) NOT NULL,
      PRIMARY KEY (`id`)
    );
    CREATE INDEX x_rms_deletion_log_IDX_svc_ver ON x_rms_deletion_log(ll_service_id, version);
    CREATE INDEX x_rms_deletion_log_IDX_version ON x_rms_deletion_log(version);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'x_rms_mapping_provider'
      AND column_name = 'deletion_tracking_from_version'
  ) THEN
    ALTER TABLE x_rms_mapping_provider
      ADD COLUMN deletion_tracking_from_version bigint(20) DEFAULT 0;
  END IF;
END;;
DELIMITER ;
CALL create_rms_deletion_log();
DROP PROCEDURE IF EXISTS create_rms_deletion_log;
