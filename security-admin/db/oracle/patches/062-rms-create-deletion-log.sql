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
-- watermark column on x_rms_mapping_provider.

DECLARE
  v_count NUMBER := 0;
BEGIN
  SELECT COUNT(*) INTO v_count FROM user_tables WHERE table_name = 'X_RMS_DELETION_LOG';
  IF v_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE TABLE x_rms_deletion_log (
        id                NUMBER(20) NOT NULL,
        version           NUMBER(20) NOT NULL,
        change_timestamp  TIMESTAMP NULL,
        hl_resource_guid  VARCHAR2(64) NULL,
        ll_resource_guid  VARCHAR2(64) NOT NULL,
        ll_service_id     NUMBER(20) NOT NULL,
        PRIMARY KEY (id))';
    EXECUTE IMMEDIATE 'CREATE SEQUENCE X_RMS_DELETION_LOG_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE';
    EXECUTE IMMEDIATE 'CREATE INDEX x_rms_deletion_log_IDX_svc_ver ON x_rms_deletion_log(ll_service_id, version)';
    EXECUTE IMMEDIATE 'CREATE INDEX x_rms_deletion_log_IDX_version ON x_rms_deletion_log(version)';
  END IF;

  SELECT COUNT(*) INTO v_count FROM user_tab_columns
  WHERE table_name = 'X_RMS_MAPPING_PROVIDER' AND column_name = 'DELETION_TRACKING_FROM_VERSION';
  IF v_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE x_rms_mapping_provider ADD deletion_tracking_from_version NUMBER(20) DEFAULT 0';
  END IF;
END;
/
