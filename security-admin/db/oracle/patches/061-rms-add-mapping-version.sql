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

-- Add mapping_version column for RMS delta/incremental download support.

DECLARE
  v_count NUMBER := 0;
BEGIN
  SELECT COUNT(*) INTO v_count FROM user_tab_columns
  WHERE table_name = 'X_RMS_RESOURCE_MAPPING' AND column_name = 'MAPPING_VERSION';

  IF v_count = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE x_rms_resource_mapping ADD mapping_version NUMBER(20) DEFAULT 0';
    EXECUTE IMMEDIATE 'CREATE INDEX x_rms_resource_mapping_IDX_mapping_version ON x_rms_resource_mapping(mapping_version)';
  END IF;
END;/
