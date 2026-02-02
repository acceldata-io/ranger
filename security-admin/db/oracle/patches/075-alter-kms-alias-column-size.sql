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
--
-- ODP-5806: Increase ranger_keystore.kms_alias column size to avoid truncation
--

DECLARE
  v_table_exists NUMBER := 0;
  v_column_exists NUMBER := 0;
BEGIN
  -- Check if table exists
  SELECT COUNT(*) INTO v_table_exists FROM user_tables WHERE table_name = 'RANGER_KEYSTORE';

  IF v_table_exists > 0 THEN
    -- Check if column exists
    SELECT COUNT(*) INTO v_column_exists FROM user_tab_cols
    WHERE table_name = 'RANGER_KEYSTORE' AND column_name = 'KMS_ALIAS';

    IF v_column_exists > 0 THEN
      -- Alter column size from VARCHAR(255) to VARCHAR(512)
      EXECUTE IMMEDIATE 'ALTER TABLE ranger_keystore MODIFY kms_alias VARCHAR2(512)';
    END IF;
  END IF;

  COMMIT;
END;/
