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

-- MySQL doesn't support "ALTER TABLE ... MODIFY COLUMN ... IF EXISTS", use dynamic SQL.
SET @ranger_kms_sql := (
  SELECT
    IF(
      EXISTS(
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = DATABASE()
           AND table_name = 'ranger_keystore'
           AND column_name = 'kms_alias'
      ),
      'ALTER TABLE `ranger_keystore` MODIFY COLUMN `kms_alias` varchar(512) NOT NULL',
      'SELECT 1'
    )
);

PREPARE ranger_kms_stmt FROM @ranger_kms_sql;
EXECUTE ranger_kms_stmt;
DEALLOCATE PREPARE ranger_kms_stmt;

