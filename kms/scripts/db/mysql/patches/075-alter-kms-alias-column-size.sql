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

DROP PROCEDURE IF EXISTS alter_kms_alias_column_size;

DELIMITER ;;
CREATE PROCEDURE alter_kms_alias_column_size() BEGIN
  IF EXISTS (SELECT * FROM information_schema.tables WHERE table_schema=database() AND table_name = 'ranger_keystore') THEN
    IF EXISTS (SELECT * FROM information_schema.columns WHERE table_schema=database() AND table_name = 'ranger_keystore' AND column_name = 'kms_alias') THEN
      ALTER TABLE `ranger_keystore` MODIFY COLUMN `kms_alias` varchar(512) NOT NULL;
    END IF;
  END IF;
END;;

DELIMITER ;
CALL alter_kms_alias_column_size();
DROP PROCEDURE IF EXISTS alter_kms_alias_column_size;

