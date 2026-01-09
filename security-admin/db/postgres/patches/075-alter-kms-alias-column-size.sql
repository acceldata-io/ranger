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

select 'delimiter start';

CREATE OR REPLACE FUNCTION alter_kms_alias_column_size()
RETURNS void AS $$
DECLARE
 v_table_exists integer := 0;
 v_column_exists integer := 0;
BEGIN
 -- Check if table exists
 select count(*) into v_table_exists
   from pg_class
  where relname='ranger_keystore';

 IF v_table_exists > 0 THEN
  -- Check if column exists
  select count(*) into v_column_exists
    from pg_attribute
   where attrelid in(select oid from pg_class where relname='ranger_keystore')
     and attname='kms_alias';

  IF v_column_exists > 0 THEN
   -- Alter column size from VARCHAR(255) to VARCHAR(512)
   ALTER TABLE ranger_keystore
     ALTER COLUMN kms_alias TYPE VARCHAR(512);
  END IF;
 END IF;
END;
$$ LANGUAGE plpgsql;

select 'delimiter end';

select alter_kms_alias_column_size();

select 'delimiter end';

DROP FUNCTION IF EXISTS alter_kms_alias_column_size();

commit;