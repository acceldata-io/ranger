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


CREATE OR REPLACE FUNCTION getXportalUIdByLoginId(input_val IN VARCHAR2) RETURN NUMBER IS
  myid NUMBER := 0;
BEGIN
  SELECT id INTO myid FROM x_portal_user WHERE login_id = input_val;
  RETURN myid;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN 0;
END;/

CREATE OR REPLACE FUNCTION getModulesIdByName(input_val IN VARCHAR2) RETURN NUMBER IS
  myid NUMBER := 0;
BEGIN
  SELECT id INTO myid FROM x_modules_master WHERE module = input_val;
  RETURN myid;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    RETURN 0;
END;/

DECLARE
  v_count NUMBER := 0;
  v_admin_id NUMBER := 0;
  v_rangerusersync_id NUMBER := 0;
  v_rangertagsync_id NUMBER := 0;
  v_module_id NUMBER := 0;
BEGIN
  v_admin_id := getXportalUIdByLoginId('admin');
  v_rangerusersync_id := getXportalUIdByLoginId('rangerusersync');
  v_rangertagsync_id := getXportalUIdByLoginId('rangertagsync');

  -- Insert module if not exists
  SELECT COUNT(*) INTO v_count FROM x_modules_master WHERE module = 'Governed Data Sharing';
  IF v_count = 0 THEN
    INSERT INTO x_modules_master(create_time, update_time, added_by_id, upd_by_id, module, url)
    VALUES(SYSDATE, SYSDATE, v_admin_id, v_admin_id, 'Governed Data Sharing', '');
  END IF;

  v_module_id := getModulesIdByName('Governed Data Sharing');

  -- Add permission for admin
  IF v_admin_id > 0 AND v_module_id > 0 THEN
    SELECT COUNT(*) INTO v_count FROM x_user_module_perm WHERE user_id = v_admin_id AND module_id = v_module_id;
    IF v_count = 0 THEN
      INSERT INTO x_user_module_perm(user_id, module_id, create_time, update_time, added_by_id, upd_by_id, is_allowed)
      VALUES(v_admin_id, v_module_id, SYSDATE, SYSDATE, v_admin_id, v_admin_id, 1);
    END IF;
  END IF;

  -- Add permission for rangerusersync
  IF v_rangerusersync_id > 0 AND v_module_id > 0 THEN
    SELECT COUNT(*) INTO v_count FROM x_user_module_perm WHERE user_id = v_rangerusersync_id AND module_id = v_module_id;
    IF v_count = 0 THEN
      INSERT INTO x_user_module_perm(user_id, module_id, create_time, update_time, added_by_id, upd_by_id, is_allowed)
      VALUES(v_rangerusersync_id, v_module_id, SYSDATE, SYSDATE, v_admin_id, v_admin_id, 1);
    END IF;
  END IF;

  -- Add permission for rangertagsync
  IF v_rangertagsync_id > 0 AND v_module_id > 0 THEN
    SELECT COUNT(*) INTO v_count FROM x_user_module_perm WHERE user_id = v_rangertagsync_id AND module_id = v_module_id;
    IF v_count = 0 THEN
      INSERT INTO x_user_module_perm(user_id, module_id, create_time, update_time, added_by_id, upd_by_id, is_allowed)
      VALUES(v_rangertagsync_id, v_module_id, SYSDATE, SYSDATE, v_admin_id, v_admin_id, 1);
    END IF;
  END IF;

  COMMIT;
END;/

