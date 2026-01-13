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

DECLARE
    v_count NUMBER := 0;
    v_admin_id NUMBER := 0;
    v_usersync_id NUMBER := 0;
    v_tagsync_id NUMBER := 0;
    v_module_id NUMBER := 0;
BEGIN
    SELECT id INTO v_admin_id FROM x_portal_user WHERE login_id = 'admin';
    BEGIN SELECT id INTO v_usersync_id FROM x_portal_user WHERE login_id = 'rangerusersync'; EXCEPTION WHEN NO_DATA_FOUND THEN v_usersync_id := 0; END;
    BEGIN SELECT id INTO v_tagsync_id FROM x_portal_user WHERE login_id = 'rangertagsync'; EXCEPTION WHEN NO_DATA_FOUND THEN v_tagsync_id := 0; END;
    SELECT COUNT(*) INTO v_count FROM x_modules_master WHERE module = 'Governed Data Sharing';
    IF v_count = 0 THEN
        INSERT INTO x_modules_master VALUES(X_MODULES_MASTER_SEQ.NEXTVAL, SYSDATE, SYSDATE, v_admin_id, v_admin_id, 'Governed Data Sharing', '');
    END IF;
    SELECT id INTO v_module_id FROM x_modules_master WHERE module = 'Governed Data Sharing';
    IF v_admin_id > 0 AND v_module_id > 0 THEN
        SELECT COUNT(*) INTO v_count FROM x_user_module_perm WHERE user_id = v_admin_id AND module_id = v_module_id;
        IF v_count = 0 THEN
            INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.NEXTVAL,v_admin_id,v_module_id,SYSDATE,SYSDATE,v_admin_id,v_admin_id,1);
        END IF;
    END IF;
    IF v_usersync_id > 0 AND v_module_id > 0 THEN
        SELECT COUNT(*) INTO v_count FROM x_user_module_perm WHERE user_id = v_usersync_id AND module_id = v_module_id;
        IF v_count = 0 THEN
            INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.NEXTVAL,v_usersync_id,v_module_id,SYSDATE,SYSDATE,v_admin_id,v_admin_id,1);
        END IF;
    END IF;
    IF v_tagsync_id > 0 AND v_module_id > 0 THEN
        SELECT COUNT(*) INTO v_count FROM x_user_module_perm WHERE user_id = v_tagsync_id AND module_id = v_module_id;
        IF v_count = 0 THEN
            INSERT INTO x_user_module_perm (id,user_id,module_id,create_time,update_time,added_by_id,upd_by_id,is_allowed) VALUES (X_USER_MODULE_PERM_SEQ.NEXTVAL,v_tagsync_id,v_module_id,SYSDATE,SYSDATE,v_admin_id,v_admin_id,1);
        END IF;
    END IF;
    COMMIT;
END;/
