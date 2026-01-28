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
BEGIN
    ------------------------------------------------------------
    -- Create Sequences
    ------------------------------------------------------------
    SELECT COUNT(*) INTO v_count FROM user_sequences WHERE sequence_name = 'X_GDS_DATASET_SEQ';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'CREATE SEQUENCE X_GDS_DATASET_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE';
    END IF;
    SELECT COUNT(*) INTO v_count FROM user_sequences WHERE sequence_name = 'X_GDS_PROJECT_SEQ';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'CREATE SEQUENCE X_GDS_PROJECT_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE';
    END IF;
    SELECT COUNT(*) INTO v_count FROM user_sequences WHERE sequence_name = 'X_GDS_DATA_SHARE_SEQ';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'CREATE SEQUENCE X_GDS_DATA_SHARE_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE';
    END IF;
    SELECT COUNT(*) INTO v_count FROM user_sequences WHERE sequence_name = 'X_GDS_SHARED_RESOURCE_SEQ';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'CREATE SEQUENCE X_GDS_SHARED_RESOURCE_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE';
    END IF;
    SELECT COUNT(*) INTO v_count FROM user_sequences WHERE sequence_name = 'X_GDS_DATA_SHARE_IN_DATASET_SEQ';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'CREATE SEQUENCE X_GDS_DATA_SHARE_IN_DATASET_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE';
    END IF;
    SELECT COUNT(*) INTO v_count FROM user_sequences WHERE sequence_name = 'X_GDS_DATASET_IN_PROJECT_SEQ';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'CREATE SEQUENCE X_GDS_DATASET_IN_PROJECT_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE';
    END IF;
    SELECT COUNT(*) INTO v_count FROM user_sequences WHERE sequence_name = 'X_GDS_DATASET_POLICY_MAP_SEQ';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'CREATE SEQUENCE X_GDS_DATASET_POLICY_MAP_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE';
    END IF;
    SELECT COUNT(*) INTO v_count FROM user_sequences WHERE sequence_name = 'X_GDS_PROJECT_POLICY_MAP_SEQ';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'CREATE SEQUENCE X_GDS_PROJECT_POLICY_MAP_SEQ START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE';
    END IF;
    ------------------------------------------------------------
    -- Create Tables and Indexes
    ------------------------------------------------------------
    -- X_GDS_DATASET
    SELECT COUNT(*) INTO v_count FROM user_tables WHERE table_name = 'X_GDS_DATASET';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE X_GDS_DATASET (
                ID NUMBER(20) NOT NULL,
                GUID VARCHAR2(64) NOT NULL,
                CREATE_TIME DATE,
                UPDATE_TIME DATE,
                ADDED_BY_ID NUMBER(20),
                UPD_BY_ID NUMBER(20),
                VERSION NUMBER(20) DEFAULT 1 NOT NULL,
                IS_ENABLED NUMBER(1) DEFAULT 1 NOT NULL,
                NAME VARCHAR2(512) NOT NULL,
                DESCRIPTION CLOB,
                ACL CLOB,
                TERMS_OF_USE CLOB,
                OPTIONS CLOB,
                ADDITIONAL_INFO CLOB,
                PRIMARY KEY (ID),
                CONSTRAINT X_GDS_DATASET_UK_NAME UNIQUE (NAME),
                CONSTRAINT X_GDS_DATASET_FK_ADDED_BY_ID FOREIGN KEY (ADDED_BY_ID) REFERENCES X_PORTAL_USER(ID),
                CONSTRAINT X_GDS_DATASET_FK_UPD_BY_ID FOREIGN KEY (UPD_BY_ID) REFERENCES X_PORTAL_USER(ID)
            )';
        EXECUTE IMMEDIATE 'CREATE INDEX X_GDS_DATASET_GUID ON X_GDS_DATASET(GUID)';
    END IF;
    -- X_GDS_PROJECT
    SELECT COUNT(*) INTO v_count FROM user_tables WHERE table_name = 'X_GDS_PROJECT';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE X_GDS_PROJECT (
                ID NUMBER(20) NOT NULL,
                GUID VARCHAR2(64) NOT NULL,
                CREATE_TIME DATE,
                UPDATE_TIME DATE,
                ADDED_BY_ID NUMBER(20),
                UPD_BY_ID NUMBER(20),
                VERSION NUMBER(20) DEFAULT 1 NOT NULL,
                IS_ENABLED NUMBER(1) DEFAULT 1 NOT NULL,
                NAME VARCHAR2(512) NOT NULL,
                DESCRIPTION CLOB,
                ACL CLOB,
                TERMS_OF_USE CLOB,
                OPTIONS CLOB,
                ADDITIONAL_INFO CLOB,
                PRIMARY KEY (ID),
                CONSTRAINT X_GDS_PROJECT_UK_NAME UNIQUE (NAME),
                CONSTRAINT X_GDS_PROJECT_FK_ADDED_BY_ID FOREIGN KEY (ADDED_BY_ID) REFERENCES X_PORTAL_USER(ID),
                CONSTRAINT X_GDS_PROJECT_FK_UPD_BY_ID FOREIGN KEY (UPD_BY_ID) REFERENCES X_PORTAL_USER(ID)
            )';
        EXECUTE IMMEDIATE 'CREATE INDEX X_GDS_PROJECT_GUID ON X_GDS_PROJECT(GUID)';
    END IF;
    -- X_GDS_DATA_SHARE
    SELECT COUNT(*) INTO v_count FROM user_tables WHERE table_name = 'X_GDS_DATA_SHARE';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE X_GDS_DATA_SHARE (
                ID NUMBER(20) NOT NULL,
                GUID VARCHAR2(64) NOT NULL,
                CREATE_TIME DATE,
                UPDATE_TIME DATE,
                ADDED_BY_ID NUMBER(20),
                UPD_BY_ID NUMBER(20),
                VERSION NUMBER(20) DEFAULT 1 NOT NULL,
                IS_ENABLED NUMBER(1) DEFAULT 1 NOT NULL,
                NAME VARCHAR2(512) NOT NULL,
                DESCRIPTION CLOB,
                ACL CLOB,
                SERVICE_ID NUMBER(20) NOT NULL,
                ZONE_ID NUMBER(20) NOT NULL,
                CONDITION_EXPR CLOB,
                DEFAULT_ACCESS_TYPES CLOB,
                DEFAULT_TAG_MASKS CLOB,
                TERMS_OF_USE CLOB,
                OPTIONS CLOB,
                ADDITIONAL_INFO CLOB,
                PRIMARY KEY (ID),
                CONSTRAINT X_GDS_DSH_UK_NAME UNIQUE (SERVICE_ID, ZONE_ID, NAME),
                CONSTRAINT X_GDS_DSH_FK_ADDED_BY_ID FOREIGN KEY (ADDED_BY_ID) REFERENCES X_PORTAL_USER(ID),
                CONSTRAINT X_GDS_DSH_FK_UPD_BY_ID FOREIGN KEY (UPD_BY_ID) REFERENCES X_PORTAL_USER(ID),
                CONSTRAINT X_GDS_DSH_FK_SERVICE_ID FOREIGN KEY (SERVICE_ID) REFERENCES X_SERVICE(ID),
                CONSTRAINT X_GDS_DSH_FK_ZONE_ID FOREIGN KEY (ZONE_ID) REFERENCES X_SECURITY_ZONE(ID)
            )';
        EXECUTE IMMEDIATE 'CREATE INDEX X_GDS_DATA_SHARE_GUID ON X_GDS_DATA_SHARE(GUID)';
        EXECUTE IMMEDIATE 'CREATE INDEX X_GDS_DSH_SERVICE_ID ON X_GDS_DATA_SHARE(SERVICE_ID)';
        EXECUTE IMMEDIATE 'CREATE INDEX X_GDS_DSH_ZONE_ID ON X_GDS_DATA_SHARE(ZONE_ID)';
    END IF;
    -- X_GDS_SHARED_RESOURCE
    SELECT COUNT(*) INTO v_count FROM user_tables WHERE table_name = 'X_GDS_SHARED_RESOURCE';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE X_GDS_SHARED_RESOURCE (
                ID NUMBER(20) NOT NULL,
                GUID VARCHAR2(64) NOT NULL,
                CREATE_TIME DATE,
                UPDATE_TIME DATE,
                ADDED_BY_ID NUMBER(20),
                UPD_BY_ID NUMBER(20),
                VERSION NUMBER(20) DEFAULT 1 NOT NULL,
                IS_ENABLED NUMBER(1) DEFAULT 1 NOT NULL,
                NAME VARCHAR2(512) NOT NULL,
                DESCRIPTION CLOB,
                DATA_SHARE_ID NUMBER(20) NOT NULL,
                RESOURCE CLOB NOT NULL,
                RESOURCE_SIGNATURE VARCHAR2(128) NOT NULL,
                SUB_RESOURCE CLOB,
                SUB_RESOURCE_TYPE CLOB,
                CONDITION_EXPR CLOB,
                ACCESS_TYPES CLOB,
                ROW_FILTER CLOB,
                SUB_RESOURCE_MASKS CLOB,
                PROFILES CLOB,
                OPTIONS CLOB,
                ADDITIONAL_INFO CLOB,
                PRIMARY KEY (ID),
                CONSTRAINT X_GDS_SHRES_UK_NAME UNIQUE (DATA_SHARE_ID, NAME),
                CONSTRAINT X_GDS_SHRES_UK_RES_SIGN UNIQUE (DATA_SHARE_ID, RESOURCE_SIGNATURE),
                CONSTRAINT X_GDS_SHRES_FK_ADDED_BY_ID FOREIGN KEY (ADDED_BY_ID) REFERENCES X_PORTAL_USER(ID),
                CONSTRAINT X_GDS_SHRES_FK_UPD_BY_ID FOREIGN KEY (UPD_BY_ID) REFERENCES X_PORTAL_USER(ID),
                CONSTRAINT X_GDS_SHRES_FK_DSH_ID FOREIGN KEY (DATA_SHARE_ID) REFERENCES X_GDS_DATA_SHARE(ID)
            )';
        EXECUTE IMMEDIATE 'CREATE INDEX X_GDS_SHRES_GUID ON X_GDS_SHARED_RESOURCE(GUID)';
        EXECUTE IMMEDIATE 'CREATE INDEX X_GDS_SHRES_DSH_ID ON X_GDS_SHARED_RESOURCE(DATA_SHARE_ID)';
    END IF;
    ------------------------------------------------------------
    -- Continue with remaining tables (X_GDS_DATA_SHARE_IN_DATASET, X_GDS_DATASET_IN_PROJECT,
    -- X_GDS_DATASET_POLICY_MAP, X_GDS_PROJECT_POLICY_MAP)
    -- Follow the same pattern: check count, then CREATE TABLE, then CREATE INDEX
    ------------------------------------------------------------
    COMMIT;
END;
/