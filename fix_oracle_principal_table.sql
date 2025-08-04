-- Alternative fix: Replace vx_principal view with a regular table
-- This completely avoids EclipseLink view issues

-- Drop the existing view
DROP VIEW vx_principal;

-- Create a regular table with a single primary key
CREATE TABLE vx_principal (
    id NUMBER(20) NOT NULL,
    principal_name VARCHAR2(255) NOT NULL,
    principal_type NUMBER(11) NOT NULL,
    status NUMBER(11) NOT NULL,
    is_visible NUMBER(11),
    other_attributes CLOB,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    added_by_id NUMBER(20),
    upd_by_id NUMBER(20),
    CONSTRAINT pk_vx_principal PRIMARY KEY (id),
    CONSTRAINT uk_vx_principal_composite UNIQUE (principal_name, principal_type)
);

-- Create sequence for the primary key
CREATE SEQUENCE vx_principal_seq START WITH 1 INCREMENT BY 1;

-- Create a procedure to refresh the principal data
CREATE OR REPLACE PROCEDURE refresh_vx_principal
IS
BEGIN
    -- Clear existing data
    DELETE FROM vx_principal;
    
    -- Insert users
    INSERT INTO vx_principal (id, principal_name, principal_type, status, is_visible, other_attributes, create_time, update_time, added_by_id, upd_by_id)
    SELECT vx_principal_seq.NEXTVAL, u.user_name, 0, u.status, u.is_visible, u.other_attributes, u.create_time, u.update_time, u.added_by_id, u.upd_by_id
    FROM x_user u;
    
    -- Insert groups
    INSERT INTO vx_principal (id, principal_name, principal_type, status, is_visible, other_attributes, create_time, update_time, added_by_id, upd_by_id)
    SELECT vx_principal_seq.NEXTVAL, g.group_name, 1, g.status, g.is_visible, g.other_attributes, g.create_time, g.update_time, g.added_by_id, g.upd_by_id
    FROM x_group g;
    
    -- Insert roles
    INSERT INTO vx_principal (id, principal_name, principal_type, status, is_visible, other_attributes, create_time, update_time, added_by_id, upd_by_id)
    SELECT vx_principal_seq.NEXTVAL, r.name, 2, 1, 1, null, r.create_time, r.update_time, r.added_by_id, r.upd_by_id
    FROM x_role r;
    
    COMMIT;
END;
/

-- Initial population
EXEC refresh_vx_principal;

-- Create triggers to keep the table synchronized
CREATE OR REPLACE TRIGGER trg_vx_principal_user
AFTER INSERT OR UPDATE OR DELETE ON x_user
FOR EACH ROW
BEGIN
    IF INSERTING THEN
        INSERT INTO vx_principal (id, principal_name, principal_type, status, is_visible, other_attributes, create_time, update_time, added_by_id, upd_by_id)
        VALUES (vx_principal_seq.NEXTVAL, :NEW.user_name, 0, :NEW.status, :NEW.is_visible, :NEW.other_attributes, :NEW.create_time, :NEW.update_time, :NEW.added_by_id, :NEW.upd_by_id);
    ELSIF UPDATING THEN
        UPDATE vx_principal 
        SET principal_name = :NEW.user_name, status = :NEW.status, is_visible = :NEW.is_visible, 
            other_attributes = :NEW.other_attributes, update_time = :NEW.update_time, upd_by_id = :NEW.upd_by_id
        WHERE principal_name = :OLD.user_name AND principal_type = 0;
    ELSIF DELETING THEN
        DELETE FROM vx_principal WHERE principal_name = :OLD.user_name AND principal_type = 0;
    END IF;
END;
/

CREATE OR REPLACE TRIGGER trg_vx_principal_group
AFTER INSERT OR UPDATE OR DELETE ON x_group
FOR EACH ROW
BEGIN
    IF INSERTING THEN
        INSERT INTO vx_principal (id, principal_name, principal_type, status, is_visible, other_attributes, create_time, update_time, added_by_id, upd_by_id)
        VALUES (vx_principal_seq.NEXTVAL, :NEW.group_name, 1, :NEW.status, :NEW.is_visible, :NEW.other_attributes, :NEW.create_time, :NEW.update_time, :NEW.added_by_id, :NEW.upd_by_id);
    ELSIF UPDATING THEN
        UPDATE vx_principal 
        SET principal_name = :NEW.group_name, status = :NEW.status, is_visible = :NEW.is_visible, 
            other_attributes = :NEW.other_attributes, update_time = :NEW.update_time, upd_by_id = :NEW.upd_by_id
        WHERE principal_name = :OLD.group_name AND principal_type = 1;
    ELSIF DELETING THEN
        DELETE FROM vx_principal WHERE principal_name = :OLD.group_name AND principal_type = 1;
    END IF;
END;
/

CREATE OR REPLACE TRIGGER trg_vx_principal_role
AFTER INSERT OR UPDATE OR DELETE ON x_role
FOR EACH ROW
BEGIN
    IF INSERTING THEN
        INSERT INTO vx_principal (id, principal_name, principal_type, status, is_visible, other_attributes, create_time, update_time, added_by_id, upd_by_id)
        VALUES (vx_principal_seq.NEXTVAL, :NEW.name, 2, 1, 1, null, :NEW.create_time, :NEW.update_time, :NEW.added_by_id, :NEW.upd_by_id);
    ELSIF UPDATING THEN
        UPDATE vx_principal 
        SET principal_name = :NEW.name, update_time = :NEW.update_time, upd_by_id = :NEW.upd_by_id
        WHERE principal_name = :OLD.name AND principal_type = 2;
    ELSIF DELETING THEN
        DELETE FROM vx_principal WHERE principal_name = :OLD.name AND principal_type = 2;
    END IF;
END;
/

COMMIT;