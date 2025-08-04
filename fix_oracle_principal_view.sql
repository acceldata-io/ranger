-- Fix for ODP-4248 Oracle ORA-00923 error
-- Replace vx_principal view with a materialized view that has a single primary key
-- This avoids EclipseLink composite key issues

-- Drop the existing view
DROP VIEW vx_principal;

-- Create a materialized view with a single synthetic primary key
CREATE MATERIALIZED VIEW vx_principal
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT 
    ROWNUM as id,
    principal_name,
    principal_type,
    status,
    is_visible,
    other_attributes,
    create_time,
    update_time,
    added_by_id,
    upd_by_id
FROM (
    SELECT u.user_name  AS principal_name, 0 AS principal_type, u.status AS status, u.is_visible AS is_visible, u.other_attributes AS other_attributes, u.create_time AS create_time, u.update_time AS update_time, u.added_by_id AS added_by_id, u.upd_by_id AS upd_by_id FROM x_user u
    UNION ALL
    SELECT g.group_name AS principal_name, 1 AS principal_type, g.status AS status, g.is_visible AS is_visible, g.other_attributes AS other_attributes, g.create_time AS create_time, g.update_time AS update_time, g.added_by_id AS added_by_id, g.upd_by_id AS upd_by_id FROM x_group g
    UNION ALL
    SELECT r.name       AS principal_name, 2 AS principal_type, 1        AS status, 1            AS is_visible, null               AS other_attributes, r.create_time AS create_time, r.update_time AS update_time, r.added_by_id AS added_by_id, r.upd_by_id AS upd_by_id FROM x_role r
);

-- Add primary key constraint on the synthetic ID
ALTER TABLE vx_principal ADD CONSTRAINT pk_vx_principal PRIMARY KEY (id);

-- Create unique index on the original composite key for performance
CREATE UNIQUE INDEX idx_vx_principal_composite ON vx_principal (principal_name, principal_type);

-- Refresh the materialized view
EXEC DBMS_MVIEW.REFRESH('vx_principal', 'C');

COMMIT;