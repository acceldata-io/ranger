-- Simplest fix: Recreate vx_principal view with a synthetic single primary key
-- Uses hash of principal_name + principal_type to create a unique ID

-- Drop the existing view
DROP VIEW vx_principal;

-- Create view with synthetic ID based on hash of composite key
CREATE VIEW vx_principal AS
SELECT 
    ABS(ORA_HASH(principal_name || '_' || principal_type)) as id,
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

COMMIT;