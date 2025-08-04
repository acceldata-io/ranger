# ODP-4248 Oracle SQL Fix Guide

## Problem Description

The ORA-00923 error ("FROM keyword not found where expected") occurs because EclipseLink JPA cannot properly handle the composite primary key in the `vx_principal` view. The view has two `@Id` fields (`principal_name` and `principal_type`) which causes EclipseLink to generate malformed SQL queries.

## Root Cause

- **EclipseLink Limitation**: EclipseLink generates incorrect SQL when querying views with composite primary keys without proper `@IdClass` mapping
- **View Structure**: The `vx_principal` view combines data from `x_user`, `x_group`, and `x_role` tables with a composite key
- **SQL Generation**: EclipseLink tries to create WHERE clauses that result in malformed SQL syntax

## SQL-Based Solutions

I've provided three different SQL-based approaches to fix this issue:

### Solution 1: Simple View with Synthetic ID (RECOMMENDED)

**File**: `fix_oracle_simple_view.sql`

This is the **simplest and recommended** solution. It recreates the view with a hash-based synthetic ID:

```sql
-- Drop existing view
DROP VIEW vx_principal;

-- Create view with synthetic ID
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
    -- Union of users, groups, and roles
    ...
);
```

**Advantages**:
- ✅ Simple to implement
- ✅ No triggers or additional maintenance
- ✅ Maintains view semantics
- ✅ Single primary key for EclipseLink

**Usage**:
```bash
sqlplus rangertest1/password@//orclnode01.acceldata.ce:1521/ORCLPDB1 @fix_oracle_simple_view.sql
```

### Solution 2: Materialized View (ADVANCED)

**File**: `fix_oracle_principal_view.sql`

Creates a materialized view with better performance but requires refresh management:

```sql
CREATE MATERIALIZED VIEW vx_principal
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS SELECT ROWNUM as id, ...
```

**Advantages**:
- ✅ Better query performance
- ✅ Single primary key
- ❌ Requires periodic refresh
- ❌ More complex maintenance

### Solution 3: Regular Table with Triggers (COMPLEX)

**File**: `fix_oracle_principal_table.sql`

Replaces the view with a regular table and keeps it synchronized with triggers:

**Advantages**:
- ✅ Best performance
- ✅ Full table features
- ❌ Complex trigger maintenance
- ❌ Potential sync issues

## Implementation Steps

### Step 1: Choose Your Solution

For most cases, use **Solution 1** (simple view with synthetic ID).

### Step 2: Backup Your Database

```sql
-- Create backup of current view definition
CREATE TABLE vx_principal_backup AS SELECT * FROM vx_principal;
```

### Step 3: Apply the Fix

```bash
# Connect to Oracle as Ranger schema owner
sqlplus rangertest1/password@//orclnode01.acceldata.ce:1521/ORCLPDB1

# Run the chosen fix script
@fix_oracle_simple_view.sql
```

### Step 4: Verify the Fix

```sql
-- Check view structure
DESC vx_principal;

-- Verify data integrity
SELECT COUNT(*) FROM vx_principal;
SELECT principal_type, COUNT(*) FROM vx_principal GROUP BY principal_type;

-- Test a simple query
SELECT * FROM vx_principal WHERE principal_name = 'admin';
```

### Step 5: Restart Ranger Admin

```bash
# Stop Ranger Admin
systemctl stop ranger-admin

# Clear any cached metadata if needed
rm -rf /tmp/ranger-admin-cache/* 

# Start Ranger Admin
systemctl start ranger-admin
```

### Step 6: Test Password Change

```bash
# Test the password change operation that was failing
cd /usr/odp/current/ranger-admin
./setup.sh
```

## Schema File Updates

The following Oracle schema files have been updated with the fix:

1. `security-admin/db/oracle/optimized/current/ranger_core_db_oracle.sql`
2. `security-admin/db/oracle/patches/068-create-view-principal.sql`

These changes ensure that new installations will have the correct view definition.

## Troubleshooting

### If you still get ORA-00923 errors:

1. **Check view exists**:
   ```sql
   SELECT * FROM user_views WHERE view_name = 'VX_PRINCIPAL';
   ```

2. **Verify ID column**:
   ```sql
   SELECT id, principal_name, principal_type FROM vx_principal WHERE ROWNUM <= 5;
   ```

3. **Clear Oracle shared pool**:
   ```sql
   ALTER SYSTEM FLUSH SHARED_POOL;
   ```

4. **Restart Ranger Admin service**

### If EclipseLink still complains:

1. Check that no cached query plans reference the old view structure
2. Verify that the `id` column is properly recognized as the primary key
3. Clear any application-level caches

## Rollback Plan

If you need to rollback to the original view:

```sql
-- Drop the fixed view
DROP VIEW vx_principal;

-- Recreate original view
CREATE VIEW vx_principal as
    (SELECT u.user_name  AS principal_name, 0 AS principal_type, u.status AS status, u.is_visible AS is_visible, u.other_attributes AS other_attributes, u.create_time AS create_time, u.update_time AS update_time, u.added_by_id AS added_by_id, u.upd_by_id AS upd_by_id FROM x_user u)  UNION ALL
    (SELECT g.group_name AS principal_name, 1 AS principal_type, g.status AS status, g.is_visible AS is_visible, g.other_attributes AS other_attributes, g.create_time AS create_time, g.update_time AS update_time, g.added_by_id AS added_by_id, g.upd_by_id AS upd_by_id FROM x_group g) UNION ALL
    (SELECT r.name       AS principal_name, 2 AS principal_type, 1        AS status, 1            AS is_visible, null               AS other_attributes, r.create_time AS create_time, r.update_time AS update_time, r.added_by_id AS added_by_id, r.upd_by_id AS upd_by_id FROM x_role r);
```

## Testing Checklist

After applying the fix, verify:

- [ ] Ranger Admin starts without errors
- [ ] No more ORA-00923 errors in logs
- [ ] Password change operations work
- [ ] User authentication works
- [ ] Policy operations work
- [ ] User/group sync works
- [ ] Performance is acceptable

This SQL-based approach completely avoids the need to modify Java code and resolves the EclipseLink composite key issue at the database level.