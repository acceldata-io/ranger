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

-- Add mapping_version column for RMS delta/incremental download support.

IF NOT EXISTS (
  SELECT 1 FROM sys.columns
  WHERE object_id = OBJECT_ID('x_rms_resource_mapping')
    AND name = 'mapping_version'
)
BEGIN
  ALTER TABLE [dbo].[x_rms_resource_mapping] ADD [mapping_version] [bigint] DEFAULT 0 NULL
END
GO

-- SQL Server does NOT backfill existing rows when ADD COLUMN is nullable with
-- a DEFAULT — the DEFAULT applies only to future inserts, so pre-existing
-- mappings would remain NULL. That diverges from MySQL/Oracle/Postgres/SQL
-- Anywhere (which all populate 0 on add) and breaks the delta query on this
-- column: `WHERE mapping_version > :sinceVersion` with sinceVersion=-1 matches
-- 0>-1 elsewhere but not NULL>-1 (unknown) here, so SQL Server plugins would
-- silently miss their baseline mappings on the first delta.
UPDATE [dbo].[x_rms_resource_mapping] SET [mapping_version] = 0 WHERE [mapping_version] IS NULL
GO

IF NOT EXISTS (
  SELECT 1 FROM sys.indexes
  WHERE object_id = OBJECT_ID('x_rms_resource_mapping')
    AND name = 'x_rms_resource_mapping_IDX_mapping_version'
)
BEGIN
  CREATE NONCLUSTERED INDEX [x_rms_resource_mapping_IDX_mapping_version] ON [dbo].[x_rms_resource_mapping]([mapping_version] ASC)
END
GO
