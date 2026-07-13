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

IF NOT EXISTS (
  SELECT 1 FROM sys.indexes
  WHERE object_id = OBJECT_ID('x_rms_resource_mapping')
    AND name = 'x_rms_resource_mapping_IDX_mapping_version'
)
BEGIN
  CREATE NONCLUSTERED INDEX [x_rms_resource_mapping_IDX_mapping_version] ON [dbo].[x_rms_resource_mapping]([mapping_version] ASC)
END
GO
