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

-- Create persistent RMS deletion log so plugins can be served correct
-- delta downloads across Admin restarts and HA failovers, and add a
-- watermark column on x_rms_mapping_provider.

IF NOT EXISTS (
  SELECT 1 FROM sys.tables WHERE name = 'x_rms_deletion_log'
)
BEGIN
  CREATE TABLE [dbo].[x_rms_deletion_log] (
    [id]               [bigint] IDENTITY(1,1) NOT NULL,
    [version]          [bigint] NOT NULL,
    [change_timestamp] [datetime2] NULL,
    [hl_resource_guid] [varchar](64) NULL,
    [ll_resource_guid] [varchar](64) NOT NULL,
    [ll_service_id]    [bigint] NOT NULL,
    PRIMARY KEY CLUSTERED ([id] ASC)
  )
END
GO

IF NOT EXISTS (
  SELECT 1 FROM sys.indexes
  WHERE object_id = OBJECT_ID('x_rms_deletion_log')
    AND name = 'x_rms_deletion_log_IDX_svc_ver'
)
BEGIN
  CREATE NONCLUSTERED INDEX [x_rms_deletion_log_IDX_svc_ver] ON [dbo].[x_rms_deletion_log]([ll_service_id] ASC, [version] ASC)
END
GO

IF NOT EXISTS (
  SELECT 1 FROM sys.indexes
  WHERE object_id = OBJECT_ID('x_rms_deletion_log')
    AND name = 'x_rms_deletion_log_IDX_version'
)
BEGIN
  CREATE NONCLUSTERED INDEX [x_rms_deletion_log_IDX_version] ON [dbo].[x_rms_deletion_log]([version] ASC)
END
GO

IF NOT EXISTS (
  SELECT 1 FROM sys.columns
  WHERE object_id = OBJECT_ID('x_rms_mapping_provider')
    AND name = 'deletion_tracking_from_version'
)
BEGIN
  ALTER TABLE [dbo].[x_rms_mapping_provider] ADD [deletion_tracking_from_version] [bigint] DEFAULT 0 NULL
END
GO
