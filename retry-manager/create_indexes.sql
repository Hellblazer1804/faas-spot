-- Database Indexes for Retry Service Performance
-- Run this script in your MySQL database to create all required indexes
-- These indexes significantly improve query performance for the retry service
--
-- Note: If you get "Duplicate key name" errors, the indexes already exist (safe to ignore)
-- Note: MySQL 5.7+ supports IF NOT EXISTS, older versions may need manual checking

USE wms;

-- ============================================
-- Indexes for serverless_workflows table
-- ============================================

-- Composite index for workflow_name + baseline queries
-- Used frequently in get_uids_incomplete_workflows()
CREATE INDEX IF NOT EXISTS idx_swv_wb 
ON serverless_workflows(workflow_name, baseline);

-- Index for UUID lookups
-- Used for finding workflows by UUID
CREATE INDEX IF NOT EXISTS idx_swv_uid 
ON serverless_workflows(uuid_passed);

-- Composite index for UUID + workflow + baseline queries
-- Used in get_completed_tasks() for fast UUID-based lookups
CREATE INDEX IF NOT EXISTS idx_swv_uid_wb 
ON serverless_workflows(uuid_passed, workflow_name, baseline);

-- Composite index for workflow + baseline + stage + response_code queries
-- Used for finding successful final tasks efficiently
CREATE INDEX IF NOT EXISTS idx_swv_wb_stage_code 
ON serverless_workflows(workflow_name, baseline, workflow_stage, response_code);

-- ============================================
-- Indexes for task_checkpoints table
-- ============================================

-- Composite index for uid + workflow + baseline lookups
-- Used for checkpoint queries
CREATE INDEX IF NOT EXISTS idx_tc_uwb 
ON task_checkpoints(uid, workflow_name, baseline);

-- Composite index for uid + workflow + baseline + timestamp (for latest checkpoint)
-- Used in get_latest_checkpoint() - ORDER BY timestamp DESC
CREATE INDEX IF NOT EXISTS idx_tc_uwb_ts 
ON task_checkpoints(uid, workflow_name, baseline, timestamp);

-- ============================================
-- Indexes for checkpoint_retries table
-- ============================================

-- Composite index for workflow + baseline + retry_count (for over-retried check)
-- Used to find UIDs that have exceeded max retries
CREATE INDEX IF NOT EXISTS idx_cr_wb_retry 
ON checkpoint_retries(workflow_name, baseline, retry_count);

-- Composite index for uid + workflow + baseline
-- Used for UID-specific retry queries
CREATE INDEX IF NOT EXISTS idx_cr_uid_wb 
ON checkpoint_retries(uid, workflow_name, baseline);

-- Composite index for workflow + baseline + last_attempt_at (for cooldown check)
-- Used for adaptive retry cooldown calculations
CREATE INDEX IF NOT EXISTS idx_cr_wb_time 
ON checkpoint_retries(workflow_name, baseline, last_attempt_at);

-- ============================================
-- Verification Queries (run after creating indexes)
-- ============================================
-- Uncomment these to verify all indexes were created:

-- SHOW INDEX FROM serverless_workflows WHERE Key_name LIKE 'idx_%';
-- SHOW INDEX FROM task_checkpoints WHERE Key_name LIKE 'idx_%';
-- SHOW INDEX FROM checkpoint_retries WHERE Key_name LIKE 'idx_%';

-- Expected output: 4 indexes for serverless_workflows, 2 for task_checkpoints, 3 for checkpoint_retries

