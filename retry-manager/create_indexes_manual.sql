-- Alternative: Manual Index Creation (if IF NOT EXISTS doesn't work)
-- Use this version if you get syntax errors with IF NOT EXISTS
-- Run each CREATE INDEX statement individually and ignore "Duplicate key name" errors

USE wms;

-- ============================================
-- Indexes for serverless_workflows table
-- ============================================

CREATE INDEX idx_swv_wb ON serverless_workflows(workflow_name, baseline);
CREATE INDEX idx_swv_uid ON serverless_workflows(uuid_passed);
CREATE INDEX idx_swv_uid_wb ON serverless_workflows(uuid_passed, workflow_name, baseline);
CREATE INDEX idx_swv_wb_stage_code ON serverless_workflows(workflow_name, baseline, workflow_stage, response_code);

-- ============================================
-- Indexes for task_checkpoints table
-- ============================================

CREATE INDEX idx_tc_uwb ON task_checkpoints(uid, workflow_name, baseline);
CREATE INDEX idx_tc_uwb_ts ON task_checkpoints(uid, workflow_name, baseline, timestamp);

-- ============================================
-- Indexes for checkpoint_retries table
-- ============================================

CREATE INDEX idx_cr_wb_retry ON checkpoint_retries(workflow_name, baseline, retry_count);
CREATE INDEX idx_cr_uid_wb ON checkpoint_retries(uid, workflow_name, baseline);
CREATE INDEX idx_cr_wb_time ON checkpoint_retries(workflow_name, baseline, last_attempt_at);



