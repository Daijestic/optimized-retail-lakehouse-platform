-- Fail early when manually executing with psql.
\set ON_ERROR_STOP on

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id UUID PRIMARY KEY,
    pipeline_name TEXT NOT NULL,

    status TEXT NOT NULL
        CHECK (status IN ('running', 'success', 'failed')),

    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ,

    input_rows BIGINT NOT NULL DEFAULT 0
        CHECK (input_rows >= 0),

    output_rows BIGINT NOT NULL DEFAULT 0
        CHECK (output_rows >= 0),

    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CHECK (
        finished_at IS NULL
        OR finished_at >= started_at
    )
);

CREATE TABLE IF NOT EXISTS data_quality_results (
    id BIGSERIAL PRIMARY KEY,

    run_id UUID REFERENCES pipeline_runs(run_id)
        ON DELETE CASCADE,

    rule_name TEXT NOT NULL,

    passed BOOLEAN NOT NULL,
    failed_count BIGINT NOT NULL DEFAULT 0
        CHECK (failed_count >= 0),

    details JSONB NOT NULL DEFAULT '{}'::JSONB,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_data_quality_results_run_id
    ON data_quality_results(run_id);

CREATE TABLE IF NOT EXISTS benchmark_runs (
    id BIGSERIAL PRIMARY KEY,

    run_id UUID,
    experiment_name TEXT NOT NULL,

    status TEXT NOT NULL
        CHECK (status IN ('running', 'success', 'failed')),

    metrics JSONB NOT NULL DEFAULT '{}'::JSONB,

    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CHECK (
        finished_at IS NULL
        OR finished_at >= started_at
    )
);

CREATE INDEX IF NOT EXISTS idx_benchmark_runs_experiment_name
    ON benchmark_runs(experiment_name);

CREATE INDEX IF NOT EXISTS idx_benchmark_runs_created_at
    ON benchmark_runs(created_at);