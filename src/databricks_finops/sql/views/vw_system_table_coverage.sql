CREATE OR REPLACE VIEW {view_name} AS
SELECT
    run_id,
    check_name,
    status,
    severity,
    message,
    affected_output,
    created_at
FROM {health_table}
WHERE check_name LIKE '%available'
ORDER BY severity DESC, check_name
