WITH
    single_all AS (
        SELECT COUNT(*) AS row_count, SUM(dpos) AS value_sum
        FROM hpcc_scan('regress::multi::searchindex')
    ),
    multi_all AS (
        SELECT COUNT(*) AS row_count, SUM(dpos) AS value_sum
        FROM hpcc_scan('regress::multi::searchindex_default')
    ),
    single_kind AS (
        SELECT COUNT(*) AS row_count, SUM(dpos) AS value_sum
        FROM hpcc_scan('regress::multi::searchindex')
        WHERE kind = 1
    ),
    multi_kind AS (
        SELECT COUNT(*) AS row_count, SUM(dpos) AS value_sum
        FROM hpcc_scan('regress::multi::searchindex_default')
        WHERE kind = 1
    ),
    single_payload AS (
        SELECT COUNT(*) AS row_count, SUM(dpos) AS value_sum
        FROM hpcc_scan('regress::multi::searchindex')
        WHERE dpos > 70000
    ),
    multi_payload AS (
        SELECT COUNT(*) AS row_count, SUM(dpos) AS value_sum
        FROM hpcc_scan('regress::multi::searchindex_default')
        WHERE dpos > 70000
    ),
    key_window AS (
        SELECT dpos FROM hpcc_scan('regress::multi::searchindex_default')
        OFFSET 1000 ROWS FETCH NEXT 25 ROWS ONLY
    )
SELECT
    (SELECT row_count FROM single_all) = (SELECT row_count FROM multi_all)
        AND (SELECT value_sum FROM single_all) = (SELECT value_sum FROM multi_all)
        AS all_rows_match,
    (SELECT row_count FROM single_kind) = (SELECT row_count FROM multi_kind)
        AND (SELECT value_sum FROM single_kind) = (SELECT value_sum FROM multi_kind)
        AS keyed_filter_rows_match,
    (SELECT row_count FROM single_payload) = (SELECT row_count FROM multi_payload)
        AND (SELECT value_sum FROM single_payload) = (SELECT value_sum FROM multi_payload)
        AS payload_filter_rows_match,
    (SELECT COUNT(*) FROM key_window) AS offset_window_row_count;
