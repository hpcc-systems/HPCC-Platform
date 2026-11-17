SELECT COUNT(*) AS row_count, COALESCE(SUM(dpos), 0) AS dpos_total
FROM hpcc_scan('regress::multi::searchindex')
WHERE kind <> -1 AND wpos < 3
