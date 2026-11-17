SELECT COUNT(*) AS row_count
FROM hpcc_scan('regress::multi::searchindex') a
JOIN (
    SELECT DISTINCT kind AS k FROM hpcc_scan('regress::multi::searchindex')
    WHERE kind = 2 LIMIT 1
) b ON a.kind = b.k
WHERE a.kind >= 1
