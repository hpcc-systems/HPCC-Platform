SELECT a.doc, COUNT(*) AS row_count
FROM hpcc_scan('regress::multi::searchindex') a
JOIN (
    SELECT DISTINCT doc AS k FROM hpcc_scan('regress::multi::searchindex')
    WHERE kind = 1 AND word = 'p                   ' ORDER BY k LIMIT 20
) b ON a.doc = b.k
WHERE a.kind = 1
GROUP BY a.doc
ORDER BY a.doc
