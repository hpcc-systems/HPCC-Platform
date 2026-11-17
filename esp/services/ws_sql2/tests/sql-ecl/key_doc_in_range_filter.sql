SELECT COUNT(*) AS row_count, COALESCE(SUM(dpos), 0) AS dpos_total
FROM hpcc_scan('regress::multi::searchindex')
WHERE kind = 1 AND word = 'p                   '
  AND doc IN (281474976739771, 281474976746516, 281474976740000) AND doc > 281474976739771
