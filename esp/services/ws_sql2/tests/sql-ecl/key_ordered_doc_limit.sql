SELECT kind, doc, dpos
FROM hpcc_scan('regress::multi::searchindex')
WHERE kind = 1 AND word = 'p                   '
  AND doc BETWEEN 281474976739771 AND 281474976746516
ORDER BY doc DESC, dpos
LIMIT 5
