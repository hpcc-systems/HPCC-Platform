SELECT wip, dpos
FROM hpcc_scan('regress::multi::searchindex')
WHERE kind = 1 AND word = 'p                   '
ORDER BY wip DESC, dpos
LIMIT 5
