SELECT kind, word, COUNT(*) AS n
FROM hpcc_scan('regress::multi::searchindex')
GROUP BY kind, word
ORDER BY n DESC, kind, word
LIMIT 100
