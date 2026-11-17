SELECT word, COUNT(*) AS n
FROM hpcc_scan('regress::multi::searchsource')
WHERE kind = 1
GROUP BY word
ORDER BY n DESC, word
LIMIT 100
