SELECT kind, doc, segment, wpos, wip, original, word, dpos
FROM hpcc_scan('regress::multi::searchindex')
WHERE kind IN (1, 2)
ORDER BY kind DESC, dpos, doc, segment, wpos, wip, original, word
LIMIT 5
