SELECT kind, word, doc, segment, wpos, wip, dpos
FROM hpcc_scan('regress::multi::searchindex')
WHERE kind = 1 AND word = 'a                   '
ORDER BY kind, word, doc, segment, wpos, wip, dpos
LIMIT 256
