SELECT kind, word, dpos FROM hpcc_scan('regress::multi::searchindex') WHERE kind = 1 AND word = 'a                   ' LIMIT 20
