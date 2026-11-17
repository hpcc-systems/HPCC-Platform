SELECT kind, word, dpos FROM hpcc_scan('regress::multi::searchindex') WHERE word = 'a' LIMIT 20
