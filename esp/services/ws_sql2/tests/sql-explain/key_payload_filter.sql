SELECT kind, word, dpos FROM hpcc_scan('regress::multi::searchindex') WHERE dpos > 70000 LIMIT 20
