SELECT kind, word, dpos FROM hpcc_scan('regress::multi::searchindex') WHERE kind IS NOT DISTINCT FROM 1 LIMIT 20
