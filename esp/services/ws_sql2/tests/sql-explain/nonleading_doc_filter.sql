SELECT kind, word, doc, dpos FROM hpcc_scan('regress::multi::searchindex') WHERE doc = 281474976710657 LIMIT 20
