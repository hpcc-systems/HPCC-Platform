SELECT kind, word, doc, dpos FROM hpcc_scan('regress::multi::searchindex') WHERE kind = 1 AND word = 'a                   ' AND doc > 281474976710757 AND doc < 281474976710657 LIMIT 100
