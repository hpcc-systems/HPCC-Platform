SELECT kind, word, doc, dpos FROM hpcc_scan('regress::multi::searchindex') WHERE kind = 1 AND word = 'a                   ' AND doc IN (281474976710657, 281474976710658) LIMIT 50
