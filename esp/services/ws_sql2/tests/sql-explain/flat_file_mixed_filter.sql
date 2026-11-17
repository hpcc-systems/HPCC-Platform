SELECT * FROM hpcc_scan('regress::multi::dg_flat') WHERE dg_parentid < 3 AND least(dg_parentid, 3) < 3
