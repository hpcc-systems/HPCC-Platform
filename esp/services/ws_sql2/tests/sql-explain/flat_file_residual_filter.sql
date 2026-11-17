SELECT * FROM hpcc_scan('regress::multi::dg_flat') WHERE least(dg_parentid, 3) < 3
