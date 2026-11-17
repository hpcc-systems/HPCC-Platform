SELECT lname, fname, tfn, __filepos FROM hpcc_scan('regress::multi::dg_fetchtransindex') WHERE tfn = 'Jane' LIMIT 20
