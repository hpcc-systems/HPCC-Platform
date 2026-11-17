SELECT dg_parentid, dg_firstname, dg_lastname
FROM hpcc_scan('regress::multi::dg_flat_evens')
ORDER BY dg_parentid DESC, dg_firstname, dg_lastname
LIMIT 24
