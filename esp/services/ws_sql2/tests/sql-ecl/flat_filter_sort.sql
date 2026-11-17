SELECT dg_parentid, dg_firstname, dg_lastname, dg_prange
FROM hpcc_scan('regress::multi::dg_flat')
WHERE dg_parentid < 3
ORDER BY dg_parentid, dg_firstname, dg_lastname, dg_prange
