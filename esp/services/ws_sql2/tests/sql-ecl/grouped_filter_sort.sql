SELECT dg_parentid, dg_prange
FROM hpcc_scan('regress::multi::dg_grouped')
WHERE dg_prange >= 2
ORDER BY dg_parentid, dg_prange
LIMIT 36
