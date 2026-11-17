SELECT dg_firstname, dg_lastname, dg_prange
FROM hpcc_scan('regress::multi::dg_keyed_index')
WHERE dg_firstname BETWEEN 'CLAIRE    ' AND 'KELLY     ' AND dg_prange >= 2
ORDER BY dg_firstname, dg_lastname, dg_prange
LIMIT 32
