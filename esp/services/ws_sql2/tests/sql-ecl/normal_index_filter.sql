SELECT dg_firstname, dg_lastname, filepos
FROM hpcc_scan('regress::multi::dg_index')
WHERE dg_firstname BETWEEN 'CLAIRE    ' AND 'KELLY     '
ORDER BY dg_firstname, dg_lastname, filepos
LIMIT 32
