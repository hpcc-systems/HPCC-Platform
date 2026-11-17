SELECT dg_parentid, dg_firstname, dg_lastname
FROM hpcc_scan('regress::multi::dg_flat')
WHERE dg_firstname = 'CLAIRE    '
ORDER BY dg_parentid, dg_lastname
