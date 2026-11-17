SELECT dg_firstname, COUNT(*) AS n
FROM hpcc_scan('regress::multi::dg_flat')
GROUP BY dg_firstname
ORDER BY dg_firstname
