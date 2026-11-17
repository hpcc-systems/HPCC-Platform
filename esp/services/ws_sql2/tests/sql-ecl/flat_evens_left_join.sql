SELECT a.dg_parentid, a.dg_firstname, COALESCE(b.dg_parentid, 0) AS even_parentid
FROM hpcc_scan('regress::multi::dg_flat') a
LEFT JOIN hpcc_scan('regress::multi::dg_flat_evens') b
  ON a.dg_parentid = b.dg_parentid
ORDER BY a.dg_parentid, a.dg_firstname, even_parentid
