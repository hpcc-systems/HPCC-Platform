SELECT a.dg_parentid, a.dg_firstname, b.dg_lastname
FROM hpcc_scan('regress::multi::dg_flat') a
JOIN hpcc_scan('regress::multi::dg_flat_evens') b
  ON a.dg_parentid = b.dg_parentid
ORDER BY a.dg_parentid, a.dg_firstname, b.dg_lastname
