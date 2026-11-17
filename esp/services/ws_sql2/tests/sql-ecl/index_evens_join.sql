SELECT a.dg_parentid, a.dg_firstname, a.dg_lastname, b.filepos
FROM hpcc_scan('regress::multi::dg_flat') a
JOIN hpcc_scan('regress::multi::dg_index_evens') b
  ON a.dg_firstname = b.dg_firstname AND a.dg_lastname = b.dg_lastname
ORDER BY a.dg_parentid, a.dg_firstname, a.dg_lastname, b.filepos
