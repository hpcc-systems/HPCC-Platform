SELECT a.dg_parentid, a.dg_firstname
FROM hpcc_scan('regress::multi::dg_flat') a
WHERE NOT EXISTS (
    SELECT 1 FROM hpcc_scan('regress::multi::dg_flat_evens') b
    WHERE b.dg_parentid = a.dg_parentid
)
ORDER BY a.dg_parentid, a.dg_firstname
