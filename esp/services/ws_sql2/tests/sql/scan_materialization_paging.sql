WITH
    search_index AS (
        SELECT dpos
        FROM hpcc_scan('regress::multi::searchindex')
        LIMIT 3
    ),
    fetch_index AS (
        SELECT fname, lname, blobfield
        FROM hpcc_scan('regress::multi::dg_fetchindex1')
        LIMIT 3
    ),
    prior_page AS (
        SELECT dpos
        FROM hpcc_scan('regress::multi::searchindex')
        OFFSET 9 ROWS FETCH NEXT 4 ROWS ONLY
    ),
    next_page AS (
        SELECT dpos
        FROM hpcc_scan('regress::multi::searchindex')
        OFFSET 10 ROWS FETCH NEXT 3 ROWS ONLY
    )
SELECT
    NOT EXISTS (
        SELECT 1 FROM search_index
        WHERE typeof(dpos) NOT IN ('TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT',
                                   'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT')
    ) AS dpos_is_integer,
    NOT EXISTS (
        SELECT 1 FROM fetch_index
        WHERE rtrim(blobfield) IS DISTINCT FROM fname || rtrim(lname)
    ) AS multipart_blob_matches_fields,
    (SELECT list(dpos) FROM (SELECT dpos FROM prior_page OFFSET 1) tail)
        IS NOT DISTINCT FROM (SELECT list(dpos) FROM next_page) AS offset_pages_match;
