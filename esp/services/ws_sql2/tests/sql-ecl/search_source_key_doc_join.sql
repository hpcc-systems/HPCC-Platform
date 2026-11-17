WITH source_docs AS (
    SELECT doc, COUNT(*) AS source_count
    FROM hpcc_scan('regress::multi::searchsource')
    GROUP BY doc
),
key_docs AS (
    SELECT doc, COUNT(*) AS key_count
    FROM hpcc_scan('regress::multi::searchindex')
    WHERE kind = 1 AND word = 'a                   '
    GROUP BY doc
)
SELECT s.doc, s.source_count, k.key_count
FROM source_docs s JOIN key_docs k ON s.doc = k.doc
ORDER BY s.doc
LIMIT 512
