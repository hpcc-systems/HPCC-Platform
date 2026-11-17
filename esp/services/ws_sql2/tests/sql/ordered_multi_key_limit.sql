WITH
    complete AS (
        SELECT wip, original, dpos
        FROM hpcc_scan('regress::multi::searchindex')
        WHERE kind IN (1,2)
            AND doc BETWEEN 281474976739771 AND 281474976746516
            AND word = 'p                   '
            AND wpos < 16
        ORDER BY wip DESC
    ),
    limited AS (
        SELECT wip, original, dpos
        FROM hpcc_scan('regress::multi::searchindex')
        WHERE kind IN (1,2)
            AND doc BETWEEN 281474976739771 AND 281474976746516
            AND word = 'p                   '
            AND wpos < 16
        ORDER BY wip DESC
        LIMIT 100
    ),
    complete_keys AS (
        SELECT wip, row_number() OVER (ORDER BY wip DESC) AS position
        FROM complete
        LIMIT 100
    ),
    limited_keys AS (
        SELECT wip, row_number() OVER (ORDER BY wip DESC) AS position
        FROM limited
    )
SELECT
    NOT EXISTS (
        SELECT * FROM complete_keys
        EXCEPT ALL
        SELECT * FROM limited_keys
    )
        AND NOT EXISTS (
            SELECT * FROM limited_keys
            EXCEPT ALL
            SELECT * FROM complete_keys
        ) AS same_ordered_sort_keys,
    NOT EXISTS (
        SELECT 1
        FROM limited l
        WHERE NOT EXISTS (
            SELECT 1
            FROM complete c
            WHERE c.wip IS NOT DISTINCT FROM l.wip
                AND c.original IS NOT DISTINCT FROM l.original
                AND c.dpos IS NOT DISTINCT FROM l.dpos
        )
    ) AS limited_rows_exist_in_complete_result;
