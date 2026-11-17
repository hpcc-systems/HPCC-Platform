WITH
    books AS (SELECT id, price FROM hpcc_scan('regress::multi::book') LIMIT 3),
    people AS (SELECT id, dob, booklimit FROM hpcc_scan('regress::multi::person') LIMIT 3)
SELECT
    (SELECT list(id ORDER BY id) FROM books) = [1, 2, 3]
        AND (SELECT list(price ORDER BY id) FROM books) = [0.0, 3.99, 0.0]
        AS packed_decimal_values_match,
    (SELECT list(id ORDER BY id) FROM people) = [1, 2, 3]
        AND (SELECT list(dob ORDER BY id) FROM people) = [19700101, 20000101, 19700909]
        AND (SELECT list(booklimit ORDER BY id) FROM people) = [1000, 40, 0]
        AS scale_zero_decimal_values_match,
    (SELECT COUNT(*) FROM hpcc_scan('regress::single::dg_grouped'))
        = (SELECT COUNT(*) FROM hpcc_scan('regress::single::dg_flat'))
        AS single_grouped_row_count_matches,
    (SELECT COUNT(*) FROM hpcc_scan('regress::multi::dg_grouped'))
        = (SELECT COUNT(*) FROM hpcc_scan('regress::multi::dg_flat'))
        AS multi_grouped_row_count_matches,
    (SELECT list(dg_parentid)
     FROM (
         SELECT dg_parentid
         FROM (SELECT dg_parentid FROM hpcc_scan('regress::multi::dg_grouped') LIMIT 8) prefix
         OFFSET 5
     ) prefix_tail)
        IS NOT DISTINCT FROM
    (SELECT list(dg_parentid)
     FROM (
         SELECT dg_parentid FROM hpcc_scan('regress::multi::dg_grouped')
         OFFSET 5 ROWS FETCH NEXT 3 ROWS ONLY
     ) page) AS grouped_offset_matches;
