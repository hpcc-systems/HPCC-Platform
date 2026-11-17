WITH comparisons AS (
    SELECT * FROM (VALUES
        (1, CAST('1.25000' AS DECIMAL(37, 5)), CAST('1.250000' AS DECIMAL(38, 6))),
        (2, CAST('-0.00001' AS DECIMAL(37, 5)), CAST('0.000000' AS DECIMAL(38, 6))),
        (3, CAST('12345678901234567890123456789012.56788' AS DECIMAL(37, 5)),
            CAST('12345678901234567890123456789012.567891' AS DECIMAL(38, 6))),
        (4, CAST('-12345678901234567890123456789012.56788' AS DECIMAL(37, 5)),
            CAST('-12345678901234567890123456789012.567891' AS DECIMAL(38, 6)))
    ) AS values_to_compare(test_id, lhs, rhs)
)
SELECT test_id,
       lhs < rhs AS less_than,
       lhs <= rhs AS less_or_equal,
       lhs = rhs AS equal,
       lhs <> rhs AS not_equal,
       lhs >= rhs AS greater_or_equal,
       lhs > rhs AS greater_than
FROM comparisons
ORDER BY test_id;

WITH comparisons AS (
    SELECT * FROM (VALUES
        (1, CAST('99999999999999999999999999999998' AS DECIMAL(32, 0)),
            CAST('99999999999999999999999999999999' AS DECIMAL(32, 0))),
        (2, CAST('-99999999999999999999999999999999' AS DECIMAL(32, 0)),
            CAST('-99999999999999999999999999999998' AS DECIMAL(32, 0)))
    ) AS values_to_compare(test_id, lhs, rhs)
)
SELECT test_id,
       lhs < rhs AS less_than,
       lhs <= rhs AS less_or_equal,
       lhs = rhs AS equal,
       lhs <> rhs AS not_equal,
       lhs >= rhs AS greater_or_equal,
       lhs > rhs AS greater_than
FROM comparisons
ORDER BY test_id;
