SELECT COUNT(*) AS row_count, SUM(i) AS value_sum
FROM range(10) t(i);
