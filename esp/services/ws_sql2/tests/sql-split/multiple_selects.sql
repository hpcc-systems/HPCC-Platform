-- Two scans over the regression index
SELECT kind, word FROM hpcc_scan('regress::multi::searchindex') WHERE word = 'a;b' LIMIT 5;

/* block comment between statements */
SELECT COUNT(*) AS n
FROM hpcc_scan('regress::multi::searchindex');
