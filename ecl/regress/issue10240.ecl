ds2 := DATASET(2, TRANSFORM({UNSIGNED line}, SELF.line := COUNTER));
summary2 := TABLE(ds2, { COUNT(GROUP) }, LOCAL);
COUNT(summary2) = 1;
