scaledComparisons := DATASET([
    {1, (DECIMAL37_5)'1.25000', (DECIMAL38_6)'1.250000'},
    {2, (DECIMAL37_5)'-0.00001', (DECIMAL38_6)'0.000000'},
    {3, (DECIMAL37_5)'12345678901234567890123456789012.56788',
        (DECIMAL38_6)'12345678901234567890123456789012.567891'},
    {4, (DECIMAL37_5)'-12345678901234567890123456789012.56788',
        (DECIMAL38_6)'-12345678901234567890123456789012.567891'}
], {UNSIGNED1 test_id, DECIMAL37_5 lhs, DECIMAL38_6 rhs});

OUTPUT(PROJECT(scaledComparisons, TRANSFORM({
    UNSIGNED1 test_id,
    BOOLEAN less_than,
    BOOLEAN less_or_equal,
    BOOLEAN equal,
    BOOLEAN not_equal,
    BOOLEAN greater_or_equal,
    BOOLEAN greater_than
},
    SELF.test_id := LEFT.test_id;
    SELF.less_than := LEFT.lhs < LEFT.rhs;
    SELF.less_or_equal := LEFT.lhs <= LEFT.rhs;
    SELF.equal := LEFT.lhs = LEFT.rhs;
    SELF.not_equal := LEFT.lhs <> LEFT.rhs;
    SELF.greater_or_equal := LEFT.lhs >= LEFT.rhs;
    SELF.greater_than := LEFT.lhs > LEFT.rhs
)), NAMED('ScaledResult'));

wideComparisons := DATASET([
    {1, (DECIMAL32_0)'99999999999999999999999999999998',
        (DECIMAL32_0)'99999999999999999999999999999999'},
    {2, (DECIMAL32_0)'-99999999999999999999999999999999',
        (DECIMAL32_0)'-99999999999999999999999999999998'}
], {UNSIGNED1 test_id, DECIMAL32_0 lhs, DECIMAL32_0 rhs});

OUTPUT(PROJECT(wideComparisons, TRANSFORM({
    UNSIGNED1 test_id,
    BOOLEAN less_than,
    BOOLEAN less_or_equal,
    BOOLEAN equal,
    BOOLEAN not_equal,
    BOOLEAN greater_or_equal,
    BOOLEAN greater_than
},
    SELF.test_id := LEFT.test_id;
    SELF.less_than := LEFT.lhs < LEFT.rhs;
    SELF.less_or_equal := LEFT.lhs <= LEFT.rhs;
    SELF.equal := LEFT.lhs = LEFT.rhs;
    SELF.not_equal := LEFT.lhs <> LEFT.rhs;
    SELF.greater_or_equal := LEFT.lhs >= LEFT.rhs;
    SELF.greater_than := LEFT.lhs > LEFT.rhs
)), NAMED('WideResult'));
