REAL    real_const := 1.123456;
REAL    rVal := real_const : STORED('real_stored');

OUTPUT(POWER(rVal,0), named('pow_0'));
OUTPUT(POWER(rVal,1), named('pow_1'));
OUTPUT(POWER(rVal,2), named('pow_2'));
OUTPUT(POWER(rVal,3), named('pow_3'));
OUTPUT(POWER(rVal,4), named('pow_4'));
OUTPUT(POWER(rVal,-1), named('pow_n1'));
OUTPUT(POWER(rVal,-2), named('pow_n2'));
OUTPUT(POWER(rVal,-3), named('pow_n3'));
OUTPUT(POWER(rVal,-4), named('pow_n4'));