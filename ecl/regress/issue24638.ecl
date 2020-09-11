Layout_Test := RECORD
  DECIMAL32_10 DecimalField;
  REAL8 RealField;
END;

FieldValFunc(Layout_Test le,UNSIGNED2 FN) := DEFINE CASE(FN,1 => le.DecimalField,2 => le.RealField,0);

FieldValFunc(DATASET([{1.234, 2.5}], Layout_Test)[1], 1);


createDecimal(real x) := DEFINE (decimal32_31)x;

pi := 3.14159265;
output(NOFOLD(pi));

one := 1 : stored('one');
oneD := createDecimal(one);
output(oneD + oneD);

//Check that it preserves 27 significant digits...
output(createDecimal(pi) + createDecimal(pi/1.0e9)  + createDecimal(pi/1.0e18));
