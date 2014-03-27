decimalRec := RECORD
DECIMAL3 A;
DECIMAL3_1 B;
END;
 //"dummy" just to open an XML scope
#DECLARE(out);
#EXPORT(out, decimalRec);
OUTPUT(%'out'%);
