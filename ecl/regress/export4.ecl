decimalRec := RECORD
DECIMAL3 A;
DECIMAL3_1 B;
END;
LOADXML('<xml/>'); //"dummy" just to open an XML scope
#DECLARE(out);
#EXPORT(out, decimalRec);
OUTPUT(%'out'%);
