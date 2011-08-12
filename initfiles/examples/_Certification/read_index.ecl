/* ******************************************************************************
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
 ******************************************************************************/

IMPORT Std;
IMPORT $ AS Certification;

STRING10 lnamein_value := 'BRYANT';

raw := Certification.DataFile;
i := Certification.IndexFile;

TYPEOF(raw) xt(raw l, i r) := TRANSFORM
  SELF.__filepos := r.__filepos;
  SELF := l;
END;

o := FETCH(raw, i(lname = Std.Str.ToUpperCase(lnamein_value)), RIGHT.__filepos, xt(LEFT, RIGHT));
OUTPUT(CHOOSEN(o,100));
