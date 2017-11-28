//
//  Example code - use without restriction.  
//
rec := RECORD
  STRING1 Letter;
END;
Inds1 := DATASET([{'A'},{'B'},{'C'},{'D'},{'E'},
                  {'F'},{'G'},{'H'},{'I'},{'J'},
                  {'K'},{'L'},{'M'},{'N'},{'O'},
                  {'P'},{'Q'},{'R'},{'S'},{'T'},
                  {'U'},{'V'},{'W'},{'X'},{'Y'}],
		     rec);
Inds2 := DATASET([{'A'},{'B'},{'C'},{'D'},{'E'},
                  {'F'},{'G'},{'H'},{'I'},{'J'},
                  {'K'},{'L'},{'M'},{'N'},{'O'},
                  {'P'},{'Q'},{'R'},{'S'},{'T'},
                  {'U'},{'V'},{'W'},{'X'},{'Y'}],
		     rec);
					
CntInDS2 := COUNT(Inds2);

rec Xform1(rec L, INTEGER C) := TRANSFORM
  SELF.Letter := Inds2[C].Letter;
END;

CP1 := NORMALIZE(Inds1,CntInDS2,Xform1(LEFT,COUNTER));
CP2 := NORMALIZE(cp1,CntInDS2,Xform1(LEFT,COUNTER));
CP3 := NORMALIZE(cp2,CntInDS2,Xform1(LEFT,COUNTER));

Out1 := OUTPUT(DISTRIBUTE(CP3,RANDOM()),,'~RTTEST::IN::CP1',OVERWRITE);
Out2 := OUTPUT(DISTRIBUTE(CP2,RANDOM()),,'~RTTEST::IN::CP2',OVERWRITE);
					
ds1 := DATASET('~RTTEST::IN::CP1',rec,thor);
ds2 := DATASET('~RTTEST::IN::CP2',rec,thor);

CntDS2 := COUNT(ds2);
SetDS2 := SET(ds2,letter);

outrec := RECORD
  STRING1 LeftLetter;
  STRING1 RightLetter;
END;

outrec Xform2(rec L, INTEGER C) := TRANSFORM
  SELF.LeftLetter := L.Letter;
  SELF.RightLetter := Setds2[C];
END;

CP := NORMALIZE(ds1,CntDS2,Xform2(LEFT,COUNTER));

Out3 := OUTPUT(CP,,'~RTTEST::OUT::CP3',overwrite);
SEQUENTIAL(Out1,Out2,Out3)
