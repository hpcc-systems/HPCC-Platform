// H := 0;
H := 0 : ONCE;

ds := DATASET([

{1,0,0,0,0}
,

{2,0,0,0,0}
],

{UNSIGNED1 rec,UNSIGNED Ival, UNSIGNED Gval , UNSIGNED Hval, UNSIGNED Aval }
);

RECORDOF(ds) XF(ds L) := TRANSFORM
SELF.Hval := H;
SELF := L;
END;

P1 := PROJECT(ds,XF(left)) : PERSIST('~RTTEST::PERSIST::IndependentVsGlobal1');

OUTPUT(P1); 

