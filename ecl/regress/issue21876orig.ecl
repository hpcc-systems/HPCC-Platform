I := RANDOM() : INDEPENDENT; //calculated once, period
G := RANDOM() : GLOBAL; //calculated once in each graph
// H := 0;
H := RANDOM() : ONCE;

ds := DATASET([

{1,0,0,0,0}
,

{2,0,0,0,0}
],

{UNSIGNED1 rec,UNSIGNED Ival, UNSIGNED Gval , UNSIGNED Hval, UNSIGNED Aval }
);

RECORDOF(ds) XF(ds L) := TRANSFORM
SELF.Ival := I;
SELF.Gval := G;
SELF.Hval := H;
SELF.Aval := RANDOM(); //calculated each time used
SELF := L;
END;

P1 := PROJECT(ds,XF(left)) : PERSIST('~RTTEST::PERSIST::IndependentVsGlobal1');
P2 := PROJECT(ds,XF(left)) : PERSIST('~RTTEST::PERSIST::IndependentVsGlobal2');

OUTPUT(P1); 
OUTPUT(P2); //this gets the same Ival values as P1, but the Gval value is different than P1

OUTPUT(P1); 
OUTPUT(P2); //this gets the same Ival values as P1, but the Gval value is different than P1
