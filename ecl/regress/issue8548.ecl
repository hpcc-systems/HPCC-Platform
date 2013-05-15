#OPTION('performWorkflowCse',true);

r := {integer num};

ds_0 := DATASET('x',r,thor);

ds := NORMALIZE(ds_0,10000,TRANSFORM(r,SELF.num:=COUNTER));

a0:=SORT(ds,-num) & ds;

a:= TABLE(a0, {num} ,num):PERSIST('temp::a');

b:= a0:PERSIST('temp::b');

a;

b;
