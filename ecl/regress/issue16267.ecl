r0 := { unsigned id0; };

r1 := { dataset(r0) id0s; };

r2 := { dataset(r1) ids };

r3 := {
    string11 f_name;
    r2 child;
};

ds := dataset([], r3);

i := index(ds, { f_name } , { ds}, 'magic');

i;
