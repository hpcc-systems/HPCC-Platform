
// Omit optimisation that could render some of the cases below trivial
#option ('percolateConstants', false);

// ====================== Record type
myRec := RECORD
    unsigned id;
    string name;
END;

// ====================== Independent transformation
myRec myTransf(unsigned c) := TRANSFORM
    SELF.id := c;
    SELF.name := 'Charlie ' + c + ' Bravo';
END;

// ====================== Dependent transformation
myRec myTransf2(myRec L, unsigned c) := TRANSFORM
    SELF.id := L.id + c;
    SELF.name := 'Charlie ' + c + ' Bravo';
    SELF := L;
END;

hundred := 100 : stored('hundred');

// ====================== This is what the optimization should look like
output('Dataset count transform');
dsO := dataset(10, myTransf(COUNTER));
output(dsO);

// ====================== Single row from transform
output('Single row from transform');
dsA := dataset(ROW(myTransf(100)));
dsAT := normalize(dsA, 11, myTransf(COUNTER));
output(dsAT);
dsAN := normalize(dsA, 11, myTransf2(LEFT, COUNTER));
output(dsAN);

// ====================== Single row from dynamic transform
output('Single row from dynamic transform');
dsAx := dataset(ROW(myTransf(hundred)));
dsAxT := normalize(dsAx, 12, myTransf(COUNTER));
output(dsAxT);
// Do not optimise, as LEFT is unknown at compilation time
dsAxN := normalize(dsAx, 12, myTransf2(LEFT, COUNTER));
output(dsAxN);

// ====================== Transform
output('Transform');
dsB := dataset([myTransf(100)]);
dsBT := normalize(dsB, 13, myTransf(COUNTER));
output(dsBT);
dsBN := normalize(dsB, 13, myTransf2(LEFT, COUNTER));
output(dsBN);

// ====================== Transform dynamic
output('Transform dynamic');
dsBx := dataset([myTransf(hundred)]);
dsBxT := normalize(dsBx, 14, myTransf(COUNTER));
output(dsBxT);
// Do not optimise, as LEFT is unknown at compilation time
dsBxN := normalize(dsBx, 14, myTransf2(LEFT, COUNTER));
output(dsBxN);

// ====================== Single value dataset
output('Single value dataset');
dsC := dataset(ROW({100,''},myRec));
dsCT := normalize(dsC, 15, myTransf(COUNTER));
output(dsCT);
dsCN := normalize(dsC, 15, myTransf2(LEFT, COUNTER));
output(dsCN);

// ====================== DO NOT OPTIMISE

// ====================== Multiple rows
output('Multiple rows');
dsM := dataset([{100,'first'},{101,'second'}], myRec);
dsMT := normalize(dsM, 17, myTransf(COUNTER));
output(dsMT);
dsMN := normalize(dsM, 17, myTransf2(LEFT, COUNTER));
output(dsMN);

// ====================== Single independent row
output('Single independent row');
dsI := ROW({1000,''},myRec) : independent;
// This should be optimised, since myTransf doesn't reference row
dsIT := normalize(dataset(dsI), 18, myTransf(COUNTER));
output(dsIT);
dsIN := normalize(dataset(dsI), 18, myTransf2(LEFT, COUNTER));
output(dsIN);

// ====================== Random row
// Random numbers don't do well in comparison tests... ;)

// ====================== Normalize with LEFT.childDS
output('LEFT referenced children');
dsL := dataset(ROW({10,'foo'},myRec));
dsLN := normalize(dsL, LEFT.id, myTransf(COUNTER));
output(dsLN);
