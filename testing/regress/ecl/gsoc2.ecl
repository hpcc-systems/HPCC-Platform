
childRecord := { unsigned cid; };
idRecord := RECORD
    unsigned id;
    DATASET(childRecord) children;
END;

makeChildren(unsigned num, unsigned first, REAL scale) :=
    DATASET(num, TRANSFORM(childRecord, SELF.cid := (unsigned)(first + (COUNTER-1)*scale)));
    
idRecord makeId(unsigned id, unsigned num, unsigned first, REAL scale) := TRANSFORM
    SELF.id := id;
    SELF.children := makeChildren(num, first, scale);
END;

inputDataset := DATASET([
    makeId(1, 5, 1, 1),
    makeId(2, 5, 2, 0.5),
    makeId(3, 8, 10, -1.0),
    makeId(4, 12, 3, 0.3)
    ]);

nestedRecord := RECORD
    DATASET(childRecord) children;
END;

nestedRecord makeNested(DATASET(childRecord) children) := TRANSFORM
    SELF.children := children;
END;

outRecord := RECORD
    unsigned id;
    nestedRecord child1;
    nestedRecord child2;
END;

//Example 2:
//A sort definition is used within different conditions
//This time assigned to a nested child row.
//The sort is now always executed - even if not used.
outRecord t1(idRecord l) := TRANSFORM
    s := SORT(l.children, HASH(cid));
    SELF.child1:= IF(l.id % 3 = 1, ROW(makeNested(s)));
    SELF.child2 := IF(l.id % 4 = 2, ROW(makeNested(s)));
    SELF := l;
END;

output(PROJECT(inputDataset, t1(LEFT)));
