
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

outRecord := RECORD
    unsigned id;
    DATASET(childRecord) child1;
    DATASET(childRecord) child2;
END;

//Example 1:
//A sort definition is used within different conditions
//A single child graph is created and the sort is conditionally executed.
//This generates and executes efficiently.
outRecord t1(idRecord l) := TRANSFORM
    s := SORT(l.children, HASH(cid));
    SELF.child1 := IF(l.id % 3 = 1, s, l.children);
    SELF.child2 := IF(l.id % 4 = 2, s, l.children);
    SELF := l;
END;

output(PROJECT(inputDataset, t1(LEFT)));
