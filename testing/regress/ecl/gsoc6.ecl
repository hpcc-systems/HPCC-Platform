
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

nullChildren := DATASET([], childRecord);

//Example 6:
//A filter definition is used within different conditions - one as is, one then sorted
//The code is generated inline in the transform for one, and in a child query for the
//second, but the filter code is not shared between them
outRecord t1(idRecord l) := TRANSFORM
    f := l.children(cid % 2 = 1);  // This could be an expensive filter operation...
    s := SORT(f, HASH(cid));
    SELF.child1:= ROW(makeNested(f));
    SELF.child2:= ROW(makeNested(s));
    SELF := l;
END;

output(PROJECT(inputDataset, t1(LEFT)));
