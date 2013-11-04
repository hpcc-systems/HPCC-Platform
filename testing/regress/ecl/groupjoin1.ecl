
namesRec := RECORD
    string name;
    string addr;
END;

nameDataset := DATASET([
    { 'Smith', 'Apple street' },
    { 'Smith', 'Banana road' },
    { 'Smith', 'Date street' },
    { 'Jones', 'Banana road' },
    { 'Jones', 'Cherry street' },
    { 'Jones', 'Date street' },
    { 'Bloggs', 'Cherry street' },
    { 'Bloggs', 'Eggplant ave' }
    ], namesRec);
    
outRec := RECORD
    string lname;
    string rname;
END;    

outRec makeOut(namesRec l, namesRec r) := TRANSFORM
    SELF.lname := l.name;
    SELF.rname := r.name;
END;

j := JOIN(nameDataset, nameDataset, LEFT.addr = RIGHT.addr AND LEFT.name != RIGHT.name, makeOut(LEFT, RIGHT), GROUP(LEFT.name));

s := SORT(j, rname);    // sort the groups.
gr2 := GROUP(s, lname, rname, local);
r := TABLE(gr2, { lname, rname, unsigned cnt := COUNT(GROUP) });

sr := sort(NOFOLD(r), lname, rname);
output(sr);
output(sr(cnt > 1));
