
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
    { 'Bloggs', 'Cherry steet' },
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

//Error: RIGHT.name isn't legal becuase you can only group on fields from LEFT
//Error: LEFT.addr isn't legal because it isn't projected into the output
j := JOIN(nameDataset, nameDataset, LEFT.addr = RIGHT.addr AND LEFT.name != RIGHT.name, makeOut(LEFT, RIGHT), GROUP(LEFT.name, RIGHT.name, LEFT.addr));

r := TABLE(j, { lname, rname, unsigned cnt := COUNT(GROUP) });
output(ungroup(r));
output(r(cnt > 1));
