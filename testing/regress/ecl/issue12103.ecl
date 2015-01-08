
//Record just containing the ids.
r1 := RECORD
    unsigned id;
END;

//Record containing ids with positions automatically added to it
r2 := RECORD(r1)
    unsigned8 filepos{virtual(fileposition)};
END;

//Record with real fileposition fields 
r3 := RECORD(r1)
    unsigned8 filepos;
END;

//Generate a datafile containing ids.
ds := DATASET(10, TRANSFORM(r1, SELF.id := COUNTER));
output(ds,,'REGRESS::temp::oraw',OVERWRITE);

//Read it using a record which will associate file positions with each record.
dsraw := DATASET('REGRESS::temp::oraw',r2, THOR);

//Write as-is (will now have physical file position fields)
output(dsraw, ,'REGRESS::temp::o1',OVERWRITE);

//Project out the file posistion fields before writing
output(dsraw, { id } ,'REGRESS::temp::o2',OVERWRITE);

//Project it to a record that has no virtuals
output(PROJECT(dsraw, TRANSFORM(r3, SELF := LEFT)), ,'REGRESS::temp::o3',OVERWRITE);

//Now check each form can be read correctly...
d1 := DATASET('REGRESS::temp::o1',r3, THOR);
output(count(d1));

d2 := DATASET('REGRESS::temp::o2',r1, THOR);
output(count(d2));

d3 := DATASET('REGRESS::temp::o3',r3, THOR);
output(count(d3));

