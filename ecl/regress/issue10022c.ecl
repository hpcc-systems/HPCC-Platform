#option ('multiplePersistInstances', true);
#option ('defaultNumPersistInstances', 4);

ds := dataset('x', { string line }, FLAT);

p1 := ds(line <> '') : persist('p1', many, single);

p2 := ds(line = '') : persist('p2', multiple);

p3 := ds(line[1]='!') : persist('p3','10Mway',multiple(10));

p4 := ds(line[1] = ' ') : persist('p4');

output(p1 + p2 + p3 + p4);
