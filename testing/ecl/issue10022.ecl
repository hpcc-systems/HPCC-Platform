//noroxie
//this really should be supported by roxie, but it doesn't support it at the moment

#option ('multiplePersistInstances', true);

ds := dataset(['','!Hello',' me ', '', 'xx', '!end'], { string line });

p1 := ds(line <> '') : persist('~p1', many, single);

p2 := ds(line = '') : persist('~p2', multiple);

p3 := ds(line[1]='!') : persist('~p3',multiple(10));

p4 := ds(line[1] = ' ') : persist('~p4');

output(p1 & p2 & p3 & p4);
