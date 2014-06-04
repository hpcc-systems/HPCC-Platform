#option ('multiplePersistInstances', true);

import Std.File;

ds := dataset(['','!Hello',' me ', '', 'xx', '!end'], { string line });

p1 := ds(line <> '') : persist('~REGRESS::persist_gh1', many, single);

p2 := ds(line = '') : persist('~REGRESS::persist_gh2', multiple);

p3 := ds(line[1]='!') : persist('~REGRESS::persist_gh3',multiple(10));

p4 := ds(line[1] = ' ') : persist('~REGRESS::persist_gh4');

lfl1 := File.LogicalFileList('REGRESS::persist_gh*');
delall := NOTHOR(APPLY(lfl1,File.DeleteLogicalFile('~'+name, true)));

sequential(
    delall,
    output(p1 & p2 & p3 & p4);
);
