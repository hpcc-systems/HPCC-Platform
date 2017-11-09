#option ('multiplePersistInstances', true);
import $.setup;
prefix := setup.Files(false, false).FilePrefix;

import Std.File;

ds := dataset(['','!Hello',' me ', '', 'xx', '!end'], { string line });

p1 := ds(line <> '') : persist(prefix + 'persist_gh1', many, single);

p2 := ds(line = '') : persist(prefix + 'persist_gh2', multiple);

p3 := ds(line[1]='!') : persist(prefix + 'persist_gh3',multiple(10));

p4 := ds(line[1] = ' ') : persist(prefix + 'persist_gh4');

lfl1 := File.LogicalFileList(prefix + 'persist_gh*');
delall := NOTHOR(APPLY(lfl1,File.DeleteLogicalFile(name, true)));

sequential(
    delall,
    output(p1 & p2 & p3 & p4);
);
