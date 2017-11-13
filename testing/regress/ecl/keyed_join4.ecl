/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

import $.setup;
prefix := setup.Files(false, false).FilePrefix;
//noroxie

import std.system.thorlib;
import Std.File AS FileServices;

rec := RECORD
 unsigned4 key;
 UNSIGNED1 node;
 STRING50 payload;
END;
 
recvfp := RECORD
 rec;
 UNSIGNED8 __filepos{virtual(fileposition)};
END;

blankDs := DATASET([{0,0,''}],rec);
 
rec addNodeNum(rec l, UNSIGNED1 c) := transform
    SELF.node := c-1;
    SELF := l;
  END;

one_per_node := normalize(blankDs, thorlib.nodes(), addNodeNum(LEFT, COUNTER));
dodist := DISTRIBUTE(one_per_node, one_per_node.node);

baseNum := 100;
perNode := 10;
p2offset := baseNum*1000; // start offset of 2nd range (in 2nd sub key)
rec normFunc(rec l, INTEGER c, unsigned4 offset) := TRANSFORM
  SELF.key := offset + ((thorlib.node()+1)*baseNum)+c;
  SELF.payload := ((STRING)RANDOM())+SELF.key;
  SELF := l;
END;
 
donorm1 := NORMALIZE(dodist,perNode,normFunc(LEFT,COUNTER,0));
donorm2 := NORMALIZE(dodist,perNode,normFunc(LEFT,COUNTER,p2offset));

writedata := PARALLEL(
OUTPUT(donorm1+donorm2, ,prefix + 'kjsuper_sub1sub2', OVERWRITE),
OUTPUT(donorm1, ,prefix + 'kjsuper_sub1', OVERWRITE), // generation means they will be globally sorted
OUTPUT(donorm2, ,prefix + 'kjsuper_sub2', OVERWRITE)  // generation means they will be globally sorted
);

d1d2 := DATASET(prefix + 'kjsuper_sub1sub2', recvfp, FLAT);
d1 := DATASET(prefix + 'kjsuper_sub1', recvfp, FLAT);
d2 := DATASET(prefix + 'kjsuper_sub2', recvfp, FLAT);

 
writeindexes := PARALLEL(
BUILDINDEX(d1d2, { key }, { payload }, prefix + 'kjsuper_index', OVERWRITE),
BUILDINDEX(SORTED(d1, key, payload), { key }, { payload }, prefix + 'kjsuper_subindex1', OVERWRITE), // generation means they will be globally sorted
BUILDINDEX(SORTED(d2, key, payload), { key }, { payload }, prefix + 'kjsuper_subindex2', OVERWRITE)  // generation means they will be globally sorted
);

sifilename := prefix + 'superindex';


iRec := RECORD
 unsigned4 key;
END;
payloadRec := RECORD
 STRING50 payload;
END; 

dummyds := DATASET([], recvfp);
i  := INDEX(dummyds, iRec, payloadRec, prefix + 'kjsuper_index');
i1 := INDEX(dummyds, iRec, payloadRec, prefix + 'kjsuper_subindex1');
i2 := INDEX(dummyds, iRec, payloadRec, prefix + 'kjsuper_subindex2');
superi := INDEX(dummyds, iRec, payloadRec, sifilename);
opti  := INDEX(dummyds, iRec, payloadRec, prefix + 'kjsuper_fake', OPT);


conditionalDelete(string lfn) := FUNCTION
        RETURN IF(FileServices.FileExists(lfn), FileServices.DeleteLogicalFile(lfn));
END;

doclean := SEQUENTIAL(
IF (FileServices.FileExists(sifilename), fileservices.DeleteSuperFile(sifilename)),
conditionalDelete(prefix + 'kjsuper_subindex1'),
conditionalDelete(prefix + 'kjsuper_subindex2'),
conditionalDelete(prefix + 'kjsuper_sub1'),
conditionalDelete(prefix + 'kjsuper_sub2')
);


makesuper := SEQUENTIAL(
fileservices.createsuperfile(sifilename),
fileservices.addsuperfile(sifilename, prefix + 'kjsuper_subindex1'),
fileservices.addsuperfile(sifilename, prefix + 'kjsuper_subindex2')
);


lhs := TABLE(d1d2, { key });

joinRec := RECORD
 unsigned4 key;
 STRING50 payload;
END;

joinRec joinFunc(recordof(lhs) l, i r) := TRANSFORM
  SELF := l;
  SELF := r;
END;

testkj(DATASET(RECORDOF(lhs)) ds, dataset(RECORDOF(i)) rhskey) := FUNCTION
    RETURN JOIN(ds, rhskey, LEFT.key = RIGHT.key, joinFunc(LEFT, RIGHT));
END;
testkjlo(DATASET(RECORDOF(lhs)) ds, dataset(RECORDOF(i)) rhskey) := FUNCTION
    RETURN JOIN(ds, rhskey, LEFT.key = RIGHT.key, joinFunc(LEFT, RIGHT), LEFT ONLY);
END;
testmatch(DATASET(RECORDOF(lhs)) ds) := FUNCTION
    RETURN JOIN(testkj(ds, i), testkj(ds, superi), LEFT.payload = RIGHT.payload, LEFT ONLY);
END;


preparedata := SEQUENTIAL(
doclean,
PARALLEL(
writedata,
writeindexes),
makesuper
);

lhsf1 := DISTRIBUTE(lhs((key > 100 AND key < 199) OR (key > p2offset+100 AND key < p2offset+199)), 0);
lhsf2 := DISTRIBUTE(lhs((key > 200 AND key < 399) OR (key > p2offset+100 AND key < p2offset+299)), 0);
lhsf3 := DISTRIBUTE(lhs((key > 100 AND key < thorlib.nodes()*baseNum+99) OR (key > p2offset+100 AND key < p2offset+(thorlib.nodes()*baseNum+99))), 0);


SEQUENTIAL(
preparedata,
PARALLEL(
    OUTPUT(testmatch(lhsf1), NAMED('testkj1')),
    OUTPUT(testmatch(lhsf2), NAMED('testkj2')),
    OUTPUT(IF(COUNT(testkj(lhsf3, superi)) = COUNT(lhs), 'Super count check: success', 'Super count check: failure'), NAMED('countcheck')),
    OUTPUT(IF(COUNT(testkjlo(lhsf3, opti)) = COUNT(lhs), 'Opt left only check: success', 'Opt left only check: failure'), NAMED('optcheck'))
 ),
doclean
);

