/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

//version useEmbedded=true,noSeek=true
//version useEmbedded=true,noSeek=false
//version useEmbedded=false,noSeek=true
//version useEmbedded=false,noSeek=false

import setup;

useEmbedded := #IFDEFINED(root.useEmbedded, false);
noSeek := #IFDEFINED(root.noSeek, false);

#if (useEmbedded)
attr := 'EMBEDDED';
#else
attr := '';
#end

#option ('noSeekBuildIndex', noSeek);

prefix := setup.Files(false, false).IndexPrefix + WORKUNIT + '::';

grandchildRec := RECORD
 string f2;
 string f3;
END;

childRec := RECORD
 string f1;
 #expand(attr) DATASET(grandchildrec) gkids;
END;

parentRec := RECORD
 unsigned uid;
 #expand(attr) DATASET(childRec) kids1;
END;

numParents := 10000;
numKids := 10;
numGKids := 5;

d3(unsigned c) := DATASET(numGKids, TRANSFORM(grandchildrec, SELF.f2 := (string)COUNTER+c;
                                                             SELF.f3 := (string)HASH(COUNTER)
                                              )
                         );
d2(unsigned c) := DATASET(numKids, TRANSFORM(childRec, SELF.f1 := (string)c+COUNTER,
                                                       SELF.gkids := d3(COUNTER+c)));
ds := DATASET(numParents, TRANSFORM(parentRec, SELF.uid := COUNTER;
                                               SELF.kids1 := d2(COUNTER);
                                   ), DISTRIBUTED);



idxRecord := RECORD
 unsigned uid;
 DATASET(childRec) payload{BLOB};
END;

p := PROJECT(ds, TRANSFORM(idxRecord, SELF.payload := LEFT.kids1; SELF := LEFT));

i := INDEX(p, {uid}, {p}, prefix+'anindex'+IF(useEmbedded,'E','L'));

lhsRec := RECORD
 unsigned uid;
END;
lhs := DATASET(numParents, TRANSFORM(lhsRec, SELF.uid := (HASH(COUNTER) % numParents) + 1));
j := JOIN(lhs, i, LEFT.uid=RIGHT.uid, KEEP(1));

SEQUENTIAL(
 BUILD(i, OVERWRITE);
 OUTPUT(count(nofold(j)) - numParents);
);
