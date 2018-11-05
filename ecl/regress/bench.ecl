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

IMPORT lib_fileservices;

UNSIGNED numrecs := 30000000 : stored('numrecs'); // per node


UNSIGNED4 TimeMS2() := BEGINC++

extern unsigned msTick();  // linux only
    #body
        return msTick();
    ENDC++;

rtl := SERVICE
 unsigned4 msTick() :       eclrtl,library='eclrtl',entrypoint='rtlTick';
END;

unsigned TimeMS() := rtl.msTick();

rec1 := RECORD
     string40 payload1;
         string20 key;
     string40 payload2;
        END;


seed := dataset([{'A','0','Z'}], rec1);

rec1 addNodeNum(rec1 L, unsigned4 c) := transform
    SELF.key := (string) c;
    SELF := L;
  END;

one_per_node := DISTRIBUTE(NORMALIZE(seed, CLUSTERSIZE, addNodeNum(LEFT, COUNTER)),(unsigned)key);

rec1 generatePseudoRandom(rec1 L, unsigned4 c) := transform
    SELF.payload1 := (string40)RANDOM();
    SELF.key := '['+(string17) RANDOM()+']';
    SELF.payload2 := (string39)HASH(SELF.payload1)+'\n';
  END;

ds := NORMALIZE(one_per_node, numrecs, generatePseudoRandom(LEFT, counter));  // 2 Gb per node.

test1 := SEQUENTIAL(
  OUTPUT(COUNT(ds(key='[12345            ] '))),
  OUTPUT('test1 - gen/filter done')
);

test2 := SEQUENTIAL(
  OUTPUT(ds,,'bench::wr1'),
  OUTPUT('test2 - gen/write done')
);

dsin := DATASET('bench::wr1',rec1,FLAT);

test3 := SEQUENTIAL(
  OUTPUT(COUNT(dsin(key='[54321            ] '))),
  OUTPUT('test3 - read/filter done')
);

test4 := SEQUENTIAL(
  OUTPUT(dsin,,'bench::wr2'),
  OUTPUT('test4 - read/write done')
);

sds := SORT(dsin,key);

rec1 checksort(rec1 l, rec1 r) := TRANSFORM
  self.key := IF (l.key <= r.key, r.key,
                  ERROR('ERROR: records not in sequence! "' + l.key + '", "'+ r.key + '"'));
  self := r;
END;

rollup_ds := NOFOLD(ROLLUP(NOFOLD(ROLLUP(sds,checksort(LEFT, RIGHT),TRUE,LOCAL)),checksort(LEFT, RIGHT),TRUE));


test5 := SEQUENTIAL(
  IF (COUNT(rollup_ds) = 1,
     output('Sort order verified'),
     FAIL('ERROR: rollup count did not match expected!')
  ),
  OUTPUT('test5 - read/sort done')
);


hds := DISTRIBUTE(dsin,hash(key));
rec1 checkdist(rec1 l, rec1 r) := TRANSFORM
  self.key := if (HASH(l.key)%CLUSTERSIZE = HASH(r.key)%CLUSTERSIZE, r.key,
                  ERROR('ERROR: records have different hash values! "' + l.key + '", "'+ r.key + '"'));
  self := r;
END;

test6 := SEQUENTIAL(
  OUTPUT(COUNT(NOFOLD(ROLLUP(hds,checkdist(LEFT, RIGHT),TRUE,LOCAL)))),
  OUTPUT('test6 read/dist done')
);


SEQUENTIAL(
  FileServices.DeleteLogicalFile('bench::wr1',true),
  FileServices.DeleteLogicalFile('bench::wr2',true),
  OUTPUT(TimeMS(),NAMED('time1')),
test1,
  OUTPUT(TimeMS()-WORKUNIT('time1',integer),NAMED('T_FILTER')),
  OUTPUT(TimeMS(),NAMED('time2')),
test2,
  OUTPUT(TimeMS()-WORKUNIT('time2',integer),NAMED('T_WRITE')),
  OUTPUT(TimeMS(),NAMED('time3')),
test3,
  OUTPUT(TimeMS()-WORKUNIT('time3',integer)-WORKUNIT('T_FILTER',integer),NAMED('T_READ')),
  OUTPUT(TimeMS(),NAMED('time4')),
test4,
  OUTPUT(TimeMS()-WORKUNIT('time4',integer),NAMED('T_READWRITE')),
  FileServices.DeleteLogicalFile('bench::wr2',true),
  OUTPUT(TimeMS(),NAMED('time5')),
test5,
  OUTPUT(TimeMS()-WORKUNIT('time5',integer)-WORKUNIT('T_READ',integer),NAMED('T_SORT')),
  OUTPUT(TimeMS(),NAMED('time6')),
test6,
  FileServices.DeleteLogicalFile('bench::wr1',true),
  OUTPUT(TimeMS()-WORKUNIT('time5',integer)-WORKUNIT('T_READ',integer),NAMED('T_DISTRIBUTE')),
  OUTPUT('Done')
);
