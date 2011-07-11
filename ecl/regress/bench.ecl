/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

rollup_ds := ROLLUP(ROLLUP(sds,checksort(LEFT, RIGHT),TRUE,LOCAL),checksort(LEFT, RIGHT),TRUE);


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
  OUTPUT(COUNT(ROLLUP(hds,checkdist(LEFT, RIGHT),TRUE,LOCAL))),
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
