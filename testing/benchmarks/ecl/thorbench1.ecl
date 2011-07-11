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

// Simple Performance Metrics for Thor Cluster



IMPORT lib_fileservices;

UNSIGNED numrecs := 30000000 : stored('numrecs'); // per node


rtl := SERVICE
  unsigned4 msTick() :      eclrtl,library='eclrtl',entrypoint='rtlTick';
END;


unsigned TimeMS() := rtl.msTick();



rec1 := RECORD
     string36   payload1;
     unsigned4  node;
         string20   key;
     string40   payload2;
        END;
       
       
seed := dataset([{'A',0,'0','Z'}], rec1);

rec1 addNodeNum(rec1 L, unsigned4 c) := transform
    SELF.node := c;
    SELF := L;
  END;

one_per_node := DISTRIBUTE(NORMALIZE(seed, CLUSTERSIZE, addNodeNum(LEFT, COUNTER)),node);

rec1 generatePseudoRandom(rec1 L, unsigned4 c) := TRANSFORM
    SELF.payload1 := (string40)RANDOM();
    SELF.key := '['+(string17) RANDOM()+']';
    SELF.payload2 := (string39)HASH(SELF.payload1)+'\n';
    SELF := L;
  END;

ds := NORMALIZE(one_per_node, numrecs, generatePseudoRandom(LEFT, counter)); 

test1 := SEQUENTIAL(
  OUTPUT(COUNT(ds(key='[12345            ] ')),NAMED('count1')),
  OUTPUT('test1 - gen/filter done',NAMED('progress1'))
);

test2 := SEQUENTIAL(
  OUTPUT(ds,,'bench::wr1'),
  OUTPUT('test2 - gen/write done',NAMED('progress2'))
);

dsin := DATASET('bench::wr1',rec1,FLAT);

test3 := SEQUENTIAL(
  OUTPUT(COUNT(dsin(key='[54321            ] '))),
  OUTPUT('test3 - read/filter done',NAMED('progress3'))
);

test4 := SEQUENTIAL(
  OUTPUT(dsin,,'bench::wr2'),
  OUTPUT('test4 - read/write done',NAMED('progress4'))
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
     OUTPUT('Sort order verified',NAMED('verify5')), 
     FAIL('ERROR: rollup count did not match expected!')
  ),
  OUTPUT('test5 - read/sort done',NAMED('progress5'))
);


hds := DISTRIBUTE(dsin,hash(key));
rec1 checkdist(rec1 l, rec1 r) := TRANSFORM
  self.key := if (HASH(l.key)%CLUSTERSIZE = HASH(r.key)%CLUSTERSIZE, r.key, 
                  ERROR('ERROR: records have different hash values! "' + l.key + '", "'+ r.key + '"'));
  self := r;
END;

test6 := SEQUENTIAL(
  OUTPUT(COUNT(ROLLUP(hds,checkdist(LEFT, RIGHT),TRUE,LOCAL)),NAMED('rollup6')),
  OUTPUT('test6 read/dist done',NAMED('progress6'))
);

hds2 := DISTRIBUTE(dsin,node+200);
rec1 checkdist2(rec1 l, rec1 r) := TRANSFORM
  self.key := if (l.node = r.node, r.key, 
                  ERROR('ERROR: records have different node values! "' + l.node + '", "'+ r.node + '"'));
  self := r;
END;

test7 := SEQUENTIAL(
  OUTPUT(COUNT(ROLLUP(hds2,checkdist2(LEFT, RIGHT),TRUE,LOCAL)),NAMED('rollup7')),
  OUTPUT('test7 read/dist2 done',NAMED('progress7'))
);


SEQUENTIAL(      
  FileServices.DeleteLogicalFile('bench::wr1',true),
  FileServices.DeleteLogicalFile('bench::wr2',true),
  OUTPUT(StringLib.GetDateYYYYMMDD(),NAMED('DATE')),
  OUTPUT(ThorLib.WUID(),NAMED('WORKUNIT')),
  OUTPUT(ThorLib.JobOwner(),NAMED('USER')),
  OUTPUT(ThorLib.Cluster(),NAMED('CLUSTER')),
  OUTPUT(CLUSTERSIZE,NAMED('CLUSTERSIZE')),
  OUTPUT(StringLib.GetBuildInfo(),NAMED('BUILD')),
  OUTPUT(TimeMS(),NAMED('time1')),
test1,
  OUTPUT((integer)((TimeMS()-WORKUNIT('time1',integer))/1000),NAMED('T_FILTER')),
  OUTPUT(TimeMS(),NAMED('time2')),
test2,
  OUTPUT((integer)((TimeMS()-WORKUNIT('time2',integer))/1000),NAMED('T_WRITE')),
  OUTPUT(TimeMS(),NAMED('time3')),
test3,
  OUTPUT((integer)((TimeMS()-WORKUNIT('time3',integer))/1000),NAMED('T_READ')),
  OUTPUT(TimeMS(),NAMED('time4')),
test4,
  OUTPUT((integer)((TimeMS()-WORKUNIT('time4',integer))/1000),NAMED('T_READWRITE')),
  FileServices.DeleteLogicalFile('bench::wr2',true),
  OUTPUT(TimeMS(),NAMED('time5')),
test5,
  OUTPUT((integer)((TimeMS()-WORKUNIT('time5',integer)-WORKUNIT('T_READ',integer))/1000),NAMED('T_SORT')),
  OUTPUT(TimeMS(),NAMED('time6')),
test6,
  OUTPUT((integer)((TimeMS()-WORKUNIT('time6',integer)-WORKUNIT('T_READ',integer))/1000),NAMED('T_DISTRIBUTE')),
  OUTPUT(TimeMS(),NAMED('time7')),
test7,
  OUTPUT((integer)((TimeMS()-WORKUNIT('time7',integer)-WORKUNIT('T_READ',integer))/1000),NAMED('T_DISTRIBUTESEQ')),
  FileServices.DeleteLogicalFile('bench::wr1',true),
  OUTPUT('Done')
);
