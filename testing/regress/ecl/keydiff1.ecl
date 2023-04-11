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
prefix := setup.Files(false, false).QueryFilePrefix;

// This warning is suppressed because it depends on the size of the thor cluster
// and consistant output is required by regression suite
// (warning relates to skew in the child record causing uneven disk read time)
#onwarning (30003,ignore);
//noRoxie
//noHthor
import Std.File AS FileServices;

unsigned numrecsbase := 1000000 : stored('numrecsbase');
unsigned numrecsadd  := numrecsbase/4 : stored('numrecsadd');
unsigned numrecsdel  := numrecsbase/4 : stored('numrecsdell');

rec := record
     unsigned4 len;
     string86 payload;
         string10 key;
       end;
irec := record
           rec;
           UNSIGNED8 filepos {virtual(fileposition)};
        end;

crec := record
           rec;
           UNSIGNED8 filepos;
           UNSIGNED seq;
        end;

seed := dataset([{0, 'A', '0'}], rec);

rec addNodeNum(rec L, unsigned4 c) := transform
    SELF.key := (string) c;
    SELF := L;
  END;

one_per_node := distribute(normalize(seed, CLUSTERSIZE, addNodeNum(LEFT, COUNTER)), (unsigned) key);

rec generatePseudoRandom(rec L, unsigned4 c) := transform
    SELF.payload := (string) RANDOM() + (string) RANDOM()+(string) RANDOM() + (string) RANDOM()+(string) RANDOM() + (string) RANDOM();
    SELF.key := (string) RANDOM() + (string) RANDOM();
    SELF.len := LENGTH(SELF.payload)+LENGTH(SELF.key)+4;
  END;

dsbase := NORMALIZE(one_per_node, numrecsbase, generatePseudoRandom(LEFT, counter)) : INDEPENDENT;
dsadd := NORMALIZE(one_per_node, numrecsadd, generatePseudoRandom(LEFT, counter)) ;  
dsdel := NORMALIZE(one_per_node, numrecsdel, generatePseudoRandom(LEFT, counter)) ;  ;  

dsold := dsbase+dsdel;
dsnew := dsbase+dsadd;

outoldName := prefix + 'kd_old_dat';
outold := OUTPUT(dsold,,outoldName,overwrite);
outnewName := prefix + 'kd_new_dat';
outnew := OUTPUT(dsnew,,outnewName,overwrite);

oldindexName := prefix + 'kd_old';
oldindex := INDEX(dataset(outoldName,irec ,FLAT), {key,filepos} , {payload}, oldindexName);
newindexName := prefix + 'kd_new';
newindex := INDEX(dataset(outnewName,irec ,FLAT), {key,filepos} , {payload}, newindexName);
patchedindexName := prefix + 'kd_patched';
patchedindex := INDEX(dataset(outnewName,irec ,FLAT), {key,filepos} , {payload}, patchedindexName);

diffName := prefix + 'kd_diff';
patchedName := prefix + 'kd_patched';

bldold := buildindex(oldindex,overwrite);
bldnew := buildindex(newindex,overwrite);

crec T1(recordof(newindex) l,UNSIGNED c) := TRANSFORM
  self.len := 0;
  self.payload := l.payload;
  self.key := l.key;
  self.filepos := l.filepos;
  self.seq := c;
end;


crec T2(recordof(patchedindex) l,UNSIGNED c) := TRANSFORM
  self.len := 0;
  self.payload := l.payload;
  self.key := l.key;
  self.filepos := l.filepos;
  self.seq := c;
end;


SEQUENTIAL(
  outold,
  outnew,
  bldold,
  bldnew,
  KEYDIFF(oldindex, newindex, diffName, OVERWRITE),
  KEYPATCH(oldindex, diffName, patchedName, OVERWRITE),
  OUTPUT(
    COUNT(
      (PROJECT(newindex,T1(LEFT,COUNTER))-PROJECT(patchedindex,T2(LEFT,COUNTER)))+
      (PROJECT(patchedindex,T2(LEFT,COUNTER))-PROJECT(newindex,T1(LEFT,COUNTER)))
    )
  ),
  OUTPUT('Done'),
  
  // Clean-up
  FileServices.DeleteLogicalFile(patchedindexName),
  FileServices.DeleteLogicalFile(newindexName),
  FileServices.DeleteLogicalFile(oldindexName),
  FileServices.DeleteLogicalFile(diffName),
  FileServices.DeleteLogicalFile(patchedName),
  FileServices.DeleteLogicalFile(outnewName),
  FileServices.DeleteLogicalFile(outoldName)
);
