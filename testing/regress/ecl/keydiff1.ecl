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

outold := OUTPUT(dsold,,prefix + 'kd_old_dat',overwrite);
outnew := OUTPUT(dsnew,,prefix + 'kd_new_dat',overwrite);

oldindex := INDEX(dataset(prefix + 'kd_old_dat',irec ,FLAT), {key,filepos} , {payload}, prefix + 'kd_old');
newindex := INDEX(dataset(prefix + 'kd_new_dat',irec ,FLAT), {key,filepos} , {payload}, prefix + 'kd_new');
patchedindex := INDEX(dataset(prefix + 'kd_new_dat',irec ,FLAT), {key,filepos} , {payload}, prefix + 'kd_patched');

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
  KEYDIFF(oldindex, newindex, prefix + 'kd_diff', OVERWRITE),
  KEYPATCH(oldindex, prefix + 'kd_diff', prefix + 'kd_patched', OVERWRITE),
  OUTPUT(
    COUNT(
      (PROJECT(newindex,T1(LEFT,COUNTER))-PROJECT(patchedindex,T2(LEFT,COUNTER)))+
      (PROJECT(patchedindex,T2(LEFT,COUNTER))-PROJECT(newindex,T1(LEFT,COUNTER)))
    )
  ),
  OUTPUT('Done')
);
