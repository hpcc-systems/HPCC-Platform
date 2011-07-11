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

//noRoxie
//noHthor
import Std.System.ThorLib;
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

outold := OUTPUT(dsold,,'regress::kd_old_dat',overwrite);
outnew := OUTPUT(dsnew,,'regress::kd_new_dat',overwrite);

oldindex := INDEX(dataset('regress::kd_old_dat',irec ,FLAT), {key,filepos} , {payload}, 'regress::kd_old');
newindex := INDEX(dataset('regress::kd_new_dat',irec ,FLAT), {key,filepos} , {payload}, 'regress::kd_new');
patchedindex := INDEX(dataset('regress::kd_new_dat',irec ,FLAT), {key,filepos} , {payload}, 'regress::kd_patched');

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
  KEYDIFF(oldindex, newindex, 'regress::kd_diff', OVERWRITE),
  KEYPATCH(oldindex, 'regress::kd_diff', 'regress::kd_patched', OVERWRITE),
  OUTPUT(
    COUNT(
      (PROJECT(newindex,T1(LEFT,COUNTER))-PROJECT(patchedindex,T2(LEFT,COUNTER)))+
      (PROJECT(patchedindex,T2(LEFT,COUNTER))-PROJECT(newindex,T1(LEFT,COUNTER)))
    )
  ),
  OUTPUT('Done')
);
