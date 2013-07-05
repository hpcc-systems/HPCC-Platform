/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#option ('targetClusterType', 'thorlcr');
#option ('allowScopeMigrate', false);

level1Rec := RECORD
    string1 a;
    string1 b;
    string1 c;
END;

level2Rec := RECORD
    unsigned1 nogrouping;
    level1Rec r;
END;

dsraw := DATASET('ds1', level2Rec, thor);

ds1 := PROJECT(dsraw, TRANSFORM(level2Rec, SELF.nogrouping := 0; SELF := LEFT));

r1 := RECORD
  unsigned8 nogrouping;
  unsigned8 countgroup;
 END;


ds0 := DATASET([{0,(unsigned8) (COUNT(ds1))}], r1);

layout_uniques := RECORD
  unsigned8 countgroup;
  unsigned8 nogrouping;
  unsigned8 r_a_unique;
  unsigned8 r_b_unique;
  unsigned8 r_c_unique;
 END;

da := ds1(r.a != '');
dax := TABLE(da, { nogrouping, string1 r_a_unique := r.a });
dax1l := DISTRIBUTE(dax, RANDOM());
dax2l := TABLE(dax1l, {  nogrouping, r_a_unique }, nogrouping, r_a_unique, local); 
dax1g := DISTRIBUTE(dax2l, RANDOM());
dax2g := TABLE(dax1g, {  nogrouping, r_a_unique }, nogrouping, r_a_unique); 
daxc := TABLE(dax2g, { nogrouping, unsigned8 cnt := COUNT(GROUP); }, nogrouping, few);


db := ds1(r.b != '');
dbx := TABLE(db, { nogrouping, string1 r_b_unique := r.b });
dbx1l := DISTRIBUTE(dbx, RANDOM());
dbx2l := TABLE(dbx1l, {  nogrouping, r_b_unique }, nogrouping, r_b_unique, local); 
dbx1g := DISTRIBUTE(dbx2l, RANDOM());
dbx2g := TABLE(dbx1g, {  nogrouping, r_b_unique }, nogrouping, r_b_unique); 
dbxc := TABLE(dbx2g, { nogrouping, unsigned8 cnt := COUNT(GROUP); }, nogrouping, few);

layout_uniques t(r1 l) := TRANSFORM
    SELF.r_a_unique := daxc(nogrouping = l.nogrouping)[1].cnt;
    SELF.r_b_unique := dbxc(nogrouping = l.nogrouping)[1].cnt;
    SELF := l;
    SELF := [];
END;

p := PROJECT(ds0, t(LEFT));

output(p);
