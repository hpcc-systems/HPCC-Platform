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

//Normalize a denormalised dataset...

householdRecord := RECORD
unsigned4 house_id;
string20  address1;
string20  zip;
unsigned4 score;
string10 s_SequenceKey ;
unsigned2 scoreRank;
unsigned2 elementsMatched;
data10    m_l90key;
    END;


householdDataset := DATASET('household',householdRecord,FLAT);
filtered := householdDataset(zip != '');

a1 := sort(householdDataset, WHOLE RECORD);
output(a1);

a2 := sort(filtered, EXCEPT address1, filtered.zip, householdDataset.scoreRank, LOCAL);
output(a2);

b1 := dedup(householdDataset, WHOLE RECORD, all);
output(b1);

b2 := dedup(filtered, WHOLE RECORD, EXCEPT address1, EXCEPT filtered.zip, EXCEPT householdDataset.scoreRank, all);
output(b2);


g2 := group(filtered, WHOLE RECORD, EXCEPT address1, EXCEPT filtered.zip, EXCEPT householdDataset.scoreRank, all);
output(sort(g2, zip));


householdRecord tr(householdrecord l, householdrecord r) := transform
    self.zip := r.zip;
    self := l;
    end;

r2 := rollup(filtered, tr(left, right), WHOLE RECORD, EXCEPT address1, EXCEPT filtered.zip, EXCEPT householdDataset.scoreRank, local);
output(r2);

t2 := table(filtered, {count(group)}, WHOLE RECORD, EXCEPT address1, EXCEPT filtered.zip, EXCEPT householdDataset.scoreRank);
output(t2);

r3 := rollup(filtered, tr(left, right), score * scoreRank);
output(r3);
