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
householdDataset2 := DATASET('household',householdRecord,FLAT);

a1 := distribute(householdDataset, hash(house_id));
a2 := sort(a1, house_id, -score, local);
a3 := dedup(a2, house_id);

b1 := distribute(householdDataset2, hash(house_id));


a4 := join(a3, b1, left.house_id = right.house_id, local);
output(a4,,'out.d00');
