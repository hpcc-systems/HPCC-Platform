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

//Normalize a denormalised dataset...

childId := RECORD
    unsigned a;
    unsigned b;
END;
householdRecord := RECORD
unsigned4 house_id;
string20  address1;
string20  zip;
unsigned4 score;
string10 s_SequenceKey ;
unsigned2 scoreRank;
unsigned2 elementsMatched;
data10    m_l90key;
childId     child;
    END;


householdDataset := DATASET('household',householdRecord,FLAT);

householdRecord z(householdRecord l, householdRecord r) := transform
   self := l;
   end;


queue1 := join(householdDataset, householdDataset, left.zip = right.zip, z(left, right));
BestHits:= Dedup(Queue1, whole record, except house_id,except zip);

output(besthits);

BestHits2 := Dedup(Queue1, ROW(Queue1));

output(besthits2);

householdRecord t(houseHoldRecord l, houseHoldRecord r) := TRANSFORM
    SELF.score := l.score + r.score;
    SELF := l;
END; 
BestHits3 := ROLLUP(Queue1, t(LEFT, RIGHT), ROW(Queue1));

output(besthits3);

BestHits4 := Dedup(Queue1, zip, child);

output(besthits4);
