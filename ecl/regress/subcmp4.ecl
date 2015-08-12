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

//Check a single child records can be treated as a blob
import dt;

tradeRecord := RECORD
unsigned4   trade_id;
real        trd_balance;
dt.pstring  trd_notes;
unsigned1   bankNameLen;
dt.vstring(self.bankNameLen) bankName;
unsigned8   holepos;
    END;

personRecord := RECORD
unsigned4 person_id;
string20  per_surname;
string20  per_forename;
unsigned2 numTrades;
dataset(tradeRecord, count(SELF.numTrades)) personTrades;
unsigned8 holepos;
    END;

householdRecord :=
                RECORD
unsigned8           household_id;
string20            address1;
string20            address2;
string20            address3;
unsigned2           numPeople;
DATASET(personRecord, COUNT(SELF.numPeople))   people;
string10            postcode;
                END;

householdDataset := DATASET('test',householdRecord,FLAT);

outRecord := RECORD
typeof(householdDataset.household_id)   household_id;
boolean                     sameChildren;
boolean                     sameFirstChild;
    END;

outRecord compareChildren(householdDataset l, householdDataset r) :=
                TRANSFORM
                    SELF.household_id := l.household_id;
                    SELF.sameFirstChild := (l.people[1] = r.people[1]);
                    SELF.sameChildren := false;//l.people = r.people;
                END;

o1 := join(householdDataset(address1<>''), householdDataset(address2<>''), LEFT.household_id = RIGHT.household_id, compareChildren(LEFT, RIGHT));

output(o1,,'out.d00');
