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
