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

childRecord := RECORD
unsigned4 person_id;
string20  per_surname;
string20  per_forename;
unsigned8 holepos;
    END;

parentRecord :=
                RECORD
unsigned8           id;
string20            address1;
string20            address2;
string20            address3;
unsigned2           numPeople;
DATASET(childRecord, COUNT(SELF.numPeople))   children;
string10            postcode;
                END;

parentDataset := DATASET('test',parentRecord,FLAT);


outRecord := RECORD
typeof(parentDataset.id)    household_id;
unsigned4 person_id;
string20  per_surname;
string20  per_forename;
    END;


outRecord normalizeChildren(parentRecord l, childRecord r, integer c) :=
                TRANSFORM
                    SELF.household_id := l.id;
                    SELF := r;
                END;


normalizedNames := normalize(parentDataset, LEFT.children, normalizeChildren(LEFT, RIGHT, COUNTER));


output(normalizedNames,,'out.d00');
