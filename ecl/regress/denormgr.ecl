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

#option ('targetClusterType', 'hthor');
//Normalize a denormalised dataset...

householdRecord := RECORD
unsigned4 house_id;
string20  address1;
string20  zip;
    END;


personRecord := RECORD
unsigned4 house_id;
unsigned4 person_id;
string20  surname;
string20  forename;
    END;

childPersonRecord := RECORD
unsigned4 person_id;
string20  surname;
string20  forename;
    END;

combinedRecord :=
                RECORD
householdRecord;
unsigned4            numPeople;
DATASET(childPersonRecord, COUNT(SELF.numPeople))   children;
                END;


personDataset := DATASET('person',personRecord,FLAT);
householdDataset := DATASET('household',householdRecord,FLAT);

combinedRecord doDenormalize(householdRecord l, dataset(personRecord) r) :=
                TRANSFORM
                    SELF.numPeople := count(r);
                    SELF.children := sort(project(r, transform(childPersonRecord, self := left)), person_id);
                    SELF := l;
                END;


o2 := denormalize(householdDataset, personDataset, LEFT.house_id = RIGHT.house_id, group, doDenormalize(LEFT, rows(RIGHT)));

output(o2,,'out.d00');
