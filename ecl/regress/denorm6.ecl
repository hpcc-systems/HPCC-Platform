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
string1   pcode;
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
string1              hasGavin            ;
string1              hasLiz;
string1              code;
                END;


personDataset := DATASET('person',personRecord,FLAT,OPT);
householdDataset := DATASET('household',householdRecord,FLAT);
combinedDataset := DATASET('combined',combinedRecord,FLAT);



combinedRecord householdToCombined(householdRecord l) :=
                TRANSFORM
                    SELF.numPeople := 0;
                    SELF.hasGavin := 'N';
                    SELF.hasLiz := 'N';
                    SELF.code := '4';
                    SELF := l;
                END;


combinedRecord doDenormalize(combinedRecord l, personRecord r) :=
                TRANSFORM
                    SELF.numPeople := l.numPeople + 1;
                    SELF.hasGavin := if(r.forename='Gavin','Y',l.hasGavin);
                    SELF.hasLiz := if(r.forename='Mia','Y',l.hasLiz);
                    SELF.code := IF(R.pcode = '7' AND L.code IN  ['1', '5'], '4', L.code);
                    SELF := l;
                END;


o1 := PROJECT(householdDataset, householdToCombined(LEFT));

o2 := denormalize(o1, personDataset, LEFT.house_id = RIGHT.house_id, doDenormalize(LEFT, RIGHT));

output(o2,,'out.d00');
