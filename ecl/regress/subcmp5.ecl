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

//Normalize a denormalised dataset... Using element indexing on a child record.

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
childRecord         children[4];
string10            postcode;
                END;

parentDataset := DATASET('test',parentRecord,FLAT);


outRecord := RECORD
typeof(parentDataset.id)    household_id;
boolean                     sameChildren;
    END;



outRecord compareChildren(parentRecord l, parentRecord r) :=
                TRANSFORM
                    SELF.household_id := l.id;
                    SELF.sameChildren := l.children = r.children;
                END;

o1 := join(parentDataset, parentDataset, LEFT.id = RIGHT.id , compareChildren(LEFT, RIGHT));

output(o1,,'out.d00');
