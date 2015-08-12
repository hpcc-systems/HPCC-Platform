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

#option ('optimizeGraph', false);
//Variable size target project used inside a child dataset operation.
//First example should be more efficient since should assign directly to the target dataset
//second example will need to create a temporary row.

import dt;

childRecord := RECORD
unsigned4 person_id;
string20  per_surname;
dt.pstring per_forename;
unsigned8 holepos;
    END;

parentRecord :=
                RECORD
unsigned8           id;
string20            address1;
string20            address2;
string20            address3;
unsigned2           numPeople;
DATASET(childRecord)   children;
string10            postcode;
                END;

parentDataset := DATASET('test',parentRecord,FLAT);


newChildRecord := RECORD,maxlength(78)
unsigned4 person_id;
string  per_surname;
    END;

newParentRecord :=
                RECORD
unsigned8           id;
string20            address1;
string20            address2;
string20            address3;
unsigned2           numPeople;
DATASET(newChildRecord)   children;
string10            postcode;
                END;


newChildRecord copyChild(childRecord l) := TRANSFORM
        SELF.per_surname := TRIM(l.per_surname);
        SELF := l;
    END;

newParentRecord copyProjectChoose(parentRecord l) :=
TRANSFORM
    SELF.children := project(choosen(l.children, 2), copyChild(LEFT));
    SELF := l;
END;

output(PROJECT(parentDataset,copyProjectChoose(LEFT)),,'out1.d00');

newParentRecord copyChooseProject(parentRecord l) :=
TRANSFORM
    SELF.children := choosen(project(l.children, copyChild(LEFT)), 2);
    SELF := l;
END;

output(PROJECT(parentDataset,copyChooseProject(LEFT)),,'out2.d00');
