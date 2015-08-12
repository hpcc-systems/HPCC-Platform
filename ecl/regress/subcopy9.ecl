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

//Project used inside a child dataset operation.
//First example should be more efficient since should assign directly to the target dataset
//second example will need to create a temporary row.

import dt;

childRecord := RECORD
unsigned4 person_id;
dt.pstring per_forename;
string20  per_surname;
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


childRecord selectBestChild(parentRecord l) :=
TRANSFORM
    SELF := sort(l.children, per_surname, -per_forename)[1];
END;

output(PROJECT(parentDataset,selectBestChild(LEFT)),,'out1.d00');

