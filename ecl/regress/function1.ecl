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


newChildRecord := RECORD
unsigned4 person_id;
string20  per_surname;
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


//MORE: Really need a way of passing dataset parameters...
doProjectParent(unsigned8 idAdjust, unsigned8 numChildren) :=
FUNCTION
    newParentRecord copyChooseProject(parentRecord l) :=
        TRANSFORM

            doProjectChild(unsigned8 idAdjust2) :=
            FUNCTION
                newChildRecord copyChild(childRecord l) :=
                    TRANSFORM
                        SELF.person_id := l.person_id + idAdjust2;
                        SELF := l;
                    END;
                RETURN project(choosen(l.children, numChildren), copyChild(LEFT));
            END;

            SELF.children := doProjectChild(l.id);
            SELF.id := l.id + idAdjust;
            SELF := l;
        END;

    RETURN project(parentDataset,copyChooseProject(LEFT));
END;


output(doProjectParent(10, 2),,'out1.d00');
