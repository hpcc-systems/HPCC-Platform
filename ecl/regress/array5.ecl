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
//Household,person,children,toys

toysRecord := RECORD
dt.pstring      toyName;
              END;

childRecord := RECORD
string20            childName;
unsigned4           numToys;
toysRecord          toy,dim(SELF.numToys);
              END;

personRecord := RECORD
pstring             personName;
bool                poor;
unsigned4           numChildren;
                    RECORD
string20                childName;
                        IFBLOCK(SELF.poor)
unsigned4                   numToys;
toysRecord                  toy,dim(SELF.numToys);
                        END;
                    END child,dim(SELF.numChildren);
              END;

houseRecord := RECORD
dt.pstring          address;
unsigned4           numPeople;
personRecord        occupier,dim(SELF.numPeople);
            END;


houseDataset := DATASET('in.d00', houseRecord, FLAT);


output(houseDataset,,'out.d00');

