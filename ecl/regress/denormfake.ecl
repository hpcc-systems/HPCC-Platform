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


/* Used to test denormalization in hthor - output record cannot change size */
householdRecord := RECORD
string4 house_id;
string20  address1;
string50  allNames := '';
    END;


personRecord := RECORD
string4 house_id;
string20  forename;
    END;

householdDataset := dataset([
        {'0001','166 Woodseer Street'},
        {'0002','10 Slapdash Lane'},
        {'0004','Buckingham Palace'},
        {'0005','Bomb site'}], householdRecord);

personDataset := dataset([
        {'0002','Spiders'},
        {'0001','Gavin'},
        {'0002','Gavin'},
        {'0002','Mia'},
        {'0003','Extra'},
        {'0001','Mia'},
        {'0004','King'},
        {'0004','Queen'}], personRecord);

householdRecord doDenormalize(householdRecord l, personRecord r) :=
                TRANSFORM
                    SELF.allNames := IF(l.allNames<>'', TRIM(l.allNames) + ',' + r.forename, r.forename);
                    SELF := l;
                END;


o := denormalize(householdDataset, personDataset, LEFT.house_id = RIGHT.house_id, doDenormalize(LEFT, RIGHT));

output(o,,'out.d00');
