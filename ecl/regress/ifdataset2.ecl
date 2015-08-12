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

personRecord := RECORD
unsigned4 house_id;
unsigned4 person_id;
string20  surname;
string20  forename;
string1   pcode;
    END;

personDataset := DATASET('person',personRecord,FLAT,OPT);
seedDataset := DATASET('person',personRecord,FLAT,OPT);

personRecord t(personRecord l, personRecord r) := TRANSFORM
    SELF := r;
  END;
seeds := JOIN(personDataset, seedDataset, LEFT.pcode = RIGHT.pcode, t(LEFT, RIGHT));

seedPersonDataset := personDataset + seeds;

cond := count(personDataset) > 1000;
Wanted := IF( cond, seedPersonDataset, personDataset);

output(Wanted);
