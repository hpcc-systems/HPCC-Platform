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
    END;

newPersonRecord := RECORD
unsigned4 person_id;
string20  surname;
    END;

personIdRecord := RECORD
unsigned4 person_id;
    END;

personDataset := DATASET('person',personRecord,FLAT);

o1 := personDataset(surname <> 'Hawthorn');
output(o1,,'out1.d00');

o2 := personDataset(surname <> 'Hawthorn',forename <>'Gavin');
output(o2,,'out2.d00');

newPersonRecord t(personRecord l) := TRANSFORM SELF := l; END;
personidRecord t2(newPersonRecord l) := TRANSFORM SELF := l; END;

o3 := project(personDataset, t(LEFT));
output(o3,,'out3.d00');

o4a := personDataset(surname <> 'Page');
o4b := project(o4a, t(LEFT));
output(o4b,,'out4.d00');

o5a := project(personDataset, t(LEFT));
o5b := o5a(surname <> 'Hewit');
output(o5b,,'out5.d00');

o6a := personDataset(forename <> 'Richard');
o6b := project(o6a, t(LEFT));
o6c := o6b(surname <> 'Drimbad');
output(o6c,,'out6.d00');

o7a := project(personDataset, t(LEFT));
o7b := project(o7a, t2(LEFT));
output(o7b,,'out7.d00');

o8a := personDataset(forename <> 'Richard');
output(o8a,{forename,surname},'out8.d00');

o9a := personDataset(forename <> 'Richard');
o9b := table(o9a,{forename,surname});
output(o9b,,'out9.d00');

o10a := personDataset(forename <> 'Richard');
o10b := table(o10a,{count(group),surname},surname);
output(o10b,,'out10.d00');

