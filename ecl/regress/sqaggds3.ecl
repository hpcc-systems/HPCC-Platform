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

#option ('childQueries', true);
//Working

import sq;
sq.DeclareCommon();

unsigned xxid := 0 : stored('xxid');

udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);

// Test the different child operators on related datasets.

// Different child operators, all inline.
//persons := relatedPersons(sqHouseDs);
//books := relatedBooks(persons);
persons := sqPersonDs(houseid = sqHouseDs.id);
books := sqBookDs(personid = persons.id);

personByAgeDesc1 := sort(persons, nofold(sqHouseDs.addr)[id], dob);
output(sqHouseDs, { addr, oldest := personByAgeDesc1[1].forename + ' ' + personByAgeDesc1[1].surname });

personByAgeDesc2 := sort(persons, nofold(xxid)=id, nofold(sqHouseDs.addr[id]), dob);
output(sqHouseDs, { addr, oldest := personByAgeDesc2[1].forename + ' ' + personByAgeDesc2[1].surname });

personByAgeDesc3 := sort(persons, nofold(xxid)=id, nofold(sqHouseDs.filepos=id), dob);
output(sqHouseDs, { addr, oldest := personByAgeDesc3[1].forename + ' ' + personByAgeDesc3[1].surname });

output(sqHouseDs, { count(persons(count(books(id!= sqHouseDs.id)) != 0)) });
