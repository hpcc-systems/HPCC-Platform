/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
