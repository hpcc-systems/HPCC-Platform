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

import sq;
sq.DeclareCommon();

#option ('childQueries', true);
#option ('pickBestEngine', false);
#option ('targetClusterType', 'hthor');

// Test that shared subqueries are handled correctly, and that shared cses are also handled


udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);
unsigned4 yob(udecimal8 dob) := dob / 10000D;

house := sqHousePersonBookDs;
persons := sqHousePersonBookDs.persons;
books := persons.books;

booksDs := sqBookDs(personid = persons.id);
personsDs := sqPersonDs(houseid = sqHousePersonBookDs.id);
booksDsDs := sqBookDs(personid = personsDs.id);
personsDsDs := sqPersonDs(houseid = sqHouseDs.id);
booksDsDsDs := sqBookDs(personid = personsDsDs.id);

//Someone in the house is older than the house and total price of books is less than book limit
oldest5People := sort(persons, dob)[1..5];
aveAgeTop5 := ave(oldest5People, age(dob));
maxAgeTop5 := max(oldest5People, age(dob));
minAgeTop5 := min(oldest5People, age(dob));
output(house, { addr, aveAgeTop5, (unsigned)(aveAgeTop5*1.0000000001) });
output(house, { addr, aveAgeTop5, maxAgeTop5, minAgeTop5 });

oldest5PeopleDs := sort(personsDs, dob)[1..5];
aveAgeTop5Ds := ave(oldest5PeopleDs, age(dob));
maxAgeTop5Ds := max(oldest5PeopleDs, age(dob));
minAgeTop5Ds := min(oldest5PeopleDs, age(dob));
output(house, { addr, aveAgeTop5Ds, (unsigned)(aveAgeTop5Ds*1.0000000001) });
output(house, { addr, aveAgeTop5Ds, maxAgeTop5Ds, minAgeTop5Ds });

oldest5PeopleDsDs := sort(personsDsDs, dob)[1..5];
aveAgeTop5DsDs := ave(oldest5PeopleDsDs, age(dob));
maxAgeTop5DsDs := max(oldest5PeopleDsDs, age(dob));
minAgeTop5DsDs := min(oldest5PeopleDsDs, age(dob));
output(sqHouseDs, { addr, aveAgeTop5DsDs, (unsigned)(aveAgeTop5DsDs*1.0000000001) });
output(sqHouseDs, { addr, aveAgeTop5DsDs, maxAgeTop5DsDs, minAgeTop5DsDs });

