/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

import $.setup;
sq := setup.sq('hthor');

// Test that shared subqueries are handled correctly, and that shared cses are also handled

udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);
unsigned4 yob(udecimal8 dob) := dob / 10000D;

house := sq.HousePersonBookDs;
persons := sq.HousePersonBookDs.persons;
books := persons.books;

booksDs := sq.BookDs(personid = persons.id);
personsDs := sq.PersonDs(houseid = sq.HousePersonBookDs.id);
booksDsDs := sq.BookDs(personid = personsDs.id);
personsDsDs := sq.PersonDs(houseid = sq.HouseDs.id);
booksDsDsDs := sq.BookDs(personid = personsDsDs.id);

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
output(sq.HouseDs, { addr, aveAgeTop5DsDs, (unsigned)(aveAgeTop5DsDs*1.0000000001) });
output(sq.HouseDs, { addr, aveAgeTop5DsDs, maxAgeTop5DsDs, minAgeTop5DsDs });
