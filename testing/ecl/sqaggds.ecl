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

//UseStandardFiles
//nothor
//nothorlcr
#option ('optimizeDiskSource',true)
#option ('optimizeChildSource',true)
#option ('optimizeIndexSource',true)
#option ('optimizeThorCounts',false)
#option ('countIndex',false)

boolean missingLevelOk := false;

boolean storedTrue := true : stored('storedTrue');

udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);

// Test the different child operators on related datasets.

// Different child operators, all inline.
//persons := relatedPersons(sqHouseDs);
//books := relatedBooks(persons);
persons := sqPersonDs(houseid = sqHouseDs.id);
books := sqBookDs(personid = persons.id);

personByAgeDesc := sort(persons, dob);

o1 := table(sqHouseDs, { addr, count(persons), ave(persons, age(dob)), max(persons, dob)});
o2 := table(sqHouseDs, { addr, oldest := personByAgeDesc[1].forename + ' ' + personByAgeDesc[1].surname });
o3 := table(sqHouseDs, { addr, firstPerson := persons[1].forename + ' ' + persons[1].surname });

// Grand children, again all inline.

booksByRatingDesc := sort(books, -rating100);

//More: Need to think about walking 3rd level children e.g., in ave, and [1]:
o4 := table(sqHouseDs, { addr, numBooks := sum(persons, count(books(storedTrue))), max(persons, max(books, rating100))});
o5 := table(sqHouseDs, { addr, firstBook := evaluate(persons[1], books[1].name) + ': ' + evaluate(persons[1], books[1].author) });
#if (missingLevelOk)
//This really needs the idea of implicit relationships between files before it is going to work
o6 := table(sqHouseDs, { addr, numBooks := count(books), ave(books, rating100), max(books, rating100)});
o7 := table(sqHouseDs, { addr, bestBook := booksByRatingDesc[1].name + ': ' + booksByRatingDesc[1].author});
o8 := table(sqHouseDs, { addr, firstBook := books[1].name + ': ' + books[1].author });      //NB: Different from above.
#end

//--------- Now perform the aggregate operations with person as outer iteration ----------

o9 := table(sqPersonDs, { surname, numBooks := count(books), ave(books, rating100), max(books, rating100)});
o10 := table(sqPersonDs, { surname, bestBook := booksByRatingDesc[1].name + ': ' + booksByRatingDesc[1].author});
o11 := table(sqPersonDs, { surname, firstBook := books[1].name + ': ' + books[1].author });     //NB: Different from above.

//More: Need to think about acceessing fields in house - need some sort of relation construct
#if (false)
o12 := table(sqPersonDs, { sqHouseDs.addr, surname, numBooks := count(books), ave(books, rating100), max(persons.books, rating100)});
o13 := table(sqPersonDs, { sqHouseDs.addr, surname, bestBook := booksByRatingDesc[1].name + ': ' + booksByRatingDesc[1].author});
o14 := (sqPersonDs, { sqHouseDs.addr, surname, firstBook := books[1].name + ': ' + books[1].author });      //NB: Different from above.
#end

output(o1);
output(o2);
output(o3);
output(o4);
output(o5);

#if (missingLevelOk)
output(o6);
output(o7);
output(o8);
#end

output(o9);
output(o10);
output(o11);

#if (false)
output(o12);
output(o13);
output(o14);
#end

output(allnodes(local(o1)));
output(allnodes(local(o2)));
output(allnodes(local(o3)));
output(allnodes(local(o4)));
output(allnodes(local(o5)));

#if (missingLevelOk)
output(allnodes(local(o6)));
output(allnodes(local(o7)));
output(allnodes(local(o8)));
#end

output(allnodes(local(o9)));
output(allnodes(local(o10)));
output(allnodes(local(o11)));

#if (false)
output(allnodes(local(o12)));
output(allnodes(local(o13)));
output(allnodes(local(o14)));
#end
