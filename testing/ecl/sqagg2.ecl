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
#option ('optimizeDiskSource',true)
#option ('optimizeChildSource',true)
#option ('optimizeIndexSource',true)
#option ('optimizeThorCounts',false)
#option ('countIndex',false)

// Similar to sqagg.hql.  This time using a compound aggregate operator and selcting the results out.
persons := sqHousePersonBookDs.persons;
books := persons.books;

udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);

// Grand children, again all inline.

summary1 := table(persons, { numBooks := sum(group, count(books)), maxRating := max(group, max(books, rating100))})[1];
summary2 := table(books, { numBooks := count(group), aveRating := ave(group, rating100), maxRating := max(group, rating100)})[1];
output(sqHousePersonBookDs, { addr, summary1.numBooks, summary1.maxRating });
output(sqHousePersonBookDs, { addr, summary2.numBooks, summary2.aveRating, summary2.maxRating });

//Now do the same, but unsure the children and main activity are not inline or compound

//sort order is deliberatley different from anything that will be used later
xpersons := sort(sqHousePersonBookDs.persons, surname + (string)dob + forename)[1..200];

// Grand children out of line, children are inline
xbooks := sort(sqHousePersonBookDs.persons.books, name + (string)rating100 + author)[1..200];

xsummary1 := table(persons, { numBooks := sum(group, count(xbooks)), maxRating := max(group, max(xbooks, rating100))})[1];
xsummary2 := table(xbooks, { numBooks := count(group), aveRating := ave(group, rating100), maxRating := max(group, rating100)})[1];
output(sqHousePersonBookDs, { addr, xsummary1.numBooks, xsummary1.maxRating });
output(sqHousePersonBookDs, { addr, xsummary2.numBooks, xsummary2.aveRating, xsummary2.maxRating });

// Grand children out of line, children also out of line
xxbooks := sort(xpersons.books, name + (string)rating100 + author)[1..200];

xxsummary1 := table(xpersons, { numBooks := sum(group, count(xxbooks)), maxRating := max(group, max(xxbooks, rating100))})[1];
xxsummary2 := table(xxbooks, { numBooks := count(group), aveRating := ave(group, rating100), maxRating := max(group, rating100)})[1];
output(sqHousePersonBookDs, { addr, xxsummary1.numBooks, xxsummary1.maxRating });
output(sqHousePersonBookDs, { addr, xxsummary2.numBooks, xxsummary2.aveRating, xxsummary2.maxRating });

