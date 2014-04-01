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

import $.setup.sq;

// Similar to sqagg.hql.  This time using a compound aggregate operator and selecting the results out.
persons := sq.HousePersonBookDs.persons;
books := persons.books;

udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);

// Grand children, again all inline.

summary1 := table(persons, { numBooks := sum(group, count(books)), maxRating := max(group, max(books, rating100))})[1];
summary2 := table(books, { numBooks := count(group), aveRating := ave(group, rating100), maxRating := max(group, rating100)})[1];
output(sq.HousePersonBookDs, { addr, summary1.numBooks, summary1.maxRating });
output(sq.HousePersonBookDs, { addr, summary2.numBooks, summary2.aveRating, summary2.maxRating });

//Now do the same, but unsure the children and main activity are not inline or compound

//sort order is deliberatley different from anything that will be used later
xpersons := sort(sq.HousePersonBookDs.persons, surname + (string)dob + forename)[1..200];

// Grand children out of line, children are inline
xbooks := sort(sq.HousePersonBookDs.persons.books, name + (string)rating100 + author)[1..200];

xsummary1 := table(persons, { numBooks := sum(group, count(xbooks)), maxRating := max(group, max(xbooks, rating100))})[1];
xsummary2 := table(xbooks, { numBooks := count(group), aveRating := ave(group, rating100), maxRating := max(group, rating100)})[1];
output(sq.HousePersonBookDs, { addr, xsummary1.numBooks, xsummary1.maxRating });
output(sq.HousePersonBookDs, { addr, xsummary2.numBooks, xsummary2.aveRating, xsummary2.maxRating });

// Grand children out of line, children also out of line
xxbooks := sort(xpersons.books, name + (string)rating100 + author)[1..200];

xxsummary1 := table(xpersons, { numBooks := sum(group, count(xxbooks)), maxRating := max(group, max(xxbooks, rating100))})[1];
xxsummary2 := table(xxbooks, { numBooks := count(group), aveRating := ave(group, rating100), maxRating := max(group, rating100)})[1];
output(sq.HousePersonBookDs, { addr, xxsummary1.numBooks, xxsummary1.maxRating });
output(sq.HousePersonBookDs, { addr, xxsummary2.numBooks, xxsummary2.aveRating, xxsummary2.maxRating });
