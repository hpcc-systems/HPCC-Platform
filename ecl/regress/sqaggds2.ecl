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

udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);

// Test the different child operators on related datasets.

persons := sqPersonDs(houseid = sqHouseDs.id);
books := sqBookDs(personid = persons.id);


// Grand children, again all out of line.

summary1 := table(persons, { numBooks := sum(group, count(books)), maxRating := max(group, max(books, rating100))})[1];
summary2 := table(books, { numBooks := count(group), aveRating := ave(group, rating100), maxRating := max(group, rating100)})[1];
output(sqHouseDs, { addr, summary1.numBooks, summary1.maxRating });
output(sqHouseDs, { addr, summary2.numBooks, summary2.aveRating, summary2.maxRating });

