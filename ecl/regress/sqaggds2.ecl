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

