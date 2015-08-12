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

//--------------- Test group aggregation on child datasets --------------------
// Should clone this for datasets as well.

udecimal8 todaysDate := 20040602D;
unsigned4 ageInDecades(udecimal8 dob) := ((todaysDate - dob) / 100000D);


// How many people of each decade, and total number of books they have.
summaryRec :=
        RECORD
            decade := ageInDecades(sqHousePersonBookDs.persons.dob),
            cntpersons := count(group),
            cntbooks := sum(group, count(sqHousePersonBookDs.persons.books))
        END;

ageSummary := table(sqHousePersonBookDs.persons, summaryRec, ageInDecades(sqHousePersonBookDs.persons.dob));
mostBooks := sort(ageSummary, -cntbooks)[1];

//Each address and a summary for each address
//output(sqHousePersonBookDs, { addr, dataset(summaryRec) summary := ageSummary; });

//Each address, and the summary for the decade which has the most books.
output(sqHousePersonBookDs, { addr, summaryRec rec := mostBooks });

