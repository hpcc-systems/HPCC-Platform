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

