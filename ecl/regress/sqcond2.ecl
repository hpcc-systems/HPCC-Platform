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

#option ('targetClusterType', 'hthor');

import sq;
sq.DeclareCommon();

// Test the different child operators.  Try and test inline and out of line, also part of a compound
// source activity and not part.


peopleByAge := sort(sqHousePersonBookExDs.persons, dob);

dedupedPeople := dedup(peopleByAge, surname);

filteredPeople := dedupedPeople(aage != 0);

myPeople := if(exists(filteredPeople), filteredPeople, peopleByAge);

output(sqHousePersonBookExDs, { id, dataset people := myPeople; });
