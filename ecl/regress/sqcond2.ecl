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
