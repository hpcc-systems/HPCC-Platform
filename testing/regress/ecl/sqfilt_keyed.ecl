/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

//Variable size comparisons only supported using the new field filters
#option ('createValueSets', true);

//more: need to implement field filters in roxie
//noroxie

//version multiPart=false

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, false);

//--- end of version configuration ---

import $.setup;
sq := setup.sq(multiPart);

// Test filtering at different levels, making sure parent fields are available in the child query.
// Also tests scoping of sub expressions using within.

udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);

set of string10 searchNames := ['Elizabeth', 'Gavin'] : independent;


//Once the new keyed segment filtering has been created this test should be changed to use KEYED on each of the following.
//There should also be an option to force all disk and index access to be remote and test that code.

output(sq.houseDs(keyed(addr = 'Bedrock')));
output(sq.houseDs(keyed(yearbuilt <= 1720)));
output(sq.personDs(keyed(forename in ['Brian', 'Liz', 'Abigail', 'Elizabeth                  NOMATCH!'])));

//MORE: keyed version not implemented yet
output(sq.personDs(forename[1..2] in ['Br', 'Li', 'Ab']));

output(sq.personDs(keyed(forename in searchNames)));
output(sq.personDs(keyed(surname = 'Jones  ')));
output(sq.personDs(keyed(dob > 19700101)));
output(sq.bookDs(keyed((unsigned2)rating100 > 257)));
