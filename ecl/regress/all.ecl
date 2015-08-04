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

#option ('globalFold', false);


// Following should be true

all >= all;
all>[1,2];
[1,2]<>all;
all>[1,2];
all>[1,2] AND [1,2]<>all;

[] < ALL;
[] <= ALL;
[] != ALL;
all > [];
all >= [];
all != [];

123 in ALL;
'a' in ALL;

// Following should be false

all > all;
[1,2]>all;
[1,2]=all;
145 not in ALL;

[] > ALL;
[] >= ALL;
[] = all;
all < [];
all <= [];
all = [];
all <= [];

person := dataset('person', { unsigned8 person_id, string10 per_ssn; }, thor);
count(person(per_ssn in ALL));
count(person(per_ssn in []));
