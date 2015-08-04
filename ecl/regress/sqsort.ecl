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

import * from sq;
DeclareCommon();


myPeople := sqSimplePersonBookDs(surname <> '');

childBookRec :=
            RECORD
dataset(sqBookIdRec)        books{blob};
            END;

slimPeopleRec :=
            RECORD
string20        surname;
childBookRec        child;
            END;

//Perverse coding to ensure we sort by a linked child record - to ensure it is correctly serialised
p := project(myPeople, transform(slimPeopleRec, self.child.books := left.books; self := left));
s := sort(p, -p.child);
output(s);
