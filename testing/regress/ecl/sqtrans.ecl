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

//version multiPart=false

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, false);

//--- end of version configuration ---

//check that compound disk read activities workcorrectly when the input file is translated
//include keyed filters to ensure they are also translated correctly

import $.setup;
sq := setup.sq(multiPart);

r1 := recordof(sq.simplePersonBookDs);
r2 := r1 - [surname];
r3 := RECORD
    r2;
    string surname;
END;

translatedFile := DATASET(__NAMEOF__(sq.simplePersonBookDs), r3, thor);
//translatedFile := sq.simplePersonBookDs;
filtered := translatedFile(KEYED(forename != 'Wilma'));

sequential(
    output(TABLE(filtered, { surname, forename })),
    output(TABLE(translatedFile.books, { name, author })),
    output(TABLE(filtered, { max(group, surname) })),
    output(SORT(NOFOLD(TABLE(filtered, { unsigned cnt := count(group), surname }, surname, FEW )), -cnt, surname))
);
