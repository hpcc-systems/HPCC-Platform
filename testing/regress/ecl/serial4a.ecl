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

//Test spilling a file containing child and grand-child dictionary

import Setup.SerialTest;

inDs := DATASET(SerialTest.DsFilename, SerialTest.LibraryDsRec, THOR);

sort1 := NOFOLD(SORT(inDs, owner));

//Two sorts will cause a spill in thor, two outputs will cause one in hthor.
//We really should have a SPILL() activity which introduces one.
sort2 := SORT(sort1, books[1].title);

output(sort1);
output(sort2);
