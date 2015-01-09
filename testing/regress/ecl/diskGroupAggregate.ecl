/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

import $.setup;
sq := setup.sq(multiPart);

// Test a case that needs serialize due to child dataset...
sort(table(sq.SimplePersonBookds, { dataset books := sq.SimplePersonBookDs.books, sq.SimplePersonBookDs.surname, sq.SimplePersonBookDs.forename, count(group) }, sq.SimplePersonBookDs.surname, few), surname, forename);

// ... and a case that doesn't
sort(table(sq.SimplePersonBookds, { sq.SimplePersonBookDs.surname, sq.SimplePersonBookDs.forename, count(group) }, sq.SimplePersonBookDs.surname, few), surname, forename);
