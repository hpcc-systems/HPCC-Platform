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

import $.setup;
sq := setup.sq('hthor');

sequential(
//Simple disk aggregate
output(table(sq.SimplePersonBookDs, { sum(group, aage),exists(group),exists(group,aage>0),exists(group,aage>100),count(group,aage>20) })),

//Filtered disk aggregate, which also requires a beenProcessed flag
output(table(sq.SimplePersonBookDs(surname != 'Halliday'), { max(group, aage) })),

//Special case count.
output(table(sq.SimplePersonBookDs(forename = 'Gavin'), { count(group) })),

output(count(sq.SimplePersonBookDs)),

//Special case count.
output(table(sq.SimplePersonBookDs, { count(group, (forename = 'Gavin')) })),

//existance checks
output(exists(sq.SimplePersonBookDs)),
output(exists(sq.SimplePersonBookDs(forename = 'Gavin'))),
output(exists(sq.SimplePersonBookDs(forename = 'Joshua')))
);
