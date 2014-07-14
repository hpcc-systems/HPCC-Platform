/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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

import std.system.debug;

unsigned startTime := debug.msTick() : independent;

sortRecord :=
            RECORD
unsigned8       id;
            END;

unsigned numRows := 0x1000000 : stored('numRows');

allIds := DATASET(numRows, TRANSFORM(sortRecord, SELF.id := HASH64(COUNTER)));

s := SORT(allIds, id);

output(COUNT(NOFOLD(s)));
output('Time taken = ' + (debug.msTick() - startTime));
