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

import std.System.Debug;

startTime := Debug.msTick() : independent;

namesRecord :=
            RECORD
string         x;
            END;

namesRecord t(unsigned c) := TRANSFORM
    len := [4,16,32,64][(c % 4) + 1];
    SELF.x := 'x'[1..len];
END;

numIter := 1000000;
ds := NOFOLD(DATASET(numIter, t(COUNTER)));

output(COUNT(ds));

output('Time taken = ' + (string)(Debug.msTick()-startTime));
