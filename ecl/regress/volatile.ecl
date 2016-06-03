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

import Std.System.Debug;

startTime := Debug.msTick() : independent;

output(startTime-Debug.msTick());
Debug.Sleep(10);

output(startTime-Debug.msTick());
Debug.Sleep(10);

//Evaluate this once
output(startTime-Debug.msTick());

//Evaluate tick twice
output(startTime*startTime-Debug.msTick()*Debug.msTick());

//Only evalaute tick once
now := Debug.msTick();
output(startTime*startTime-now*now);


nowTime() volatile := define Debug.msTick();

//Evaluate nowTime twice
output(startTime*startTime-nowTime()*nowTime());

//Only evalaute nowTime once
now2 := nowTime();
output(startTime*startTime-now2*now2);
