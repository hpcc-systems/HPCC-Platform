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


integer actionPrototype(integer v1, integer v2) := 0;

integer aveValues(integer v1, integer v2) := (v1 + v2) DIV 2;
integer addValues(integer v1, integer v2) := v1 + v2;
integer multiValues(integer v1, integer v2) := v1 * v2;

integer applyPrototype(integer v1, actionPrototype actionFunc) := 0;
integer applyValue2(integer v1, actionPrototype actionFunc = aveValues) := actionFunc(v1, v1+1)*2;
integer applyValue4(integer v1, actionPrototype actionFunc = aveValues) := actionFunc(v1, v1+1)*4;
integer doApplyValue(integer v1, actionPrototype actionFunc) := applyValue2(v1+1, actionFunc);

output(applyValue2(1));                 // 2
output(applyValue2(2));                 // 4
output(applyValue2(1, addValues));      // 6
output(applyValue2(2, addValues));      // 10
output(applyValue2(1, multiValues));        // 4
output(applyValue2(2, multiValues));        // 12

output(doApplyValue(1, multiValues));       // 12
output(doApplyValue(2, multiValues));       // 24


//Phew: An attribute taking functional parameters which themselves have parameters which are functional...
//check it all binds correctly
string doMany(integer v1,
                applyPrototype firstAction,
                applyPrototype secondAction,
                actionPrototype actionFunc) :=
                (string)firstAction(v1, actionFunc) + ':' + (string)secondaction(v1, actionFunc);


output(doMany(1, applyValue2, applyValue4, addValues));     // "6:12"
output(doMany(2, applyValue4, applyValue2, multiValues));   // "24:12"
