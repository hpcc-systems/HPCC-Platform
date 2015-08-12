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


unsigned addValues(integer v1, integer v2) := v1 + v2;
integer multiValues(string v1, integer v2) := (unsigned)(v1[1]) * v2;

integer applyValue(integer v1, integer actionFunc(integer v1, integer v2)) := actionFunc(v1, v1+1)*2;
integer doApplyValue(integer v1, integer actionFunc(integer v1, integer v2)) := applyValue(v1+1, actionFunc);
integer applyValueX(integer v1, integer actionFunc(string v1, integer v2)) := actionFunc((string)v1, v1+1)*2;

output(applyValue(1, addValues));       // not compatible - ret type
output(applyValue(2, addValues));       // not compatible - ret type
output(applyValue(1, multiValues));     // not compatible - argument
output(applyValue(2, multiValues));     // not compatible - argument

output(doApplyValue(1, multiValues));       // not compatible
output(doApplyValue(2, multiValues));       // not compatible
output(doApplyValue(multiValues, 1));       // func passed to non func
output(doApplyValue(2, 3));                 // non func passed to func


//Phew: An attribute taking functional parameters which themselves have parameters which are functional...
//check it all binds correctly
string doMany(integer v1,
                integer firstAction(integer v1, integer actionFunc(integer v1, integer v2)),
                integer secondAction(integer v1, integer actionFunc(integer v1, integer v2)),
                integer actionFunc(integer v1, integer v2)) :=
                (string)firstAction(v1, actionFunc) + ':' + (string)secondaction(v1, actionFunc);


output(doMany(2, applyValue, applyValueX, multiValues));    // Second is incompatible - deeply nested difference


integer applyValue2(integer v1, integer actionFunc(integer v1, integer v2) = addValues) := actionFunc(v1, v1+1)*2;


output(applyValue2(1));     // default is not compatible
