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

//The first time this workunit executes it gets an exception in a conditional workflow item.
//If it is "recovered" the condition must be re-evaluated (otherwise the false branch is taken the
//second time.

o0 := OUTPUT('...');
o1 := FAIL('Oh no!') : independent;
o2 := OUTPUT('Should not be output') : independent;
o3 := output('Begin');
o4 := output('Done');

b1 := SEQUENTIAL(o0, o1);
b2 := SEQUENTIAL(o0, o2);

cond := true : STORED('cond');

x := IF(cond, b1, b2);

SEQUENTIAL(o3, x, o4);
