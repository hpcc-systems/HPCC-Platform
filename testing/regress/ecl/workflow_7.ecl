/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the 'License');
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an 'AS IS' BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */
//version parallel=false
//version parallel=true,nothor

import ^ as root;
optParallel := #IFDEFINED(root.parallel, false);

#option ('parallelWorkflow', optParallel);
#option('numWorkflowThreads', 5);

display(String8 thisString) := FUNCTION
  ds := dataset([thisString], {String8 text});
  RETURN Output(ds, NAMED('logging'), EXTEND);
END;
Import sleep from std.System.Debug;

//this test makes sure that item c is not activated too early (before item b has finished)
a := display('a') : independent;
b := ORDERED(sleep(1000), display('b')) : independent;
c := display('c');

a0 := a : independent;

SEQUENTIAL(a0, b, ORDERED(a,c));
