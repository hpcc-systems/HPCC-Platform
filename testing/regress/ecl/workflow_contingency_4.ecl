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
#onwarning(5102, ignore);

Import sleep from std.System.Debug;

display(String thisString) := FUNCTION
  ds := dataset([thisString], {String text});
  RETURN Output(ds, NAMED('logging'), EXTEND);
END;

//this tests that items are aborted once the workflow fails
b := SEQUENTIAL(display('b'), FAIL(5103)) : independent;

c0 := sleep(2000) : independent;
c1 := sleep(2001) : independent;
c2 := sleep(2002) : independent;
c3 := sleep(2003) : independent;

c := SEQUENTIAL(c0, c1, c2, c3, display('c'));

//Note: the sequential engine behaves differently if Parallel is used, because the workflow output from the code generator is inconsistent
IF(optParallel, PARALLEL(b, c), SEQUENTIAL(b,c));
