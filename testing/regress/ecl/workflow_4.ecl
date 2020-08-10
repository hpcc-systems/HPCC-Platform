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

//The sleep times mean that when executed in parallel, the numbers should be in order. Currently, they are out of order.
x := SEQUENTIAL(display('one'),
           sleep(1000),
           display('two'));
y := SEQUENTIAL(sleep(2000),
           display('three'),
           sleep(1000),
           display('four'));

//Use sequential to get a consistent order in Output
IF(optParallel, PARALLEL(x,y), SEQUENTIAL(x,y));
//PARALLEL(x,y);
