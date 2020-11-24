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
#onwarning(100, ignore);

//This is to check that the workflow terminates and doesn't run forever.
//the same exception will cause a contingency failure and a workflow failure
x := SEQUENTIAL(OUTPUT(2), FAIL(100)) : INDEPENDENT;
A0 := x;
A := OUTPUT(1) : INDEPENDENT, SUCCESS(A0);

B := PARALLEL(OUTPUT(3), x) : INDEPENDENT;

SEQUENTIAL(A,B);
