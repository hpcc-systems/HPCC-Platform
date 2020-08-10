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

//This test checks that every failure clause is executed
display(Integer8 thisInteger) := FUNCTION
  ds := dataset([thisInteger], {Integer8 value});
  RETURN Output(ds, NAMED('logging'), EXTEND);
END;

c := display(3);
b := SEQUENTIAL(display(2), FAIL(5102)) : Failure(c);
a := SEQUENTIAL(display(1), FAIL(5103)) : Failure(b);

a;

//expect 1,2,3
