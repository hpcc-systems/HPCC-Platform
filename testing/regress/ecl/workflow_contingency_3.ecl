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
//#option('reportFailureToFirstDependant', false);

//This verifies that the parallel engine doesn't execute Failure clauses when the corresponding item is inactive
display(String thisString) := FUNCTION
  ds := dataset([thisString], {String text});
  RETURN Output(ds, NAMED('logging'), EXTEND);
END;

a := SEQUENTIAL(display('a'), FAIL(5103)) : independent;

b0 := display('failure contingency for b');
c0 := display('failure contingency for c');

b := a : Failure(b0);
c := a : Failure(c0);

e := SEQUENTIAL(b,c);

e;
//expect a, failure ... for b
