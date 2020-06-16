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

//this test makes sure that the secondary dependencies of Ordered items are active from the start
display(String thisString) := FUNCTION
  ds := dataset([thisString], {String text});
  RETURN Output(ds, NAMED('logging'), EXTEND);
END;

a0 := 'a' : independent;
a1 := display(a0) : independent;

b0 := 'b' : independent;
b1 := display(b0) : independent;

ORDERED(a1,b1);
