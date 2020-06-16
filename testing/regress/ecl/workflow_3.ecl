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

display(Integer8 thisInteger) := FUNCTION
  ds := dataset([thisInteger], {Integer8 value});
  RETURN Output(ds, NAMED('logging'), EXTEND);
END;

//Each of the second set of items has the same set of dependencies. The prior items should only be executed once by the workflow algorithm.
a1 := 1 : independent;
a2 := 2 : independent;
a3 := 3 : independent;
a4 := 4 : independent;
a5 := 5 : independent;

s := SUM(a1,a2,a3,a4,a5): independent;
mi := MIN(a1,a2,a3,a4,a5): independent;
ma := MAX(a1,a2,a3,a4,a5): independent;
av := AVE(a1,a2,a3,a4,a5): independent;

//Use sequential to get a consistent order in Output
sequential(display(s),display(mi),display(ma),display(av));
//Parallel(display(s),display(mi),display(ma),display(av));
