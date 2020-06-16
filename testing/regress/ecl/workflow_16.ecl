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

Import sleep from std.System.Debug;

display(Integer8 thisInteger) := FUNCTION
  ds := dataset([thisInteger], {Integer8 value});
  RETURN Output(ds, NAMED('logging'), EXTEND);
END;


cond := true;

ctrue := sleep(2001) : independent;
cfalse := display(0);

b := ctrue : independent;
d := display(2) : independent;
b2 := SEQUENTIAL(sleep(2000),IF(NOFOLD(cond), ctrue, cfalse),b,d);

c10 := sleep(3000) : independent;
c11 := display(1) : independent;
c1 := SEQUENTIAL(c10, c11);

c0 := display(0) : independent;

//use sequential to get a consistent order in output. This can be changed once the parallel code is merged
SEQUENTIAL(c0,c1,b2);
//IF(optParallel, PARALLEL(c0,c1,b2), SEQUENTIAL(c0,c1,b2));
