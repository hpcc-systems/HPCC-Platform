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

display(REAL8 thisReal) := FUNCTION
  ds := dataset([thisReal], {REAL8 value});
  RETURN Output(ds, NAMED('logging'), EXTEND);
END;

//There are 3 'levels' to the tree. The middle two items each depend on the first. The last item depends on the middle two.
//f(x) ax^2 + bx + c = 0
// a(x-p)(x-q) = 0
//S := p+q = -b/a
//D := p-q
//P := pq = c/a

thisRecord := RECORD
    Integer8 a,
    Integer8 b,
    Integer8 c,
END;
f := row({5,15,3}, thisRecord) : independent;

S := -f.b/f.a;
Prod := f.c/f.a;
D := SQRT(S*S-4*Prod);

p := (S+D)/2 : independent;
q := (S-D)/2 : independent;

//Use sequential to get a consistent order in Output
sequential(Display(p), Display(q));
//Parallel(Display(p), Display(q));
