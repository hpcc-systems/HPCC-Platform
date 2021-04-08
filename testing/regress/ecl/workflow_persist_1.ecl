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
//version parallel=false,noroxie
//version parallel=true,noroxie

import ^ as root;
optParallel := #IFDEFINED(root.parallel, false);
import $.setup;
prefix := setup.Files(false, false).FilePrefix;

#option ('parallelWorkflow', optParallel);
#option('numWorkflowThreads', 5);

thisRecord := RECORD
    STRING6 name;
    INTEGER7 id;
END;

thisRecord thisTransform(INTEGER7 C) := TRANSFORM
    SELF.name := 'Nathan';
    SELF.id := C^3 % 1000000;
END;

dSet := DATASET(1000000, thisTransform(COUNTER)) : independent(many);

a1 := SORT(dSet, -id) : persist(prefix + 'sort1');

a2 := SORT(dSet, id) : persist(prefix + 'sort2');

PARALLEL(COUNT(NOFOLD(a1)), COUNT(NOFOLD(a2)));
