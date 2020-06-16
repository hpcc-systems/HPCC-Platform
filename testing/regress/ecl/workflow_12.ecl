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
//version parallel=true,nothor,noroxie

import ^ as root;
optParallel := #IFDEFINED(root.parallel, false);

#option ('parallelWorkflow', optParallel);
#option('numWorkflowThreads', 5);

//graphs are going to run concurrently in eclagent/roxie. Roxie can be enabled once multi threading issues are resolved
MyRec := RECORD
    STRING1 Value1;
    STRING1 Value2;
    UNSIGNED1 Value3;
END;

SomeFile := DATASET([   {'C','G',1},
                        {'C','C',2},
                        {'A','X',3},
                        {'B','G',4},
                        {'A','B',5}],MyRec);

SortedTable1 := SORT(SomeFile,-Value1):independent; //sort in reverse order
SortedTable2 := SORT(SomeFile,Value1):independent;


OUTPUT(COUNT(NOFOLD(SortedTable1)) + COUNT(NOFOLD(SortedTable2)));
