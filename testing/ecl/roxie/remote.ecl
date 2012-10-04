/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

IMPORT Std.system.thorlib as thorlib;

//UseStandardFiles
// NOTE - uses sequential as otherwise we use too many threads (allegedly)

// Try some remote activities reading from normal indexes and local indexes

index_best := SORT(LIMIT(DG_FetchIndex1(LName[1]='A'),1000000), LName, FName);
index_best_all := allnodes(index_best);
s1 := sort(index_best_all, LName, FName);
count(s1) / thorlib.nodes();  // Every node should return all matching results
o1 := output(DEDUP(s1,LName, FName, KEEP(1)), {LName, FName});

index_best_all_local := allnodes(LOCAL(index_best));
s2 := sort(index_best_all_local, LName, FName);
count(s2); // Every node should return only local matching results
o2 := output(DEDUP(s2,LName, FName, KEEP(1)), {LName, FName});

// Now try with disk files

disk_best := SORT(DG_FetchFile(LName[1]='A'), LName, FName);
disk_best_all := allnodes(disk_best);
s3 := sort(disk_best_all, LName, FName);
count(s3) / thorlib.nodes();    // Every node should return all matching results
o3 := output(DEDUP(s3,LName, FName, KEEP(1)), {LName, FName});

disk_best_all_local := allnodes(LOCAL(disk_best));
s4 := sort(disk_best_all_local, LName, FName);
count(s4);  // Every node should return only local matching results
o4 := output(DEDUP(s4,LName, FName, KEEP(1)), {LName, FName});

// Now try with in-memory files

preload_best := sort(DG_FetchFilePreload(LName[1]='A'), LName, FName);
preload_best_all := allnodes(preload_best);
s5 := sort(preload_best_all, LName, FName);
count(s5) / thorlib.nodes();  // Every node should return all matching results
o5 := output(DEDUP(s5,LName, FName, KEEP(1)), {LName, FName});

preload_best_all_local := allnodes(LOCAL(preload_best));
s6 := sort(preload_best_all_local, LName, FName);
count(s6);  // Every node should return only local matching results
o6 := output(DEDUP(s6,LName, FName, KEEP(1)), {LName, FName});

// Now try with keyed in-memory files

preload_indexed_best := SORT(DG_FetchFilePreloadIndexed(KEYED(LName[1]='A')), LName, FName);
preload_indexed_best_all := allnodes(preload_indexed_best);
s7 := sort(preload_indexed_best_all, LName, FName);
count(s7) / thorlib.nodes();  // Every node should return all matching results
o7 := output(DEDUP(s7,LName, FName, KEEP(1)), {LName, FName});

preload_indexed_best_all_local := allnodes(LOCAL(preload_indexed_best));
s8 := sort(preload_indexed_best_all_local, LName, FName);
count(s8);   // Every node should return only local matching results
o8 := output(DEDUP(s8,LName, FName, KEEP(1)), {LName, FName});


SEQUENTIAL(o1,o2,o3,o4,o5,o6,o7,o8);
