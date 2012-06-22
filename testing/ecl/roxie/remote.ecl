/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

IMPORT Std.system.thorlib as thorlib;

//UseStandardFiles
// NOTE - uses sequential as otherwise we use too many threads (allegedly)

// Try some remote activities reading from normal indexes and local indexes

index_best := topn(LIMIT(DG_FetchIndex1(LName[1]='A'),1000000), 3, LName, FName);
index_best_all := allnodes(index_best);
s1 := sort(index_best_all, LName, FName);
count(s1) / thorlib.nodes();
o1 := output(DEDUP(s1,LName, FName, KEEP(1)), {LName, FName});

index_best_all_local := allnodes(LOCAL(index_best));
s2 := sort(index_best_all_local, LName, FName);
count(s2) / thorlib.nodes();
o2 := output(DEDUP(s2,LName, FName, KEEP(1)), {LName, FName});

// Now try with disk files

disk_best := topn(DG_FetchFile(LName[1]='A'), 3, LName, FName);
disk_best_all := allnodes(disk_best);
s3 := sort(disk_best_all, LName, FName);
count(s3) / thorlib.nodes();
o3 := output(DEDUP(s3,LName, FName, KEEP(1)), {LName, FName});

disk_best_all_local := allnodes(LOCAL(disk_best));
s4 := sort(disk_best_all_local, LName, FName);
count(s4) / thorlib.nodes();
o4 := output(DEDUP(s4,LName, FName, KEEP(1)), {LName, FName});

// Now try with in-memory files

preload_best := topn(DG_FetchFilePreload(LName[1]='A'), 3, LName, FName);
preload_best_all := allnodes(preload_best);
s5 := sort(preload_best_all, LName, FName);
count(s5) / thorlib.nodes();
o5 := output(DEDUP(s5,LName, FName, KEEP(1)), {LName, FName});

preload_best_all_local := allnodes(LOCAL(preload_best));
s6 := sort(preload_best_all_local, LName, FName);
count(s6) / thorlib.nodes();
o6 := output(DEDUP(s6,LName, FName, KEEP(1)), {LName, FName});

// Now try with keyed in-memory files

preload_indexed_best := topn(DG_FetchFilePreloadIndexed(KEYED(LName[1]='A')), 3, LName, FName);
preload_indexed_best_all := allnodes(preload_indexed_best);
s7 := sort(preload_indexed_best_all, LName, FName);
count(s7) / thorlib.nodes();
o7 := output(DEDUP(s7,LName, FName, KEEP(1)), {LName, FName});

preload_indexed_best_all_local := allnodes(LOCAL(preload_indexed_best));
s8 := sort(preload_indexed_best_all_local, LName, FName);
count(s8) / thorlib.nodes();
o8 := output(DEDUP(s8,LName, FName, KEEP(1)), {LName, FName});


SEQUENTIAL(o1,o2,o3,o4,o5,o6,o7,o8);
