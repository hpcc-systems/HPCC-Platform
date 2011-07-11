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

//UseStandardFiles
// NOTE - uses sequential as otherwise we use too many threads (allegedly)

// Try some remote activities reading from normal indexes and local indexes

index_best := topn(DG_FetchIndex1(LName[1]='A'), 3, LName, FName);
index_best_all := allnodes(index_best);
o1 := output(DEDUP(sort(index_best_all, LName, FName),LName, FName, KEEP(2)), {LName, FName});

index_best_all_local := allnodes(LOCAL(index_best));
o2 := output(sort(index_best_all_local, LName, FName), {LName, FName});

// Now try with disk files

disk_best := topn(DG_FetchFile(LName[1]='A'), 3, LName, FName);
disk_best_all := allnodes(disk_best);
o3 := output(DEDUP(sort(disk_best_all, LName, FName), LName, FName, KEEP(2)), {LName, FName});

disk_best_all_local := allnodes(LOCAL(disk_best));
o4 := output(sort(disk_best_all_local, LName, FName), {LName, FName});

// Now try with in-memory files

preload_best := topn(DG_FetchFilePreload(LName[1]='A'), 3, LName, FName);
preload_best_all := allnodes(preload_best);
o5 := output(DEDUP(sort(preload_best_all, LName, FName), LName, FName, KEEP(2)), {LName, FName});

preload_best_all_local := allnodes(LOCAL(preload_best));
o6 := output(sort(preload_best_all_local, LName, FName), {LName, FName});

// Now try with keyed in-memory files

preload_indexed_best := topn(DG_FetchFilePreloadIndexed(KEYED(LName[1]='A')), 3, LName, FName);
preload_indexed_best_all := allnodes(preload_indexed_best);
o7 := output(DEDUP(sort(preload_indexed_best_all, LName, FName), LName, FName, KEEP(2)), {LName, FName});

preload_indexed_best_all_local := allnodes(LOCAL(preload_indexed_best));
o8 := output(sort(preload_indexed_best_all_local, LName, FName), {LName, FName});


SEQUENTIAL(o1,o2,o3,o4,o5,o6,o7,o8);
