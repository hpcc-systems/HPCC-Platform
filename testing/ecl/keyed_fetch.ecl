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

//UseStandardFiles
//UseIndexes
IMPORT Std;

TYPEOF(DG_FetchFile) copy(DG_FetchFile l) := TRANSFORM
    SELF.__filepos := 0; // filepos is not consistent between different test modes, so suppress it from output
    SELF := l;
END;

TYPEOF(DG_FetchFile) maybesort(TYPEOF(DG_FetchFile) in) :=
#if (useLocal)
  SORT(in, fname, lname);
#else
  IF(Std.System.Thorlib.platform() = 'thor', SORT(in, fname, lname), in);
#end

output(maybesort(FETCH(DG_FetchFile, DG_FetchIndex1(Lname='Anderson'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(DG_FetchFile, DG_FetchIndex1(Lname='Johnson'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(DG_FetchFile, DG_FetchIndex1(Lname='Smith'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(DG_FetchFile, DG_FetchIndex1(Lname='Doe'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(DG_FetchFile, DG_FetchIndex1(Fname='Frank'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(DG_FetchFile, DG_FetchIndex1(Fname='Sue'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(DG_FetchFile, DG_FetchIndex1(Fname='Jane'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(DG_FetchFile, DG_FetchIndex1(Fname='Larry'),RIGHT.__filepos, copy(LEFT))));

