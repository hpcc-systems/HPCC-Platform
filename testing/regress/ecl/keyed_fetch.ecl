/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

//class=file
//class=index
//version multiPart=false
//version multiPart=true
//version multiPart=true,useLocal=true
//version multiPart=false,useLocal=true,forceRemoteKeyedLookup=true,forceRemoteKeyedFetch=true
//version multiPart=true,useLocal=true,forceRemoteKeyedLookup=true,forceRemoteKeyedFetch=true

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);
useTranslation := #IFDEFINED(root.useTranslation, false);
forceRemoteKeyedLookup := #IFDEFINED(root.forceRemoteKeyedLookup, false);
forceRemoteKeyedFetch := #IFDEFINED(root.forceRemoteKeyedLookup, false);


//--- end of version configuration ---

#option('forceRemoteKeyedLookup', forceRemoteKeyedLookup);
#option('forceRemoteKeyedFetch', forceRemoteKeyedFetch);
#option ('layoutTranslationEnabled', useTranslation);
#onwarning (4515, ignore);
#onwarning (4523, ignore);

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);

IMPORT Std;

TYPEOF(Files.DG_FetchFile) copy(Files.DG_FetchFile l) := TRANSFORM
    SELF.__filepos := 0; // filepos is not consistent between different test modes, so suppress it from output
    SELF := l;
END;

TYPEOF(Files.DG_FetchFile) maybesort(TYPEOF(Files.DG_FetchFile) in) :=
#if (useLocal)
  SORT(in, fname, lname);
#else
  IF(Std.System.Thorlib.platform() = 'thor', SORT(in, fname, lname), in);
#end

output(maybesort(FETCH(Files.DG_FetchFile, Files.DG_FetchIndex(Lname='Anderson'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(Files.DG_FetchFile, Files.DG_FetchIndex(Lname='Johnson'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(Files.DG_FetchFile, Files.DG_FetchIndex(Lname='Smith'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(Files.DG_FetchFile, Files.DG_FetchIndex(Lname='Doe'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(Files.DG_FetchFile, Files.DG_FetchIndex(Fname='Frank'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(Files.DG_FetchFile, Files.DG_FetchIndex(Fname='Sue'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(Files.DG_FetchFile, Files.DG_FetchIndex(Fname='Jane'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(Files.DG_FetchFile, Files.DG_FetchIndex(Fname='Larry'),RIGHT.__filepos, copy(LEFT))));

