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

//class=index
//version multiPart=false
//version multiPart=true
//version multiPart=true,useLocal=true
//version multiPart=false,useLocal=true,forceRemoteKeyedLookup=true,forceRemoteKeyedFetch=true
//version multiPart=true,useLocal=true,forceRemoteKeyedLookup=true,forceRemoteKeyedFetch=true

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);
useTranslation := false;    // keyed limits do not produce the same results.
forceRemoteKeyedLookup := #IFDEFINED(root.forceRemoteKeyedLookup, false);
forceRemoteKeyedFetch := #IFDEFINED(root.forceRemoteKeyedLookup, false);


//--- end of version configuration ---

#option('forceRemoteKeyedLookup', forceRemoteKeyedLookup);
#option('forceRemoteKeyedFetch', forceRemoteKeyedFetch);


#onwarning (5402, ignore);

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);

IMPORT Std;

// try it with just one limit

o3 := output(LIMIT(Files.DG_FetchIndex(Lname='Anderson'),1,SKIP,KEYED), {Lname,Fname,TRIM(tfn),state,TRIM(blobfield)});
o4 := output(LIMIT(Files.DG_FetchIndex(Lname='Anderson'),10,SKIP,KEYED), {Lname,Fname,TRIM(tfn),state,TRIM(blobfield)});

iresult := Files.DG_FetchIndex(Lname IN ['Anderson', 'Taylor']);

lkresult := LIMIT(iresult,10,KEYED);
lsresult := LIMIT(lkresult,10,SKIP);
sresult := IF(useTranslation OR Std.System.Thorlib.platform() != 'hthor', SORT(lsresult,Lname), lsresult);
o5 := output(sresult, {Lname,Fname,TRIM(tfn),state,TRIM(blobfield)});

// then try with a keyed and unkeyed....

o6 := output(LIMIT(LIMIT(Files.DG_FetchIndex(Lname='Anderson'),1,SKIP,keyed),1,skip), {Lname,Fname,TRIM(tfn),state,TRIM(blobfield)});
o7 := output(LIMIT(LIMIT(Files.DG_FetchIndex(Lname='Anderson'),10,SKIP,keyed),10,skip), {Lname,Fname,TRIM(tfn),state,TRIM(blobfield)});

 o3:independent;
 o4:independent;
 o5:independent;
 o6:independent;
 o7:independent;
