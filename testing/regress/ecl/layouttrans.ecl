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

//version multiPart=true,version=1
//version multiPart=true,version=2
//version multiPart=true,version=3

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);
version := #IFDEFINED(root.version, 1);

//--- end of version configuration ---

#option ('layoutTranslation', 'payload');
#onwarning (4515, ignore);
#onwarning (4522, ignore);
#onwarning (4523, ignore);
#onwarning (5402, ignore);

import $.setup;
Files := setup.Files(multiPart, useLocal, false);

#IF (version=1)
DG_FetchIndex1Alt1 := INDEX(Files.DG_FetchFile,{Lname,Fname,__filepos},Files.DG_FetchIndex1Name);
DG_FetchIndex1Alt2 := INDEX(Files.DG_FetchFile,{Lname,Fname,__filepos},Files.DG_FetchIndex1Name);
#ELIF (version=2)
DG_FetchIndex1Alt1 := INDEX(Files.DG_FetchFile,{Lname,Fname},{state, STRING100 blobfield {blob}:= fname, STRING tfn := TRIM(Fname), __filepos},Files.DG_FetchIndex1Name);
DG_FetchIndex1Alt2 := INDEX(Files.DG_FetchFile,{Lname,Fname},{ STRING100 blobfield {blob}:= fname, __filepos},Files.DG_FetchIndex1Name);
#ELSE
DG_FetchIndex1Alt1 := INDEX(Files.DG_FetchFile,{Lname,Fname},{state ,__filepos},Files.DG_FetchIndex1Name);
DG_FetchIndex1Alt2 := INDEX(Files.DG_FetchFile,{Lname,Fname},{__filepos},Files.DG_FetchIndex1Name);
#END
DG_FetchIndex1Alt3 := INDEX(Files.DG_FetchFile,{Lname,Fname},{varstring state := '', string2 newstate := 'UK', string newfield { default('new')}},Files.DG_FetchIndex1Name); // Note - the := 'UK' would only take effect at index build time, it does not set a default for the field when reading

ds := DATASET([{'Anderson'}, {'Doe'}], {STRING25 Lname});

SEQUENTIAL(
    OUTPUT(SORT(Files.DG_FetchIndex1(Lname = 'Smith'), record), {Fname, Lname}),
    OUTPUT(SORT(DG_FetchIndex1Alt1(Lname = 'Smith'), record), {Fname, Lname}),
    OUTPUT(SORT(DG_FetchIndex1Alt2(Lname = 'Smith'), record), {Fname, Lname}),
    OUTPUT(SORT(DG_FetchIndex1Alt3(Lname = 'Smith'), record), {Fname, Lname, state, newstate, newfield}),
    OUTPUT(SORT(Files.DG_FetchIndex1((Lname = 'Smith') AND (Fname >= 'Z')), record), {Fname, Lname}),
    OUTPUT(SORT(DG_FetchIndex1Alt1((Lname = 'Smith') AND (Fname >= 'Z')), record), {Fname, Lname}),
    OUTPUT(SORT(DG_FetchIndex1Alt2((Lname = 'Smith') AND (Fname >= 'Z')), record), {Fname, Lname}),
    OUTPUT(SORT(DG_FetchIndex1Alt3(Lname = 'Smith' AND (Fname >= 'Z')), record), {Fname, Lname, state, newstate, newfield}),
    OUTPUT(SORT(JOIN(ds, Files.DG_FetchIndex1, LEFT.Lname = RIGHT.Lname), record), {Fname, Lname}),
    OUTPUT(SORT(JOIN(ds, DG_FetchIndex1Alt1, LEFT.Lname = RIGHT.Lname), record), {Fname, Lname}),
    OUTPUT(SORT(JOIN(ds, DG_FetchIndex1Alt2, LEFT.Lname = RIGHT.Lname), record), {Fname, Lname}),
    OUTPUT(SORT(JOIN(ds, DG_FetchIndex1Alt3, LEFT.Lname = RIGHT.Lname), record), {Fname, Lname, state, newstate, newfield})
);
