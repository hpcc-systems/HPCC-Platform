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
//noversion multiPart=true,useTranslation=true,nothor

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);
useTranslation := #IFDEFINED(root.useTranslation, false);

//--- end of version configuration ---

#option ('layoutTranslationEnabled', useTranslation);
#onwarning (4522, ignore);
#onwarning (5402, ignore);

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);

lhs := DATASET([{['Anderson', 'Taylor']}], {SET OF STRING25 Lnames{MAXLENGTH(100)}});

{STRING15 Fname, string15 LName} xfm(Files.DG_FetchIndex r) := TRANSFORM
    SELF.Fname := r.Fname;
    SELF.Lname := r.Lname;
END;

j1 := JOIN(lhs, Files.DG_FetchIndex, RIGHT.Lname IN LEFT.Lnames, xfm(RIGHT));

#if (useLocal OR useTranslation)
OUTPUT(SORT(j1, lname, fname), {fname});
#else
OUTPUT(j1, {fname});
#end
