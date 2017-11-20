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
#onwarning (5402, ignore);

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);


recordof(Files.DG_FetchIndex) createError(boolean isKeyed) := TRANSFORM
    SELF.fname := IF(isKeyed, 'Keyed Limit exceeded', 'Limit exceeded');
    SELF := [];
END;

//Use different filters to ensure the index reads aren't commoned up - and the limit is done within the index read activity

// try it with just one limit
output(LIMIT(Files.DG_FetchIndex(Lname='Anderson',fname<>'nomatch1'),1,SKIP), {fname});
output(LIMIT(Files.DG_FetchIndex(Lname='Anderson',fname<>'nomatch2'),10,SKIP), {fname});
output(LIMIT(Files.DG_FetchIndex(Lname='Anderson',fname<>'nomatch3'),1,SKIP,KEYED), {fname});
output(LIMIT(Files.DG_FetchIndex(Lname='Anderson',fname<>'nomatch4'),10,SKIP,KEYED), {fname});

// then try with a keyed and unkeyed....

output(LIMIT(LIMIT(Files.DG_FetchIndex(Lname='Anderson',fname<>'nomatch5'),1,SKIP,keyed),1,skip), {fname});
output(LIMIT(LIMIT(Files.DG_FetchIndex(Lname='Anderson',fname<>'nomatch6'),10,SKIP,keyed),10,skip), {fname});


// try it with just one limit

output(LIMIT(Files.DG_FetchIndex(Lname='Anderson',fname<>'nomatch8'),1,ONFAIL(createError(false))), {fname});
output(LIMIT(Files.DG_FetchIndex(Lname='Anderson',fname<>'nomatch9'),10,ONFAIL(createError(false))), {fname});
output(LIMIT(Files.DG_FetchIndex(Lname='Anderson',fname<>'nomatch10'),1,ONFAIL(createError(true)),KEYED), {fname});
output(LIMIT(Files.DG_FetchIndex(Lname='Anderson',fname<>'nomatch11'),10,ONFAIL(createError(true)),KEYED), {fname});

// then try with a keyed and unkeyed....

output(LIMIT(LIMIT(Files.DG_FetchIndex(Lname='Anderson',fname<>'nomatch12'),1,ONFAIL(createError(true)),keyed),1,ONFAIL(createError(false))), {fname});
output(LIMIT(LIMIT(Files.DG_FetchIndex(Lname='Anderson',fname<>'nomatch13'),10,ONFAIL(createError(true)),keyed),10,ONFAIL(createError(false))), {fname});

