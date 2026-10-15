/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

#onwarning (4523, ignore);

//version childSinkOption='sequential'
//version childSinkOption='parallel'
//version childSinkOption='parallelPersistent'
//version childSinkOption='automatic'
//version childSinkOption='pooled'


import ^ as root;
multiPart := #IFDEFINED(root.multiPart, false);
useLocal := #IFDEFINED(root.useLocal, false);
useTranslation := #IFDEFINED(root.useTranslation, false);
sinkMode := #IFDEFINED(root.childSinkOption, 'pooled');

//--- end of version configuration ---

#option ('layoutTranslation', useTranslation);
#onwarning (5402, ignore);

import $.setup;
import $.setup.ts;

Files := setup.Files(multiPart, useLocal, useTranslation);



numInput := 1000;

r := { unsigned id; unsigned num; };

ds := DATASET(numInput, TRANSFORM(r, SELF.id := COUNTER; SELF.num := 0x1000000000000 + (COUNTER % 10) + 10), DISTRIBUTED);

r2 := RECORD(r)
    unsigned cnum;
END;

r2 createMatch(r l) := TRANSFORM
    matches(UNSIGNED id) := COUNT(NOCOMBINE(Files.getWordIndex()(keyed(kind = TS.kindType.TextEntry and word = 'boy' and doc = id))));
    matches5(UNSIGNED id) := matches(id) + matches(id+1) + matches(id+2) + matches(id+3) + matches(id+4);
    matches25(UNSIGNED id) := matches5(id) + matches5(id+5) + matches5(id+10) + matches5(id+15) + matches5(id+20);

    SELF.cnum := matches25(l.num) + matches25(l.num+25) + matches25(l.num+50) + matches25(l.num+75);
    SELF := l;
END;

output(COUNT(NOFOLD(PROJECT(ds, createMatch(LEFT),HINT(sinkmode(sinkMode))))));
