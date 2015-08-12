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

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);
useTranslation := #IFDEFINED(root.useTranslation, false);

//--- end of version configuration ---

import $.setup;
import setup.TS;
Files := setup.Files(multiPart, useLocal, useTranslation);

//Multi level smart stepping, with priorities in the correct order

words := DATASET(['a','the'], { string searchWord; });

p(STRING searchWord) := FUNCTION
    TS_searchIndex := Files.getSearchIndex();
    i2 := STEPPED(TS_searchIndex(kind=1 AND word=searchWord), doc, PRIORITY(3),HINT(maxseeklookahead(50)));
    i1 := STEPPED(TS_searchIndex(kind=1 AND word='walls'), doc, PRIORITY(2),HINT(maxseeklookahead(50)));

    j1 := MERGEJOIN([i1, i2], STEPPED(LEFT.doc =RIGHT.doc ), SORTED(doc));
    RETURN TABLE(j1, {src := TS.docid2source(doc); UNSIGNED doc := TS.docid2doc(doc), cnt := COUNT(GROUP)},doc);
END;


OUTPUT(NOFOLD(words), { COUNT(p(searchWord)); });
