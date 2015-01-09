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

//class=textsearch
//version multiPart=false
//version multiPart=true

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, false);

//--- end of version configuration ---


import $.Setup;
import $.Setup.TS;
searchIndex := Setup.Files(multiPart, false).getSearchIndex();

//Multi level smart stepping, with priorities in the correct order

i1 := STEPPED(searchIndex(kind=1 AND word='the'), doc, PRIORITY(3),HINT(maxseeklookahead(50)));
i2 := STEPPED(searchIndex(kind=1 AND word='walls'), doc, PRIORITY(2),HINT(maxseeklookahead(50)));
i3 := STEPPED(searchIndex(kind=1 AND word='jericho'), doc, PRIORITY(2),HINT(maxseeklookahead(50)));

output(COUNT(TABLE(i1, { src := TS.docid2source(doc); UNSIGNED doc := TS.docid2doc(doc), cnt := COUNT(GROUP)},doc)));
output(COUNT(TABLE(i2, { src := TS.docid2source(doc); UNSIGNED doc := TS.docid2doc(doc), cnt := COUNT(GROUP)},doc)));
output(COUNT(TABLE(i3, { src := TS.docid2source(doc); UNSIGNED doc := TS.docid2doc(doc), cnt := COUNT(GROUP)},doc)));
