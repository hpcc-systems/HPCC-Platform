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
//version multiPart=true

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);
useTranslation := #IFDEFINED(root.useTranslation, false);

//--- end of version configuration ---

#option ('layoutTranslationEnabled', useTranslation);

import $.setup;
import setup.TS;
Files := setup.Files(multiPart, useLocal, useTranslation);

in := dataset([{'boy'}, {'sheep'}], {string5 word});

outrec := record
  string5 word;
  unsigned doc;
END;

outrec trans1(in L) := TRANSFORM
  SELF.word := L.word;
  SELF.doc := SORTED(Files.getSearchSuperIndex()(KEYED(kind = TS.kindType.TextEntry),keyed(word = l.word)), doc)[1].doc;
END;

outrec trans2(in L) := TRANSFORM
  SELF.word := L.word;
  SELF.doc := SORTED(Files.getSearchSuperIndex()(KEYED(kind = TS.kindType.TextEntry),keyed(word = 'gobbledegook'+l.word)), doc)[1].doc;
END;

outrec trans3(in L) := TRANSFORM
  SELF.word := L.word;
  SELF.doc := STEPPED(Files.getSearchSuperIndex()(KEYED(kind = TS.kindType.TextEntry),keyed(word = l.word)), doc)[1].doc;
END;


output(project(in, trans1(LEFT))) : independent;
output(project(in, trans1(LEFT), PREFETCH(20))) : independent;
output(project(in, trans2(LEFT))) : independent;
output(project(in, trans2(LEFT), PREFETCH(20, PARALLEL))) : independent;
output(project(in, trans3(LEFT))) : independent;
output(project(in, trans3(LEFT), PREFETCH(20, PARALLEL))) : independent;
