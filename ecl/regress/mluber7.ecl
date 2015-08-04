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

import text;

rULE WORD := text.alpha+;
RULE Article := 'The' | 'A' | 'An';
RULE NounPhraseComponent := (Word penalty(1)) | (article Text.ws Word);

rs :=
RECORD
STRING100 line;
END;

ds := DATASET([{'The Fox and The Hen'}], rs);

Matches :=
RECORD
match := MATCHTEXT(NounPhraseComponent);
END;

ret1 := PARSE(ds, line, NounPhraseComponent, Matches, BEST, MANY, SCAN ALL, NOCASE);
ret2 := PARSE(ds, line, NounPhraseComponent, Matches, BEST, MANY, SCAN, NOCASE);
ret3 := PARSE(ds, line, NounPhraseComponent, Matches, BEST, MANY, NOCASE);
ret4 := PARSE(ds, line, NounPhraseComponent, Matches, BEST, MANY, MAX);
ret5 := PARSE(ds, line, NounPhraseComponent, Matches, BEST, MANY);

output(ret1);
output(ret2);
output(ret3);
output(ret4);
output(ret5);
