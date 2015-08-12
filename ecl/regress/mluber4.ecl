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

export PATTERN AlphaUpper := PATTERN('[A-Z]');
export PATTERN AlphaLower := PATTERN('[a-z]');
export pattern Alpha := PATTERN('[A-Za-z]');
export pattern ws := [' ','\t','\n']+;

RULE WORD := alpha+;
RULE Article := 'The' | 'A' | 'An';
RULE NounPhraseComponent := (Word penalty(1)) | (article ws Word);

rs :=
        RECORD
STRING100   line;
        END;

ds := DATASET([{'The Fox and The Pigeon danced on the roof'}], rs);

Matches :=
        RECORD
            match := MATCHTEXT(NounPhraseComponent);
        END;

ret := PARSE(ds, line, NounPhraseComponent, Matches, SCAN, BEST, MANY, NOCASE);
output(ret);
