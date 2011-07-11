/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
