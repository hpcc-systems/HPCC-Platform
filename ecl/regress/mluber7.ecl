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
