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

rs :=
RECORD
    STRING line;
END;

ds := DATASET([{'Mr Marcus was here!'}, {'MR Marcus'}, {'mr Marcus'}], rs);

PATTERN salutation := NOCASE(['Mr', 'Mrs']);
PATTERN per := salutation Text.ws Text.alpha+;

PATTERN wrd := Text.alpha+;
PATTERN beginning := FIRST | Text.ws;
PATTERN ending := LAST | Text.ws;
PATTERN noSal := ((wrd NOT IN salutation) AFTER beginning) BEFORE ending;

matchRec2 :=
RECORD
    matching := MATCHTEXT(noSal);
END;
ParseIt2 := PARSE(ds, line, noSal, matchRec2, MAX, MANY);
output(ParseIt2);