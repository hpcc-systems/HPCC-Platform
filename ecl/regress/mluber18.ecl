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

import Text;
ds := DATASET([{'hello my'}, {'name is ed'}], {STRING line});

PATTERN inPattern := (Text.alpha|'-')+;

outl :=
RECORD
    ds.line;
    STRING wrd;
END;

outl t(ds f) :=
TRANSFORM
    SELF.wrd := MATCHTEXT(inPattern);
    SELF.line := f.line;
END;
outp := PARSE(ds, line, inPattern, t(LEFT), MAX);
output(outp);
