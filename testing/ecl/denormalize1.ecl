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

rec := RECORD
    STRING5 name;
    INTEGER2 i;
END;

rec zero(rec l) := TRANSFORM
    SELF.i := 0;
    SELF := l;
END;

rec sumi(rec l, rec r) := TRANSFORM
    SELF.i := l.i + r.i;
    SELF := l;
END;

raw := DATASET([{'adam', 23}, {'eve', 26}, {'adam', 9}, {'eve', 19}, {'adam', 0}
, {'eve', 10}, {'adam', 4}, {'eve', 10}], rec);

inp := GROUP(SORT(raw, name, i), name);

names := PROJECT(DEDUP(inp, name), zero(LEFT));

denorm := DENORMALIZE(names, inp, (LEFT.name = RIGHT.name) AND (LEFT.i < 30), 
sumi(LEFT, RIGHT));

OUTPUT(denorm);

