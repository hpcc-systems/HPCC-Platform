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

level1Rec := RECORD
    UNSIGNED a;
    UNSIGNED b;
    UNSIGNED c;
END;

level2Rec := RECORD
    level1Rec a;
    level1Rec b;
    level1Rec c;
END;

level3Rec := RECORD
    UNSIGNED id;
    level2Rec a;
    level2Rec b;
    level2Rec c;
END;

ds := DATASET('ds', level3Rec, thor);

level3Rec t(level3Rec l, unsigned c) := TRANSFORM
    SELF.a.b.c := l.id;
    SELF.c.c.c := 1;
    SELF := l;
END;

p := PROJECT(NOFOLD(ds), t(LEFT, COUNTER));

f := p(a.b.c != 10);

output(count(dedup(f,a.b.c)));
