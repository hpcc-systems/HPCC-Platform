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
    level1Rec b;
    UNSIGNED a;
    level1Rec c;
END;

#option ('peephole', false);

unsigned times100(unsigned x) := function
    y := x;
    return y * 100;
END;

ds := DATASET('ds', level2Rec, thor);

level2Rec t(level2Rec l) := TRANSFORM
    level1Rec t2 := transform
        self.a := 100;
        self.b := times100(self.a);
        self.c := 12;
    END;
    SELF.b := row(t2);
    SELF.a := 999999;
    SELF.c.a := SELF.a*12;
    SELF := l;
END;

p := project(ds, t(LEFT));
output(p);
