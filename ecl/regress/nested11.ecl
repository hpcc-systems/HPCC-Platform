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

t1(unsigned a) := transform(level1Rec, self.a := a-1; self.b := a; self.c := a+1;);

level2Rec t2() := transform
    SELF.a := IF(random() = 1, row(t1(10)), row(t1(20)));
    SELF.b := iF(random() = 3, row(t1(9)), row(t1(12)));
    SELF.c := iF(random() = 8, row(t1(9)), row(t1(12)));
END;


ds := DATASET('ds', level3Rec, thor);

level3Rec tx(level3Rec l) := transform
    self.c := row(t2());
    self := l;
END;

p := project(ds, tx(LEFT));

f := p(a.b.c != 10);

//prevent a compound disk operation
d := dedup(f, c.b.a);
output(d, { b.a });
