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

r := RECORD
 unsigned4 flags;
END;

ds := DATASET('test',r,FLAT);

// legal
r tranx0(r p1, r p2) :=
TRANSFORM
    SELF := p1;
END;

rst0 := ITERATE(ds,tranx0(LEFT,RIGHT));
//errors
rst0x := ITERATE(ds,tranx0(RIGHT,LEFT));
rst0y := ITERATE(ds,tranx0(LEFT));
rst0z := ITERATE(ds,tranx0(LEFT,RIGHT,RIGHT));
rst0w := ITERATE(ds,tranx0(ds,RIGHT));
rst0v := ITERATE(ds,tranx0(LEFT,LEFT));
rst0u := ITERATE(ds,tranx0(LEFT,ds));

// legal
r tranx1(r p1) :=
TRANSFORM
    SELF := p1;
END;

rst1 := ITERATE(ds,tranx1(LEFT));
// errors
rst1x := ITERATE(ds,tranx1(RIGHT));
rst1y := ITERATE(ds,tranx1(ds));
rst1z := ITERATE(ds,tranx1(LEFT,RIGHT));


// illegal
r tranx2(r p1,r p2, r p3) :=
TRANSFORM
    SELF := p1;
END;

rst2 := ITERATE(ds,tranx2(LEFT));
rst2x := ITERATE(ds,tranx2(LEFT,RIGHT,ds));


