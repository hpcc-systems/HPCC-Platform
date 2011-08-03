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

Layout_inner :=
RECORD
    INTEGER i;
END;

Layout_outer :=
RECORD
    INTEGER seq;
    Layout_inner is;
END;

Layout_in :=
RECORD
    INTEGER seq;
    INTEGER i;
END;

d_in := dataset([{1, 1}, {1, 2}, {1, 3}, {2, 1}, {3, 2}, {3, 3}], Layout_in);
output(d_in);

// We want to denormalize some incoming dataset into a
// sequence number and child records
blank_inner := dataset([{0}], Layout_inner)(false);

// first project into Layout_outer
Layout_outer to_outer(Layout_in le) :=
TRANSFORM
    SELF.seq := le.seq;
    SELF.is := ROW({0}, layout_inner);
END;

p := dedup(PROJECT(d_in, to_outer(LEFT)), seq, ALL);

// then denorm
Layout_outer denorm(Layout_outer le, Layout_in ri, INTEGER c) :=
TRANSFORM
    SELF.seq := le.seq;
    SELF.is := ROW({ri.i}, layout_inner);
END;
d := DENORMALIZE(p, d_in, LEFT.seq = RIGHT.seq, denorm(LEFT, RIGHT, COUNTER));

output(d);
