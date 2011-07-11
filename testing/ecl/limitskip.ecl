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

irec := RECORD
    UNSIGNED1 i;
END;

ijrec := RECORD
    UNSIGNED i;
    UNSIGNED j;
END;

ijrec xfm(irec l, irec r) := TRANSFORM
    SELF.j := r.i;
    SELF := l;
END;

vector := DATASET([{1}, {2}, {3}, {4}, {5}], irec);

matrix := JOIN(vector, vector, LEFT.i <= RIGHT.i, xfm(LEFT, RIGHT), ALL);

grpd := GROUP(SORT(matrix, j), j);

lim1 := LIMIT(grpd, 15, SKIP);
lim2 := LIMIT(grpd, 14, SKIP);
lim3 := LIMIT(grpd, 0, SKIP);

OUTPUT(SORT(lim1, -i));
OUTPUT(SORT(lim2, -i));
OUTPUT(SORT(lim3, -i));

ijrec createError := TRANSFORM
    SELF.i := 99999;
    SELF.j := 99999;
END;


lim4 := LIMIT(grpd, 15, ONFAIL(createError));
lim5 := LIMIT(grpd, 14, ONFAIL(createError));
lim6 := LIMIT(grpd, 0, ONFAIL(createError));

OUTPUT(SORT(lim4, -i));
OUTPUT(SORT(lim5, -i));
OUTPUT(SORT(lim6, -i));

ijrec noCreateError := TRANSFORM,skip(true)
    SELF.i := 99999;
    SELF.j := 99999;
END;


lim7 := LIMIT(grpd, 15, ONFAIL(noCreateError));
lim8 := LIMIT(grpd, 14, ONFAIL(noCreateError));
lim9 := LIMIT(grpd, 0, ONFAIL(noCreateError));

OUTPUT(SORT(lim7, -i));
OUTPUT(SORT(lim8, -i));
OUTPUT(SORT(lim9, -i));

