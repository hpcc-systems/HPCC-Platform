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

myrec := RECORD
    UNSIGNED6 did;
    STRING1 let;
END;

myoutrec := RECORD
    UNSIGNED6 did;
    STRING1 letL;
    STRING1 letR;
    STRING40 label;
END;

lhs := DATASET([{1234,'A'}, {5678,'B'}, {9876,'C'}], myrec);
rhs := DATASET([{1234,'A'}, {5678,'B'}, {1234,'D'}, {1234,'E'}, {9999,'F'}], myrec);
 
myoutrec xfm(lhs l, rhs r, STRING40 lab) := TRANSFORM
    self.did := l.did;
    self.letL := l.let;
    self.letR := r.let;
    self.label := lab;
END;
 
j1 := JOIN(lhs, rhs, LEFT.did=RIGHT.did, xfm(LEFT,  RIGHT, 'RIGHT OUTER'), RIGHT OUTER);
j2 := JOIN(lhs, rhs, LEFT.did=RIGHT.did, xfm(LEFT,  RIGHT, 'RIGHT OUTER, LIMIT(SKIP)'), RIGHT OUTER, LIMIT(2, SKIP));
j3 := JOIN(lhs, rhs, LEFT.did=RIGHT.did, xfm(LEFT,  RIGHT, 'RIGHT OUTER, LIMIT, ONFAIL'), RIGHT OUTER, LIMIT(2), ONFAIL(xfm(LEFT, RIGHT, 'FAILED: RIGHT OUTER, LIMIT, ONFAIL')));
 
output(j1);
output(j2);
output(j3);
