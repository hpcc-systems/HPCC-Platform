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

myrec := record
 unsigned6 did;
 string1 let;
end;

myoutrec := record
 unsigned6 did;
 string1 letL;
 string1 letR;
end;

d1 := dataset([{1234,'A'},{5678,'B'}],myrec);
d2 := dataset([{1234,'A'},{5678,'B'},{9876,'C'}],myrec);
 
myoutrec testLookup(d2 L, d1 r) := transform
 self.did := l.did;
 self.letL := l.let;
 self.letR := r.let;
end;
 
j1 := join(d2,d1,left.did=right.did,testLookup(left, right),all);
j2 := join(d2,d1,left.did=right.did,testLookup(left, right),all, LEFT OUTER);
j3 := join(d2,d1,left.did=right.did,testLookup(left, right),all, LEFT ONLY);
 
output(j1);
output(j2);
output(j3);