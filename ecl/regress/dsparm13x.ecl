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

rec := record
  string1 n1x;
  string1 n2;
  string1 n3x;
  string1 n4;
  string1 n5x;
  string1 n6;
end;

dd := dataset('dd',rec,flat);

r1 := record  string1 n1;  string1 n2; string1 n3;  string1 n4; end;
r2 := record  string1 n1;  string1 n2; string1 n5;  string1 n6; end;

f := dd(n1x>'A',n2>'B',n3x='C',n4='D');

g := f(n1x<'E',n2<'F',n5x='G',n6='H');

output(g);