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

x := 'on' : stored('x');

/*
y := dataset(x, { unsigned4 id, unsigned8 filepos{virtual(fileposition)} }, thor);

output(y);


z := index(y, { id, filepos }, 'ix' + x);

output(z(id > 10));


y2 := dataset('inxx'+, { unsigned4 id, unsigned8 filepos{virtual(fileposition)} }, thor);

output(y);


z := index(y, { id, filepos }, 'ix' + x);

output(z(id > 10));
*/

string2 previous_file := '' : stored('AccessPreviousFile');
boolean prev_file := previous_file = 'on';

export version :=
    if(prev_file,
        '20021031',
        '20021125');

infile := dataset('in', { unsigned4 id, unsigned8 filepos{virtual(fileposition)} }, thor);


y := dataset('zz', { unsigned4 id, unsigned8 filepos{virtual(fileposition)} }, thor);

z2 := index(y, { id, filepos }, 'ix' + version);

infile t(infile l) := transform
    self := l;
    end;

a := join(infile, y, left.id = right.id,t(left),keyed(z2));

output(a);
