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

#option ('globalFold', false);
//The following should be true:

[1]=[1];
[]=[];
[1]>[];
[1,2]>[1];
[2]>[1,2];

['Gavin','Halliday']>=['Elizabeth','Halliday'];
['Gavin','Halliday']>=['Gavin   ','Hall'];

[1]=[1];
[]<[1];
[1]<[1,2];
[1,2]<[2];

boolean z(set of integer a) := a >= [1,2,3];
boolean y(set of unsigned4 b) := z(b);

y([1,2,3]);
y([1,2,4]);

integer c2(set of integer a, set of integer b) := (a <=> b);
integer setcompare(set of unsigned4 a, set of unsigned4 b) := c2(a, b);


setcompare([1,2,3],[1,2,3]) = 0;
setcompare([],[]) = 0;
setcompare([1,2],[1,2,3]) < 0;
setcompare([1,2,2],[1,2,3]) < 0;
setcompare([1,2,3],[1,2]) > 0;
setcompare([1,2,4],[1,2,3]) > 0;


//The following should be false:

[1]=[2];
[1]=[1,2];
[2,1]=[2];
[1]<[];
[1,2]<[1];
[2]<[1,2];

3 <=> 4;
