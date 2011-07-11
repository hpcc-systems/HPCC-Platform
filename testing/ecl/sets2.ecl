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

#stored ('useX1', true);

set of integer x1 := [1, 2, 3, 4];
set of integer x2 := [5, 6, 7, 8];

boolean useX1 := false : stored('useX1');
integer search1 := 1    : stored('search1');
integer search2 := 5    : stored('search2');

set of integer x := if(useX1, x1, x2);

search1 in x;
search2 in x;


set of integer y1 := [1, 2, 3, 4] : stored('y1');
set of integer2 y2 := [5, 6, 7, 8] : stored('y2');

set of integer y := if(useX1, y1, y2);

search1 in y;
search2 in y;


set of string z1 := ['Gavin', 'Jason', 'Emma', 'Vicky'];
set of string z2 := ['Liz', 'Rochelle', '?', 'David'];

set of string z := if(useX1, z1, z2);

string ssearch1 := 'Gavin'  : stored('ssearch1');
string ssearch2 := 'Liz'    : stored('ssearch2');

ssearch1 in z;
ssearch2 in z;



set of integer5 ya1 := [1, 2, 3, 4];
set of integer5 ya2 := [5, 6, 7, 8] : stored('ya2');

set of integer ya := if(useX1, ya1, ya2);

search1 in ya;
search2 in ya;

output(y1);

set of integer2 x3 := (set of integer1)([500, 600, 700, 800]);
output(x3);
