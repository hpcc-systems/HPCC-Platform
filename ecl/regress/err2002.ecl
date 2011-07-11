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

WAIT('STR');
v1 := 10.2 % 11.2;
v2 := 10.2 DIV 11.2;
v3 := 10.2 | 11.2;
v4 := 10.2 & 11.2;
v5 := 10.2 ^ 11.2;
STRING s := 'This is a string. ';
s[0.23];
s[0.23..2.34];
s[..2.3];
s[2.3..];
INTFORMAT(1.2, 2.2, 3.3);
REALFORMAT(1.0, 2.0, 3.0);
CHOOSE(3.2, 9, 8, 7, 6, 5);

aaa := DATASET('aaa',{STRING1 fa; }, FLAT);

OUTPUT(choosen(aaa,10.2));
OUTPUT(DEDUP(aaa, fa = 'A', KEEP 2.2));
