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
andsearch := 'OSAMA;BIN;LADEN';

MAC_String2Set(a, b, c, outset) :=
MACRO
set of string30 outset := [a, b, c];
ENDMACRO;

MAC_String2Set(andsearch[1..StringLib.StringFind(andsearch, ';', 1)-1],
                         andsearch[StringLib.StringFind(andsearch, ';', 1)+1..
                                   StringLib.StringFind(andsearch, ';', 2)-1],
                         andsearch[StringLib.StringFind(andsearch, ';', 2)+1..],
                         outAnd)

output(outAnd[1]);
output(outAnd[2]);
output(outAnd[3]);
