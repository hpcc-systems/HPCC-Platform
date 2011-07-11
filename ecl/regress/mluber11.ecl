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
STRING100 and_terms1 := 'ONE;TWO;THREE';

STRING and1 := StringLib.StringToUpperCase(and_terms1);

MAC_String2Set(a, b, c, outset) :=
MACRO
set of string30 outset := [a, b, c];
ENDMACRO;

MAC_String2Set((and1[1..StringLib.StringFind(and1, ';', 1)-1]),
                (and1[StringLib.StringFind(and1, ';', 1)+1..StringLib.StringFind(and1, ';', 2)-1]),
                (and1[StringLib.StringFind(and1, ';', 2)+1..]),
                         and1Out)

STRING30 test := 'ONE';

output(test IN and1Out);
