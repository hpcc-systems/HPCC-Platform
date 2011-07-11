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

// A deep macro test
// Note: first two macros q, p are OK. Only f,g,h are indirectly recursive and have a bug: extra ';'.

f(x) := MACRO
 g(x) + 3;
ENDMACRO;

g(x) := MACRO
 h(x) + 4
ENDMACRO;

h(x) := MACRO
 f(x) + 5
ENDMACRO;

p(x) := MACRO
 q(x) + 6
ENDMACRO;

q(x) := MACRO
 f(x) + 7
ENDMACRO;

q(2);
