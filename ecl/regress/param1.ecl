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

export abc(unsigned x, unsigned y) := module
    export f1(unsigned a) := x*a;
    export f2(unsigned a, unsigned b = y) := a*b;
    export f3(unsigned a, unsigned x = x) := a*x;
end;

abc(100,5).f1(9);   // 900
abc(100,5).f2(9);   // 45
abc(100,5).f3(9);   // 900
