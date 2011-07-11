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

//Error : recursive value

m1 := MODULE,virtual
export unsigned getFibValue(unsigned n) := 0;
export unsigned fibValue(unsigned n) := IF(n = 1, 1, getFibValue(n-1) + getFibValue(n-2));
        END;


m2 := MODULE(m1)
export unsigned getFibValue(unsigned n) := fibValue(n);
        END;


output(m2.getFibValue(5));          //recursive function definition, even through theoretically evaluatable
