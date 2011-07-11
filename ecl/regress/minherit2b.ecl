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

//Example of two cross-dependent pure module definitions

m1 := MODULE,virtual
export value := 1;
export f(integer sc) := value * sc;
        END;


m2(m1 mp) := MODULE, virtual
export value2 := 10;
export g(integer sc2) := mp.f(value2) + sc2;
        END;


m3 := MODULE(m1)
export value := 7;
        END;

m4(m1 mp) := MODULE(m2(mp))
export value2 := 21;
        END;


output(m4(m3).g(99));       // Expected 246.

