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

m1 := MODULE,interface
export integer value;
export integer f(integer sc);
        END;


m2 := MODULE(m1)
export value := 20;
        END;

m3 := MODULE(m2)
export f(integer sc) := value * sc + 1;
        END;

output(m3.f(10));       //Correct


