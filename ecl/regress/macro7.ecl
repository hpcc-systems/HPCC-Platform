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

import macro7b;

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

macro7b.z(namesTable, xsum);

output(xsum);


//NB: __LAMBDA__ is undocumented and unsupported!

xsum2 := __LAMBDA__ FUNCTION
    z := namesTable(age != 10);
    return z + z;
END + namesTable;
output(xsum2);


xsum4 := __LAMBDA__ FUNCTION
    dataset gr := group(namesTable, age);

    return table(gr, {count(gr)});
END;

output(xsum4);


xsum3 := macro7b.z2(namesTable);

output(xsum3);


split := macro7b.ageSplit(namesTable);
output(split.young + split.old);
