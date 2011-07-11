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

mainRecord := 
        RECORD
integer8            sequence;
string20            forename;
string20            surname;
        END;

pstring := type
    export integer physicallength(string x) := transfer(x[1], unsigned1)+1;
    export string load(string x) := x[2..transfer(x[1], unsigned1)+1];
    export string store(string x) := transfer(length(x), string1)+x;
end;

varRecord :=
    RECORD
integer8            sequence;
pstring         forename;
pstring         surname;
    END;
            
d := DATASET('keyed.d00', mainRecord, THOR);

varRecord t(mainRecord l) := 
    TRANSFORM
        SELF.forename := TRIM(l.forename);
        SELF.surname := TRIM(l.surname);
        SELF := l;
    END;

x := project(d, t(left));

output(x,,'var.d00'); 

